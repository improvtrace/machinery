// Package machinery is an asynchronous task queue based on distributed
// message passing, backed by Redis (single instance, Sentinel or cluster).
package machinery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/opentracing/opentracing-go"
	"github.com/robfig/cron/v3"
)

// Server is the main Machinery object and stores all configuration
// All the tasks workers process are registered against the server
type Server struct {
	config            *Config
	registeredTasks   *sync.Map
	broker            Broker
	backend           Backend
	lock              Lock
	scheduler         *cron.Cron
	prePublishHandler func(*Signature)
}

// NewServer creates Server instance
func NewServer(cnf *Config, brokerServer Broker, backendServer Backend, lock Lock) *Server {
	srv := &Server{
		config:          cnf,
		registeredTasks: new(sync.Map),
		broker:          brokerServer,
		backend:         backendServer,
		lock:            lock,
		scheduler:       cron.New(),
	}

	// Run scheduler job
	go srv.scheduler.Run()

	return srv
}

// NewWorker creates Worker instance
func (server *Server) NewWorker(consumerTag string, concurrency int) *Worker {
	return &Worker{
		server:      server,
		ConsumerTag: consumerTag,
		Concurrency: concurrency,
		Queue:       "",
	}
}

// NewCustomQueueWorker creates Worker instance with Custom Queue
func (server *Server) NewCustomQueueWorker(consumerTag string, concurrency int, queue string) *Worker {
	return &Worker{
		server:      server,
		ConsumerTag: consumerTag,
		Concurrency: concurrency,
		Queue:       queue,
	}
}

// GetBroker returns broker
func (server *Server) GetBroker() Broker {
	return server.broker
}

// SetBroker sets broker
func (server *Server) SetBroker(broker Broker) {
	server.broker = broker
}

// GetBackend returns backend
func (server *Server) GetBackend() Backend {
	return server.backend
}

// SetBackend sets backend
func (server *Server) SetBackend(backend Backend) {
	server.backend = backend
}

// GetConfig returns config
func (server *Server) GetConfig() *Config {
	return server.config
}

// SetConfig sets config
func (server *Server) SetConfig(cnf *Config) {
	server.config = cnf
}

// SetPreTaskHandler Sets pre publish handler
func (server *Server) SetPreTaskHandler(handler func(*Signature)) {
	server.prePublishHandler = handler
}

// RegisterTasks registers all tasks at once
func (server *Server) RegisterTasks(namedTaskFuncs map[string]any) error {
	for _, task := range namedTaskFuncs {
		if err := ValidateTask(task); err != nil {
			return err
		}
	}
	for k, v := range namedTaskFuncs {
		server.registeredTasks.Store(k, v)
	}
	server.broker.SetRegisteredTaskNames(server.GetRegisteredTaskNames())
	return nil
}

// RegisterTask registers a single task
func (server *Server) RegisterTask(name string, taskFunc any) error {
	if err := ValidateTask(taskFunc); err != nil {
		return err
	}
	server.registeredTasks.Store(name, taskFunc)
	server.broker.SetRegisteredTaskNames(server.GetRegisteredTaskNames())
	return nil
}

// IsTaskRegistered returns true if the task name is registered with this broker
func (server *Server) IsTaskRegistered(name string) bool {
	_, ok := server.registeredTasks.Load(name)
	return ok
}

// GetRegisteredTask returns registered task by name
func (server *Server) GetRegisteredTask(name string) (any, error) {
	taskFunc, ok := server.registeredTasks.Load(name)
	if !ok {
		return nil, fmt.Errorf("Task not registered error: %s", name)
	}
	return taskFunc, nil
}

// SendTaskWithContext will inject the trace context in the signature headers before publishing it
func (server *Server) SendTaskWithContext(ctx context.Context, signature *Signature) (*AsyncResult, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendTask", ProducerOption(), MachineryTag)
	defer span.Finish()

	// tag the span with some info about the signature
	signature.Headers = HeadersWithSpan(signature.Headers, span)

	// Make sure result backend is defined
	if server.backend == nil {
		return nil, errors.New("Result backend required")
	}

	// Auto generate a UUID if not set already
	if signature.UUID == "" {
		taskID := uuid.New().String()
		signature.UUID = fmt.Sprintf("task_%v", taskID)
	}

	// Set initial task state to PENDING
	if err := server.backend.SetStatePending(signature); err != nil {
		return nil, fmt.Errorf("Set state pending error: %w", err)
	}

	if server.prePublishHandler != nil {
		server.prePublishHandler(signature)
	}

	if err := server.broker.Publish(ctx, signature); err != nil {
		return nil, fmt.Errorf("Publish message error: %w", err)
	}

	return NewAsyncResult(signature, server.backend), nil
}

// SendTask publishes a task to the default queue
func (server *Server) SendTask(signature *Signature) (*AsyncResult, error) {
	return server.SendTaskWithContext(context.Background(), signature)
}

// SendChainWithContext will inject the trace context in all the signature headers before publishing it
func (server *Server) SendChainWithContext(ctx context.Context, chain *Chain) (*ChainAsyncResult, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendChain", ProducerOption(), MachineryTag, WorkflowChainTag)
	defer span.Finish()

	AnnotateSpanWithChainInfo(span, chain)

	return server.SendChain(chain)
}

// SendChain triggers a chain of tasks
func (server *Server) SendChain(chain *Chain) (*ChainAsyncResult, error) {
	_, err := server.SendTask(chain.Tasks[0])
	if err != nil {
		return nil, err
	}

	return NewChainAsyncResult(chain.Tasks, server.backend), nil
}

// SendGroupWithContext will inject the trace context in all the signature headers before publishing it
func (server *Server) SendGroupWithContext(ctx context.Context, group *Group, sendConcurrency int) ([]*AsyncResult, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendGroup", ProducerOption(), MachineryTag, WorkflowGroupTag)
	defer span.Finish()

	AnnotateSpanWithGroupInfo(span, group, sendConcurrency)

	// Make sure result backend is defined
	if server.backend == nil {
		return nil, errors.New("Result backend required")
	}

	if sendConcurrency < 0 {
		return nil, fmt.Errorf("sendConcurrency must be >= 0, got %d", sendConcurrency)
	}

	asyncResults := make([]*AsyncResult, len(group.Tasks))

	var wg sync.WaitGroup
	wg.Add(len(group.Tasks))
	errorsChan := make(chan error, len(group.Tasks)*2)

	// Init group
	server.backend.InitGroup(group.GroupUUID, group.GetUUIDs())

	// Init the tasks Pending state first
	for _, signature := range group.Tasks {
		if err := server.backend.SetStatePending(signature); err != nil {
			errorsChan <- err
			continue
		}
	}

	pool := make(chan struct{}, sendConcurrency)
	go func() {
		for i := 0; i < sendConcurrency; i++ {
			pool <- struct{}{}
		}
	}()

	for i, signature := range group.Tasks {

		if sendConcurrency > 0 {
			<-pool
		}

		go func(s *Signature, index int) {
			defer wg.Done()

			// Publish task

			err := server.broker.Publish(ctx, s)

			if sendConcurrency > 0 {
				pool <- struct{}{}
			}

			if err != nil {
				errorsChan <- fmt.Errorf("Publish message error: %w", err)
				return
			}

			asyncResults[index] = NewAsyncResult(s, server.backend)
		}(signature, i)
	}

	done := make(chan int)
	go func() {
		wg.Wait()
		done <- 1
	}()

	select {
	case err := <-errorsChan:
		// wait for in-flight publish goroutines before returning the shared slice
		wg.Wait()
		return asyncResults, err
	case <-done:
		return asyncResults, nil
	}
}

// SendGroup triggers a group of parallel tasks
func (server *Server) SendGroup(group *Group, sendConcurrency int) ([]*AsyncResult, error) {
	return server.SendGroupWithContext(context.Background(), group, sendConcurrency)
}

// SendChordWithContext will inject the trace context in all the signature headers before publishing it
func (server *Server) SendChordWithContext(ctx context.Context, chord *Chord, sendConcurrency int) (*ChordAsyncResult, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendChord", ProducerOption(), MachineryTag, WorkflowChordTag)
	defer span.Finish()

	AnnotateSpanWithChordInfo(span, chord, sendConcurrency)

	_, err := server.SendGroupWithContext(ctx, chord.Group, sendConcurrency)
	if err != nil {
		return nil, err
	}

	return NewChordAsyncResult(
		chord.Group.Tasks,
		chord.Callback,
		server.backend,
	), nil
}

// SendChord triggers a group of parallel tasks with a callback
func (server *Server) SendChord(chord *Chord, sendConcurrency int) (*ChordAsyncResult, error) {
	return server.SendChordWithContext(context.Background(), chord, sendConcurrency)
}

// GetRegisteredTaskNames returns slice of registered task names
func (server *Server) GetRegisteredTaskNames() []string {
	taskNames := make([]string, 0)

	server.registeredTasks.Range(func(key, value any) bool {
		taskNames = append(taskNames, key.(string))
		return true
	})
	return taskNames
}

// RegisterPeriodicTask register a periodic task which will be triggered periodically
func (server *Server) RegisterPeriodicTask(spec, name string, signature *Signature) error {
	//check spec
	schedule, err := cron.ParseStandard(spec)
	if err != nil {
		return err
	}

	f := func() {
		//get lock
		err := server.lock.LockWithRetries(GetLockName(name, spec), schedule.Next(time.Now()).UnixNano()-1)
		if err != nil {
			return
		}

		//send task
		_, err = server.SendTask(CopySignature(signature))
		if err != nil {
			slog.Error("periodic task failed", "task", name, "error", err)
		}
	}

	_, err = server.scheduler.AddFunc(spec, f)
	return err
}

// RegisterPeriodicChain register a periodic chain which will be triggered periodically
func (server *Server) RegisterPeriodicChain(spec, name string, signatures ...*Signature) error {
	//check spec
	schedule, err := cron.ParseStandard(spec)
	if err != nil {
		return err
	}

	f := func() {
		// new chain
		chain, _ := NewChain(CopySignatures(signatures...)...)

		//get lock
		err := server.lock.LockWithRetries(GetLockName(name, spec), schedule.Next(time.Now()).UnixNano()-1)
		if err != nil {
			return
		}

		//send task
		_, err = server.SendChain(chain)
		if err != nil {
			slog.Error("periodic task failed", "task", name, "error", err)
		}
	}

	_, err = server.scheduler.AddFunc(spec, f)
	return err
}

// RegisterPeriodicGroup register a periodic group which will be triggered periodically
func (server *Server) RegisterPeriodicGroup(spec, name string, sendConcurrency int, signatures ...*Signature) error {
	//check spec
	schedule, err := cron.ParseStandard(spec)
	if err != nil {
		return err
	}

	f := func() {
		// new group
		group, _ := NewGroup(CopySignatures(signatures...)...)

		//get lock
		err := server.lock.LockWithRetries(GetLockName(name, spec), schedule.Next(time.Now()).UnixNano()-1)
		if err != nil {
			return
		}

		//send task
		_, err = server.SendGroup(group, sendConcurrency)
		if err != nil {
			slog.Error("periodic task failed", "task", name, "error", err)
		}
	}

	_, err = server.scheduler.AddFunc(spec, f)
	return err
}

// RegisterPeriodicChord register a periodic chord which will be triggered periodically
func (server *Server) RegisterPeriodicChord(spec, name string, sendConcurrency int, callback *Signature, signatures ...*Signature) error {
	//check spec
	schedule, err := cron.ParseStandard(spec)
	if err != nil {
		return err
	}

	f := func() {
		// new chord
		group, _ := NewGroup(CopySignatures(signatures...)...)
		chord, _ := NewChord(group, CopySignature(callback))

		//get lock
		err := server.lock.LockWithRetries(GetLockName(name, spec), schedule.Next(time.Now()).UnixNano()-1)
		if err != nil {
			return
		}

		//send task
		_, err = server.SendChord(chord, sendConcurrency)
		if err != nil {
			slog.Error("periodic task failed", "task", name, "error", err)
		}
	}

	_, err = server.scheduler.AddFunc(spec, f)
	return err
}

// Worker represents a single worker process
type Worker struct {
	server            *Server
	ConsumerTag       string
	Concurrency       int
	Queue             string
	errorHandler      func(err error)
	preTaskHandler    func(*Signature)
	postTaskHandler   func(*Signature)
	preConsumeHandler func(*Worker) bool
}

var (
	// ErrWorkerQuitGracefully is return when worker quit gracefully
	ErrWorkerQuitGracefully = errors.New("Worker quit gracefully")
	// ErrWorkerQuitGracefully is return when worker quit abruptly
	ErrWorkerQuitAbruptly = errors.New("Worker quit abruptly")
)

// Launch starts a new worker process. The worker subscribes
// to the default queue and processes incoming registered tasks
func (worker *Worker) Launch() error {
	errorsChan := make(chan error, 2)

	worker.LaunchAsync(errorsChan)

	return <-errorsChan
}

// LaunchAsync is a non blocking version of Launch
func (worker *Worker) LaunchAsync(errorsChan chan<- error) {
	cnf := worker.server.GetConfig()
	broker := worker.server.GetBroker()

	// Log some useful information about worker configuration
	queue := cnf.DefaultQueue
	if worker.Queue != "" {
		queue = worker.Queue
	}
	slog.Info("launching worker",
		"broker", RedactURL(cnf.Broker),
		"queue", queue,
		"backend", RedactURL(cnf.ResultBackend),
	)

	var signalWG sync.WaitGroup
	// Goroutine to start broker consumption and handle retries when broker connection dies
	go func() {
		for {
			retry, err := broker.StartConsuming(worker.ConsumerTag, worker.Concurrency, worker)

			if retry {
				if worker.errorHandler != nil {
					worker.errorHandler(err)
				} else {
					slog.Warn("broker failed", "error", err)
				}
			} else {
				signalWG.Wait()
				errorsChan <- err // stop the goroutine
				return
			}
		}
	}()
	if !cnf.NoUnixSignals {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		var signalsReceived uint

		// Goroutine Handle SIGINT and SIGTERM signals
		go func() {
			for s := range sig {
				slog.Warn("signal received", "signal", s)
				signalsReceived++

				if signalsReceived < 2 {
					// After first Ctrl+C start quitting the worker gracefully
					slog.Warn("waiting for running tasks to finish before shutting down")
					signalWG.Add(1)
					go func() {
						worker.Quit()
						errorsChan <- ErrWorkerQuitGracefully
						signalWG.Done()
					}()
				} else {
					// Abort the program when user hits Ctrl+C second time in a row
					errorsChan <- ErrWorkerQuitAbruptly
				}
			}
		}()
	}
}

// CustomQueue returns Custom Queue of the running worker process
func (worker *Worker) CustomQueue() string {
	return worker.Queue
}

// Quit tears down the running worker process
func (worker *Worker) Quit() {
	worker.server.GetBroker().StopConsuming()
}

// Process handles received tasks and triggers success/error callbacks
func (worker *Worker) Process(signature *Signature) error {
	// If the task is not registered with this worker, do not continue
	// but only return nil as we do not want to restart the worker process
	if !worker.server.IsTaskRegistered(signature.Name) {
		return nil
	}

	taskFunc, err := worker.server.GetRegisteredTask(signature.Name)
	if err != nil {
		return nil
	}

	// Update task state to RECEIVED
	if err = worker.server.GetBackend().SetStateReceived(signature); err != nil {
		return fmt.Errorf("Set state to 'received' for task %s returned error: %w", signature.UUID, err)
	}

	// Prepare task for processing
	task, err := NewWithSignature(taskFunc, signature)
	// if this failed, it means the task is malformed, probably has invalid
	// signature, go directly to task failed without checking whether to retry
	if err != nil {
		worker.taskFailed(signature, err)
		return err
	}

	// try to extract trace span from headers and add it to the function context
	// so it can be used inside the function if it has context.Context as the first
	// argument. Start a new span if it isn't found.
	taskSpan := StartSpanFromHeaders(signature.Headers, signature.Name)
	AnnotateSpanWithSignatureInfo(taskSpan, signature)
	task.Context = opentracing.ContextWithSpan(task.Context, taskSpan)

	// Update task state to STARTED
	if err = worker.server.GetBackend().SetStateStarted(signature); err != nil {
		return fmt.Errorf("Set state to 'started' for task %s returned error: %w", signature.UUID, err)
	}

	//Run handler before the task is called
	if worker.preTaskHandler != nil {
		worker.preTaskHandler(signature)
	}

	//Defer run handler for the end of the task
	if worker.postTaskHandler != nil {
		defer worker.postTaskHandler(signature)
	}

	// Call the task
	results, err := task.Call()
	if err != nil {
		// If a ErrRetryTaskLater was returned from the task,
		// retry the task after specified duration
		retriableErr, ok := any(err).(ErrRetryTaskLater)
		if ok {
			return worker.retryTaskIn(signature, retriableErr.RetryIn())
		}

		// Otherwise, execute default retry logic based on signature.RetryCount
		// and signature.RetryTimeout values
		if signature.RetryCount > 0 {
			return worker.taskRetry(signature)
		}

		return worker.taskFailed(signature, err)
	}

	return worker.taskSucceeded(signature, results)
}

// retryTask decrements RetryCount counter and republishes the task to the queue
func (worker *Worker) taskRetry(signature *Signature) error {
	// Update task state to RETRY
	if err := worker.server.GetBackend().SetStateRetry(signature); err != nil {
		return fmt.Errorf("Set state to 'retry' for task %s returned error: %w", signature.UUID, err)
	}

	// Decrement the retry counter, when it reaches 0, we won't retry again
	signature.RetryCount--

	// Increase retry timeout
	signature.RetryTimeout = FibonacciNext(signature.RetryTimeout)

	// Delay task by signature.RetryTimeout seconds
	eta := time.Now().UTC().Add(time.Second * time.Duration(signature.RetryTimeout))
	signature.ETA = &eta

	slog.Warn("task failed, retrying", "task", signature.UUID, "retryInSeconds", signature.RetryTimeout)

	// Send the task back to the queue
	_, err := worker.server.SendTask(signature)
	return err
}

// retryTaskIn republishes the task to the queue with ETA of now + retryIn.Seconds()
func (worker *Worker) retryTaskIn(signature *Signature, retryIn time.Duration) error {
	// Update task state to RETRY
	if err := worker.server.GetBackend().SetStateRetry(signature); err != nil {
		return fmt.Errorf("Set state to 'retry' for task %s returned error: %w", signature.UUID, err)
	}

	// Delay task by retryIn duration
	eta := time.Now().UTC().Add(retryIn)
	signature.ETA = &eta

	slog.Warn("task failed, retrying", "task", signature.UUID, "retryIn", retryIn)

	// Send the task back to the queue
	_, err := worker.server.SendTask(signature)
	return err
}

// taskSucceeded updates the task state and triggers success callbacks or a
// chord callback if this was the last task of a group with a chord callback
func (worker *Worker) taskSucceeded(signature *Signature, taskResults []*TaskResult) error {
	// Update task state to SUCCESS
	if err := worker.server.GetBackend().SetStateSuccess(signature, taskResults); err != nil {
		return fmt.Errorf("Set state to 'success' for task %s returned error: %w", signature.UUID, err)
	}

	// Log human readable results of the processed task
	var debugResults = "[]"
	results, err := ReflectTaskResults(taskResults)
	if err != nil {
		slog.Warn("failed to reflect task results", "error", err)
	} else {
		debugResults = HumanReadableResults(results)
	}
	slog.Debug("task processed", "task", signature.UUID, "results", debugResults)

	// Trigger success callbacks

	for _, successTask := range signature.OnSuccess {
		if signature.Immutable == false {
			// Pass results of the task to success callbacks
			for _, taskResult := range taskResults {
				successTask.Args = append(successTask.Args, Arg{
					Type:  taskResult.Type,
					Value: taskResult.Value,
				})
			}
		}

		worker.server.SendTask(successTask)
	}

	// If the task was not part of a group, just return
	if signature.GroupUUID == "" {
		return nil
	}

	// There is no chord callback, just return
	if signature.ChordCallback == nil {
		return nil
	}

	// Check if all task in the group has completed
	groupCompleted, err := worker.server.GetBackend().GroupCompleted(
		signature.GroupUUID,
		signature.GroupTaskCount,
	)
	if err != nil {
		return fmt.Errorf("Completed check for group %s returned error: %w", signature.GroupUUID, err)
	}

	// If the group has not yet completed, just return
	if !groupCompleted {
		return nil
	}

	// Trigger chord callback
	shouldTrigger, err := worker.server.GetBackend().TriggerChord(signature.GroupUUID)
	if err != nil {
		return fmt.Errorf("Triggering chord for group %s returned error: %w", signature.GroupUUID, err)
	}

	// Chord has already been triggered
	if !shouldTrigger {
		return nil
	}

	// Get task states
	taskStates, err := worker.server.GetBackend().GroupTaskStates(
		signature.GroupUUID,
		signature.GroupTaskCount,
	)
	if err != nil {
		slog.Error("failed to get task states for group, chord may not be triggered",
			"group", signature.GroupUUID,
			"taskCount", signature.GroupTaskCount,
			"error", err,
		)
		return nil
	}

	// Append group tasks' return values to chord task if it's not immutable
	for _, taskState := range taskStates {
		if !taskState.IsSuccess() {
			return nil
		}

		if signature.ChordCallback.Immutable == false {
			// Pass results of the task to the chord callback
			for _, taskResult := range taskState.Results {
				signature.ChordCallback.Args = append(signature.ChordCallback.Args, Arg{
					Type:  taskResult.Type,
					Value: taskResult.Value,
				})
			}
		}
	}

	// Send the chord task
	_, err = worker.server.SendTask(signature.ChordCallback)
	if err != nil {
		return err
	}

	return nil
}

// taskFailed updates the task state and triggers error callbacks
func (worker *Worker) taskFailed(signature *Signature, taskErr error) error {
	// Update task state to FAILURE
	if err := worker.server.GetBackend().SetStateFailure(signature, taskErr.Error()); err != nil {
		return fmt.Errorf("Set state to 'failure' for task %s returned error: %w", signature.UUID, err)
	}

	if worker.errorHandler != nil {
		worker.errorHandler(taskErr)
	} else {
		slog.Error("failed processing task", "task", signature.UUID, "error", taskErr)
	}

	// Trigger error callbacks
	for _, errorTask := range signature.OnError {
		// Pass error as a first argument to error callbacks
		args := append([]Arg{{
			Type:  "string",
			Value: taskErr.Error(),
		}}, errorTask.Args...)
		errorTask.Args = args
		worker.server.SendTask(errorTask)
	}

	if signature.StopTaskDeletionOnError {
		return ErrStopTaskDeletion
	}

	return nil
}

// SetErrorHandler sets a custom error handler for task errors
// A default behavior is just to log the error after all the retry attempts fail
func (worker *Worker) SetErrorHandler(handler func(err error)) {
	worker.errorHandler = handler
}

// SetPreTaskHandler sets a custom handler func before a job is started
func (worker *Worker) SetPreTaskHandler(handler func(*Signature)) {
	worker.preTaskHandler = handler
}

// SetPostTaskHandler sets a custom handler for the end of a job
func (worker *Worker) SetPostTaskHandler(handler func(*Signature)) {
	worker.postTaskHandler = handler
}

// SetPreConsumeHandler sets a custom handler for the end of a job
func (worker *Worker) SetPreConsumeHandler(handler func(*Worker) bool) {
	worker.preConsumeHandler = handler
}

// GetServer returns server
func (worker *Worker) GetServer() *Server {
	return worker.server
}

func (worker *Worker) PreConsumeHandler() bool {
	if worker.preConsumeHandler == nil {
		return true
	}

	return worker.preConsumeHandler(worker)
}

func RedactURL(urlString string) string {
	u, err := url.Parse(urlString)
	if err != nil {
		return urlString
	}
	return fmt.Sprintf("%s://%s", u.Scheme, u.Host)
}
