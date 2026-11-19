package machinery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/redis/go-redis/v9"
)

// Broker - a common interface for all brokers
type Broker interface {
	GetConfig() *Config
	SetRegisteredTaskNames(names []string)
	IsTaskRegistered(name string) bool
	StartConsuming(consumerTag string, concurrency int, p TaskProcessor) (bool, error)
	StopConsuming()
	Publish(ctx context.Context, task *Signature) error
	GetPendingTasks(queue string) ([]*Signature, error)
	GetDelayedTasks() ([]*Signature, error)
	AdjustRoutingKey(s *Signature)
}

// TaskProcessor - can process a delivered task
// This will probably always be a worker instance
type TaskProcessor interface {
	Process(signature *Signature) error
	CustomQueue() string
	PreConsumeHandler() bool
}

type registeredTaskNames struct {
	sync.RWMutex
	items []string
}

// BaseBroker represents a base broker structure
type BaseBroker struct {
	cnf                 *Config
	registeredTaskNames registeredTaskNames
	// stopped flips to true in StopConsuming; zero value (false) means the
	// broker is accepting consumption, so a literal-initialized BaseBroker is
	// valid without extra setup. Accessed atomically: read from consumer
	// goroutines, written by StopConsuming.
	stopped       atomic.Bool
	retryFunc     func(chan int)
	retryStopChan chan int
	stopChan      chan int
}

// NewBaseBroker creates new BaseBroker instance
func NewBaseBroker(cnf *Config) BaseBroker {
	return BaseBroker{
		cnf:           cnf,
		stopChan:      make(chan int),
		retryStopChan: make(chan int),
	}
}

// GetConfig returns config
func (b *BaseBroker) GetConfig() *Config {
	return b.cnf
}

// GetRetry reports whether the broker should retry reconnecting after a failure.
func (b *BaseBroker) GetRetry() bool {
	return !b.stopped.Load()
}

// GetRetryFunc returns the retry closure used to space out reconnection attempts.
func (b *BaseBroker) GetRetryFunc() func(chan int) {
	return b.retryFunc
}

// GetRetryStopChan returns the channel used to abort the retry closure early.
func (b *BaseBroker) GetRetryStopChan() chan int {
	return b.retryStopChan
}

// GetStopChan returns the channel that is closed when consumption stops.
func (b *BaseBroker) GetStopChan() chan int {
	return b.stopChan
}

// Publish places a new message on the default queue
func (b *BaseBroker) Publish(signature *Signature) error {
	return errors.New("Not implemented")
}

// SetRegisteredTaskNames sets registered task names
func (b *BaseBroker) SetRegisteredTaskNames(names []string) {
	b.registeredTaskNames.Lock()
	defer b.registeredTaskNames.Unlock()
	b.registeredTaskNames.items = names
}

// IsTaskRegistered returns true if the task is registered with this broker
func (b *BaseBroker) IsTaskRegistered(name string) bool {
	b.registeredTaskNames.RLock()
	defer b.registeredTaskNames.RUnlock()
	for _, registeredTaskName := range b.registeredTaskNames.items {
		if registeredTaskName == name {
			return true
		}
	}
	return false
}

// GetPendingTasks returns a slice of task.Signatures waiting in the queue
func (b *BaseBroker) GetPendingTasks(queue string) ([]*Signature, error) {
	return nil, errors.New("Not implemented")
}

// GetDelayedTasks returns a slice of task.Signatures that are scheduled, but not yet in the queue
func (b *BaseBroker) GetDelayedTasks() ([]*Signature, error) {
	return nil, errors.New("Not implemented")
}

// StartConsuming is a common part of StartConsuming method
func (b *BaseBroker) StartConsuming(consumerTag string, concurrency int, taskProcessor TaskProcessor) {
	if b.retryFunc == nil {
		b.retryFunc = Closure()
	}

}

// StopConsuming is a common part of StopConsuming
func (b *BaseBroker) StopConsuming() {
	// Do not retry from now on
	b.stopped.Store(true)
	// Stop the retry closure earlier
	select {
	case b.retryStopChan <- 1:
		slog.Warn("stopping retry closure")
	default:
	}
	// Notifying the stop channel stops consuming of messages
	close(b.stopChan)
	slog.Warn("stop channel closed")
}

// GetRegisteredTaskNames returns registered tasks names
func (b *BaseBroker) GetRegisteredTaskNames() []string {
	b.registeredTaskNames.RLock()
	defer b.registeredTaskNames.RUnlock()
	items := b.registeredTaskNames.items
	return items
}

// AdjustRoutingKey makes sure the routing key is correct.
// If the routing key is an empty string:
// a) set it to binding key for direct exchange type
// b) set it to default queue name
func (b *BaseBroker) AdjustRoutingKey(s *Signature) {
	if s.RoutingKey != "" {
		return
	}

	s.RoutingKey = b.GetConfig().DefaultQueue
}

// ErrCouldNotUnmarshalTaskSignature is returned when a delivery cannot be decoded into a task signature.
type ErrCouldNotUnmarshalTaskSignature struct {
	msg    []byte
	reason string
}

// Error implements the error interface
func (e ErrCouldNotUnmarshalTaskSignature) Error() string {
	return fmt.Sprintf("Could not unmarshal '%s' into a task signature: %v", e.msg, e.reason)
}

// NewErrCouldNotUnmarshalTaskSignature returns new ErrCouldNotUnmarshalTaskSignature instance
func NewErrCouldNotUnmarshalTaskSignature(msg []byte, err error) ErrCouldNotUnmarshalTaskSignature {
	return ErrCouldNotUnmarshalTaskSignature{msg: msg, reason: err.Error()}
}

// ErrConsumerStopped indicates that the operation is now illegal because of the consumer being stopped.
var ErrConsumerStopped = errors.New("the server has been stopped")

// ErrStopTaskDeletion indicates that the task should not be deleted from source after task failure
var ErrStopTaskDeletion = errors.New("task should not be deleted")

const defaultRedisDelayedTasksKey = "delayed_tasks"

// RedisBroker represents a Redis broker
type RedisBroker struct {
	BaseBroker
	rclient              redis.UniversalClient
	consumingWG          sync.WaitGroup // wait group to make sure whole consumption completes
	processingWG         sync.WaitGroup // use wait group to make sure task processing completes
	delayedWG            sync.WaitGroup
	redsync              *redsync.Redsync
	redisDelayedTasksKey string
	ownsRClient          bool
}

// NewRedisBroker creates RedisBroker instance
func NewRedisBroker(cnf *Config, addrs []string, db int) Broker {
	b := &RedisBroker{
		BaseBroker: NewBaseBroker(cnf),
		rclient:    NewGoRedisClient(cnf, addrs, db),
	}
	b.ownsRClient = true
	if cnf.Redis != nil && cnf.Redis.DelayedTasksKey != "" {
		b.redisDelayedTasksKey = cnf.Redis.DelayedTasksKey
	} else {
		b.redisDelayedTasksKey = defaultRedisDelayedTasksKey
	}
	return b
}

// NewRedisBrokerWithClient creates RedisBroker instance with an external redis.UniversalClient
func NewRedisBrokerWithClient(cnf *Config, client redis.UniversalClient) Broker {
	b := &RedisBroker{
		BaseBroker: NewBaseBroker(cnf),
		rclient:    client,
	}
	if cnf.Redis != nil && cnf.Redis.DelayedTasksKey != "" {
		b.redisDelayedTasksKey = cnf.Redis.DelayedTasksKey
	} else {
		b.redisDelayedTasksKey = defaultRedisDelayedTasksKey
	}
	return b
}

// StartConsuming enters a loop and waits for incoming messages
func (b *RedisBroker) StartConsuming(consumerTag string, concurrency int, taskProcessor TaskProcessor) (bool, error) {
	b.consumingWG.Add(1)
	defer b.consumingWG.Done()

	if concurrency < 1 {
		concurrency = runtime.NumCPU() * 2
	}

	b.BaseBroker.StartConsuming(consumerTag, concurrency, taskProcessor)

	// Ping the server to make sure connection is live
	_, err := b.rclient.Ping(context.Background()).Result()
	if err != nil {
		b.GetRetryFunc()(b.GetRetryStopChan())

		// Return err if retry is still true.
		// If retry is false, broker.StopConsuming() has been called and
		// therefore Redis might have been stopped. Return nil exit
		// StartConsuming()
		if b.GetRetry() {
			return b.GetRetry(), err
		}
		return b.GetRetry(), ErrConsumerStopped
	}

	// Channel to which we will push tasks ready for processing by worker
	deliveries := make(chan []byte, concurrency)
	pool := make(chan struct{}, concurrency)

	// initialize worker pool with maxWorkers workers
	for i := 0; i < concurrency; i++ {
		pool <- struct{}{}
	}

	// A receiving goroutine keeps popping messages from the queue by BLPOP
	// If the message is valid and can be unmarshaled into a proper structure
	// we send it to the deliveries channel
	go func() {

		slog.Info("waiting for messages. to exit press CTRL+C")

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.GetStopChan():
				close(deliveries)
				return
			case <-pool:
				task, _ := b.nextTask(getQueue(b.GetConfig(), taskProcessor))
				//TODO: should this error be ignored?
				if len(task) > 0 {
					deliveries <- task
				}

				pool <- struct{}{}
			}
		}
	}()

	// A goroutine to watch for delayed tasks and push them to deliveries
	// channel for consumption by the worker
	b.delayedWG.Add(1)
	go func() {
		defer b.delayedWG.Done()

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.GetStopChan():
				return
			default:
				task, err := b.nextDelayedTask(b.redisDelayedTasksKey)
				if err != nil {
					continue
				}

				signature := new(Signature)
				decoder := json.NewDecoder(bytes.NewReader(task))
				decoder.UseNumber()
				if err := decoder.Decode(signature); err != nil {
					slog.Error("failed to unmarshal task signature", "error", NewErrCouldNotUnmarshalTaskSignature(task, err))
				}

				if err := b.Publish(context.Background(), signature); err != nil {
					slog.Error("failed to publish delayed task", "error", err)
				}
			}
		}
	}()

	if err := b.consume(deliveries, concurrency, taskProcessor); err != nil {
		return b.GetRetry(), err
	}

	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()

	return b.GetRetry(), nil
}

// StopConsuming quits the loop
func (b *RedisBroker) StopConsuming() {
	b.BaseBroker.StopConsuming()
	// Waiting for the delayed tasks goroutine to have stopped
	b.delayedWG.Wait()
	// Waiting for consumption to finish
	b.consumingWG.Wait()

	if b.ownsRClient {
		// only close the client if this broker created it
		b.rclient.Close()
	}
}

// Publish places a new message on the default queue
func (b *RedisBroker) Publish(ctx context.Context, signature *Signature) error {
	// Adjust routing key (this decides which queue the message will be published to)
	b.BaseBroker.AdjustRoutingKey(signature)

	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %w", err)
	}

	// Check the ETA signature field, if it is set and it is in the future,
	// delay the task
	if signature.ETA != nil {
		now := time.Now().UTC()

		if signature.ETA.After(now) {
			score := signature.ETA.UnixNano()
			err = b.rclient.ZAdd(ctx, b.redisDelayedTasksKey, redis.Z{Score: float64(score), Member: msg}).Err()
			return err
		}
	}

	err = b.rclient.RPush(ctx, signature.RoutingKey, msg).Err()
	return err
}

// GetPendingTasks returns a slice of task signatures waiting in the queue
func (b *RedisBroker) GetPendingTasks(queue string) ([]*Signature, error) {

	if queue == "" {
		queue = b.GetConfig().DefaultQueue
	}
	results, err := b.rclient.LRange(context.Background(), queue, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	taskSignatures := make([]*Signature, len(results))
	for i, result := range results {
		signature := new(Signature)
		decoder := json.NewDecoder(strings.NewReader(result))
		decoder.UseNumber()
		if err := decoder.Decode(signature); err != nil {
			return nil, err
		}
		taskSignatures[i] = signature
	}
	return taskSignatures, nil
}

// GetDelayedTasks returns a slice of task signatures that are scheduled, but not yet in the queue
func (b *RedisBroker) GetDelayedTasks() ([]*Signature, error) {
	results, err := b.rclient.ZRange(context.Background(), b.redisDelayedTasksKey, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	taskSignatures := make([]*Signature, len(results))
	for i, result := range results {
		signature := new(Signature)
		decoder := json.NewDecoder(strings.NewReader(result))
		decoder.UseNumber()
		if err := decoder.Decode(signature); err != nil {
			return nil, err
		}
		taskSignatures[i] = signature
	}
	return taskSignatures, nil
}

// consume takes delivered messages from the channel and manages a worker pool
// to process tasks concurrently
func (b *RedisBroker) consume(deliveries <-chan []byte, concurrency int, taskProcessor TaskProcessor) error {
	errorsChan := make(chan error, concurrency*2)
	pool := make(chan struct{}, concurrency)

	// init pool for Worker tasks execution, as many slots as Worker concurrency param
	go func() {
		for i := 0; i < concurrency; i++ {
			pool <- struct{}{}
		}
	}()

	for {
		select {
		case err := <-errorsChan:
			return err
		case d, open := <-deliveries:
			if !open {
				return nil
			}
			if concurrency > 0 {
				// get execution slot from pool (blocks until one is available)
				<-pool
			}

			b.processingWG.Add(1)

			// Consume the task inside a goroutine so multiple tasks
			// can be processed concurrently
			go func() {
				if err := b.consumeOne(d, taskProcessor); err != nil {
					errorsChan <- err
				}

				b.processingWG.Done()

				if concurrency > 0 {
					// give slot back to pool
					pool <- struct{}{}
				}
			}()
		}
	}
}

// consumeOne processes a single message using TaskProcessor
func (b *RedisBroker) consumeOne(delivery []byte, taskProcessor TaskProcessor) error {
	signature := new(Signature)
	decoder := json.NewDecoder(bytes.NewReader(delivery))
	decoder.UseNumber()
	if err := decoder.Decode(signature); err != nil {
		return NewErrCouldNotUnmarshalTaskSignature(delivery, err)
	}

	// If the task is not registered, we requeue it,
	// there might be different workers for processing specific tasks
	if !b.IsTaskRegistered(signature.Name) {
		if signature.IgnoreWhenTaskNotRegistered {
			return nil
		}
		slog.Info("task not registered with this worker, requeuing", "delivery", string(delivery))

		b.rclient.RPush(context.Background(), getQueue(b.GetConfig(), taskProcessor), delivery)
		return nil
	}

	slog.Debug("received new message", "delivery", string(delivery))

	return taskProcessor.Process(signature)
}

// nextTask pops next available task from the default queue
func (b *RedisBroker) nextTask(queue string) (result []byte, err error) {

	pollPeriodMilliseconds := 1000 // default poll period for normal tasks
	if b.GetConfig().Redis != nil {
		configuredPollPeriod := b.GetConfig().Redis.NormalTasksPollPeriod
		if configuredPollPeriod > 0 {
			pollPeriodMilliseconds = configuredPollPeriod
		}
	}
	pollPeriod := time.Duration(pollPeriodMilliseconds) * time.Millisecond

	items, err := b.rclient.BLPop(context.Background(), pollPeriod, queue).Result()
	if err != nil {
		return []byte{}, err
	}

	// items[0] - the name of the key where an element was popped
	// items[1] - the value of the popped element
	if len(items) != 2 {
		return []byte{}, redis.Nil
	}

	result = []byte(items[1])

	return result, nil
}

// nextDelayedTask pops a value from the ZSET key using WATCH/MULTI/EXEC commands.
func (b *RedisBroker) nextDelayedTask(key string) (result []byte, err error) {
	var (
		items []string
	)

	pollPeriod := 500 // default poll period for delayed tasks
	if b.GetConfig().Redis != nil {
		configuredPollPeriod := b.GetConfig().Redis.DelayedTasksPollPeriod
		// the default period is 0, which bombards redis with requests, despite
		// our intention of doing the opposite
		if configuredPollPeriod > 0 {
			pollPeriod = configuredPollPeriod
		}
	}

	for {
		// Space out queries to ZSET so we don't bombard redis
		// server with relentless ZRANGEBYSCOREs
		time.Sleep(time.Duration(pollPeriod) * time.Millisecond)
		watchFunc := func(tx *redis.Tx) error {

			now := time.Now().UTC().UnixNano()

			// https://redis.io/commands/zrangebyscore
			ctx := context.Background()
			items, err = tx.ZRevRangeByScore(ctx, key, &redis.ZRangeBy{
				Min: "0", Max: strconv.FormatInt(now, 10), Offset: 0, Count: 1,
			}).Result()
			if err != nil {
				return err
			}
			if len(items) != 1 {
				return redis.Nil
			}

			// only return the first zrange value if there are no other changes in this key
			// to make sure a delayed task would only be consumed once
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.ZRem(ctx, key, items[0])
				result = []byte(items[0])
				return nil
			})

			return err
		}

		if err = b.rclient.Watch(context.Background(), watchFunc, key); err != nil {
			return
		} else {
			break
		}
	}

	return
}

func getQueue(cnf *Config, taskProcessor TaskProcessor) string {
	customQueue := taskProcessor.CustomQueue()
	if customQueue == "" {
		return cnf.DefaultQueue
	}
	return customQueue
}
