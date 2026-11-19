package machinery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/go-redsync/redsync/v4"
	redsyncgoredis "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
)

// Backend - a common interface for all result backends
type Backend interface {
	// Group related functions
	InitGroup(groupUUID string, taskUUIDs []string) error
	GroupCompleted(groupUUID string, groupTaskCount int) (bool, error)
	GroupTaskStates(groupUUID string, groupTaskCount int) ([]*TaskState, error)
	TriggerChord(groupUUID string) (bool, error)

	// Setting / getting task state
	SetStatePending(signature *Signature) error
	SetStateReceived(signature *Signature) error
	SetStateStarted(signature *Signature) error
	SetStateRetry(signature *Signature) error
	SetStateSuccess(signature *Signature, results []*TaskResult) error
	SetStateFailure(signature *Signature, err string) error
	GetState(taskUUID string) (*TaskState, error)

	// Purging stored stored tasks states and group meta data
	PurgeState(taskUUID string) error
	PurgeGroupMeta(groupUUID string) error
}

// BaseBackend represents a base backend structure
type BaseBackend struct {
	cnf *Config
}

// NewBaseBackend creates new BaseBackend instance
func NewBaseBackend(cnf *Config) BaseBackend {
	return BaseBackend{cnf: cnf}
}

// GetConfig returns config
func (b *BaseBackend) GetConfig() *Config {
	return b.cnf
}

// RedisBackend represents a Redis result backend
type RedisBackend struct {
	BaseBackend
	rclient   redis.UniversalClient
	redsync   *redsync.Redsync
	redisOnce sync.Once
}

// NewRedisBackend creates RedisBackend instance
func NewRedisBackend(cnf *Config, addrs []string, db int) Backend {
	b := &RedisBackend{
		BaseBackend: NewBaseBackend(cnf),
		rclient:     NewGoRedisClient(cnf, addrs, db),
	}
	b.redsync = redsync.New(redsyncgoredis.NewPool(b.rclient))
	return b
}

// NewRedisBackendWithClient creates RedisBackend instance with an external redis.UniversalClient
func NewRedisBackendWithClient(cnf *Config, client redis.UniversalClient) Backend {
	return &RedisBackend{
		BaseBackend: NewBaseBackend(cnf),
		rclient:     client,
		redsync:     redsync.New(redsyncgoredis.NewPool(client)),
	}
}

// InitGroup creates and saves a group meta data object
func (b *RedisBackend) InitGroup(groupUUID string, taskUUIDs []string) error {
	groupMeta := &GroupMeta{
		GroupUUID: groupUUID,
		TaskUUIDs: taskUUIDs,
		CreatedAt: time.Now().UTC(),
	}

	encoded, err := json.Marshal(groupMeta)
	if err != nil {
		return err
	}

	expiration := b.getExpiration()
	err = b.rclient.Set(context.Background(), groupUUID, encoded, expiration).Err()
	if err != nil {
		return err
	}

	return nil
}

// GroupCompleted returns true if all tasks in a group finished
func (b *RedisBackend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return false, err
	}

	taskStates, err := b.getStates(groupMeta.TaskUUIDs...)
	if err != nil {
		return false, err
	}

	var countSuccessTasks = 0
	for _, taskState := range taskStates {
		if taskState.IsCompleted() {
			countSuccessTasks++
		}
	}

	return countSuccessTasks == groupTaskCount, nil
}

// GroupTaskStates returns states of all tasks in the group
func (b *RedisBackend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*TaskState, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return []*TaskState{}, err
	}

	return b.getStates(groupMeta.TaskUUIDs...)
}

// TriggerChord flags chord as triggered in the backend storage to make sure
// chord is never trigerred multiple times. Returns a boolean flag to indicate
// whether the worker should trigger chord (true) or no if it has been triggered
// already (false)
func (b *RedisBackend) TriggerChord(groupUUID string) (bool, error) {
	m := b.redsync.NewMutex("TriggerChordMutex")
	if err := m.Lock(); err != nil {
		return false, err
	}
	defer m.Unlock()

	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return false, err
	}

	// Chord has already been triggered, return false (should not trigger again)
	if groupMeta.ChordTriggered {
		return false, nil
	}

	// Set flag to true
	groupMeta.ChordTriggered = true

	// Update the group meta
	encoded, err := json.Marshal(&groupMeta)
	if err != nil {
		return false, err
	}

	expiration := b.getExpiration()
	err = b.rclient.Set(context.Background(), groupUUID, encoded, expiration).Err()
	if err != nil {
		return false, err
	}

	return true, nil
}

func (b *RedisBackend) mergeNewTaskState(newState *TaskState) {
	state, err := b.GetState(newState.TaskUUID)
	if err == nil {
		newState.CreatedAt = state.CreatedAt
		newState.TaskName = state.TaskName
	}
}

// SetStatePending updates task state to PENDING
func (b *RedisBackend) SetStatePending(signature *Signature) error {
	taskState := NewPendingTaskState(signature)
	return b.updateState(taskState)
}

// SetStateReceived updates task state to RECEIVED
func (b *RedisBackend) SetStateReceived(signature *Signature) error {
	taskState := NewReceivedTaskState(signature)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// SetStateStarted updates task state to STARTED
func (b *RedisBackend) SetStateStarted(signature *Signature) error {
	taskState := NewStartedTaskState(signature)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// SetStateRetry updates task state to RETRY
func (b *RedisBackend) SetStateRetry(signature *Signature) error {
	taskState := NewRetryTaskState(signature)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// SetStateSuccess updates task state to SUCCESS
func (b *RedisBackend) SetStateSuccess(signature *Signature, results []*TaskResult) error {
	taskState := NewSuccessTaskState(signature, results)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// SetStateFailure updates task state to FAILURE
func (b *RedisBackend) SetStateFailure(signature *Signature, err string) error {
	taskState := NewFailureTaskState(signature, err)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// GetState returns the latest task state
func (b *RedisBackend) GetState(taskUUID string) (*TaskState, error) {

	item, err := b.rclient.Get(context.Background(), taskUUID).Bytes()
	if err != nil {
		return nil, err
	}
	state := new(TaskState)
	decoder := json.NewDecoder(bytes.NewReader(item))
	decoder.UseNumber()
	if err := decoder.Decode(state); err != nil {
		return nil, err
	}

	return state, nil
}

// PurgeState deletes stored task state
func (b *RedisBackend) PurgeState(taskUUID string) error {
	err := b.rclient.Del(context.Background(), taskUUID).Err()
	if err != nil {
		return err
	}

	return nil
}

// PurgeGroupMeta deletes stored group meta data
func (b *RedisBackend) PurgeGroupMeta(groupUUID string) error {
	err := b.rclient.Del(context.Background(), groupUUID).Err()
	if err != nil {
		return err
	}

	return nil
}

// getGroupMeta retrieves group meta data, convenience function to avoid repetition
func (b *RedisBackend) getGroupMeta(groupUUID string) (*GroupMeta, error) {
	item, err := b.rclient.Get(context.Background(), groupUUID).Bytes()
	if err != nil {
		return nil, err
	}

	groupMeta := new(GroupMeta)
	decoder := json.NewDecoder(bytes.NewReader(item))
	decoder.UseNumber()
	if err := decoder.Decode(groupMeta); err != nil {
		return nil, err
	}

	return groupMeta, nil
}

// getStates returns multiple task states
func (b *RedisBackend) getStates(taskUUIDs ...string) ([]*TaskState, error) {
	taskStates := make([]*TaskState, len(taskUUIDs))
	// to avoid CROSSSLOT error, use pipeline
	cmders, err := b.rclient.Pipelined(context.Background(), func(pipeliner redis.Pipeliner) error {
		for _, uuid := range taskUUIDs {
			pipeliner.Get(context.Background(), uuid)
		}
		return nil
	})
	if err != nil {
		return taskStates, err
	}
	for i, cmder := range cmders {
		stateBytes, err1 := cmder.(*redis.StringCmd).Bytes()
		if err1 != nil {
			return taskStates, err1
		}
		taskState := new(TaskState)
		decoder := json.NewDecoder(bytes.NewReader(stateBytes))
		decoder.UseNumber()
		if err1 = decoder.Decode(taskState); err1 != nil {
			ERROR.Print(err1)
			return taskStates, err1
		}
		taskStates[i] = taskState
	}

	return taskStates, nil
}

// updateState saves current task state
func (b *RedisBackend) updateState(taskState *TaskState) error {
	encoded, err := json.Marshal(taskState)
	if err != nil {
		return err
	}

	expiration := b.getExpiration()
	_, err = b.rclient.Set(context.Background(), taskState.TaskUUID, encoded, expiration).Result()
	if err != nil {
		return err
	}

	return nil
}

// getExpiration returns expiration for a stored task state
func (b *RedisBackend) getExpiration() time.Duration {
	expiresIn := b.GetConfig().ResultsExpireIn
	if expiresIn == 0 {
		// expire results after 1 hour by default
		expiresIn = DefaultResultsExpireIn
	}

	return time.Duration(expiresIn) * time.Second
}

var (
	// ErrBackendNotConfigured ...
	ErrBackendNotConfigured = errors.New("Result backend not configured")
	// ErrTimeoutReached ...
	ErrTimeoutReached = errors.New("Timeout reached")
)

// AsyncResult represents a task result
type AsyncResult struct {
	Signature *Signature
	taskState *TaskState
	backend   Backend
}

// ChordAsyncResult represents a result of a chord
type ChordAsyncResult struct {
	groupAsyncResults []*AsyncResult
	chordAsyncResult  *AsyncResult
	backend           Backend
}

// ChainAsyncResult represents a result of a chain of tasks
type ChainAsyncResult struct {
	asyncResults []*AsyncResult
	backend      Backend
}

// NewAsyncResult creates AsyncResult instance
func NewAsyncResult(signature *Signature, backend Backend) *AsyncResult {
	return &AsyncResult{
		Signature: signature,
		taskState: new(TaskState),
		backend:   backend,
	}
}

// NewChordAsyncResult creates ChordAsyncResult instance
func NewChordAsyncResult(groupTasks []*Signature, chordCallback *Signature, backend Backend) *ChordAsyncResult {
	asyncResults := make([]*AsyncResult, len(groupTasks))
	for i, task := range groupTasks {
		asyncResults[i] = NewAsyncResult(task, backend)
	}
	return &ChordAsyncResult{
		groupAsyncResults: asyncResults,
		chordAsyncResult:  NewAsyncResult(chordCallback, backend),
		backend:           backend,
	}
}

// NewChainAsyncResult creates ChainAsyncResult instance
func NewChainAsyncResult(tasks []*Signature, backend Backend) *ChainAsyncResult {
	asyncResults := make([]*AsyncResult, len(tasks))
	for i, task := range tasks {
		asyncResults[i] = NewAsyncResult(task, backend)
	}
	return &ChainAsyncResult{
		asyncResults: asyncResults,
		backend:      backend,
	}
}

// Touch the state and don't wait
func (asyncResult *AsyncResult) Touch() ([]reflect.Value, error) {
	if asyncResult.backend == nil {
		return nil, ErrBackendNotConfigured
	}

	asyncResult.GetState()

	if asyncResult.taskState.IsFailure() {
		return nil, errors.New(asyncResult.taskState.Error)
	}

	if asyncResult.taskState.IsSuccess() {
		return ReflectTaskResults(asyncResult.taskState.Results)
	}

	return nil, nil
}

// Get returns task results (synchronous blocking call)
func (asyncResult *AsyncResult) Get(sleepDuration time.Duration) ([]reflect.Value, error) {
	for {
		results, err := asyncResult.Touch()

		if results == nil && err == nil {
			time.Sleep(sleepDuration)
		} else {
			return results, err
		}
	}
}

// GetWithTimeout returns task results with a timeout (synchronous blocking call)
func (asyncResult *AsyncResult) GetWithTimeout(timeoutDuration, sleepDuration time.Duration) ([]reflect.Value, error) {
	timeout := time.NewTimer(timeoutDuration)

	for {
		select {
		case <-timeout.C:
			return nil, ErrTimeoutReached
		default:
			results, err := asyncResult.Touch()

			if results == nil && err == nil {
				time.Sleep(sleepDuration)
			} else {
				return results, err
			}
		}
	}
}

// GetState returns latest task state
func (asyncResult *AsyncResult) GetState() *TaskState {
	if asyncResult.taskState.IsCompleted() {
		return asyncResult.taskState
	}

	taskState, err := asyncResult.backend.GetState(asyncResult.Signature.UUID)
	if err == nil {
		asyncResult.taskState = taskState
	}

	return asyncResult.taskState
}

// Get returns results of a chain of tasks (synchronous blocking call)
func (chainAsyncResult *ChainAsyncResult) Get(sleepDuration time.Duration) ([]reflect.Value, error) {
	if chainAsyncResult.backend == nil {
		return nil, ErrBackendNotConfigured
	}

	var (
		results []reflect.Value
		err     error
	)

	for _, asyncResult := range chainAsyncResult.asyncResults {
		results, err = asyncResult.Get(sleepDuration)
		if err != nil {
			return nil, err
		}
	}

	return results, err
}

// Get returns result of a chord (synchronous blocking call)
func (chordAsyncResult *ChordAsyncResult) Get(sleepDuration time.Duration) ([]reflect.Value, error) {
	if chordAsyncResult.backend == nil {
		return nil, ErrBackendNotConfigured
	}

	var err error
	for _, asyncResult := range chordAsyncResult.groupAsyncResults {
		_, err = asyncResult.Get(sleepDuration)
		if err != nil {
			return nil, err
		}
	}

	return chordAsyncResult.chordAsyncResult.Get(sleepDuration)
}

// GetWithTimeout returns results of a chain of tasks with timeout (synchronous blocking call)
func (chainAsyncResult *ChainAsyncResult) GetWithTimeout(timeoutDuration, sleepDuration time.Duration) ([]reflect.Value, error) {
	if chainAsyncResult.backend == nil {
		return nil, ErrBackendNotConfigured
	}

	var (
		results []reflect.Value
		err     error
	)

	timeout := time.NewTimer(timeoutDuration)
	ln := len(chainAsyncResult.asyncResults)
	lastResult := chainAsyncResult.asyncResults[ln-1]

	for {
		select {
		case <-timeout.C:
			return nil, ErrTimeoutReached
		default:

			for _, asyncResult := range chainAsyncResult.asyncResults {
				_, err = asyncResult.Touch()
				if err != nil {
					return nil, err
				}
			}

			results, err = lastResult.Touch()
			if err != nil {
				return nil, err
			}
			if results != nil {
				return results, err
			}
			time.Sleep(sleepDuration)
		}
	}
}

// GetWithTimeout returns result of a chord with a timeout (synchronous blocking call)
func (chordAsyncResult *ChordAsyncResult) GetWithTimeout(timeoutDuration, sleepDuration time.Duration) ([]reflect.Value, error) {
	if chordAsyncResult.backend == nil {
		return nil, ErrBackendNotConfigured
	}

	var (
		results []reflect.Value
		err     error
	)

	timeout := time.NewTimer(timeoutDuration)
	for {
		select {
		case <-timeout.C:
			return nil, ErrTimeoutReached
		default:
			for _, asyncResult := range chordAsyncResult.groupAsyncResults {
				_, errcur := asyncResult.Touch()
				if errcur != nil {
					return nil, err
				}
			}

			results, err = chordAsyncResult.chordAsyncResult.Touch()
			if err != nil {
				return nil, nil
			}
			if results != nil {
				return results, err
			}
			time.Sleep(sleepDuration)
		}
	}
}
