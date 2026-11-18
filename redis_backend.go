package machinery

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/go-redsync/redsync/v4"
	redsyncgoredis "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
)

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
