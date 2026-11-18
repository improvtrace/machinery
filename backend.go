package machinery

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
