package machinery

import (
	"context"
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
