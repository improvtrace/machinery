package machinery

import (
	"crypto/tls"
	"time"
)

const (
	// DefaultResultsExpireIn is a default time used to expire task states and group metadata from the backend
	DefaultResultsExpireIn = 3600
)

var (
	// Start with sensible default values
	defaultCnf = &Config{
		Broker:          "redis://localhost:6379",
		DefaultQueue:    "machinery_tasks",
		ResultBackend:   "redis://localhost:6379",
		ResultsExpireIn: DefaultResultsExpireIn,
		Redis: &RedisConfig{
			NormalTasksPollPeriod:  1000,
			DelayedTasksPollPeriod: 500,
		},
	}

	reloadDelay = time.Second * 10
)

// Config holds all configuration for our program
type Config struct {
	Broker                  string       `yaml:"broker" envconfig:"BROKER"`
	Lock                    string       `yaml:"lock" envconfig:"LOCK"`
	MultipleBrokerSeparator string       `yaml:"multiple_broker_separator" envconfig:"MULTIPLE_BROKEN_SEPARATOR"`
	DefaultQueue            string       `yaml:"default_queue" envconfig:"DEFAULT_QUEUE"`
	ResultBackend           string       `yaml:"result_backend" envconfig:"RESULT_BACKEND"`
	ResultsExpireIn         int          `yaml:"results_expire_in" envconfig:"RESULTS_EXPIRE_IN"`
	Redis                   *RedisConfig `yaml:"redis"`
	TLSConfig               *tls.Config
	// NoUnixSignals - when set disables signal handling in machinery
	NoUnixSignals bool `yaml:"no_unix_signals" envconfig:"NO_UNIX_SIGNALS"`
}

// RedisConfig ...
type RedisConfig struct {
	// NormalTasksPollPeriod specifies the period in milliseconds when polling redis for normal tasks
	// Default: 1000
	NormalTasksPollPeriod int `yaml:"normal_tasks_poll_period" envconfig:"REDIS_NORMAL_TASKS_POLL_PERIOD"`

	// DelayedTasksPollPeriod specifies the period in milliseconds when polling redis for delayed tasks
	// Default: 20
	DelayedTasksPollPeriod int    `yaml:"delayed_tasks_poll_period" envconfig:"REDIS_DELAYED_TASKS_POLL_PERIOD"`
	DelayedTasksKey        string `yaml:"delayed_tasks_key" envconfig:"REDIS_DELAYED_TASKS_KEY"`

	// MasterName specifies a redis master name in order to configure a sentinel-backed redis FailoverClient
	MasterName string `yaml:"master_name" envconfig:"REDIS_MASTER_NAME"`

	// ClusterEnabled specifies whether cluster mode is enabled, regardless the number of addresses.
	// This helps create ClusterClient for Redis servers that enabled cluster mode with 1 node, or using AWS configuration endpoint
	ClusterEnabled bool `yaml:"cluster_enabled" envconfig:"REDIS_CLUSTER_ENABLED"`

	// SentinelPassword specifies the password to be used when connecting to a Redis server via Sentinel
	SentinelPassword string `yaml:"sentinel_password" envconfig:"REDIS_SENTINEL_PASSWORD"`
}
