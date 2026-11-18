package machinery

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
