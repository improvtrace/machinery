package machinery

import (
	"strings"

	goredis "github.com/redis/go-redis/v9"
)

// NewGoRedisClient creates a redis.UniversalClient from config and addresses.
// It parses username/password from the first address if present (format: user:password@host).
func NewGoRedisClient(cnf *Config, addrs []string, db int) goredis.UniversalClient {
	var password string
	var username string
	parts := strings.Split(addrs[0], "@")
	if len(parts) >= 2 {
		options := strings.SplitN(strings.Join(parts[:len(parts)-1], "@"), ":", 2)
		if len(options) >= 2 {
			username = options[0]
			password = options[1]
		} else {
			password = options[0]
		}
		addrs[0] = parts[len(parts)-1]
	}

	ropt := &goredis.UniversalOptions{
		Addrs:    addrs,
		DB:       db,
		Username: username,
		Password: password,
	}
	if cnf.Redis != nil {
		ropt.MasterName = cnf.Redis.MasterName
		if cnf.Redis.SentinelPassword != "" {
			ropt.SentinelPassword = cnf.Redis.SentinelPassword
		}
	}
	if cnf.TLSConfig != nil {
		ropt.TLSConfig = cnf.TLSConfig
	}

	if cnf.Redis != nil && cnf.Redis.ClusterEnabled {
		return goredis.NewClusterClient(ropt.Cluster())
	}
	return goredis.NewUniversalClient(ropt)
}
