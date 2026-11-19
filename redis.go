package machinery

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
	goredis "github.com/redis/go-redis/v9"

	"strconv"
	"strings"
	"time"
)

// NewGoRedisClient creates a redis.UniversalClient from config and addresses.
// It parses username/password from the first address if present (format: user:password@host).
func NewGoRedisClient(cnf *Config, addrs []string, db int) goredis.UniversalClient {
	if len(addrs) == 0 {
		return nil
	}
	// work on a copy so the caller's slice is never mutated
	addrs = append([]string(nil), addrs...)
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

type Lock interface {
	//Acquire the lock with retry
	//key: the name of the lock,
	//value: at the nanosecond timestamp that lock needs to be released automatically
	LockWithRetries(key string, value int64) error

	//Acquire the lock with once
	//key: the name of the lock,
	//value: at the nanosecond timestamp that lock needs to be released automatically
	Lock(key string, value int64) error
}

var (
	ErrRedisLockFailed = errors.New("redis lock: failed to acquire lock")
)

// defaultLockRetryInterval is the sleep between lock acquisition attempts.
const defaultLockRetryInterval = 500 * time.Millisecond

type RedisLock struct {
	rclient  redis.UniversalClient
	retries  int
	interval time.Duration
}

func NewRedisLock(cnf *Config, addrs []string, db, retries int) RedisLock {
	if retries < 1 {
		retries = 1
	}
	return RedisLock{
		rclient:  NewGoRedisClient(cnf, addrs, db),
		retries:  retries,
		interval: defaultLockRetryInterval,
	}
}

func NewRedisLockWithClient(client redis.UniversalClient, retries int) RedisLock {
	if retries < 1 {
		retries = 1
	}
	return RedisLock{
		rclient:  client,
		retries:  retries,
		interval: defaultLockRetryInterval,
	}
}

func (r RedisLock) LockWithRetries(key string, unixTsToExpireNs int64) error {
	var lastErr error
	for i := 0; i <= r.retries; i++ {
		err := r.Lock(key, unixTsToExpireNs)
		if err == nil {
			// 成功拿到锁，返回
			return nil
		}
		lastErr = err

		time.Sleep(r.interval)
	}
	// keep ErrRedisLockFailed as the sentinel while preserving the last cause
	return fmt.Errorf("%w: %w", ErrRedisLockFailed, lastErr)
}

func (r RedisLock) Lock(key string, unixTsToExpireNs int64) error {
	now := time.Now().UnixNano()
	expiration := time.Duration(unixTsToExpireNs + 1 - now)
	ctx := context.Background()

	success, err := r.rclient.SetNX(ctx, key, unixTsToExpireNs, expiration).Result()
	if err != nil {
		return err
	}

	if !success {
		v, err := r.rclient.Get(ctx, key).Result()
		if err != nil {
			return err
		}
		timeout, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return err
		}

		if timeout != 0 && now > timeout {
			newTimeout, err := r.rclient.GetSet(ctx, key, unixTsToExpireNs).Result()
			if err != nil {
				return err
			}

			curTimeout, err := strconv.ParseInt(newTimeout, 10, 64)
			if err != nil {
				return err
			}

			if now > curTimeout {
				// success to acquire lock with get set
				// set the expiration of redis key
				if err := r.rclient.Expire(ctx, key, expiration).Err(); err != nil {
					return err
				}
				return nil
			}

			return ErrRedisLockFailed
		}

		return ErrRedisLockFailed
	}

	return nil
}
