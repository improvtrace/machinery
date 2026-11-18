package machinery

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrRedisLockFailed = errors.New("redis lock: failed to acquire lock")
)

type RedisLock struct {
	rclient  redis.UniversalClient
	retries  int
	interval time.Duration
}

func NewRedisLock(cnf *Config, addrs []string, db, retries int) RedisLock {
	if retries <= 0 {
		return RedisLock{}
	}
	return RedisLock{
		rclient: NewGoRedisClient(cnf, addrs, db),
		retries: retries,
	}
}

func NewRedisLockWithClient(client redis.UniversalClient, retries int) RedisLock {
	if retries <= 0 {
		return RedisLock{}
	}
	return RedisLock{
		rclient: client,
		retries: retries,
	}
}

func (r RedisLock) LockWithRetries(key string, unixTsToExpireNs int64) error {
	for i := 0; i <= r.retries; i++ {
		err := r.Lock(key, unixTsToExpireNs)
		if err == nil {
			// 成功拿到锁，返回
			return nil
		}

		time.Sleep(r.interval)
	}
	return ErrRedisLockFailed
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
		timeout, err := strconv.Atoi(v)
		if err != nil {
			return err
		}

		if timeout != 0 && now > int64(timeout) {
			newTimeout, err := r.rclient.GetSet(ctx, key, unixTsToExpireNs).Result()
			if err != nil {
				return err
			}

			curTimeout, err := strconv.Atoi(newTimeout)
			if err != nil {
				return err
			}

			if now > int64(curTimeout) {
				// success to acquire lock with get set
				// set the expiration of redis key
				r.rclient.Expire(ctx, key, expiration)
				return nil
			}

			return ErrRedisLockFailed
		}

		return ErrRedisLockFailed
	}

	return nil
}
