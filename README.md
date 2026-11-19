# Machinery

Machinery is an asynchronous task queue / job queue for Go, based on distributed
message passing.

This is a trimmed-down fork of [RichardKnop/machinery](https://github.com/RichardKnop/machinery):

- **Redis only** — broker, result backend and distributed lock are implemented
  on top of [go-redis v9](https://github.com/redis/go-redis) (single instance,
  Sentinel failover and cluster modes supported).
- **Single flat package** — all code lives in the project root under one
  `package machinery`, module path `github.com/RichardKnop/machinery`.
- No AMQP / SQS / GCPPubSub / DynamoDB / Mongo / Memcache implementations, no
  v1 code, no build or test tooling.

## Installation

```
go get github.com/RichardKnop/machinery
```

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/RichardKnop/machinery"
)

func main() {
	cnf := &machinery.Config{
		Broker:        "redis://localhost:6379",
		ResultBackend: "redis://localhost:6379",
		DefaultQueue:  "machinery_tasks",
		Redis:         &machinery.RedisConfig{NormalTasksPollPeriod: 1000},
	}

	// Build one redis client and inject it into broker, backend and lock.
	// The caller owns the client lifecycle.
	client := machinery.NewGoRedisClient(cnf, []string{"localhost:6379"}, 0)

	broker := machinery.NewRedisBrokerWithClient(cnf, client)
	backend := machinery.NewRedisBackendWithClient(cnf, client)
	lock := machinery.NewRedisLockWithClient(client, 3)

	server := machinery.NewServer(cnf, broker, backend, lock)
	server.RegisterTask("add", func(a, b int) (int, error) {
		return a + b, nil
	})

	// Send a task
	asyncResult, err := server.SendTask(
		&machinery.Signature{Name: "add", Args: []machinery.Arg{{
			Type: "int", Value: 1,
		}, {
			Type: "int", Value: 2,
		}}},
	)
	if err != nil {
		panic(err)
	}

	results, err := asyncResult.Get(time.Second)
	if err != nil {
		panic(err)
	}
	fmt.Println(results[0].Int()) // 3

	// Run a worker (blocking)
	worker := server.NewWorker("worker_1", 10)
	if err := worker.Launch(); err != nil {
		panic(err)
	}
}
```

The broker, backend and lock are always constructed with an externally owned
`redis.UniversalClient` (build one with `NewGoRedisClient`, or bring your own —
for example a client that already shares a connection pool with your app).
The library never closes the injected client.

## Configuration

`Config` is constructed in code and passed to `NewServer` together with a
broker, a backend and a lock.

Key fields:

| Field | Description |
|---|---|
| `Broker` | Redis URL for the broker |
| `ResultBackend` | Redis URL for the result backend |
| `DefaultQueue` | Default queue name (`machinery_tasks`) |
| `ResultsExpireIn` | Seconds to keep task states / group metas (default 3600) |
| `Redis.NormalTasksPollPeriod` | Poll period in ms for normal tasks (default 1000) |
| `Redis.DelayedTasksPollPeriod` | Poll period in ms for delayed tasks |
| `Redis.DelayedTasksKey` | Sorted-set key used for delayed tasks (`delayed_tasks`) |
| `Redis.MasterName` / `Redis.SentinelPassword` | Sentinel mode |
| `Redis.ClusterEnabled` | Use a cluster client |
| `TLSConfig` | TLS settings applied to the redis client |

## API overview

| Identifier | Purpose |
|---|---|
| `Server` / `NewServer` | Register tasks, send workflows, create workers |
| `Worker` | `Launch`, `LaunchAsync`, `Quit`, custom handlers (`SetErrorHandler`, `SetPreTaskHandler`, ...) |
| `NewRedisBrokerWithClient` | Redis broker (externally owned client) |
| `NewRedisBackendWithClient` | Redis result backend (externally owned client) |
| `NewRedisLock` / `NewRedisLockWithClient` | Distributed lock used by periodic tasks |
| `Signature`, `Arg`, `Chain`, `Group`, `Chord` | Task / workflow definitions |
| `AsyncResult`, `ChainAsyncResult`, `ChordAsyncResult` | Result handles (`Get`, `GetWithTimeout`, `Touch`) |
| `NewGoRedisClient` | Build a `redis.UniversalClient` from `Config` (single / Sentinel / cluster) for injection |
| `RegisterPeriodicTask/Chain/Group/Chord` | Cron-scheduled workflows (cron spec, guarded by the lock) |

## Project layout

Everything lives in one flat `package machinery` at the repo root:

| File | Contents |
|---|---|
| `machinery.go` | `Server` and `Worker` — task registration, publishing, consumption |
| `broker.go` | `Broker` / `TaskProcessor` interfaces, `BaseBroker`, errors, `RedisBroker` |
| `backend.go` | `Backend` interface, `BaseBackend`, `RedisBackend`, result handles |
| `redis.go` | `NewGoRedisClient` (single / Sentinel / cluster), `Lock` interface and `RedisLock` |
| `config.go` | `Config` and `RedisConfig` |
| `tasks.go` | `Signature`, `Task`, task states, chains / groups / chords, reflection helpers |
| `utils.go` | Misc helpers, deep copy, retry closure and Fibonacci backoff |
| `tracing.go` | OpenTracing integration |

Logging uses the standard library's `log/slog`: the library emits records at
`Debug`, `Info`, `Warn` and `Error` levels through `slog.Default()`. Configure
logging in your application with `slog.SetDefault` (for example with a JSON
handler) before launching workers.

## License

MIT — see [LICENSE](LICENSE).
