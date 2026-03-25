package lock

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
	"github.com/sirupsen/logrus"
)

const (
	lockKey = "migrations:lock"
)

func IsMigrationRunning(ctx context.Context, redis *redis.Client) (bool, error) {
	result, err := redis.Exists(ctx, lockKey).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check if key exists: %w", err)
	}

	return result > 0, nil
}

type Locker struct {
	logger *logrus.Logger

	redis *redis.Client
	rs    *redsync.Redsync

	mutex      *redsync.Mutex
	autoExtend chan struct{}
	cancel     context.CancelFunc

	releaseOnce sync.Once
}

func NewLocker(logger *logrus.Logger, redis *redis.Client) *Locker {
	pool := goredis.NewPool(redis)

	return &Locker{
		logger: logger,
		redis:  redis,
		rs:     redsync.New(pool),
	}
}

func (locker *Locker) IsMigrationRunning(ctx context.Context) (bool, error) {
	return IsMigrationRunning(ctx, locker.redis)
}

func (locker *Locker) IsClientConnected(ctx context.Context, name string) (bool, error) {
	result, err := locker.redis.ClientList(ctx).Result()
	if err != nil {
		return false, fmt.Errorf("failed to list clients: %w", err)
	}

	clients := strings.SplitSeq(result, "\n")
	for client := range clients {
		if strings.Contains(client, fmt.Sprintf("name=%s", name)) {
			return true, nil
		}
	}

	return false, nil
}

func (locker *Locker) Acquire(ctx context.Context, duration time.Duration) (context.Context, error) {
	mutex := locker.rs.NewMutex(lockKey, redsync.WithExpiry(duration))

	err := mutex.LockContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}

	ret, cancel := context.WithCancel(ctx)

	locker.cancel = cancel
	locker.mutex = mutex
	locker.autoExtend = make(chan struct{})

	// Start auto extend goroutine
	go func() {
		ticker := time.NewTicker(duration / 2)

		defer ticker.Stop()
		defer locker.cancel()
		defer close(locker.autoExtend)

		for {
			select {
			case <-ticker.C:
				extended, err := locker.mutex.ExtendContext(ret)
				if err != nil {
					locker.logger.WithError(err).Error("failed to extend lock")

					return
				}

				if !extended {
					locker.logger.Error("lock was not extended")

					return
				}
			case <-ret.Done():
				return
			}
		}
	}()

	return ret, nil
}

func (locker *Locker) Release(ctx context.Context) error {
	if locker.mutex == nil {
		return errors.New("lock not acquired")
	}

	var err error

	locker.releaseOnce.Do(func() {
		locker.cancel()
		<-locker.autoExtend

		var unlocked bool

		unlocked, err = locker.mutex.UnlockContext(ctx)
		if err != nil {
			err = fmt.Errorf("failed to release lock: %w", err)

			return
		}

		if !unlocked {
			err = errors.New("lock was not unlocked")

			return
		}
	})

	return err
}
