package migrations

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/openshift-assisted/assisted-events-streams/internal/config"
	"github.com/openshift-assisted/assisted-events-streams/internal/migrations/domain/repo/valkey"
	"github.com/openshift-assisted/assisted-events-streams/internal/migrations/lock"
	"github.com/openshift-assisted/assisted-events-streams/internal/migrations/processing"
	"github.com/openshift-assisted/assisted-events-streams/internal/repository/redis"
	"github.com/sirupsen/logrus"
)

// Migration is the interface for all migrations
// Migrations should be run in order and each migration must be idempotent
type Migration interface {
	Run(ctx context.Context) error
}

type Runner struct {
	logger *logrus.Logger
	cfg    config.Migration

	cache  valkey.Repo
	locker *lock.Locker
}

func NewRunner(logger *logrus.Logger, cfg config.Migration, cache valkey.Repo, locker *lock.Locker) *Runner {
	return &Runner{
		logger: logger,
		cfg:    cfg,
		cache:  cache,
		locker: locker,
	}
}

func (r *Runner) Run(defaultCtx context.Context) error {
	ctx, err := lockForMigration(defaultCtx, r.locker)
	if err != nil {
		return fmt.Errorf("failed to lock for migration: %w", err)
	}

	defer func() {
		err = r.locker.Release(defaultCtx)
		if err != nil {
			r.logger.WithError(err).Error("Failed to release lock after migrations")
		}
	}()

	migrations := createMigrations(r.logger, r.cache, r.cfg.Processing)
	err = runMigrations(ctx, migrations)
	if err != nil {
		return err
	}

	return nil
}

func createMigrations(logger *logrus.Logger, cache valkey.Repo, cfg config.Processing) []Migration {
	migrations := make([]Migration, 0)

	if cfg.DeleteNoTTL.Enabled {
		migrations = append(migrations, processing.NewDeleteNoTTL(logger, cache, cfg.DeleteNoTTL))
	}

	if cfg.SplitClusters.Enabled {
		migrations = append(migrations, processing.NewSplitClusters(logger, cache, cfg.SplitClusters))
	}

	return migrations
}

func runMigrations(ctx context.Context, migrations []Migration) error {
	for _, migration := range migrations {
		err := migration.Run(ctx)
		if err != nil {
			return fmt.Errorf("failed to run migration: %w", err)
		}
	}

	return nil
}

func lockForMigration(ctx context.Context, locker *lock.Locker) (context.Context, error) {
	// First ensure that no migration is running
	// Not useful for say since Acquire should fail, but sueful to fail fast with a clear message
	isMigrationRunning, err := locker.IsMigrationRunning(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to check if migration is running: %w", err)
	}

	if isMigrationRunning {
		return nil, fmt.Errorf("migration is already running")
	}

	// Then acquire the lock
	ret, err := locker.Acquire(ctx, 10*time.Minute)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}

	// Finally ensure that no processing client is connected
	isClientConnected, err := locker.IsClientConnected(ctx, redis.ProcessingClientName)
	if err != nil {
		errRelease := locker.Release(ctx)
		if errRelease != nil {
			err = errors.Join(err, fmt.Errorf("failed to release lock: %w", errRelease))
		}

		return nil, fmt.Errorf("failed to check if client is connected: %w", err)
	}

	if isClientConnected {
		err = fmt.Errorf("processing client is connected")

		errRelease := locker.Release(ctx)
		if errRelease != nil {
			err = errors.Join(err, fmt.Errorf("failed to release lock: %w", errRelease))
		}

		return nil, err
	}

	return ret, nil
}
