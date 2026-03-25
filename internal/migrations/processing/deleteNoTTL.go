package processing

import (
	"context"
	"fmt"

	"github.com/openshift-assisted/assisted-events-streams/internal/config"
	"github.com/openshift-assisted/assisted-events-streams/internal/migrations/domain/entity"
	"github.com/openshift-assisted/assisted-events-streams/internal/migrations/domain/repo"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

/*
This migration deletes all keys for hosts and infra_envs from the valkey that do not have a TTL set.
*/

var noTTL = false

type DeleteNoTTL struct {
	logger *logrus.Logger
	cache  repo.Cache

	batchSize int64
}

func NewDeleteNoTTL(logger *logrus.Logger, cache repo.Cache, cfg config.DeleteNoTTL) DeleteNoTTL {
	return DeleteNoTTL{
		logger:    logger,
		cache:     cache,
		batchSize: cfg.BatchSize,
	}
}

func (p DeleteNoTTL) Run(ctx context.Context) error {
	p.logger.Info("Start deleting keys for hosts in Valkey without TTL")

	err := p.runForEntityType(ctx, entity.TypeHost)
	if err != nil {
		return fmt.Errorf("failed to process hosts: %w", err)
	}

	p.logger.Info("Start deleting keys for infra_envs in Valkey without TTL")

	err = p.runForEntityType(ctx, entity.TypeInfraEnv)
	if err != nil {
		return fmt.Errorf("failed to process infra_envs: %w", err)
	}

	p.logger.Info("Finished deleting keys without TTL")

	return nil
}

func (p DeleteNoTTL) runForEntityType(ctx context.Context, entityType entity.Type) error {
	logger := p.logger.WithField("entity_type", entityType)

	logger.Info("Start processing")

	keys := make(chan string, p.batchSize)
	opts := repo.ListKeysOptions{
		Type:   entityType,
		Count:  p.batchSize,
		TTLSet: &noTTL,
	}

	g := errgroup.Group{}
	g.Go(p.collectKeys(ctx, keys, opts))
	g.Go(p.deleteKeys(ctx, keys))

	err := g.Wait()
	if err != nil {
		return fmt.Errorf("failed to process: %w", err)
	}

	logger.Info("Processing is done")

	return nil
}

func (p DeleteNoTTL) collectKeys(ctx context.Context, keys chan<- string, opts repo.ListKeysOptions) func() error {
	return func() error {
		err := p.cache.ListKeys(ctx, keys, opts)
		if err != nil {
			return fmt.Errorf("failed to collect keys: %w", err)
		}

		return nil
	}
}

func (p DeleteNoTTL) deleteKeys(ctx context.Context, keys <-chan string) func() error {
	return func() error {
		chunk := make([]string, 0, p.batchSize)

		for key := range keys {
			chunk = append(chunk, key)

			if len(chunk) >= int(p.batchSize) {
				err := p.cache.DeleteKeys(ctx, chunk)
				if err != nil {
					return fmt.Errorf("failed to delete keys: %w", err)
				}

				chunk = make([]string, 0, p.batchSize)
			}
		}

		// Last chunk
		err := p.cache.DeleteKeys(ctx, chunk)
		if err != nil {
			return fmt.Errorf("failed to delete keys: %w", err)
		}

		return nil
	}
}
