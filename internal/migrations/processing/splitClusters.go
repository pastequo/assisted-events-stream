package processing

import (
	"context"
	"fmt"
	"time"

	"github.com/openshift-assisted/assisted-events-streams/internal/config"
	"github.com/openshift-assisted/assisted-events-streams/internal/migrations/domain/entity"
	"github.com/openshift-assisted/assisted-events-streams/internal/migrations/domain/repo"
	"github.com/openshift-assisted/assisted-events-streams/internal/migrations/domain/repo/valkey"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

/*
This migration transforms the single key for all clusters in the valkey to one key per cluster.

The previous key was: clusters
The new key format is: clusters_<cluster_id>

hset/hget shouldn't be used anymore, use set/get instead.
The previous key will be deleted after the migration is done.
*/

type SplitClusters struct {
	logger *logrus.Logger
	cache  repo.Cache

	batchSize  int64
	expiration time.Duration
}

func NewSplitClusters(logger *logrus.Logger, cache repo.Cache, cfg config.SplitClusters) SplitClusters {
	return SplitClusters{
		logger: logger,
		cache:  cache,

		batchSize:  cfg.BatchSize,
		expiration: cfg.Expiration,
	}
}

func (p SplitClusters) Run(ctx context.Context) error {
	p.logger.Info("Start splitting clusters")

	kvs := make(chan repo.KeyValue, p.batchSize)

	g := errgroup.Group{}

	g.Go(p.collectClusters(ctx, kvs))
	g.Go(p.addClusters(ctx, kvs))

	err := g.Wait()
	if err != nil {
		return fmt.Errorf("failed to split clusters: %w", err)
	}

	err = p.cache.DeleteKeys(ctx, []string{valkey.ClustersRedisHKey})
	if err != nil {
		return fmt.Errorf("failed to delete previous clusters key: %w", err)
	}

	p.logger.Info("Done splitting clusters")

	return nil
}

func (p SplitClusters) collectClusters(ctx context.Context, kvs chan<- repo.KeyValue) func() error {
	return func() error {
		opts := repo.ListHValuesOptions{
			Type:  entity.TypeCluster,
			Count: p.batchSize,
		}

		err := p.cache.ListHValues(ctx, kvs, opts)
		if err != nil {
			return fmt.Errorf("failed to collect clusters: %w", err)
		}

		return nil
	}
}

func (p SplitClusters) addClusters(ctx context.Context, kvs <-chan repo.KeyValue) func() error {
	return func() error {
		chunk := make([]repo.KeyValue, 0, p.batchSize)

		for kv := range kvs {
			chunk = append(chunk, kv)

			if len(chunk) >= int(p.batchSize) {
				err := p.cache.AddClusters(ctx, chunk, p.expiration)
				if err != nil {
					return fmt.Errorf("failed to add clusters: %w", err)
				}

				chunk = make([]repo.KeyValue, 0, p.batchSize)
			}
		}

		// Last chunk
		err := p.cache.AddClusters(ctx, chunk, p.expiration)
		if err != nil {
			return fmt.Errorf("failed to add clusters: %w", err)
		}

		return nil
	}
}
