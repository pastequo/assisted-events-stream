package valkey

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/openshift-assisted/assisted-events-streams/internal/migrations/domain/entity"
	"github.com/openshift-assisted/assisted-events-streams/internal/migrations/domain/repo"
	"github.com/sirupsen/logrus"
)

type Repo struct {
	logger *logrus.Logger
	redis  redis.Cmdable
}

const (
	hostsRedisHKeyPrefix     = "hosts_"
	infraEnvsRedisHKeyPrefix = "infraenvs_"
	clustersRedisHKeyPrefix  = "clusters_"

	ClustersRedisHKey = "clusters"
)

func NewRepo(logger *logrus.Logger, redis redis.Cmdable) Repo {
	return Repo{
		logger: logger,
		redis:  redis,
	}
}

func (r Repo) ListKeys(ctx context.Context, output chan<- string, opts repo.ListKeysOptions) error {
	defer close(output)

	prefix := ""

	switch opts.Type {
	case entity.TypeHost:
		prefix = fmt.Sprintf("%s*", hostsRedisHKeyPrefix)
	case entity.TypeInfraEnv:
		prefix = fmt.Sprintf("%s*", infraEnvsRedisHKeyPrefix)
	default:
		return fmt.Errorf("invalid entity type: %s", opts.Type)
	}

	cursor := uint64(0)

	totalKeys := int64(0)
	filteredKeys := int64(0)

	for {
		keys, next, err := r.redis.Scan(ctx, cursor, prefix, opts.Count).Result()
		if err != nil {
			return fmt.Errorf("failed to scan keys: %w", err)
		}

		totalKeys += int64(len(keys))

		keys, err = r.filterKeys(ctx, keys, opts)
		if err != nil {
			return fmt.Errorf("failed to filter keys: %w", err)
		}

		filteredKeys += int64(len(keys))

		for _, key := range keys {
			output <- key
		}

		if next == 0 {
			break
		}

		cursor = next
	}

	r.logger.Infof("Total keys: %d, filtered keys: %d", totalKeys, filteredKeys)

	return nil
}

func (r Repo) ListHValues(ctx context.Context, output chan<- repo.KeyValue, opts repo.ListHValuesOptions) error {
	defer close(output)

	key := ""

	switch opts.Type {
	case entity.TypeCluster:
		key = ClustersRedisHKey
	default:
		return fmt.Errorf("invalid entity type: %s", opts.Type)
	}

	cursor := uint64(0)

	totalKeys := int64(0)

	for {
		kvs, next, err := r.redis.HScan(ctx, key, cursor, "", opts.Count).Result()
		if err != nil {
			return fmt.Errorf("failed to hscan keys: %w", err)
		}

		totalKeys += int64(len(kvs) / 2)

		if len(kvs)%2 != 0 {
			return fmt.Errorf("expected even number of keys, got %d", len(kvs))
		}

		for i := 0; i < len(kvs); i += 2 {
			output <- repo.KeyValue{
				Key:   kvs[i],
				Value: kvs[i+1],
			}
		}

		if next == 0 {
			break
		}

		cursor = next
	}

	r.logger.Infof("Total keys: %d", totalKeys)

	return nil
}

func (r Repo) DeleteKeys(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	pipeline := r.redis.Pipeline()

	for _, key := range keys {
		pipeline.Del(ctx, key)
	}

	_, err := pipeline.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to delete keys: %w", err)
	}

	return nil
}

func (r Repo) AddClusters(ctx context.Context, kvs []repo.KeyValue, expiration time.Duration) error {
	if len(kvs) == 0 {
		return nil
	}

	pipeline := r.redis.Pipeline()

	for _, kv := range kvs {
		pipeline.Set(ctx, fmt.Sprintf("%s%s", clustersRedisHKeyPrefix, kv.Key), kv.Value, expiration)
	}

	_, err := pipeline.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to add clusters: %w", err)
	}

	return nil
}

func (r Repo) filterKeys(ctx context.Context, keys []string, opts repo.ListKeysOptions) ([]string, error) {
	if opts.TTLSet == nil {
		return keys, nil
	}

	ret := make([]string, 0, len(keys))

	pipeline := r.redis.Pipeline()
	for _, key := range keys {
		pipeline.TTL(ctx, key)
	}

	results, err := pipeline.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get TTLs: %w", err)
	}

	for i, result := range results {
		ttl := result.(*redis.DurationCmd).Val()

		switch {
		// key does not exist
		case ttl == time.Duration(-2):
			continue

		// key exists, has no associated expire and the caller asked for no TTL
		case ttl == time.Duration(-1) && !*opts.TTLSet:
			ret = append(ret, keys[i])

		// key exists, has an associated expire and the caller asked for a TTL
		case ttl >= time.Duration(0) && *opts.TTLSet:
			ret = append(ret, keys[i])
		}
	}

	return ret, nil
}
