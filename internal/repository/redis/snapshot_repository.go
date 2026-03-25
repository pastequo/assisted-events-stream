package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/openshift-assisted/assisted-events-streams/internal/types"
	"github.com/sirupsen/logrus"
)

const (
	ClustersRedisKeyPrefix   = "clusters_"
	HostsRedisHKeyPrefix     = "hosts_"
	InfraEnvsRedisHKeyPrefix = "infraenvs_"
)

//go:generate mockgen -source=snapshot_repository.go -package=redis -destination=mock_snapshot_repository.go

type SnapshotRepositoryInterface interface {
	SetCluster(ctx context.Context, clusterID string, event *types.Event) error
	SetHost(ctx context.Context, clusterID, hostID string, event *types.Event) error
	SetInfraEnv(ctx context.Context, clusterID, infraEnvID string, event *types.Event) error
	GetCluster(ctx context.Context, clusterID string) (map[string]interface{}, error)
	GetHosts(ctx context.Context, clusterID string) ([]map[string]interface{}, error)
	GetInfraEnvs(ctx context.Context, clusterID string) ([]map[string]interface{}, error)
}

type SnapshotRepository struct {
	logger     *logrus.Logger
	redis      redis.Cmdable
	expiration time.Duration
}

func NewSnapshotRepository(logger *logrus.Logger, redis redis.Cmdable, expiration time.Duration) *SnapshotRepository {
	return &SnapshotRepository{
		logger:     logger,
		redis:      redis,
		expiration: expiration,
	}
}

func (s *SnapshotRepository) hset(ctx context.Context, key string, field string, event *types.Event) error {
	var err error
	eventBytes, err := createPayload(event)
	if err != nil {
		return fmt.Errorf("failed to create payload: %w", err)
	}

	err = s.redis.HSet(ctx, key, field, eventBytes).Err()
	if err != nil {
		return fmt.Errorf("failed to set key: %w", err)
	}

	err = s.redis.Expire(ctx, key, s.expiration).Err()
	if err != nil {
		return fmt.Errorf("failed to set ttl: %w", err)
	}

	return nil
}

func getClustersKey(clusterID string) string {
	return ClustersRedisKeyPrefix + clusterID
}

func getHostsHKey(clusterID string) string {
	return HostsRedisHKeyPrefix + clusterID
}

func getInfraEnvsHKey(clusterID string) string {
	return InfraEnvsRedisHKeyPrefix + clusterID
}

func createPayload(event *types.Event) ([]byte, error) {
	ret := []byte("")
	if event == nil {
		return ret, nil
	}

	ret, err := json.Marshal(event.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	return ret, nil
}

func (s *SnapshotRepository) SetCluster(ctx context.Context, clusterID string, event *types.Event) error {
	payload, err := createPayload(event)
	if err != nil {
		return fmt.Errorf("failed to create payload: %w", err)
	}

	err = s.redis.Set(ctx, getClustersKey(clusterID), payload, s.expiration).Err()
	if err != nil {
		return fmt.Errorf("failed to set key: %w", err)
	}

	return nil
}

func (s *SnapshotRepository) SetHost(ctx context.Context, clusterID, hostID string, event *types.Event) error {
	return s.hset(ctx, getHostsHKey(clusterID), hostID, event)
}

func (s *SnapshotRepository) SetInfraEnv(ctx context.Context, clusterID, infraEnvID string, event *types.Event) error {
	return s.hset(ctx, getInfraEnvsHKey(clusterID), infraEnvID, event)
}

func (s *SnapshotRepository) GetCluster(ctx context.Context, clusterID string) (map[string]interface{}, error) {
	cluster := map[string]interface{}{}
	clusterRaw, err := s.redis.Get(ctx, getClustersKey(clusterID)).Bytes()
	if err != nil {
		return cluster, nil
	}
	err = json.Unmarshal(clusterRaw, &cluster)
	return cluster, err
}

func (s *SnapshotRepository) GetHosts(ctx context.Context, clusterID string) ([]map[string]interface{}, error) {
	var hosts []map[string]interface{}
	hostsRaw, err := s.redis.HGetAll(ctx, getHostsHKey(clusterID)).Result()
	if err != nil {
		return hosts, err
	}
	for _, v := range hostsRaw {
		hostState := map[string]interface{}{}
		err := json.Unmarshal([]byte(v), &hostState)
		if err != nil {
			// maybe log error but return other objects?
			return hosts, err
		}
		hosts = append(hosts, hostState)
	}
	return hosts, nil
}

func (s *SnapshotRepository) GetInfraEnvs(ctx context.Context, clusterID string) ([]map[string]interface{}, error) {
	var infraEnvs []map[string]interface{}
	infraEnv, err := s.redis.HGetAll(ctx, getInfraEnvsHKey(clusterID)).Result()
	if err != nil {
		return infraEnvs, err
	}
	for _, v := range infraEnv {
		infraEnvState := map[string]interface{}{}
		err := json.Unmarshal([]byte(v), &infraEnvState)
		if err != nil {
			// maybe log error but return other objects?
			return infraEnvs, err
		}
		infraEnvs = append(infraEnvs, infraEnvState)
	}
	return infraEnvs, nil
}
