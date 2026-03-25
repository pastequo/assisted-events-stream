package valkey_test

import (
	"context"
	"io"
	"sort"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/openshift-assisted/assisted-events-streams/internal/migrations/domain/entity"
	"github.com/openshift-assisted/assisted-events-streams/internal/migrations/domain/repo"
	valkeyrepo "github.com/openshift-assisted/assisted-events-streams/internal/migrations/domain/repo/valkey"
	redisrepo "github.com/openshift-assisted/assisted-events-streams/internal/repository/redis"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func startValkey(t *testing.T) testcontainers.Container {
	req := testcontainers.ContainerRequest{
		Image:        "quay.io/sclorg/valkey-7-c10s:bf91acf0827dc5db216164aafe3d34beb245dcec",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections tcp"),
	}
	ret, err := testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})

	testcontainers.CleanupContainer(t, ret)

	require.NoError(t, err, "failed to start valkey instance")

	return ret
}

func createRedisClient(t *testing.T, container testcontainers.Container) *redis.Client {
	endpoint, err := container.Endpoint(context.Background(), "")
	require.NoError(t, err, "failed to get valkey endpoint")

	redisClient, err := redisrepo.NewRedisClient(context.Background(), discardLogger(), redisrepo.MigrationsClientName, endpoint, "")
	require.NoError(t, err, "failed to create redis client")

	return redisClient
}

func discardLogger() *logrus.Logger {
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	return logger
}

func newTestRepo(client *redis.Client) valkeyrepo.Repo {
	return valkeyrepo.NewRepo(discardLogger(), client)
}

type ValkeyRepoTestSuite struct {
	suite.Suite

	client *redis.Client
	repo   valkeyrepo.Repo

	container testcontainers.Container
}

func (s *ValkeyRepoTestSuite) SetupSuite() {
	t := s.T()

	s.container = startValkey(t)
	s.client = createRedisClient(t, s.container)
	s.repo = newTestRepo(s.client)
}

func (s *ValkeyRepoTestSuite) TearDownTest() {
	ctx := context.Background()

	err := s.client.FlushAll(ctx).Err()
	require.NoError(s.T(), err, "failed to clean valkey")
}

func TestValkeyRepoTestSuite(t *testing.T) {
	t.Parallel()

	suite.Run(t, new(ValkeyRepoTestSuite))
}

func collectListKeys(ctx context.Context, t *testing.T, r valkeyrepo.Repo, opts repo.ListKeysOptions) ([]string, error) {
	t.Helper()

	out := make(chan string)
	errCh := make(chan error, 1)

	go func() {
		errCh <- r.ListKeys(ctx, out, opts)
	}()

	var keys []string

	for k := range out {
		keys = append(keys, k)
	}

	err := <-errCh
	if err != nil {
		return keys, err
	}

	return keys, nil
}

func collectListHValues(ctx context.Context, t *testing.T, r valkeyrepo.Repo, opts repo.ListHValuesOptions) ([]repo.KeyValue, error) {
	t.Helper()

	out := make(chan repo.KeyValue)
	errCh := make(chan error, 1)

	go func() {
		errCh <- r.ListHValues(ctx, out, opts)
	}()

	var kvs []repo.KeyValue

	for kv := range out {
		kvs = append(kvs, kv)
	}

	err := <-errCh
	if err != nil {
		return kvs, err
	}

	return kvs, nil
}

func (s *ValkeyRepoTestSuite) TestListKeys_Hosts() {
	ctx := context.Background()
	t := s.T()

	require.NoError(t, s.client.Set(ctx, "hosts_a", "1", 0).Err())
	require.NoError(t, s.client.Set(ctx, "hosts_b", "2", 0).Err())
	require.NoError(t, s.client.Set(ctx, "other_key", "x", 0).Err())

	keys, err := collectListKeys(ctx, t, s.repo, repo.ListKeysOptions{
		Type:  entity.TypeHost,
		Count: 100,
	})
	require.NoError(t, err)

	sort.Strings(keys)
	require.Equal(t, []string{"hosts_a", "hosts_b"}, keys)
}

func (s *ValkeyRepoTestSuite) TestListKeys_InfraEnv() {
	ctx := context.Background()
	t := s.T()

	require.NoError(t, s.client.Set(ctx, "infraenvs_1", "a", 0).Err())
	require.NoError(t, s.client.Set(ctx, "infraenvs_2", "b", 0).Err())

	keys, err := collectListKeys(ctx, t, s.repo, repo.ListKeysOptions{
		Type:  entity.TypeInfraEnv,
		Count: 100,
	})
	require.NoError(t, err)

	sort.Strings(keys)
	require.Equal(t, []string{"infraenvs_1", "infraenvs_2"}, keys)
}

func (s *ValkeyRepoTestSuite) TestListKeys_InvalidType() {
	ctx := context.Background()
	t := s.T()

	out := make(chan string)
	err := s.repo.ListKeys(ctx, out, repo.ListKeysOptions{
		Type:  entity.TypeCluster,
		Count: 10,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid entity type")
}

func (s *ValkeyRepoTestSuite) TestListKeys_TTLFilter_KeysWithoutExpire() {
	ctx := context.Background()
	t := s.T()

	require.NoError(t, s.client.Set(ctx, "hosts_persistent", "v", 0).Err())
	require.NoError(t, s.client.Set(ctx, "hosts_with_ttl", "v", time.Hour).Err())

	ttlFalse := false
	keys, err := collectListKeys(ctx, t, s.repo, repo.ListKeysOptions{
		Type:   entity.TypeHost,
		Count:  100,
		TTLSet: &ttlFalse,
	})
	require.NoError(t, err)

	require.Equal(t, []string{"hosts_persistent"}, keys)
}

func (s *ValkeyRepoTestSuite) TestListKeys_TTLFilter_KeysWithExpire() {
	ctx := context.Background()
	t := s.T()

	require.NoError(t, s.client.Set(ctx, "hosts_persistent", "v", 0).Err())
	require.NoError(t, s.client.Set(ctx, "hosts_with_ttl", "v", time.Hour).Err())

	ttlTrue := true
	keys, err := collectListKeys(ctx, t, s.repo, repo.ListKeysOptions{
		Type:   entity.TypeHost,
		Count:  100,
		TTLSet: &ttlTrue,
	})
	require.NoError(t, err)

	require.Equal(t, []string{"hosts_with_ttl"}, keys)
}

func (s *ValkeyRepoTestSuite) TestListHValues_Clusters() {
	ctx := context.Background()
	t := s.T()

	require.NoError(t, s.client.HSet(ctx, valkeyrepo.ClustersRedisHKey, "f1", "v1", "f2", "v2").Err())

	kvs, err := collectListHValues(ctx, t, s.repo, repo.ListHValuesOptions{
		Type:  entity.TypeCluster,
		Count: 100,
	})
	require.NoError(t, err)

	sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key < kvs[j].Key })
	require.Equal(t, []repo.KeyValue{
		{Key: "f1", Value: "v1"},
		{Key: "f2", Value: "v2"},
	}, kvs)
}

func (s *ValkeyRepoTestSuite) TestListHValues_EmptyHash() {
	ctx := context.Background()
	t := s.T()

	require.NoError(t, s.client.Del(ctx, valkeyrepo.ClustersRedisHKey).Err())

	kvs, err := collectListHValues(ctx, t, s.repo, repo.ListHValuesOptions{
		Type:  entity.TypeCluster,
		Count: 100,
	})
	require.NoError(t, err)
	require.Empty(t, kvs)
}

func (s *ValkeyRepoTestSuite) TestListHValues_InvalidType() {
	ctx := context.Background()
	t := s.T()

	out := make(chan repo.KeyValue)
	err := s.repo.ListHValues(ctx, out, repo.ListHValuesOptions{
		Type:  entity.TypeHost,
		Count: 10,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid entity type")
}

func (s *ValkeyRepoTestSuite) TestDeleteKeys() {
	ctx := context.Background()
	t := s.T()

	require.NoError(t, s.client.Set(ctx, "hosts_x", "1", 0).Err())
	require.NoError(t, s.client.Set(ctx, "hosts_y", "2", 0).Err())

	require.NoError(t, s.repo.DeleteKeys(ctx, []string{"hosts_x", "hosts_y"}))

	n, err := s.client.Exists(ctx, "hosts_x", "hosts_y").Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), n)
}

func (s *ValkeyRepoTestSuite) TestDeleteKeys_EmptySlice() {
	ctx := context.Background()
	t := s.T()

	require.NoError(t, s.repo.DeleteKeys(ctx, nil))
}

func (s *ValkeyRepoTestSuite) TestAddClusters() {
	ctx := context.Background()
	t := s.T()

	err := s.repo.AddClusters(ctx, []repo.KeyValue{
		{Key: "id1", Value: `{"name":"c1"}`},
		{Key: "id2", Value: `{"name":"c2"}`},
	}, time.Hour)
	require.NoError(t, err)

	v1, err := s.client.Get(ctx, "clusters_id1").Result()
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"c1"}`, v1)

	v2, err := s.client.Get(ctx, "clusters_id2").Result()
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"c2"}`, v2)

	ttl, err := s.client.TTL(ctx, "clusters_id1").Result()
	require.NoError(t, err)
	require.Greater(t, ttl, time.Duration(0))
}

func (s *ValkeyRepoTestSuite) TestAddClusters_EmptySlice() {
	ctx := context.Background()
	t := s.T()

	require.NoError(t, s.repo.AddClusters(ctx, nil, time.Hour))
}
