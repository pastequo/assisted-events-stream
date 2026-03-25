package processing_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/openshift-assisted/assisted-events-streams/internal/config"
	valkeyrepo "github.com/openshift-assisted/assisted-events-streams/internal/migrations/domain/repo/valkey"
	"github.com/openshift-assisted/assisted-events-streams/internal/migrations/processing"
	redisrepo "github.com/openshift-assisted/assisted-events-streams/internal/repository/redis"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func idempotenceStartValkey(t *testing.T) testcontainers.Container {
	t.Helper()

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

func idempotenceRedisClient(t *testing.T, container testcontainers.Container) *redis.Client {
	t.Helper()

	endpoint, err := container.Endpoint(context.Background(), "")
	require.NoError(t, err, "failed to get valkey endpoint")

	redisClient, err := redisrepo.NewRedisClient(context.Background(), idempotenceDiscardLogger(), redisrepo.MigrationsClientName, endpoint, "")
	require.NoError(t, err, "failed to create redis client")

	return redisClient
}

func idempotenceDiscardLogger() *logrus.Logger {
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	return logger
}

type ProcessingIdempotenceTestSuite struct {
	suite.Suite

	client    *redis.Client
	container testcontainers.Container
}

func (s *ProcessingIdempotenceTestSuite) SetupSuite() {
	t := s.T()

	s.container = idempotenceStartValkey(t)
	s.client = idempotenceRedisClient(t, s.container)
}

func (s *ProcessingIdempotenceTestSuite) TearDownTest() {
	ctx := context.Background()

	err := s.client.FlushAll(ctx).Err()
	require.NoError(s.T(), err, "failed to clean valkey")
}

func TestProcessingIdempotenceTestSuite(t *testing.T) {
	t.Parallel()

	suite.Run(t, new(ProcessingIdempotenceTestSuite))
}

func (s *ProcessingIdempotenceTestSuite) TestDeleteNoTTL_IdempotentTwice() {
	ctx := context.Background()
	t := s.T()
	client := s.client

	require.NoError(t, client.Set(ctx, "hosts_with_ttl", "keep", time.Hour).Err())
	require.NoError(t, client.Set(ctx, "hosts_no_ttl", "drop", 0).Err())
	require.NoError(t, client.Set(ctx, "infraenvs_no_ttl", "drop", 0).Err())

	repo := valkeyrepo.NewRepo(idempotenceDiscardLogger(), client)
	p := processing.NewDeleteNoTTL(idempotenceDiscardLogger(), repo, config.DeleteNoTTL{BatchSize: 100})

	require.NoError(t, p.Run(ctx))

	n, err := client.Exists(ctx, "hosts_no_ttl", "infraenvs_no_ttl").Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), n, "keys without TTL should be removed")

	keep, err := client.Exists(ctx, "hosts_with_ttl").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), keep, "key with TTL must remain")

	require.NoError(t, p.Run(ctx))

	n, err = client.Exists(ctx, "hosts_no_ttl", "infraenvs_no_ttl").Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	keep, err = client.Exists(ctx, "hosts_with_ttl").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), keep)
}

func (s *ProcessingIdempotenceTestSuite) TestSplitClusters_IdempotentTwice() {
	ctx := context.Background()
	t := s.T()
	client := s.client

	const id1 = "cluster-1"
	const id2 = "cluster-2"
	val1 := `{"id":"cluster-1"}`
	val2 := `{"id":"cluster-2"}`

	require.NoError(t, client.HSet(ctx, valkeyrepo.ClustersRedisHKey, id1, val1, id2, val2).Err())

	repo := valkeyrepo.NewRepo(idempotenceDiscardLogger(), client)
	exp := time.Hour
	p := processing.NewSplitClusters(idempotenceDiscardLogger(), repo, config.SplitClusters{
		BatchSize:  100,
		Expiration: exp,
	})

	require.NoError(t, p.Run(ctx))

	key1 := "clusters_" + id1
	key2 := "clusters_" + id2

	got1, err := client.Get(ctx, key1).Result()
	require.NoError(t, err)
	require.JSONEq(t, val1, got1)

	got2, err := client.Get(ctx, key2).Result()
	require.NoError(t, err)
	require.JSONEq(t, val2, got2)

	exists, err := client.Exists(ctx, valkeyrepo.ClustersRedisHKey).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), exists, "legacy hash key should be removed")

	require.NoError(t, p.Run(ctx))

	got1, err = client.Get(ctx, key1).Result()
	require.NoError(t, err)
	require.JSONEq(t, val1, got1)

	got2, err = client.Get(ctx, key2).Result()
	require.NoError(t, err)
	require.JSONEq(t, val2, got2)

	exists, err = client.Exists(ctx, valkeyrepo.ClustersRedisHKey).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), exists)
}
