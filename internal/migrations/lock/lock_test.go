package lock_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	redsync "github.com/go-redsync/redsync/v4"
	"github.com/openshift-assisted/assisted-events-streams/internal/migrations/lock"
	redisrepo "github.com/openshift-assisted/assisted-events-streams/internal/repository/redis"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Helper

func startValkey(t *testing.T) testcontainers.Container {
	req := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "quay.io/sclorg/valkey-7-c10s:bf91acf0827dc5db216164aafe3d34beb245dcec",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForLog("Ready to accept connections tcp"),
		},
		Started:      true,
		ProviderType: testcontainers.ProviderPodman,
	}

	ret, err := testcontainers.GenericContainer(context.Background(), req)

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

// Test suite definition

type LockTestSuite struct {
	suite.Suite

	client    *redis.Client
	container testcontainers.Container
}

func (s *LockTestSuite) SetupSuite() {
	t := s.T()

	s.container = startValkey(t)
	s.client = createRedisClient(t, s.container)
}

func (s *LockTestSuite) TearDownTest() {
	ctx := context.Background()

	err := s.client.FlushAll(ctx).Err()
	require.NoError(s.T(), err, "failed to clean valkey")
}

// Run test

func TestLockTestSuite(t *testing.T) {
	t.Parallel()

	suite.Run(t, new(LockTestSuite))
}

// Test

func (s *LockTestSuite) TestIsMigrationRunning() {
	ctx := context.Background()
	t := s.T()

	isMigrationRunning, err := lock.IsMigrationRunning(ctx, s.client)
	require.NoError(t, err, "failed to check if migration is running")
	require.False(t, isMigrationRunning, "migration is running")
}

func (s *LockTestSuite) TestIsMigrationRunning_WhenLockHeld() {
	ctx := context.Background()
	t := s.T()

	l := lock.NewLocker(discardLogger(), s.client)

	_, err := l.Acquire(ctx, 30*time.Second)
	require.NoError(t, err)

	running, err := lock.IsMigrationRunning(ctx, s.client)
	require.NoError(t, err)
	require.True(t, running, "expected migration lock key to exist")

	require.NoError(t, l.Release(context.Background()))

	running, err = lock.IsMigrationRunning(ctx, s.client)
	require.NoError(t, err)
	require.False(t, running, "expected lock key to be cleared after release")
}

func (s *LockTestSuite) TestLocker_IsClientConnected() {
	ctx := context.Background()
	t := s.T()

	l := lock.NewLocker(discardLogger(), s.client)

	connected, err := l.IsClientConnected(ctx, redisrepo.MigrationsClientName)
	require.NoError(t, err)
	require.True(t, connected, "suite client is named %q", redisrepo.MigrationsClientName)

	connected, err = l.IsClientConnected(ctx, "no-such-client-name-for-test")
	require.NoError(t, err)
	require.False(t, connected)
}

func (s *LockTestSuite) TestLocker_Release_WithoutAcquire() {
	ctx := context.Background()
	t := s.T()

	l := lock.NewLocker(discardLogger(), s.client)

	err := l.Release(ctx)
	require.Error(t, err)
	require.Equal(t, "lock not acquired", err.Error())
}

func (s *LockTestSuite) TestLocker_Release_Idempotent() {
	ctx := context.Background()
	t := s.T()

	l := lock.NewLocker(discardLogger(), s.client)

	_, err := l.Acquire(ctx, 30*time.Second)
	require.NoError(t, err)

	require.NoError(t, l.Release(context.Background()))
	require.NoError(t, l.Release(context.Background()))
}

func (s *LockTestSuite) TestLocker_Acquire_ConflictWhenAlreadyHeld() {
	ctx := context.Background()
	t := s.T()

	l1 := lock.NewLocker(discardLogger(), s.client)
	_, err := l1.Acquire(ctx, 30*time.Second)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, l1.Release(context.Background()))
	}()

	l2 := lock.NewLocker(discardLogger(), s.client)
	// Try to acquire the lock for 2 seconds to speed up the test
	tryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	_, err = l2.Acquire(tryCtx, 30*time.Second)
	require.Error(t, err)
	require.True(t, errors.Is(err, redsync.ErrFailed), "got %v", err)
}
