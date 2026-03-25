package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/openshift-assisted/assisted-events-streams/internal/migrations/lock"
	"github.com/sirupsen/logrus"
)

const (
	defaultExpirationStr = "720h" // 30 days

	ProcessingClientName = "assisted-events-stream"
	MigrationsClientName = "migrations"
)

func NewRedisClientFromEnv(ctx context.Context, logger *logrus.Logger, clientName string) (*redis.Client, error) {
	addr := os.Getenv("VALKEY_ADDRESS")
	password := os.Getenv("VALKEY_PASSWORD")

	return NewRedisClient(ctx, logger, clientName, addr, password)
}

func NewRedisClient(ctx context.Context, logger *logrus.Logger, clientName string, addr string, password string) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		MinIdleConns: 1, // Needed to ensure at least one connection is up, which helps the migration command to ensure no processing is running simultaneously
		DB:           0,
		OnConnect: func(ctx context.Context, cn *redis.Conn) error {
			logger.Debugf("setting client name to %s", clientName)

			err := cn.ClientSetName(ctx, clientName).Err()
			if err != nil {
				logger.WithError(err).Error("failed to set client name")

				return err
			}

			return nil
		},
	})

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("could not ping redis compatible server: %w", err)
	}

	return client, nil
}

func NewSnapshotRepositoryFromEnv(ctx context.Context, logger *logrus.Logger) (*SnapshotRepository, error) {
	redis, err := NewRedisClientFromEnv(ctx, logger, ProcessingClientName)
	if err != nil {
		return nil, fmt.Errorf("failed to create redis client: %w", err)
	}

	isMigrationRunning, err := lock.IsMigrationRunning(ctx, redis)
	if err != nil {
		return nil, fmt.Errorf("failed to check if migration is running: %w", err)
	}

	if isMigrationRunning {
		return nil, errors.New("migration is running")
	}

	expirationStr := os.Getenv("VALKEY_EXPIRATION")
	if expirationStr == "" {
		expirationStr = defaultExpirationStr
	}

	expiration, err := time.ParseDuration(expirationStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse expiration duration: %w", err)
	}

	return NewSnapshotRepository(logger, redis, expiration), nil
}
