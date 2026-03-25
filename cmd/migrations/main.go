package main

import (
	"context"
	"os"

	"github.com/openshift-assisted/assisted-events-streams/internal/config"
	"github.com/openshift-assisted/assisted-events-streams/internal/migrations"
	"github.com/openshift-assisted/assisted-events-streams/internal/migrations/domain/repo/valkey"
	"github.com/openshift-assisted/assisted-events-streams/internal/migrations/lock"
	"github.com/openshift-assisted/assisted-events-streams/internal/repository/redis"
	"github.com/openshift-assisted/assisted-events-streams/internal/utils"
)

const configPathEnvVar = "MIGRATIONS_CONFIG_PATH"

func main() {
	ctx := context.Background()
	logger := utils.NewLogger()

	configPath := os.Getenv(configPathEnvVar)

	cfg, err := config.ParseMigration(configPath)
	if err != nil {
		logger.WithError(err).Fatal("Failed to parse config")
	}

	valkeyClient, err := redis.NewRedisClientFromEnv(ctx, logger, redis.MigrationsClientName)
	if err != nil {
		logger.WithError(err).Fatal("Failed to create redis client")
	}

	locker := lock.NewLocker(logger, valkeyClient)
	cache := valkey.NewRepo(logger, valkeyClient)

	runner := migrations.NewRunner(logger, cfg, cache, locker)

	err = runner.Run(ctx)
	if err != nil {
		logger.WithError(err).Fatal("Failed to run migrations")
	}

	logger.Info("Migrations completed successfully")
}
