package processing_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/openshift-assisted/assisted-events-streams/internal/config"
	"github.com/openshift-assisted/assisted-events-streams/internal/migrations/domain/entity"
	"github.com/openshift-assisted/assisted-events-streams/internal/migrations/domain/repo"
	"github.com/openshift-assisted/assisted-events-streams/internal/migrations/domain/repo/mock"
	"github.com/openshift-assisted/assisted-events-streams/internal/migrations/processing"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestDeleteNoTTL_Run_Success(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockCache := mock.NewMockCache(ctrl)
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	const batch = int64(1000)
	ttlFalse := false

	mockCache.EXPECT().ListKeys(gomock.Any(), gomock.Any(),
		gomock.Eq(repo.ListKeysOptions{Type: entity.TypeHost, Count: batch, TTLSet: &ttlFalse})).
		DoAndReturn(func(_ context.Context, ch chan<- string, _ repo.ListKeysOptions) error {
			ch <- "hosts_a"
			ch <- "hosts_b"
			close(ch)

			return nil
		})

	mockCache.EXPECT().DeleteKeys(gomock.Any(), gomock.Eq([]string{"hosts_a", "hosts_b"})).Return(nil)

	mockCache.EXPECT().ListKeys(gomock.Any(), gomock.Any(),
		gomock.Eq(repo.ListKeysOptions{Type: entity.TypeInfraEnv, Count: batch, TTLSet: &ttlFalse})).
		DoAndReturn(func(_ context.Context, ch chan<- string, _ repo.ListKeysOptions) error {
			close(ch)

			return nil
		})

	// Empty batch deletes are not part of the cache contract; allow any number (including zero).
	mockCache.EXPECT().DeleteKeys(gomock.Any(), []string{}).Return(nil).AnyTimes()

	p := processing.NewDeleteNoTTL(logger, mockCache, config.DeleteNoTTL{BatchSize: batch})
	require.NoError(t, p.Run(ctx))
}

func TestDeleteNoTTL_Run_ListKeysErrorOnHosts(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockCache := mock.NewMockCache(ctrl)
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	listErr := errors.New("scan failed")
	ttlFalse := false

	mockCache.EXPECT().ListKeys(gomock.Any(), gomock.Any(),
		gomock.Eq(repo.ListKeysOptions{Type: entity.TypeHost, Count: 10, TTLSet: &ttlFalse})).
		DoAndReturn(func(_ context.Context, ch chan<- string, _ repo.ListKeysOptions) error {
			close(ch)

			return listErr
		})

	mockCache.EXPECT().DeleteKeys(gomock.Any(), []string{}).Return(nil).AnyTimes()

	p := processing.NewDeleteNoTTL(logger, mockCache, config.DeleteNoTTL{BatchSize: 10})
	err := p.Run(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, listErr)
}

func TestDeleteNoTTL_Run_ListKeysErrorOnInfraEnvs(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockCache := mock.NewMockCache(ctrl)
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	const batch = int64(1000)
	listErr := errors.New("infra scan failed")
	ttlFalse := false

	mockCache.EXPECT().ListKeys(gomock.Any(), gomock.Any(),
		gomock.Eq(repo.ListKeysOptions{Type: entity.TypeHost, Count: batch, TTLSet: &ttlFalse})).
		DoAndReturn(func(_ context.Context, ch chan<- string, _ repo.ListKeysOptions) error {
			close(ch)

			return nil
		})

	mockCache.EXPECT().ListKeys(gomock.Any(), gomock.Any(),
		gomock.Eq(repo.ListKeysOptions{Type: entity.TypeInfraEnv, Count: batch, TTLSet: &ttlFalse})).
		DoAndReturn(func(_ context.Context, ch chan<- string, _ repo.ListKeysOptions) error {
			close(ch)

			return listErr
		})

	mockCache.EXPECT().DeleteKeys(gomock.Any(), []string{}).Return(nil).AnyTimes()

	p := processing.NewDeleteNoTTL(logger, mockCache, config.DeleteNoTTL{BatchSize: batch})
	err := p.Run(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, listErr)
}

func TestDeleteNoTTL_Run_DeleteKeysError(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockCache := mock.NewMockCache(ctrl)
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	const batch = int64(1000)
	delErr := errors.New("del failed")
	ttlFalse := false

	mockCache.EXPECT().ListKeys(gomock.Any(), gomock.Any(),
		gomock.Eq(repo.ListKeysOptions{Type: entity.TypeHost, Count: batch, TTLSet: &ttlFalse})).
		DoAndReturn(func(_ context.Context, ch chan<- string, _ repo.ListKeysOptions) error {
			ch <- "hosts_x"
			close(ch)

			return nil
		})

	mockCache.EXPECT().DeleteKeys(gomock.Any(), gomock.Eq([]string{"hosts_x"})).Return(delErr)

	p := processing.NewDeleteNoTTL(logger, mockCache, config.DeleteNoTTL{BatchSize: batch})
	err := p.Run(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, delErr)
}
