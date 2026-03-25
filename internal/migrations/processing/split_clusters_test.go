package processing_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/openshift-assisted/assisted-events-streams/internal/config"
	"github.com/openshift-assisted/assisted-events-streams/internal/migrations/domain/entity"
	"github.com/openshift-assisted/assisted-events-streams/internal/migrations/domain/repo"
	"github.com/openshift-assisted/assisted-events-streams/internal/migrations/domain/repo/mock"
	"github.com/openshift-assisted/assisted-events-streams/internal/migrations/domain/repo/valkey"
	"github.com/openshift-assisted/assisted-events-streams/internal/migrations/processing"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestSplitClusters_Run_Success(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockCache := mock.NewMockCache(ctrl)
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	const batch = int64(1000)
	exp := time.Hour

	kvs := []repo.KeyValue{
		{Key: "c1", Value: `{"id":"c1"}`},
		{Key: "c2", Value: `{"id":"c2"}`},
	}

	mockCache.EXPECT().ListHValues(gomock.Any(), gomock.Any(),
		gomock.Eq(repo.ListHValuesOptions{Type: entity.TypeCluster, Count: batch})).
		DoAndReturn(func(_ context.Context, ch chan<- repo.KeyValue, _ repo.ListHValuesOptions) error {
			// Order differs from kvs slice: HScan-like sources are unordered; AddClusters must get the same multiset.
			ch <- kvs[1]
			ch <- kvs[0]
			close(ch)

			return nil
		})

	mockCache.EXPECT().AddClusters(gomock.Any(), gomock.Any(), gomock.Eq(exp)).
		Do(func(_ context.Context, got []repo.KeyValue, _ time.Duration) {
			require.ElementsMatch(t, kvs, got)
		}).
		Return(nil)

	mockCache.EXPECT().DeleteKeys(gomock.Any(), gomock.Eq([]string{valkey.ClustersRedisHKey})).Return(nil)

	p := processing.NewSplitClusters(logger, mockCache, config.SplitClusters{
		BatchSize:  batch,
		Expiration: exp,
	})
	require.NoError(t, p.Run(ctx))
}

func TestSplitClusters_Run_SuccessBatchedAddClusters(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockCache := mock.NewMockCache(ctrl)
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	const batch = int64(2)
	exp := 30 * time.Minute

	kv1 := repo.KeyValue{Key: "a", Value: "1"}
	kv2 := repo.KeyValue{Key: "b", Value: "2"}
	kv3 := repo.KeyValue{Key: "c", Value: "3"}

	mockCache.EXPECT().ListHValues(gomock.Any(), gomock.Any(),
		gomock.Eq(repo.ListHValuesOptions{Type: entity.TypeCluster, Count: batch})).
		DoAndReturn(func(_ context.Context, ch chan<- repo.KeyValue, _ repo.ListHValuesOptions) error {
			ch <- kv2
			ch <- kv1
			ch <- kv3
			close(ch)

			return nil
		})

	gomock.InOrder(
		mockCache.EXPECT().AddClusters(gomock.Any(), gomock.Any(), gomock.Eq(exp)).
			Do(func(_ context.Context, got []repo.KeyValue, _ time.Duration) {
				require.ElementsMatch(t, []repo.KeyValue{kv1, kv2}, got)
			}).
			Return(nil),
		mockCache.EXPECT().AddClusters(gomock.Any(), gomock.Eq([]repo.KeyValue{kv3}), gomock.Eq(exp)).Return(nil),
	)

	mockCache.EXPECT().DeleteKeys(gomock.Any(), gomock.Eq([]string{valkey.ClustersRedisHKey})).Return(nil)

	p := processing.NewSplitClusters(logger, mockCache, config.SplitClusters{
		BatchSize:  batch,
		Expiration: exp,
	})
	require.NoError(t, p.Run(ctx))
}

func TestSplitClusters_Run_EmptyClusters(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockCache := mock.NewMockCache(ctrl)
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	const batch = int64(100)

	mockCache.EXPECT().ListHValues(gomock.Any(), gomock.Any(),
		gomock.Eq(repo.ListHValuesOptions{Type: entity.TypeCluster, Count: batch})).
		DoAndReturn(func(_ context.Context, ch chan<- repo.KeyValue, _ repo.ListHValuesOptions) error {
			close(ch)

			return nil
		})

	mockCache.EXPECT().AddClusters(gomock.Any(), gomock.Eq([]repo.KeyValue{}), gomock.Any()).Return(nil)

	mockCache.EXPECT().DeleteKeys(gomock.Any(), gomock.Eq([]string{valkey.ClustersRedisHKey})).Return(nil)

	p := processing.NewSplitClusters(logger, mockCache, config.SplitClusters{
		BatchSize:  batch,
		Expiration: time.Minute,
	})
	require.NoError(t, p.Run(ctx))
}

func TestSplitClusters_Run_ListHValuesError(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockCache := mock.NewMockCache(ctrl)
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	listErr := errors.New("hscan failed")

	mockCache.EXPECT().ListHValues(gomock.Any(), gomock.Any(),
		gomock.Eq(repo.ListHValuesOptions{Type: entity.TypeCluster, Count: 10})).
		DoAndReturn(func(_ context.Context, ch chan<- repo.KeyValue, _ repo.ListHValuesOptions) error {
			close(ch)

			return listErr
		})

	mockCache.EXPECT().AddClusters(gomock.Any(), gomock.Eq([]repo.KeyValue{}), gomock.Any()).Return(nil)

	p := processing.NewSplitClusters(logger, mockCache, config.SplitClusters{BatchSize: 10, Expiration: time.Hour})
	err := p.Run(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, listErr)
}

func TestSplitClusters_Run_AddClustersError(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockCache := mock.NewMockCache(ctrl)
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	const batch = int64(1000)
	addErr := errors.New("pipeline failed")
	kv := repo.KeyValue{Key: "x", Value: "y"}

	mockCache.EXPECT().ListHValues(gomock.Any(), gomock.Any(),
		gomock.Eq(repo.ListHValuesOptions{Type: entity.TypeCluster, Count: batch})).
		DoAndReturn(func(_ context.Context, ch chan<- repo.KeyValue, _ repo.ListHValuesOptions) error {
			ch <- kv
			close(ch)

			return nil
		})

	mockCache.EXPECT().AddClusters(gomock.Any(), gomock.Eq([]repo.KeyValue{kv}), gomock.Any()).Return(addErr)

	p := processing.NewSplitClusters(logger, mockCache, config.SplitClusters{
		BatchSize:  batch,
		Expiration: time.Hour,
	})
	err := p.Run(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, addErr)
}

func TestSplitClusters_Run_DeleteClustersKeyError(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockCache := mock.NewMockCache(ctrl)
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	const batch = int64(1000)
	delErr := errors.New("cannot delete legacy key")

	mockCache.EXPECT().ListHValues(gomock.Any(), gomock.Any(),
		gomock.Eq(repo.ListHValuesOptions{Type: entity.TypeCluster, Count: batch})).
		DoAndReturn(func(_ context.Context, ch chan<- repo.KeyValue, _ repo.ListHValuesOptions) error {
			close(ch)

			return nil
		})

	mockCache.EXPECT().AddClusters(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	mockCache.EXPECT().DeleteKeys(gomock.Any(), gomock.Eq([]string{valkey.ClustersRedisHKey})).Return(delErr)

	p := processing.NewSplitClusters(logger, mockCache, config.SplitClusters{
		BatchSize:  batch,
		Expiration: time.Hour,
	})
	err := p.Run(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, delErr)
}
