package repo

import (
	"context"
	"time"

	"github.com/openshift-assisted/assisted-events-streams/internal/migrations/domain/entity"
)

//go:generate mockgen -source=interfaces.go -package=mock -destination=./mock/mock_cache.go

type ListKeysOptions struct {
	Type   entity.Type
	Count  int64
	TTLSet *bool
}

type ListHValuesOptions struct {
	Type  entity.Type
	Count int64
}

type KeyValue struct {
	Key   string
	Value string
}

type CacheReader interface {
	ListKeys(ctx context.Context, output chan<- string, opts ListKeysOptions) error
	ListHValues(ctx context.Context, output chan<- KeyValue, opts ListHValuesOptions) error
}

type CacheWriter interface {
	DeleteKeys(ctx context.Context, keys []string) error
	AddClusters(ctx context.Context, kvs []KeyValue, expiration time.Duration) error
}

type Cache interface {
	CacheReader
	CacheWriter
}
