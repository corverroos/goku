package goku

import (
	"context"
	"time"

	"github.com/luno/reflex"
)

type Client interface {
	Set(ctx context.Context, key string, value []byte, opts ...SetOption) error
	Delete(ctx context.Context, key string) error
	Get(ctx context.Context, key string) (KV, error)
	List(ctx context.Context, prefix string) ([]KV, error)
	UpdateLease(ctx context.Context, leaseID int64, expiresAt time.Time) error
	ExpireLease(ctx context.Context, leaseID int64) error
	Stream(prefix string) reflex.StreamFunc
}

type KV struct {
	Key   string
	Value []byte

	Version    int64
	CreatedRef int64
	UpdatedRef int64
	DeletedRef int64
	LeaseID    int64
}

type EventType int

func (t EventType) ReflexType() int {
	return int(t)
}

const (
	EventTypeUnknown EventType = 0
	EventTypeSet     EventType = 1
	EventTypeDelete  EventType = 2
	EventTypeExpire  EventType = 2
)
