package goku

import (
	"context"
	"time"

	"github.com/luno/reflex"
)

// Client provides the main goku API.
type Client interface {
	// Set creates or updates a key-value with options.
	Set(ctx context.Context, key string, value []byte, opts ...SetOption) error

	// Delete soft-deletes the key-value for the given key. It will not be returned in Get or List.
	Delete(ctx context.Context, key string) error

	// Get returns the key-value struct for the given key.
	Get(ctx context.Context, key string) (KV, error)

	// List returns all key-values with keys matching the prefix.
	List(ctx context.Context, prefix string) ([]KV, error)

	// UpdateLease updates the expires_at field of the given lease. A zero expires at
	// implies no expiry.
	UpdateLease(ctx context.Context, leaseID int64, expiresAt time.Time) error

	// ExpireLease expires the given lease and deletes all key-values associated with it.
	ExpireLease(ctx context.Context, leaseID int64) error

	// Stream returns a reflex stream function filtering events for keys matching the prefix.
	Stream(prefix string) reflex.StreamFunc
}

type KV struct {
	// Key of the key-value. Length should be greater than 0 and less than 256.
	Key string

	// Value of the key-value. Can be empty. Max size of 4MB (grpc message limit).
	Value []byte

	// Version is incremented each time the key-value is updated.
	Version int64

	// CreatedRef is the id of the event that created the key-value.
	CreatedRef int64

	// UpdatedRef is the id of the event that last updated the key-value.
	UpdatedRef int64

	// DeletedRef is the id of the event that deleted the key-value. If zero, the key-value is not deleted.
	DeletedRef int64

	// LeaseID is id of the lease associated with the key-value. Leases can be used to
	// delete key-values; either automatically via "expires_at" or via ExpireLease API.
	LeaseID int64
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
