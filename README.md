# Goku

`goku` is a simple golang key-value store built on mysql. It implements the mutable data and immutable events pattern.

It can be run a HA service with multiple replicas serving a gRPC API. It can also be embedded in a go binary.

## Why mysql?

Mysql is a well-known stable performant data store which works very well for many use cases. 
Many systems already rely on mysql as a core component. 

There are many other key-value store implementations and technologies available on the market. 
But sometimes, using what you already have is "good enough" and avoids the costs of introducing
and maintaining additional technologies in your stack. 
 
Goku inherits the following features/limitations from mysql:
 - CP ito CAP theorem; ie. the mysql server is a single point of failure. 
 - Consistent reads if write succeed.
 - Scalable to hundreds of millions of rows. Depending on value sizes.

## Concepts

- `Key`: A key is any string (0 < len < 255). Applications can define their own key structures; a folder type structure with nesting via "/" is a common pattern. 

- `Value`: A value is any byte slice (0 <= len < 4GB).

- `Lease`: A lease is associated with one or more key-values which are deleted when the lease expires. Expiry is optional and can be configured via an "expires_at" deadline or by an explicit call to the "ExpireLease" API.

- `Events`: Each update to a key-value (`set`, `delete`, `expire`) is associated with a reflex notification event. Events can be streamed by prefix to react to changes.

- `Ref`: `CreatedRef`, `UpdatedRef`, `DeletedRef` fields keep track of global events associated with a key-value.

- `Version`: The version field is incremented on each update. It can be used to implement conditional updates.

## API

```go
package goku

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
	Key   string

	// Value of the key-value. Can be empty. Max size of 4MB (grpc message limit).
	Value []byte

	// Version is incremented each time the key-value is updated.
	Version    int64

	// CreatedRef is the id of the event that created the key-value.
	CreatedRef int64

	// UpdatedRef is the id of the event that last updated the key-value.
	UpdatedRef int64

	// DeletedRef is the id of the event that deleted the key-value. If zero, the key-value is not deleted.
	DeletedRef int64

	// LeaseID is id of the lease associated with the key-value. Leases can be used to
	// delete key-values; either automatically via "expires_at" or via ExpireLease API.
	LeaseID    int64
}
```


