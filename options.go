package goku

import "time"

type SetOption func(*SetOptions)

type SetOptions struct {
	ExpiresAt time.Time
	LeaseID   int64
}

func WithExpiresAt(t time.Time) SetOption {
	return func(o *SetOptions) {
		o.ExpiresAt = t
	}
}

func WithLeaseID(id int64) SetOption {
	return func(o *SetOptions) {
		o.LeaseID = id
	}
}
