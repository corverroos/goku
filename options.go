package goku

import "time"

type SetOption func(*SetOptions)

type SetOptions struct {
	ExpiresAt   time.Time
	LeaseID     int64
	PrevVersion int64
	CreateOnly  bool
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

func WithPrevVersion(prevVersion int64) SetOption {
	return func(o *SetOptions) {
		o.PrevVersion = prevVersion
	}
}

func WithCreateOnly() SetOption {
	return func(o *SetOptions) {
		o.CreateOnly = true
	}
}
