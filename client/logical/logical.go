package logical

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/corverroos/goku"
	"github.com/corverroos/goku/db"
	"github.com/luno/reflex"
)

var _ goku.Client = (*Client)(nil)

func New(wdbc, rdbc *sql.DB) *Client {
	if rdbc == nil {
		rdbc = wdbc
	}

	return &Client{
		wdbc: wdbc,
		rdbc: rdbc,
	}
}

type Client struct {
	wdbc, rdbc *sql.DB
}

func (c *Client) Set(ctx context.Context, key string, value []byte, opts ...goku.SetOption) error {
	var o goku.SetOptions
	for _, opt := range opts {
		opt(&o)
	}

	return db.Set(ctx, c.wdbc, db.SetReq{
		Key:         key,
		Value:       value,
		ExpiresAt:   o.ExpiresAt,
		LeaseID:     o.LeaseID,
		PrevVersion: o.PrevVersion,
		CreateOnly:  o.CreateOnly,
	})
}

func (c *Client) Delete(ctx context.Context, key string) error {
	return db.Delete(ctx, c.wdbc, key)
}

func (c *Client) Get(ctx context.Context, key string) (goku.KV, error) {
	return db.Get(ctx, c.rdbc, key)
}

func (c *Client) List(ctx context.Context, prefix string) ([]goku.KV, error) {
	var res []goku.KV
	fn := func(kv goku.KV) error {
		res = append(res, kv)
		return nil
	}

	err := db.List(ctx, c.rdbc, prefix, fn)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Client) UpdateLease(ctx context.Context, leaseID int64, expiresAt time.Time) error {
	return db.UpdateLease(ctx, c.wdbc, leaseID, expiresAt)
}

func (c *Client) ExpireLease(ctx context.Context, leaseID int64) error {
	return db.ExpireLease(ctx, c.wdbc, leaseID)
}

func (c *Client) Stream(prefix string) reflex.StreamFunc {
	return func(ctx context.Context, after string, opts ...reflex.StreamOption) (reflex.StreamClient, error) {
		cl, err := db.ToStream(c.rdbc)(ctx, after, opts...)
		if err != nil {
			return nil, err
		}

		return &prefixFilter{
			prefix: prefix,
			cl:     cl,
		}, nil
	}
}

type prefixFilter struct {
	prefix string
	cl     reflex.StreamClient
}

func (f *prefixFilter) Recv() (*reflex.Event, error) {
	for {
		e, err := f.cl.Recv()
		if err != nil {
			return nil, err
		} else if strings.HasPrefix(e.ForeignID, f.prefix) {
			return e, nil
		}
	}
}
