package logical

import (
	"context"
	"database/sql"
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

	return  db.Set(ctx, c.wdbc, db.SetReq{
		Key:                  key,
		Value:                value,
		ExpiresAt:            o.ExpiresAt,
		LeaseID:              o.LeaseID,
	})
}

func (c *Client) Delete(ctx context.Context, key string) error {
	return db.Delete(ctx, c.wdbc, key)
}

func (c *Client) Get(ctx context.Context, key string) (goku.KV, error) {
	return db.Get(ctx, c.rdbc, key)
}

func (c *Client) List(ctx context.Context, prefix string) ([]goku.KV, error) {
	return db.List(ctx, c.rdbc, prefix)
}

func (c *Client) Stream() reflex.StreamFunc {
	return db.ToStream(c.rdbc)
}
