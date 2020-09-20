package client

import (
	"context"
	"github.com/corverroos/goku"
	pb "github.com/corverroos/goku/gokupb"
	"github.com/golang/protobuf/ptypes"
	"github.com/luno/reflex"
)

var _ goku.Client = (*Client)(nil)

func New(cl pb.GokuClient) *Client{
	return &Client{clpb: cl}
}

type Client struct {
	clpb pb.GokuClient
}

func (c Client) Set(ctx context.Context, key string, value []byte, opts ...goku.SetOption) error {
	var o goku.SetOptions
	for _, opt := range opts	 {
		opt(&o)
	}

	expiresAt, err := ptypes.TimestampProto(o.ExpiresAt)
	if err != nil {
		return err
	}

	_, err =  c.clpb.Set(ctx, &pb.SetRequest{
		Key:                  key,
		Value:                value,
		ExpiresAt:            expiresAt,
		LeaseId:              o.LeaseID,
	})

	return err
}

func (c Client) Delete(ctx context.Context, key string) error {
	_, err := c.clpb.Delete(ctx, &pb.DeleteRequest{Key: key})
	return err
}

func (c Client) Get(ctx context.Context, key string) (goku.KV, error) {
	kv, err := c.clpb.Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		return goku.KV{}, err
	}

	return pb.FromProto(kv), nil
}

func (c Client) List(ctx context.Context, prefix string) ([]goku.KV, error) {
	lres, err := c.clpb.List(ctx, &pb.ListRequest{Prefix: prefix})
	if err != nil {
		return nil, err
	}

	var res []goku.KV
	for _, kvpb := range lres.Kvs {
		res = append(res, pb.FromProto(kvpb))
	}

	return res, nil
}

func (c Client) Stream() reflex.StreamFunc {
	panic("implement me")
}

