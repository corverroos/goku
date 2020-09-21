package server

import (
	"context"
	"database/sql"
	"strings"

	"github.com/corverroos/goku"
	"github.com/corverroos/goku/db"
	pb "github.com/corverroos/goku/gokupb"
	"github.com/golang/protobuf/ptypes"
	"github.com/luno/reflex"
)

var _ pb.GokuServer = (*Server)(nil)

// Server implements the addresses grpc server.
type Server struct {
	rserver    *reflex.Server
	wdbc, rdbc *sql.DB
}

func New(wdbc, rdbc *sql.DB) *Server {
	if rdbc == nil {
		rdbc = wdbc
	}

	return &Server{
		wdbc:    wdbc,
		rdbc:    rdbc,
		rserver: reflex.NewServer(),
	}
}

func (srv *Server) Stop() {
	srv.rserver.Stop()
}

func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.KV, error) {
	kv, err := db.Get(ctx, s.rdbc, req.Key)
	if err != nil {
		return nil, err
	}

	return pb.ToProto(kv), nil
}

func (s *Server) List(req *pb.ListRequest, lspb pb.Goku_ListServer) error {
	fn := func(kv goku.KV) error {
		return lspb.Send(pb.ToProto(kv))
	}
	return db.List(lspb.Context(), s.rdbc, req.Prefix, fn)
}

func (s *Server) Set(ctx context.Context, req *pb.SetRequest) (*pb.Empty, error) {
	expiresAt, err := ptypes.Timestamp(req.ExpiresAt)
	if err != nil {
		return nil, err
	}

	return new(pb.Empty), db.Set(ctx, s.wdbc, db.SetReq{
		Key:         req.Key,
		Value:       req.Value,
		LeaseID:     req.LeaseId,
		ExpiresAt:   expiresAt,
		PrevVersion: req.PrevVersion,
		CreateOnly:  req.CreateOnly,
	})
}

func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.Empty, error) {
	return new(pb.Empty), db.Delete(ctx, s.wdbc, req.Key)
}

func (s *Server) UpdateLease(ctx context.Context, req *pb.UpdateLeaseRequest) (*pb.Empty, error) {
	expiresAt, err := ptypes.Timestamp(req.ExpiresAt)
	if err != nil {
		return nil, err
	}

	return new(pb.Empty), db.UpdateLease(ctx, s.wdbc, req.LeaseId, expiresAt)
}

func (s *Server) ExpireLease(ctx context.Context, req *pb.ExpireLeaseRequest) (*pb.Empty, error) {
	return new(pb.Empty), db.ExpireLease(ctx, s.wdbc, req.LeaseId)
}

func (s *Server) Stream(req *pb.StreamRequest, sspb pb.Goku_StreamServer) error {
	streamFunc := func(ctx context.Context, after string, opts ...reflex.StreamOption) (reflex.StreamClient, error) {
		cl, err := db.ToStream(s.rdbc)(ctx, after, opts...)
		if err != nil {
			return nil, err
		}

		return &prefixFilter{
			prefix: req.Prefix,
			cl:     cl,
		}, nil
	}

	return s.rserver.Stream(streamFunc, req.Req, sspb)
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
