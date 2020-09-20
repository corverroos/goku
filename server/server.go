package server

import (
	"context"
	"database/sql"
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

func (s *Server) List(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	kvl, err := db.List(ctx, s.rdbc, req.Prefix)
	if err != nil {
		return nil, err
	}

	var res []*pb.KV
	for _, kv := range kvl {
		res = append(res, pb.ToProto(kv))
	}

	return &pb.ListResponse{
		Kvs: res,
	}, nil
}

func (s *Server) Set(ctx context.Context, req *pb.SetRequest) (*pb.Empty, error) {
	expiresAt, err := ptypes.Timestamp(req.ExpiresAt)
	if err != nil {
		return nil, err
	}

	return new(pb.Empty), db.Set(ctx, s.wdbc, db.SetReq{
		Key:       req.Key,
		Value:     req.Value,
		LeaseID:   req.LeaseId,
		ExpiresAt: expiresAt,
	})
}

func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.Empty, error) {
	return new(pb.Empty), db.Delete(ctx, s.wdbc, req.Key)
}
