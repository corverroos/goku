package test

import (
	"context"
	"database/sql"
	"net"
	"testing"
	"time"

	"github.com/corverroos/goku/client"
	"github.com/corverroos/goku/db"
	pb "github.com/corverroos/goku/gokupb"
	"github.com/corverroos/goku/server"
	"github.com/luno/jettison/interceptors"
	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

// SetupForTesting starts a goku grpc server and returns a connected client.
func SetupForTesting(t *testing.T) (*client.Client, *sql.DB) {
	db.CleanCache(t)
	dbc := db.ConnectForTesting(t)
	srv, addr := NewServer(t, dbc)

	t.Cleanup(srv.Stop)

	conn, err := grpc.Dial(addr, grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(interceptors.UnaryClientInterceptor),
		grpc.WithStreamInterceptor(interceptors.StreamClientInterceptor))
	jtest.RequireNil(t, err)

	gcl := pb.NewGokuClient(conn)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	cl := client.New(gcl)

	// Wait until connected to avoid startup race.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	for {
		if conn.GetState() == connectivity.Ready {
			break
		}

		if !conn.WaitForStateChange(ctx, conn.GetState()) {
			require.Fail(t, "grpc client connect timeout")
		}
	}

	return cl, dbc
}

// NewServer starts and returns a goku grpc server and its address.
func NewServer(t *testing.T, dbc *sql.DB) (*server.Server, string) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	jtest.RequireNil(t, err)

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(interceptors.UnaryServerInterceptor),
		grpc.StreamInterceptor(interceptors.StreamServerInterceptor))

	srv := server.New(dbc, dbc)

	pb.RegisterGokuServer(grpcServer, srv)

	go func() {
		err := grpcServer.Serve(l)
		jtest.RequireNil(t, err)
	}()

	return srv, l.Addr().String()
}
