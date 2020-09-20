package test

import (
	"context"
	"fmt"
	"github.com/corverroos/goku"
	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestSetup(t *testing.T) {
	SetupForTesting(t)
}

func TestEmptyNotFound(t *testing.T) {
	ctx := context.Background()
	cl := SetupForTesting(t)

	_, err := cl.Get(ctx, "")
	jtest.Require(t, goku.ErrNotFound, err)
}

func TestInvalidKey(t *testing.T) {
	ctx := context.Background()
	cl := SetupForTesting(t)

	err := cl.Set(ctx, "", nil)
	jtest.Require(t, goku.ErrInvalidKey, err)

	err = cl.Set(ctx, strings.Repeat("s", 256), nil)
	jtest.Require(t, goku.ErrInvalidKey, err)
}

func TestGet(t *testing.T) {
	ctx := context.Background()
	cl := SetupForTesting(t)

	const (
		key1 = "key1"
		key2 = "key2"
	)

	_, err := cl.Get(ctx, key1)
	jtest.Require(t, goku.ErrNotFound, err)

	err = cl.Set(ctx, key1, []byte(key1))
	jtest.RequireNil(t, err)

	err = cl.Set(ctx, key2, nil)
	jtest.RequireNil(t, err)

	kv, err := cl.Get(ctx, key1)
	jtest.RequireNil(t, err)

	require.Equal(t, int64(1),kv.Version)
	require.Equal(t, key1,kv.Key)
	require.Equal(t, []byte(key1),kv.Value)

kv, err = cl.Get(ctx, key2)
	jtest.RequireNil(t, err)

	require.Equal(t, int64(1),kv.Version)
	require.Equal(t, key2,kv.Key)
	require.Len(t, kv.Value, 0)
}

func TestList(t *testing.T) {
	ctx := context.Background()
	cl := SetupForTesting(t)

	const n = 20
	for i := 0; i < n; i++ {
		err := cl.Set(ctx, fmt.Sprintf("%d",i), nil)
		jtest.RequireNil(t, err)
	}

	kvs, err := cl.List(ctx, "")
	jtest.RequireNil(t, err)
	require.Len(t, kvs, n)

	kvs, err = cl.List(ctx, "1")
	jtest.RequireNil(t, err)
	require.Len(t, kvs, 11)

	require.Equal(t, "1", kvs[0].Key)
	require.Equal(t, "10", kvs[1].Key)
	require.Equal(t, "19", kvs[10].Key)
}

func TestUpdate(t *testing.T) {
	ctx := context.Background()
	cl := SetupForTesting(t)

	const key = "key"

	assert := func(t *testing.T, version int64, val string) {
		t.Helper()

		kv, err := cl.Get(ctx, key)
		jtest.RequireNil(t, err)

		require.Equal(t, version, kv.Version)
		require.Equal(t, key, kv.Key)
		require.Equal(t, val, string(kv.Value))
	}

	err := cl.Set(ctx, key, nil)
	require.NoError(t, err)
	assert(t, 1, "")

	err = cl.Set(ctx, key, []byte("1"))
	require.NoError(t, err)
	assert(t, 2, "1")

	err = cl.Set(ctx, key, []byte("aba"))
	require.NoError(t, err)
	assert(t, 3, "aba")

	err = cl.Set(ctx, key, nil)
	require.NoError(t, err)
	assert(t, 4, "")
}

func TestUpdateDelete(t *testing.T) {
	ctx := context.Background()
	cl := SetupForTesting(t)

	const key = "key"

	assert := func(t *testing.T, version int64, val string) {
		t.Helper()

		kv, err := cl.Get(ctx, key)
		jtest.RequireNil(t, err)

		require.Equal(t, version, kv.Version)
		require.Equal(t, key, kv.Key)
		require.Equal(t, val, string(kv.Value))
	}

	err := cl.Set(ctx, key, nil)
	require.NoError(t, err)
	assert(t, 1, "")

	err = cl.Set(ctx, key, []byte("1"))
	require.NoError(t, err)
	assert(t, 2, "1")

	err = cl.Delete(ctx, key)
	require.NoError(t, err)
	_, err = cl.Get(ctx, key)
	jtest.Require(t, goku.ErrNotFound, err)

	err = cl.Set(ctx, key, []byte("new"))
	require.NoError(t, err)
	assert(t, 4, "new")
	kv, err := cl.Get(ctx, key)
	jtest.RequireNil(t, err)
	require.Equal(t, int64(0), kv.DeletedRef)
	require.Equal(t, int64(1), kv.CreatedRef) // Created ref still 1 and not 4...?
	require.Equal(t, int64(2), kv.LeaseID) // New LeaseID
}

func TestSetWithLease(t *testing.T) {
	ctx := context.Background()
	cl := SetupForTesting(t)

	const (

		key1 = "key1"
		key2 = "key2"
		key3 = "key3"
	)

	assert := func(t *testing.T, key string, version int64, val string) {
		t.Helper()

		kv, err := cl.Get(ctx, key)
		jtest.RequireNil(t, err)

		require.Equal(t, version, kv.Version)
		require.Equal(t, key, kv.Key)
		require.Equal(t, val, string(kv.Value))
	}

	err := cl.Set(ctx, key1, nil)
	require.NoError(t, err)
	assert(t, key1, 1, "")

	err = cl.Set(ctx, key2, nil)
	require.NoError(t, err)
	assert(t, key2, 1, "")

	kv1, err := cl.Get(ctx, key1)
	jtest.RequireNil(t, err)

	err = cl.Set(ctx, key2, nil, goku.WithLeaseID(kv1.LeaseID))
	require.NoError(t, err)
	assert(t, key2, 2, "")

	kv2, err := cl.Get(ctx, key2)
	jtest.RequireNil(t, err)
	require.Equal(t, kv1.LeaseID, kv2.LeaseID)

	err = cl.Set(ctx, key3, nil, goku.WithLeaseID(kv1.LeaseID))
	require.NoError(t, err)
	assert(t, key3, 1, "")

	kv3, err := cl.Get(ctx, key3)
	jtest.RequireNil(t, err)
	require.Equal(t, kv1.LeaseID, kv3.LeaseID)

	err = cl.Delete(ctx, key3)
	jtest.RequireNil(t, err)

	_, err = cl.Get(ctx, key1)
	jtest.Require(t, goku.ErrNotFound, err)
	_, err = cl.Get(ctx, key2)
	jtest.Require(t, goku.ErrNotFound, err)
	_, err = cl.Get(ctx, key3)
	jtest.Require(t, goku.ErrNotFound, err)
}

