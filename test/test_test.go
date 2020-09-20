package test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/corverroos/goku"
	"github.com/corverroos/goku/db"
	"github.com/luno/jettison/jtest"
	"github.com/luno/reflex"
	"github.com/stretchr/testify/require"
)

func TestSetup(t *testing.T) {
	SetupForTesting(t)
}

func TestEmptyNotFound(t *testing.T) {
	ctx := context.Background()
	cl, _ := SetupForTesting(t)

	_, err := cl.Get(ctx, "")
	jtest.Require(t, goku.ErrNotFound, err)

	assertEvents(t, cl, "")
}

func TestInvalidKey(t *testing.T) {
	ctx := context.Background()
	cl, _ := SetupForTesting(t)

	err := cl.Set(ctx, "", nil)
	jtest.Require(t, goku.ErrInvalidKey, err)

	err = cl.Set(ctx, strings.Repeat("s", 256), nil)
	jtest.Require(t, goku.ErrInvalidKey, err)

	assertEvents(t, cl, "")
}

func TestGet(t *testing.T) {
	ctx := context.Background()
	cl, _ := SetupForTesting(t)

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

	require.Equal(t, int64(1), kv.Version)
	require.Equal(t, key1, kv.Key)
	require.Equal(t, []byte(key1), kv.Value)

	kv, err = cl.Get(ctx, key2)
	jtest.RequireNil(t, err)

	require.Equal(t, int64(1), kv.Version)
	require.Equal(t, key2, kv.Key)
	require.Len(t, kv.Value, 0)

	assertEvents(t, cl, "", goku.EventTypeSet, goku.EventTypeSet)
	assertEvents(t, cl, "key", goku.EventTypeSet, goku.EventTypeSet)
	assertEvents(t, cl, "key1", goku.EventTypeSet)
}

func TestList(t *testing.T) {
	ctx := context.Background()
	cl, _ := SetupForTesting(t)

	const n = 20
	for i := 0; i < n; i++ {
		err := cl.Set(ctx, fmt.Sprintf("%d", i), nil)
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

	var expected []reflex.EventType
	for i := 0; i < 11; i++ {
		expected = append(expected, goku.EventTypeSet)
	}
	assertEvents(t, cl, "1", expected...)
	assertEvents(t, cl, "10", goku.EventTypeSet)
}

func TestUpdate(t *testing.T) {
	ctx := context.Background()
	cl, _ := SetupForTesting(t)

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
	cl, _ := SetupForTesting(t)

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
	require.Equal(t, int64(2), kv.LeaseID)    // New LeaseID

	assertEvents(t, cl, "", goku.EventTypeSet, goku.EventTypeSet, goku.EventTypeDelete, goku.EventTypeSet)
}

func TestSetWithLease(t *testing.T) {
	ctx := context.Background()
	cl, _ := SetupForTesting(t)

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
	jtest.RequireNil(t, err)
	_, err = cl.Get(ctx, key2)
	jtest.RequireNil(t, err)
	_, err = cl.Get(ctx, key3)
	jtest.Require(t, goku.ErrNotFound, err)

	assertEvents(t, cl, "", goku.EventTypeSet, goku.EventTypeSet,
		goku.EventTypeSet, goku.EventTypeSet, goku.EventTypeDelete)

	assertEvents(t, cl, key3, goku.EventTypeSet, goku.EventTypeDelete)
}

func TestCreateOnly(t *testing.T) {
	ctx := context.Background()
	cl, _ := SetupForTesting(t)

	const key1 = "key1"

	err := cl.Set(ctx, key1, nil, goku.WithCreateOnly())
	require.NoError(t, err)

	err = cl.Set(ctx, key1, nil, goku.WithCreateOnly())
	jtest.Require(t, goku.ErrConditional, err)
}

func TestPrevVersion(t *testing.T) {
	ctx := context.Background()
	cl, _ := SetupForTesting(t)

	const key1 = "key1"

	err := cl.Set(ctx, key1, nil)
	require.NoError(t, err)

	err = cl.Set(ctx, key1, nil, goku.WithPrevVersion(1))
	require.NoError(t, err)

	err = cl.Set(ctx, key1, nil, goku.WithPrevVersion(1))
	jtest.Require(t, goku.ErrConditional, err)
}

func TestWithExpiresAt(t *testing.T) {
	ctx := context.Background()
	cl, dbc := SetupForTesting(t)

	const key = "key"

	t0 := time.Now().Round(time.Millisecond) // Round to avoid discrepancies wrt insert and query

	err := cl.Set(ctx, key, nil)
	jtest.RequireNil(t, err)

	ll, err := db.ListLeasesToExpire(ctx, dbc, t0)
	jtest.RequireNil(t, err)
	require.Empty(t, ll)

	err = cl.Set(ctx, key, nil, goku.WithExpiresAt(t0))
	jtest.RequireNil(t, err)

	ll, err = db.ListLeasesToExpire(ctx, dbc, t0)
	jtest.RequireNil(t, err)
	require.Len(t, ll, 1)

	err = cl.Set(ctx, key, nil, goku.WithExpiresAt(t0.Add(time.Minute)))
	jtest.RequireNil(t, err)

	ll, err = db.ListLeasesToExpire(ctx, dbc, t0)
	jtest.RequireNil(t, err)
	require.Len(t, ll, 0)
}

func TestUpdateLease(t *testing.T) {
	ctx := context.Background()
	cl, dbc := SetupForTesting(t)

	const key = "key"

	t0 := time.Now().Round(time.Millisecond) // Round to avoid discrepancies wrt insert and query

	err := cl.Set(ctx, key, nil)
	jtest.RequireNil(t, err)

	err = cl.UpdateLease(ctx, 1, t0)
	jtest.RequireNil(t, err)

	ll, err := db.ListLeasesToExpire(ctx, dbc, t0)
	jtest.RequireNil(t, err)
	require.Len(t, ll, 1)

	err = cl.Set(ctx, key, nil, goku.WithExpiresAt(t0.Add(time.Minute)))
	jtest.RequireNil(t, err)

	ll, err = db.ListLeasesToExpire(ctx, dbc, t0)
	jtest.RequireNil(t, err)
	require.Len(t, ll, 0)
}

func TestExpireLease(t *testing.T) {
	ctx := context.Background()
	cl, _ := SetupForTesting(t)

	const key1 = "key1"
	const key2 = "key2"

	err := cl.Set(ctx, key1, nil)
	jtest.RequireNil(t, err)

	err = cl.Set(ctx, key2, nil, goku.WithLeaseID(1))
	jtest.RequireNil(t, err)

	err = cl.ExpireLease(ctx, 1)
	jtest.RequireNil(t, err)

	_, err = cl.Get(ctx, key1)
	jtest.Require(t, goku.ErrNotFound, err)
	err = cl.Delete(ctx, key2)
	jtest.Require(t, goku.ErrNotFound, err)
	err = cl.UpdateLease(ctx, 1, time.Now())
	jtest.Require(t, goku.ErrLeaseNotFound, err)

	assertEvents(t, cl, "", goku.EventTypeSet, goku.EventTypeSet,
		goku.EventTypeExpire, goku.EventTypeExpire)

	assertEvents(t, cl, key1, goku.EventTypeSet, goku.EventTypeExpire)
}

func assertEvents(t *testing.T, cl goku.Client, prefix string, types ...reflex.EventType) {
	t.Helper()

	sc, err := cl.Stream(prefix)(context.Background(), "", reflex.WithStreamToHead())
	jtest.RequireNil(t, err)

	for i, typ := range types {
		e, err := sc.Recv()
		require.NoError(t, err, "event i=%d", i)
		require.Equal(t, typ.ReflexType(), e.Type.ReflexType(), "event i=%d", i)
	}

	_, err = sc.Recv()
	jtest.Require(t, reflex.ErrHeadReached, err)
}
