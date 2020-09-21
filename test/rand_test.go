package test

import (
	"context"
	"encoding/hex"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/corverroos/goku"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/jtest"
	"github.com/luno/reflex"
	"github.com/stretchr/testify/require"
)

func TestRandomSets(t *testing.T) {
	ctx := context.Background()
	rand.Seed(time.Now().UnixNano())
	cl, dbc := SetupForTesting(t)
	dbc.SetMaxOpenConns(100)

	var wg sync.WaitGroup
	uniq := make(map[string]bool)
	const n = 1000

	for i := 0; i < n; i++ {
		key := uniqKey(uniq)

		wg.Add(1)
		go func() {
			err := cl.Set(ctx, key, []byte(key))
			jtest.RequireNil(t, err)
			wg.Done()
		}()
	}

	wg.Wait()

	sc, err := cl.Stream("")(context.Background(), "", reflex.WithStreamToHead())
	jtest.RequireNil(t, err)

	for i := 0; i < n; i++ {
		e, err := sc.Recv()
		require.NoError(t, err, "event i=%d", i)
		require.Equal(t, goku.EventTypeSet.ReflexType(), e.Type.ReflexType(), "event i=%d", i)
		require.Equal(t, int64(i+1), e.IDInt(), "event i=%d", i)

		kv, err := cl.Get(ctx, e.ForeignID)
		jtest.RequireNil(t, err)
		require.Equal(t, e.ForeignID, kv.Key)
		require.Equal(t, e.ForeignID, string(kv.Value))
		require.Equal(t, e.IDInt(), kv.UpdatedRef)
		require.Equal(t, e.IDInt(), kv.CreatedRef)
		require.Equal(t, int64(0), kv.DeletedRef)
		require.Equal(t, int64(1), kv.Version)
		require.NotEmpty(t, kv.LeaseID)
	}

	_, err = sc.Recv()
	jtest.Require(t, reflex.ErrHeadReached, err)
}

func TestRandomUpdateDelete(t *testing.T) {
	ctx := context.Background()
	rand.Seed(time.Now().UnixNano())
	cl, dbc := SetupForTesting(t)
	dbc.SetMaxOpenConns(100)

	var wg sync.WaitGroup
	uniq := make(map[string]bool)
	const n = 10

	// Add some keys
	for i := 0; i < n; i++ {
		key := uniqKey(uniq)

		wg.Add(1)
		go func() {
			err := cl.Set(ctx, key, []byte(key))
			jtest.RequireNil(t, err)
			wg.Done()
		}()
	}

	wg.Wait()

	var onlyUpdated sync.Map
	var onlyDeleted sync.Map

	// Update and delete them
	for key := range uniq {
		key := key
		wg.Add(2)
		go func() {
			err := cl.Set(ctx, key, nil)
			if errors.Is(err, goku.ErrUpdateRace) {
				onlyDeleted.Store(key, true)
			} else {
				jtest.RequireNil(t, err)
			}

			wg.Done()
		}()
		go func() {
			err := cl.Delete(ctx, key)
			if errors.Is(err, goku.ErrUpdateRace) {
				onlyUpdated.Store(key, true)
			} else {
				jtest.RequireNil(t, err)
			}
			wg.Done()
		}()
	}

	wg.Wait()

	for key := range uniq {
		_, onlyU := onlyUpdated.Load(key)
		_, onlyD := onlyDeleted.Load(key)

		kv, err := cl.Get(ctx, key)
		if onlyD {
			jtest.Require(t, goku.ErrNotFound, err)
		} else if onlyU {
			jtest.RequireNil(t, err)
			require.Equal(t, int64(2), kv.Version)
			require.Empty(t, kv.Value)
		} else if errors.Is(err, goku.ErrNotFound) {
			// Deleted after update
		} else {
			// Updated after delete
			jtest.RequireNil(t, err)
			require.Equal(t, int64(3), kv.Version)
			require.Less(t, kv.DeletedRef, kv.UpdatedRef)
			require.Empty(t, kv.Value)
		}
	}
}

func TestRandomLeases(t *testing.T) {
	ctx := context.Background()
	rand.Seed(time.Now().UnixNano())
	cl, dbc := SetupForTesting(t)
	dbc.SetMaxOpenConns(100)

	var wg sync.WaitGroup
	uniq := make(map[string]bool)
	const parents = 100

	// Add parent keys
	for i := 0; i < parents; i++ {
		parent := uniqKey(uniq)
		wg.Add(1)
		go func() {
			err := cl.Set(ctx, parent, nil)
			jtest.RequireNil(t, err)
			wg.Done()
		}()
	}

	wg.Wait()

	var expirefailed sync.Map

	// Add some children with parent leases, update and expire the lease.
	for i := 0; i < parents; i++ {
		leaseID := int64(1 + i)
		child1 := uniqKey(uniq)
		child2 := uniqKey(uniq)
		wg.Add(4)

		go func() {
			err := cl.Set(ctx, child1, nil, goku.WithLeaseID(leaseID))
			if errors.Is(err, goku.ErrLeaseNotFound) {
				// Expire won, set failed.
			} else {
				jtest.RequireNil(t, err)
			}
			wg.Done()
		}()

		go func() {
			err := cl.Set(ctx, child2, nil, goku.WithLeaseID(leaseID))
			if errors.Is(err, goku.ErrLeaseNotFound) {
				// Expire won, set failed.
			} else {
				jtest.RequireNil(t, err)
			}
			wg.Done()
		}()

		go func() {
			err := cl.UpdateLease(ctx, leaseID, time.Now().Add(time.Minute))
			if errors.Is(err, goku.ErrLeaseNotFound) {
				// Expire won, update failed.
			} else {
				jtest.RequireNil(t, err)
			}
			wg.Done()
		}()

		go func() {
			err := cl.ExpireLease(ctx, leaseID)
			if errors.Is(err, goku.ErrUpdateRace) {
				expirefailed.Store(leaseID, true)
			} else {
				jtest.RequireNil(t, err, "leaseID=%v", leaseID)
			}
			wg.Done()
		}()
	}

	wg.Wait()

	// Some keys deleted,
	for key := range uniq {
		kv, err := cl.Get(ctx, key)
		if err == nil {
			require.LessOrEqual(t, kv.LeaseID, int64(parents))
			_, ok := expirefailed.Load(kv.LeaseID)
			require.True(t, ok)
		} else {
			jtest.Require(t, goku.ErrNotFound, err)
		}
	}
}

func uniqKey(uniq map[string]bool) string {
	klen := 1 + rand.Intn(255/2)
	key := genRand(klen)
	for uniq[key] {
		key = genRand(klen)
	}
	uniq[key] = true
	return key
}

func genRand(n int) string {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
