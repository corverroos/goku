package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
)

func TestExpireLeasesOnce(t *testing.T) {
	ctx := context.Background()
	dbc := ConnectForTesting(t)

	var expiresAt []time.Time
	for i := 0; i < 5; i++ {
		e := time.Now().Add(time.Second * time.Duration(i)).Truncate(time.Millisecond)
		err := Set(ctx, dbc, SetReq{
			Key:       fmt.Sprintf("key%d", i),
			ExpiresAt: e,
		})
		jtest.RequireNil(t, err)
		expiresAt = append(expiresAt, e.UTC())
	}

	var sleeps []time.Time
	sleepTo := func(t time.Time) {
		sleeps = append(sleeps, t.UTC())
	}

	err := expireLeasesOnce(ctx, dbc, expiresAt[len(expiresAt)-1], sleepTo)
	jtest.RequireNil(t, err)
	require.EqualValues(t, expiresAt, sleeps)

	sleeps = nil

	err = expireLeasesOnce(ctx, dbc, expiresAt[len(expiresAt)-1], sleepTo)
	jtest.RequireNil(t, err)
	require.Nil(t, sleeps)
}
