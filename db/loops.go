package db

import (
	"context"
	"database/sql"
	"sort"
	"time"

	"github.com/corverroos/goku"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/log"
)

// ExpireLeasesForever continuously calls expireLeasesOnce with a polling period of 10secs.
//
// Note that this algorithm is aimed at responsive expiry with low polling frequency. It doesn't
// however provide this for lease updates with expiry shorter than polling period.
// See expireLeasesOnce for more details.
func ExpireLeasesForever(dbc *sql.DB) {
	const period = time.Second * 10

	sleepTo := func(t time.Time) {
		time.Sleep(t.Sub(time.Now()))
	}

	for {
		ctx := context.Background()
		cutoff := time.Now().Add(period)

		err := expireLeasesOnce(ctx, dbc, cutoff, sleepTo)
		if errors.IsAny(err, goku.ErrUpdateRace) {
			// ReturnNoErr: Just try again now.
		} else if err != nil {
			// ReturnNoErr: Log and backoff.
			log.Error(ctx, errors.Wrap(err, "expire leases"))
			time.Sleep(time.Second * 10)
		} else {
			sleepTo(cutoff)
		}
	}
}

// expireLeasesOnce expires all leases with expires_at before the cutoff. Cutoff (and therefore expires_at)
// may be in the future in which case it will block until that time.
func expireLeasesOnce(ctx context.Context, dbc *sql.DB, cutoff time.Time, sleepTo func(time.Time)) error {
	ll, err := ListLeasesToExpire(ctx, dbc, cutoff)
	if err != nil {
		return err
	}

	sort.Slice(ll, func(i, j int) bool {
		return ll[i].ExpiresAt.Before(ll[j].ExpiresAt)
	})

	for _, l := range ll {
		sleepTo(l.ExpiresAt)

		err := ExpireLease(ctx, dbc, l.ID)
		if errors.Is(err, goku.ErrLeaseNotFound) {
			// Just continue
		} else if err != nil {
			return err
		}
	}

	return nil
}
