package db

import (
	"context"
	"database/sql"
	"time"

	"github.com/corverroos/goku"
	"github.com/luno/jettison/errors"
)

func UpdateLease(ctx context.Context, dbc *sql.DB, leaseID int64, expiresAt time.Time) error {
	tx, err := dbc.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	err = updateLeaseTx(ctx, tx, leaseID, expiresAt)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func ExpireLease(ctx context.Context, dbc *sql.DB, leaseID int64) error {
	tx, err := dbc.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var leaseVersion int64
	err = tx.QueryRowContext(ctx, "select version from leases where id=? and expired=false", leaseID).Scan(&leaseVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return errors.Wrap(goku.ErrLeaseNotFound, "")
	} else if err != nil {
		return errors.Wrap(err, "select lease version")
	}

	kvl, err := listWhere(ctx, tx, "lease_id=? and deleted_ref is null", leaseID)
	if err != nil {
		return err
	}

	err = execOne(ctx, tx, "update leases "+
		"set expires_at=null, version=?+1, expired=true where id=? and version=?", leaseVersion, leaseID, leaseVersion)
	if err != nil {
		return errors.Wrap(err, "expire lease")
	}

	for _, kv := range kvl {
		ref, err := insertEvent(ctx, tx, kv.Key, goku.EventTypeExpire, nil)
		if err != nil {
			return err
		}

		err = execOne(ctx, tx, "update data "+
			"set value=null, version=?+1, deleted_ref=?, updated_ref=? where `key`=? and version=?",
			kv.Version, ref, ref, kv.Key, kv.Version)
		if err != nil {
			return errors.Wrap(err, "expire data")
		}
	}

	defer notifier.Notify()

	return tx.Commit()
}

func updateLeaseTx(ctx context.Context, tx *sql.Tx, leaseID int64, expiresAt time.Time) error {
	res, err := tx.ExecContext(ctx, "update leases "+
		"set version=version+1, expires_at=? where id=? and expired=false", toNullTime(expiresAt), leaseID)
	if err != nil {
		return errors.Wrap(err, "update lease tx")
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	} else if n == 0 {
		return errors.Wrap(goku.ErrLeaseNotFound, "")
	}

	return nil
}

func ListLeasesToExpire(ctx context.Context, dbc *sql.DB, cutoff time.Time) ([]Lease, error) {
	return listLeasesWhere(ctx, dbc, "expires_at <= ?", cutoff)
}

type Lease struct {
	ID        int64
	ExpiresAt time.Time
	Version   int64
	Expired   bool
}

func listLeasesWhere(ctx context.Context, dbc *sql.DB, where string, args ...interface{}) ([]Lease, error) {
	rows, err := dbc.QueryContext(ctx, "select id, version, expires_at, expired "+
		"from leases where "+where, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []Lease
	for rows.Next() {
		var (
			l   Lease
			exp sql.NullTime
		)

		err := rows.Scan(&l.ID, &l.Version, &exp, &l.Expired)
		if err != nil {
			return nil, err
		}
		l.ExpiresAt = exp.Time
		res = append(res, l)
	}

	return res, rows.Err()
}
