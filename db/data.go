package db

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/corverroos/goku"
	"github.com/go-sql-driver/mysql"
	"github.com/luno/jettison/errors"
	"github.com/luno/reflex"
)

func Get(ctx context.Context, dbc dbc, key string) (goku.KV, error) {
	return lookupWhere(ctx, dbc, "`key`=? and deleted_ref is null", key)
}

func List(ctx context.Context, dbc dbc, prefix string, fn func(goku.KV) error) error {
	return scanWhere(ctx, dbc, fn, "`key` like ? and deleted_ref is null", prefix+"%")
}

type SetReq struct {
	Key   string
	Value []byte

	// Options
	LeaseID     int64     // Zero creates a new lease on create or updates existing on update.
	ExpiresAt   time.Time // Zero is infinite
	PrevVersion int64     // Zero ignores check
	CreateOnly  bool      // Zero ignores check
}

func Set(ctx context.Context, dbc *sql.DB, req SetReq) error {
	if len(req.Key) == 0 || len(req.Key) >= 256 || strings.Contains(req.Key, "%") {
		return goku.ErrInvalidKey
	}

	tx, err := dbc.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Step 0: Lookup existing row.
	var (
		leaseID   int64
		createRef int64
	)
	kv, err := lookupWhere(ctx, tx, "`key`=?", req.Key)
	if errors.Is(err, goku.ErrNotFound) {
		// No existing key
	} else if err != nil {
		return err
	} else if kv.DeletedRef != 0 {
		// Create a new lease and createRef if deleted
	} else {
		leaseID = kv.LeaseID
		createRef = kv.CreatedRef
	}

	if req.CreateOnly && kv.Version > 0 {
		return errors.Wrap(goku.ErrConditional, "key already created")
	} else if req.PrevVersion > 0 && kv.Version != req.PrevVersion {
		return errors.Wrap(goku.ErrConditional, "previous version mismatch")
	}

	// Maybe override with requested lease.
	if req.LeaseID != 0 {
		leaseID = req.LeaseID
	}

	// Step1: Insert event
	ref, err := insertEvent(ctx, tx, req.Key, goku.EventTypeSet, req.Value)
	if err != nil {
		return err
	}

	// Step2: Insert or update the lease.
	if leaseID == 0 {
		res, err := tx.ExecContext(ctx, "insert into leases "+
			"set version=1, expires_at=?", toNullTime(req.ExpiresAt))
		if err != nil {
			return err
		}
		leaseID, err = res.LastInsertId()
		if err != nil {
			return err
		}
	} else {
		err := updateLeaseTx(ctx, tx, leaseID, req.ExpiresAt)
		if err != nil {
			return err
		}
	}

	// Use event ref if we need to set a new created ref
	if createRef == 0 {
		createRef = ref
	}

	// Step 3: Update or insert data
	if kv.Version != 0 {
		err := execOne(ctx, tx, "update data "+
			"set value=?, version=?+1, created_ref=?, updated_ref=?, deleted_ref=null, lease_id=? "+
			"where `key`=? and version=?",
			req.Value, kv.Version, createRef, ref, leaseID, req.Key, kv.Version)
		if err != nil {
			return err
		}
	} else {
		_, err := tx.ExecContext(ctx, "insert into data "+
			"set `key`=?, value=?, version=1, created_ref=?, updated_ref=?, lease_id=?",
			req.Key, req.Value, createRef, ref, leaseID)
		if isDuplicateKeyErr(err) {
			return goku.ErrUpdateRace
		} else if err != nil {
			return err
		}
	}

	defer notifier.Notify()

	return tx.Commit()
}

func Delete(ctx context.Context, dbc *sql.DB, key string) error {
	tx, err := dbc.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	kv, err := lookupWhere(ctx, tx, "`key`=?", key)
	if err != nil {
		return err
	}

	if kv.DeletedRef != 0 {
		return errors.Wrap(goku.ErrNotFound, "")
	}

	ref, err := insertEvent(ctx, tx, key, goku.EventTypeDelete, nil)
	if err != nil {
		return err
	}

	err = execOne(ctx, tx, "update data "+
		"set value=null, version=?+1, updated_ref=?, deleted_ref=? "+
		"where `key`=? and version=?",
		kv.Version, ref, ref, key, kv.Version)
	if err != nil {
		return err
	}

	defer notifier.Notify()

	return tx.Commit()
}

func insertEvent(ctx context.Context, tx *sql.Tx, key string, typ reflex.EventType, metadata []byte) (int64, error) {
	res, err := tx.ExecContext(ctx, "insert into events "+
		"set `key`=?, `type`=?, timestamp=now(), metadata=?", key, typ, metadata)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}

	return id, nil
}

func execOne(ctx context.Context, dbc dbc, q string, args ...interface{}) error {
	res, err := dbc.ExecContext(ctx, q, args...)
	if err != nil {
		return err
	}

	n, err := res.RowsAffected()
	if err != nil {
		return err
	} else if n != 1 {
		return errors.Wrap(goku.ErrUpdateRace, "")
	}

	return nil
}

const errDupEntry = 1062

// IsDuplicateErrForKey returns true if the provided error is a mysql ER_DUP_ENTRY
// error that conflicts with the specified unique index or primary key.
func isDuplicateKeyErr(err error) bool {
	if err == nil {
		return false
	}

	me := new(mysql.MySQLError)
	if !errors.As(err, &me) {
		return false
	}

	return me.Number == errDupEntry
}

func toNullTime(t time.Time) sql.NullTime {
	return sql.NullTime{
		Time:  t,
		Valid: !t.IsZero(),
	}
}
