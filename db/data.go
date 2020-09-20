package db

import (
	"context"
	"database/sql"
	"github.com/corverroos/goku"
	"github.com/go-sql-driver/mysql"
	"github.com/luno/jettison/errors"
	"github.com/luno/reflex"
	"strings"
)

func Get(ctx context.Context, dbc dbc, key string) (goku.KV, error) {
	return lookupWhere(ctx, dbc, "`key`=?", key)
}

func List(ctx context.Context, dbc dbc, prefix string) ([]goku.KV, error) {
	return listWhere(ctx, dbc, "`key` like ?%", prefix)
}

func Set(ctx context.Context, dbc *sql.DB, key string, value []byte) error {
	tx, err := dbc.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	ref, err := insertEvent(ctx, tx, key, goku.EventTypeSet, value)
	if err != nil {
		return err
	}

	// First try to update if already exist
	res, err := tx.ExecContext(ctx, "update data "+
		"set value=?, version=version+1, updated_ref=? where `key`=? and updated_ref<?",
		value, ref, key, ref)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	} else if n == 1 {
		// Row updated.
		// TODO(corver): Trigger reflex notifier.
		return tx.Commit()
	}

	// Update failed; either due to race with another set or because it doesn't exist yet.

	// Try to insert.

	res, err = tx.ExecContext(ctx, "insert into data "+
		"set `key`=?, value=?, version=1, created_ref=?, updated_ref=?",
		key, value, ref, ref)
	if isDuplicateKeyErr(err) {
		// So key already exists and update failed due to race.
		return errors.Wrap(goku.ErrSetRace, "")
	} else if err != nil {
		return err
	}

	n, err = res.RowsAffected()
	if err != nil {
		return err
	} else if n != 1 {
		return errors.New("unexpected insert failure")
	}

	// TODO(corver): Trigger reflex notifier.
	return tx.Commit()

}

func Delete(ctx context.Context, dbc *sql.DB, key string) error {
	tx, err := dbc.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	ref, err := insertEvent(ctx, tx, key, goku.EventTypeDelete, nil)
	if err != nil {
		return err
	}

	// First try to update if already exist
	res, err := tx.ExecContext(ctx, "update data "+
		"set value=null, version=version+1, updated_ref=?, deleted_ref=? where `key`=? and updated_ref<?",
		ref, ref, key, ref)
	if err != nil {
		return err
	}

	n, err := res.RowsAffected()
	if err != nil {
		return err
	} else if n == 0 {
		return errors.Wrap(goku.ErrNotFound, "")
	} else if n != 1 {
		return errors.New("unexpected update res")
	}

	// TODO(corver): Trigger reflex notifier.
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

	if me.Number != errDupEntry {
		return false
	}

	return strings.Contains(me.Message, "key 'PRIMARY'")
}
