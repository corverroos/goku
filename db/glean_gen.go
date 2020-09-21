package db

import (
	"context"
	"database/sql"
	"time"

	"github.com/corverroos/goku"
	"github.com/luno/jettison/errors"
)

const cols = " `key`, `value`, `version`, `created_ref`, `updated_ref`, `deleted_ref`, `lease_id` "
const selectPrefix = "select " + cols + " from data where "

// lookupWhere queries the data table with the provided where clause, then scans
// and returns a single row.
func lookupWhere(ctx context.Context, dbc dbc, where string, args ...interface{}) (goku.KV, error) {
	return scan(dbc.QueryRowContext(ctx, selectPrefix+where, args...))
}

// scanWhere queries the data table with the provided where clause and
// calls callback with the results one row at a time.
//
// An intermediate buffered channel is introduced between the internal callback
// and the provided callback. This allows testing (with only a single DB connection)
// if the number of rows fits in the buffer (< 100).
func scanWhere(in context.Context, dbc dbc, fn func(goku.KV) error,
	where string, args ...interface{}) error {

	// If the reader errors, we need to cancel the streamer.
	ctx, cancel := context.WithCancel(in)
	defer func() {
		// Testing work around: allow scanWhere to return before we cancel the context.
		// See src/bitx/db/db_test.go:TestCtxCancelIssue.
		time.Sleep(time.Millisecond)
		cancel()
	}()

	// Note: tests will block if scanning more than 100 rows.
	ch := make(chan goku.KV, 100)

	fn2 := func(i goku.KV) error {
		ch <- i
		return nil
	}

	// Start streamer, from DB into the channel.
	var scanErr error
	go func() {
		// When the streamer is done, close the channel for the reader to return.
		defer close(ch)
		scanErr = innerScanWhere(ctx, dbc, fn2, where, args...)
	}()

	// Start reader, from the channel until closed or error.
	for i := range ch {
		if err := fn(i); err != nil {
			return err
		}
	}

	return scanErr
}

func innerScanWhere(ctx context.Context, dbc dbc, f func(goku.KV) error,
	where string, args ...interface{}) error {

	rows, err := dbc.QueryContext(ctx, selectPrefix+where, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		r, err := scan(rows)
		if err != nil {
			return err
		}
		err = f(r)
		if err != nil {
			return err
		}
	}
	return rows.Err()
}

// listWhere queries the data table with the provided where clause, then scans
// and returns all the rows.
func listWhere(ctx context.Context, dbc dbc, where string, args ...interface{}) ([]goku.KV, error) {

	rows, err := dbc.QueryContext(ctx, selectPrefix+where, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []goku.KV
	for rows.Next() {
		r, err := scan(rows)
		if err != nil {
			return nil, err
		}
		res = append(res, r)
	}

	return res, rows.Err()
}

func scan(row row) (goku.KV, error) {
	var g glean

	err := row.Scan(&g.Key, &g.Value, &g.Version, &g.CreatedRef, &g.UpdatedRef, &g.DeletedRef, &g.LeaseID)
	if errors.Is(err, sql.ErrNoRows) {
		return goku.KV{}, errors.Wrap(goku.ErrNotFound, "")
	} else if err != nil {
		return goku.KV{}, err
	}

	return goku.KV{
		Key:        g.Key,
		Value:      g.Value,
		Version:    g.Version,
		CreatedRef: g.CreatedRef,
		UpdatedRef: g.UpdatedRef,
		DeletedRef: g.DeletedRef.Int64,
		LeaseID:    g.LeaseID.Int64,
	}, nil
}

// dbc is a common interface for *sql.DB and *sql.Tx.
type dbc interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// row is a common interface for *sql.Rows and *sql.Row.
type row interface {
	Scan(dest ...interface{}) error
}
