package db

import (
	"context"
	"database/sql"
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
		LeaseID: g.LeaseID.Int64,
	}, nil
}

// dbc is a common interface for *sql.DB and *sql.Tx.
type dbc interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
}

// row is a common interface for *sql.Rows and *sql.Row.
type row interface {
	Scan(dest ...interface{}) error
}
