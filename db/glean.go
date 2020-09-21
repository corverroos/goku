package db

import (
	"database/sql"

	"github.com/corverroos/goku"
)

//go:generate glean -table=data --err_no_rows=github.com/corverroos/goku.ErrNotFound -scan -raw

type glean struct {
	goku.KV

	DeletedRef sql.NullInt64
	LeaseID    sql.NullInt64
}
