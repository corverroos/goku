package db

import (
	"database/sql"
	"io/ioutil"
	"runtime"
	"strings"
	"testing"

	"github.com/corverroos/truss"
	"github.com/luno/jettison/jtest"
)

func ConnectForTesting(t *testing.T) *sql.DB {
	return truss.ConnectForTesting(t, getSchema(t)...)
}

func getSchema(t *testing.T) []string {
	_, f, _, _ := runtime.Caller(1)
	file := strings.ReplaceAll(f, "db.go", "schema.sql")
	b, err := ioutil.ReadFile(file)
	jtest.RequireNil(t, err)

	ql := string(b)
	ql = strings.TrimSpace(ql)
	ql = strings.Trim(ql, ";")
	return strings.Split(ql, ";")
}
