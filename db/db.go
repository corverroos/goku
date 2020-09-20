package db

import (
	"database/sql"
	"testing"

	"github.com/corverroos/truss"
)

func ConnectForTesting(t *testing.T) *sql.DB {
	return truss.ConnectForTesting(t,
		"create table events ("+
			" id bigint not null auto_increment, "+
			" type int not null, "+
			" `key` varchar(255) not null, "+
			" timestamp datetime(3) not null, "+
			" metadata blob, "+
			" primary key (id)"+
			");",
		"create table data ("+
			" `key` varchar(255) not null,"+
			" value blob,"+
			" version bigint not null,"+
			" created_ref bigint not null,"+
			" updated_ref bigint not null,"+
			" deleted_ref bigint,"+
			" lease_id bigint,"+
			" primary key (`key`),"+
			" index lease_id (lease_id)"+
			");",
		"create table leases ("+
			" id bigint not null auto_increment,"+
			" version bigint not null,"+
			" expires_at datetime(3),"+
			" expired bool not null default false,"+
			" primary key (id),"+
			" index expires_at (expires_at)"+
			");")

}
