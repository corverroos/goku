package db

import (
	"database/sql"
	"testing"

	"github.com/luno/reflex"
	"github.com/luno/reflex/rsql"
)

var events = rsql.NewEventsTable("events",
	rsql.WithEventMetadataField("metadata"),
	rsql.WithEventTimeField("timestamp"),
	rsql.WithEventForeignIDField("`key`"),
	rsql.WithEventsInMemNotifier()) // TODO(corver): Provide a way to configure other notifiers.

// ToStream returns a reflex stream for deposit events.
func ToStream(dbc *sql.DB) reflex.StreamFunc {
	return events.ToStream(dbc)
}

// FillGaps registers the default reflex gap filler for the deposit events table.
func FillGaps(dbc *sql.DB) {
	rsql.FillGaps(dbc, events)
}

// CleanCache clears the cache after testing to clear test artifacts.
func CleanCache(t *testing.T) {
	t.Cleanup(func() {
		events = events.Clone()
	})
}
