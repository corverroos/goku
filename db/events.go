package db

import (
	"database/sql"
	"sync"
	"testing"

	"github.com/luno/reflex"
	"github.com/luno/reflex/rsql"
)

var events = rsql.NewEventsTable("events",
	rsql.WithEventMetadataField("metadata"),
	rsql.WithEventTimeField("timestamp"),
	rsql.WithEventForeignIDField("`key`"),
	rsql.WithEventsNotifier(notifier))

var notifier = new(memNotifier) // TODO(corver): Provide a way to configure other notifiers.

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

// memNotifier is an in-memory implementation of rsql EventsNotifier.
type memNotifier struct {
	mu        sync.Mutex
	listeners []chan struct{}
}

func (n *memNotifier) Notify() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, l := range n.listeners {
		select {
		case l <- struct{}{}:
		default:
		}
	}
	n.listeners = nil
}

func (n *memNotifier) C() <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()

	ch := make(chan struct{}, 1)
	n.listeners = append(n.listeners, ch)
	return ch
}
