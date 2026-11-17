// Package queue defines the pure selection boundary for render dispatch.
//
// A Selector is handed a bounded projection of the queued renders and returns
// the identifier it wants to run. It has no reference to the journal, the
// service, or a mutable State value, so replacing or reviewing a strategy
// cannot change durable state. The application service re-reads the durable
// state and commits a claim only after the selection returns.
package queue

import "time"

// Entry is the complete information a selection strategy may observe for one
// queued render. Source snapshots, plans, attempts and artifacts are omitted
// deliberately: the view stays small even when the journal history is large.
type Entry struct {
	ID        string
	CreatedAt time.Time
}

// View is an immutable-by-convention projection of the queued renders. Its
// order is unspecified; strategies must not rely on slice position.
type View struct {
	Entries []Entry
}

// Selector chooses at most one queued render. Select may be arbitrarily slow
// because it is never called while the journal or edit locks are held. It must
// return an identifier contained in the supplied View; any other identifier is
// treated as "no selection" and grants no write authority.
type Selector interface {
	Select(View) (id string, ok bool)
}

// Order is an optional pure ordering relation. A Selector that implements
// Order declares the order in which queued entries must be claimed; the claim
// transaction then verifies that the chosen entry is still first among the
// current queue, and Select must agree by returning that first entry. This
// verification is what keeps a stale view from reordering the queue.
// Precedes must be cheap, side-effect free and non-blocking: it is evaluated
// inside the journal transaction. Selectors that do not care about strict
// ordering can omit it.
type Order interface {
	Precedes(a, b Entry) bool
}

// FIFO selects the oldest queued render, using the identifier as a stable
// tie-breaker. This is the scheduling behaviour of the original service.
type FIFO struct{}

func (FIFO) Select(view View) (string, bool) {
	if len(view.Entries) == 0 {
		return "", false
	}
	best := view.Entries[0]
	for _, entry := range view.Entries[1:] {
		if (FIFO{}).Precedes(entry, best) {
			best = entry
		}
	}
	return best.ID, true
}

func (FIFO) Precedes(a, b Entry) bool {
	if a.CreatedAt.Equal(b.CreatedAt) {
		return a.ID < b.ID
	}
	return a.CreatedAt.Before(b.CreatedAt)
}

// Func adapts a function to Selector. It intentionally does not implement
// Order, so an experimental strategy can opt in to commit-time ordering
// explicitly.
type Func func(View) (string, bool)

func (f Func) Select(view View) (string, bool) { return f(view) }
