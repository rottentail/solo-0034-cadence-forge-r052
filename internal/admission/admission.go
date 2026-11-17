// Package admission holds the preconditions shared by durable state changes.
//
// Every function reads the State passed by the caller. Callers must invoke
// them from inside a journal transaction so that the read and the resulting
// write observe the same state. In particular, capacity admission, source
// retirement and claim eligibility must never be decided from a snapshot
// taken before the transaction.
package admission

import (
	"cadenceforge/internal/domain"
	"cadenceforge/internal/queue"
)

// Pending counts renders that occupy queue capacity.
func Pending(st *domain.State) int {
	pending := 0
	for _, render := range st.Renders {
		if render.State == "queued" || render.State == "running" {
			pending++
		}
	}
	return pending
}

// EnsureCapacity rejects a new queued or retried render when the bounded
// queue is full. It preserves the original 429 response and message.
func EnsureCapacity(st *domain.State, limit int) error {
	if Pending(st) >= limit {
		return domain.Capacity("render queue is full")
	}
	return nil
}

// EnsureSourceUnreferenced rejects retirement when any immutable sheet
// revision, cue revision or frozen render lineage still names the source.
func EnsureSourceUnreferenced(st *domain.State, id string) error {
	if _, ok := st.Sources[id]; !ok {
		return domain.Missing("source", id)
	}
	for _, versions := range st.Sheets {
		for _, sheet := range versions {
			for _, span := range sheet.Spans {
				if span.SourceID == id {
					return domain.Conflict("source is referenced by sheet history")
				}
			}
			for _, cue := range sheet.Cues {
				if cue.SourceID == id {
					return domain.Conflict("source is referenced by cue history")
				}
			}
		}
	}
	for _, render := range st.Renders {
		if _, ok := render.Sources[id]; ok {
			return domain.Conflict("source is retained by render lineage")
		}
	}
	return nil
}

// Claimable validates a selection made from a prior view against the current
// transaction state. The render must still be queued and be the same queue
// incarnation (CreatedAt and UpdatedAt unchanged). When order is non-nil, no
// currently queued render may precede it under that order.
//
// A false result means the view was stale; the caller must not mutate state,
// and should project a fresh view and ask the selection strategy again.
func Claimable(st *domain.State, want domain.QueueEntry, order queue.Order) (domain.Render, bool) {
	render, ok := st.Renders[want.ID]
	if !ok || render.State != "queued" {
		return domain.Render{}, false
	}
	if !render.CreatedAt.Equal(want.CreatedAt) || !render.UpdatedAt.Equal(want.QueuedAt) {
		return domain.Render{}, false
	}
	if order == nil {
		return render, true
	}
	mine := queue.Entry{ID: want.ID, CreatedAt: render.CreatedAt}
	for id, other := range st.Renders {
		if id == want.ID || other.State != "queued" {
			continue
		}
		if order.Precedes(queue.Entry{ID: id, CreatedAt: other.CreatedAt}, mine) {
			return domain.Render{}, false
		}
	}
	return render, true
}
