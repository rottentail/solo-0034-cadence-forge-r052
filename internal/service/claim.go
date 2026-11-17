package service

import (
	"cadenceforge/internal/admission"
	"cadenceforge/internal/domain"
	"cadenceforge/internal/queue"
	"context"
	"fmt"
	"time"
)

// claim asks the injected selection strategy which queued render should run,
// then commits that decision in one durable journal transaction.
//
// The strategy is called outside the mutation lock and outside the journal
// transaction, so a slow or experimental selector cannot block edits, submits
// or cancels. It only ever sees a small projection of the queue; the full
// state is never handed to it. The transaction re-reads the durable state and
// revalidates the selected entry before mutating it. If another worker claimed
// the entry, or a new render that precedes it under the selector's declared
// order arrived in between, the claim is abandoned and the loop retries with a
// fresh view. Commit-time order verification therefore preserves the FIFO
// outcome of the original implementation without holding the lock while the
// strategy runs.
func (s *Service) claim(cancel context.CancelFunc) (domain.Render, bool, error) {
	// A stale view is retried immediately, but a strategy whose Select and
	// Precedes disagree could otherwise spin forever. After a bounded number
	// of stale rounds the worker yields and relies on the wake/ticker path.
	const staleRetries = 16
	for round := 0; round <= staleRetries; round++ {
		queued := s.Store.Queued()
		view := queue.View{Entries: make([]queue.Entry, len(queued))}
		latest := make(map[string]domain.QueueEntry, len(queued))
		for i, entry := range queued {
			view.Entries[i] = queue.Entry{ID: entry.ID, CreatedAt: entry.CreatedAt}
			latest[entry.ID] = entry
		}

		id, ok := s.selector.Select(view)
		if !ok {
			return domain.Render{}, false, nil
		}
		selected, granted := latest[id]
		if !granted {
			// A strategy returned an id outside its view. Treat it as no
			// selection rather than inventing authority the view did not grant.
			return domain.Render{}, false, nil
		}

		var out domain.Render
		claimed := false
		s.mutation.Lock()
		err := s.Store.Update(func(st *domain.State) error {
			r, eligible := admission.Claimable(st, selected, s.claimOrder())
			if !eligible {
				return nil
			}
			r.State = "running"
			r.UpdatedAt = time.Now().UTC()
			r.Attempts = append(r.Attempts, domain.Attempt{Number: len(r.Attempts) + 1, StartedAt: r.UpdatedAt, Outcome: "running"})
			st.Renders[r.ID] = r
			st.Emit(r.ID, "render.started", fmt.Sprint(len(r.Attempts)))
			out = r
			claimed = true
			return nil
		})
		if claimed {
			// Register cancellation before releasing the coordination boundary,
			// so a cancel that observes the running state can always reach the
			// attempt it is revoking.
			s.activeMu.Lock()
			s.active[out.ID] = cancel
			s.activeMu.Unlock()
		}
		s.mutation.Unlock()
		if err != nil {
			return domain.Render{}, false, err
		}
		if claimed {
			return out, true, nil
		}
		// The view went stale between projection and commit; ask the strategy
		// again with the current queue rather than acting on the old view.
	}
	// Contention outlasted the bounded retries. Yield the worker instead of
	// spinning, but keep it promptly wakeable so drained work is not delayed
	// until the next periodic poll.
	s.Notify()
	return domain.Render{}, false, nil
}

func (s *Service) claimOrder() queue.Order {
	if order, ok := s.selector.(queue.Order); ok {
		return order
	}
	return nil
}
