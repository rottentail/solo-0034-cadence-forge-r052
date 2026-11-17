package service

import (
	"cadenceforge/internal/admission"
	"cadenceforge/internal/domain"
	"time"
)

func (s *Service) Cancel(id string) (domain.Render, error) {
	s.mutation.Lock()
	defer s.mutation.Unlock()
	var out domain.Render
	err := s.Store.Update(func(st *domain.State) error {
		r, ok := st.Renders[id]
		if !ok {
			return domain.Missing("render", id)
		}
		if r.State == "cancelled" {
			out = r
			return nil
		}
		if domain.Terminal(r.State) {
			return domain.Conflict("completed render cannot be cancelled")
		}
		r.State = "cancelled"
		r.UpdatedAt = time.Now().UTC()
		if len(r.Attempts) > 0 {
			a := &r.Attempts[len(r.Attempts)-1]
			a.Outcome = "cancelled"
			a.FinishedAt = &r.UpdatedAt
		}
		st.Renders[id] = r
		st.Emit(id, "render.cancelled", "artifact visibility revoked before acknowledgement")
		out = r
		return nil
	})
	if err == nil {
		s.activeMu.Lock()
		if cancel := s.active[id]; cancel != nil {
			cancel()
		}
		s.activeMu.Unlock()
	}
	return out, err
}
func (s *Service) Retry(id string) (domain.Render, error) {
	s.mutation.Lock()
	defer s.mutation.Unlock()
	var out domain.Render
	err := s.Store.Update(func(st *domain.State) error {
		r, ok := st.Renders[id]
		if !ok {
			return domain.Missing("render", id)
		}
		if r.State != "failed" {
			return domain.Conflict("only failed render can retry")
		}
		if len(r.Attempts) >= 3 {
			return domain.Conflict("three-attempt limit reached")
		}
		if err := admission.EnsureCapacity(st, s.Config.QueueLimit); err != nil {
			return err
		}
		r.State = "queued"
		r.UpdatedAt = time.Now().UTC()
		st.Renders[id] = r
		st.Emit(id, "render.retried", "original frozen inputs retained")
		out = r
		return nil
	})
	if err == nil {
		s.Notify()
	}
	return out, err
}
