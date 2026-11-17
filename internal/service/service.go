package service

import (
	"cadenceforge/internal/blob"
	"cadenceforge/internal/config"
	"cadenceforge/internal/journal"
	"cadenceforge/internal/media"
	"cadenceforge/internal/queue"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
)

type Service struct {
	Store    *journal.Store
	Blobs    *blob.Store
	Probe    media.Prober
	Config   config.Config
	selector queue.Selector
	mutation sync.Mutex
	wake     chan struct{}
	activeMu sync.Mutex
	active   map[string]context.CancelFunc
}

// New wires the service with the default FIFO selection strategy. Selector is
// a dependency seam: replacing it swaps dispatch policy without granting the
// strategy any write, lock or full-state access.
func New(c config.Config) (*Service, error) {
	return NewWithSelector(c, queue.FIFO{})
}

func NewWithSelector(c config.Config, selector queue.Selector) (*Service, error) {
	if selector == nil {
		selector = queue.FIFO{}
	}
	st, err := journal.Open(c.Root)
	if err != nil {
		return nil, err
	}
	blobs, err := blob.Open(c.Root)
	if err != nil {
		st.Close()
		return nil, err
	}
	s := &Service{Store: st, Blobs: blobs, Probe: media.Prober{Binary: c.FFprobe}, Config: c, selector: selector, wake: make(chan struct{}, 1), active: map[string]context.CancelFunc{}}
	if err = st.Recover(); err != nil {
		st.Close()
		return nil, err
	}
	return s, nil
}
func ID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Errorf("entropy unavailable: %w", err))
	}
	return hex.EncodeToString(b[:])
}
func hash(value any) string {
	b, _ := json.Marshal(value)
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}
func (s *Service) Notify() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}
