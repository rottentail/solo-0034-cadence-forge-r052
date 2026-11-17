package journal

import (
	"cadenceforge/internal/domain"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"syscall"
)

type Store struct {
	mu       sync.Mutex
	root     string
	state    domain.State
	lock     *os.File
	poisoned error
}

func Open(root string) (*Store, error) {
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(root, "process.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("data root already in use: %w", err)
	}
	s := &Store{root: root, state: domain.Empty(), lock: lock}
	data, err := os.ReadFile(filepath.Join(root, "state.json"))
	if err == nil {
		err = json.Unmarshal(data, &s.state)
	} else if os.IsNotExist(err) {
		err = nil
	}
	if err == nil {
		err = domain.ValidateState(s.state)
	}
	if err != nil {
		s.Close()
		return nil, fmt.Errorf("load state: %w", err)
	}
	return s, nil
}
func clone(s domain.State) (domain.State, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return domain.State{}, err
	}
	var out domain.State
	err = json.Unmarshal(b, &out)
	return out, err
}
func (s *Store) Snapshot() (domain.State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return clone(s.state)
}

// Queued projects only the queued renders while holding the state lock. It
// copies a bounded number of small entries instead of cloning the journal, so
// dispatch selection never receives or duplicates the full history.
func (s *Store) Queued() []domain.QueueEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.QueueEntry, 0)
	for _, r := range s.state.Renders {
		if r.State == "queued" {
			out = append(out, domain.QueueEntry{ID: r.ID, CreatedAt: r.CreatedAt, QueuedAt: r.UpdatedAt})
		}
	}
	return out
}
func (s *Store) Update(fn func(*domain.State) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisoned != nil {
		return s.poisoned
	}
	next, err := clone(s.state)
	if err != nil {
		return err
	}
	if err = fn(&next); err != nil {
		return err
	}
	if reflect.DeepEqual(next, s.state) {
		return nil
	}
	if err = domain.ValidateState(next); err != nil {
		return fmt.Errorf("state invariant: %w", err)
	}
	b, err := json.MarshalIndent(next, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(s.root, ".state-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(name, filepath.Join(s.root, "state.json")); err != nil {
		return err
	}
	s.state = next
	d, err := os.Open(s.root)
	if err != nil {
		s.poisoned = err
		return err
	}
	err = d.Sync()
	d.Close()
	if err != nil {
		s.poisoned = fmt.Errorf("state committed but durability uncertain; restart required: %w", err)
		return s.poisoned
	}
	return nil
}
func (s *Store) Close() error {
	if s.lock != nil {
		return s.lock.Close()
	}
	return nil
}
