//go:build privatecompare

// Private comparison tests for the dispatch refactor. They are excluded from
// the default build and test suite; run them explicitly with
// `go test -tags privatecompare -race ./internal/...`.
package service

import (
	"cadenceforge/internal/config"
	"cadenceforge/internal/domain"
	"cadenceforge/internal/queue"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testSourceID = "00000000000000000000000000000001"

var testBase = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func validSource() domain.Source {
	return domain.Source{
		ID:        testSourceID,
		Name:      "private-calibration",
		Digest:    strings.Repeat("a", 64),
		Bytes:     1024,
		Media:     domain.Media{DurationMS: 10000, Video: true, Audio: true, SampleRate: 48000, Channels: 2},
		CreatedAt: testBase.Add(-time.Hour),
	}
}

func validSheet() domain.Sheet {
	return domain.Sheet{
		ID:       "sheet",
		Revision: 1,
		Name:     "private",
		Spans:    []domain.Span{{SourceID: testSourceID, InMS: 0, OutMS: 1000}},
	}
}

func validRecipe() domain.Recipe {
	return domain.Recipe{ID: "recipe", Revision: 1, Name: "private", Format: "wav", SampleRate: 48000}
}

func testConfig(t *testing.T) config.Config {
	t.Helper()
	return config.Config{Root: t.TempDir(), Workers: 1, QueueLimit: 16, UploadLimit: 1 << 20}
}

func mustUpdate(t *testing.T, s *Service, fn func(*domain.State) error) {
	t.Helper()
	if err := s.Store.Update(fn); err != nil {
		t.Fatalf("seed state: %v", err)
	}
}

func renderID(i int) string { return fmt.Sprintf("%032x", i+1) }

// seedQueued inserts n queued renders with caller-supplied creation times.
func seedQueued(t *testing.T, s *Service, times []time.Time) {
	t.Helper()
	mustUpdate(t, s, func(st *domain.State) error {
		source := validSource()
		st.Sources[source.ID] = source
		for i, created := range times {
			render := domain.Render{
				ID:          renderID(i),
				Key:         fmt.Sprintf("key-%d", i),
				RequestHash: "hash",
				Sheet:       validSheet(),
				Recipe:      validRecipe(),
				Sources:     map[string]domain.Source{source.ID: source},
				State:       "queued",
				Attempts:    []domain.Attempt{},
				CreatedAt:   created,
				UpdatedAt:   created,
			}
			st.Renders[render.ID] = render
		}
		return nil
	})
}

// originalOrder is the selection relation of the pre-refactor claim: oldest
// CreatedAt first, identifier as tie-breaker, over the queued set only.
func originalOrder(st *domain.State) []string {
	ids := []string{}
	for id, r := range st.Renders {
		if r.State == "queued" {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		a, b := st.Renders[ids[i]], st.Renders[ids[j]]
		if a.CreatedAt.Equal(b.CreatedAt) {
			return ids[i] < ids[j]
		}
		return a.CreatedAt.Before(b.CreatedAt)
	})
	return ids
}

func drainClaims(t *testing.T, s *Service) []string {
	t.Helper()
	claimed := []string{}
	for {
		r, ok, err := s.claim(func() {})
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if !ok {
			return claimed
		}
		claimed = append(claimed, r.ID)
		if len(claimed) > 10000 {
			t.Fatal("claim did not terminate")
		}
	}
}

func TestPrivateFIFOClaimMatchesOriginalOrder(t *testing.T) {
	s, err := NewWithSelector(testConfig(t), queue.FIFO{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Store.Close()

	rng := rand.New(rand.NewSource(20261117))
	n := 120
	times := make([]time.Time, n)
	for i := range times {
		// Deliberately create ties: groups of four share a timestamp.
		times[i] = testBase.Add(time.Duration(i/4) * time.Millisecond)
	}
	rng.Shuffle(n, func(i, j int) { times[i], times[j] = times[j], times[i] })
	seedQueued(t, s, times)
	snapshot, err := s.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	want := originalOrder(&snapshot)
	got := drainClaims(t, s)
	if len(got) != len(want) {
		t.Fatalf("claimed %d renders, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("claim %d = %s, original order wanted %s", i, got[i], want[i])
		}
	}
	final, _ := s.Store.Snapshot()
	for _, id := range got {
		r := final.Renders[id]
		if r.State != "running" || len(r.Attempts) != 1 || r.Attempts[0].Outcome != "running" {
			t.Fatalf("render %s has unexpected claim state: %+v", id, r)
		}
	}
}

// roundBarrier releases the first n Select calls only after all of them have
// entered, forcing every participant to act on the same projected view.
type roundBarrier struct {
	mu      sync.Mutex
	pending int
	total   int
	release chan struct{}
	once    sync.Once
}

func newRoundBarrier(n int) *roundBarrier {
	return &roundBarrier{total: n, release: make(chan struct{})}
}

func (b *roundBarrier) wait() {
	b.mu.Lock()
	b.pending++
	if b.pending >= b.total {
		b.once.Do(func() { close(b.release) })
	}
	b.mu.Unlock()
	<-b.release
}

type barrierSelector struct {
	queue.FIFO
	barrier *roundBarrier
}

func (s *barrierSelector) Select(view queue.View) (string, bool) {
	s.barrier.wait()
	return s.FIFO.Select(view)
}

func TestPrivateConcurrentClaimsNeverReuseStaleView(t *testing.T) {
	s, err := NewWithSelector(testConfig(t), &barrierSelector{FIFO: queue.FIFO{}, barrier: newRoundBarrier(4)})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Store.Close()

	n := 60
	times := make([]time.Time, n)
	for i := range times {
		times[i] = testBase.Add(time.Duration(i) * time.Millisecond)
	}
	seedQueued(t, s, times)
	snapshot, _ := s.Store.Snapshot()
	want := originalOrder(&snapshot)

	var mu sync.Mutex
	claimed := []string{}
	var duplicates atomic.Int32
	stopped := make(chan struct{})
	defer close(stopped)

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopped:
					return
				default:
				}
				r, ok, err := s.claim(func() {})
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				if !ok {
					return
				}
				mu.Lock()
				claimed = append(claimed, r.ID)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(claimed) != n {
		t.Fatalf("claimed %d renders, want %d", len(claimed), n)
	}
	seen := map[string]int{}
	for _, id := range claimed {
		seen[id]++
	}
	for id, count := range seen {
		if count != 1 {
			duplicates.Add(1)
			t.Errorf("render %s claimed %d times", id, count)
		}
	}
	final, _ := s.Store.Snapshot()
	started := []string{}
	for _, e := range final.Events {
		if e.Kind == "render.started" {
			started = append(started, e.Subject)
		}
	}
	if len(started) != n {
		t.Fatalf("start events = %d, want %d", len(started), n)
	}
	for i := range want {
		if started[i] != want[i] {
			t.Fatalf("start event %d = %s, FIFO wanted %s", i, started[i], want[i])
		}
	}
	for id, r := range final.Renders {
		if len(r.Attempts) != 1 {
			t.Fatalf("render %s has %d attempts", id, len(r.Attempts))
		}
	}
}

// blockingSelector blocks the first Select call until released. It is used to
// show that no service lock is held while a strategy runs.
type blockingSelector struct {
	entered chan struct{}
	release chan struct{}
	blocked atomic.Bool
	calls   atomic.Int32
}

func newBlockingSelector() *blockingSelector {
	return &blockingSelector{entered: make(chan struct{}), release: make(chan struct{})}
}

func (b *blockingSelector) Select(queue.View) (string, bool) {
	if b.calls.Add(1) != 1 {
		return "", false
	}
	b.blocked.Store(true)
	b.entered <- struct{}{}
	<-b.release
	return "", false
}

func TestPrivateSlowSelectorHoldsNoLock(t *testing.T) {
	s, err := NewWithSelector(testConfig(t), newBlockingSelector())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Store.Close()
	seedQueued(t, s, []time.Time{testBase, testBase.Add(time.Millisecond)})

	selector := s.selector.(*blockingSelector)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, ok, err := s.claim(func() {})
		if err != nil || ok {
			t.Errorf("blocking claim = %v, %v", ok, err)
		}
	}()
	<-selector.entered

	// While the strategy is still inside Select, unrelated durable writes and
	// a queued-render cancel must complete. If selection ran under the mutation
	// boundary these would block until the strategy returned.
	blockedFrom := time.Now()
	var maxLatency time.Duration
	for i := 0; i < 20; i++ {
		start := time.Now()
		mustUpdate(t, s, func(st *domain.State) error {
			source := st.Sources[testSourceID]
			source.Name = fmt.Sprintf("private-rename-%d", i)
			st.Sources[source.ID] = source
			return nil
		})
		if latency := time.Since(start); latency > maxLatency {
			maxLatency = latency
		}
	}
	render, err := s.Cancel(renderID(1))
	if err != nil {
		t.Fatalf("cancel during blocked strategy: %v", err)
	}
	if render.State != "cancelled" {
		t.Fatalf("cancel state = %s", render.State)
	}
	if !selector.blocked.Load() {
		t.Fatal("expected strategy to still be blocked while mutations completed")
	}

	// Control: placing the same strategy call under the old coordination
	// boundary does make a cancellation wait for it.
	control := newBlockingSelector()
	controlDone := make(chan struct{})
	go func() {
		s.mutation.Lock()
		control.Select(queue.View{})
		s.mutation.Unlock()
		close(controlDone)
	}()
	<-control.entered
	cancelDone := make(chan struct{})
	go func() {
		s.Cancel(renderID(0))
		close(cancelDone)
	}()
	select {
	case <-cancelDone:
		t.Fatal("control cancel completed while the strategy held the mutation boundary")
	case <-time.After(100 * time.Millisecond):
	}
	close(control.release)
	<-controlDone
	<-cancelDone

	// Keep the strategy blocked for a measured interval so the comparison is
	// between a real wait and unrelated write latency, not a scheduling race.
	time.Sleep(300 * time.Millisecond)
	close(selector.release)
	<-done
	blockedFor := time.Since(blockedFrom)
	if blockedFor < 300*time.Millisecond {
		t.Fatalf("strategy was blocked for only %v; comparison is not meaningful", blockedFor)
	}
	if maxLatency >= blockedFor/4 {
		t.Fatalf("durable write took %v while the strategy was blocked for %v", maxLatency, blockedFor)
	}
	t.Logf("strategy blocked %v; slowest concurrent durable write %v", blockedFor, maxLatency)
}

func allocBytes(fn func()) uint64 {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

func TestPrivateSelectionViewDoesNotCopyHistory(t *testing.T) {
	s, err := NewWithSelector(testConfig(t), queue.FIFO{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Store.Close()

	const history = 4000
	const revisions = 1500
	mustUpdate(t, s, func(st *domain.State) error {
		source := validSource()
		st.Sources[source.ID] = source
		for i := 0; i < history; i++ {
			state := "failed"
			if i == history-1 {
				state = "queued"
			}
			id := renderID(i)
			created := testBase.Add(time.Duration(i) * time.Millisecond)
			st.Renders[id] = domain.Render{
				ID: id, Key: fmt.Sprintf("history-key-%d", i), RequestHash: "hash",
				Sheet: validSheet(), Recipe: validRecipe(),
				Sources: map[string]domain.Source{source.ID: source},
				State:   state, Attempts: []domain.Attempt{},
				CreatedAt: created, UpdatedAt: created,
			}
		}
		versions := make([]domain.Sheet, 0, revisions)
		for i := 0; i < revisions; i++ {
			sheet := validSheet()
			sheet.ID = sheetIDForTest
			sheet.Revision = i + 1
			sheet.Name = fmt.Sprintf("revision-%d", i)
			versions = append(versions, sheet)
		}
		st.Sheets[sheetIDForTest] = versions
		return nil
	})

	var queueEntries []domain.QueueEntry
	queuedBytes := allocBytes(func() { queueEntries = s.Store.Queued() })
	var snapshot domain.State
	snapshotBytes := allocBytes(func() { snapshot, _ = s.Store.Snapshot() })
	_ = snapshot
	if len(queueEntries) != 1 {
		t.Fatalf("projection returned %d entries, want 1", len(queueEntries))
	}
	if snapshotBytes < 1<<20 {
		t.Fatalf("snapshot unexpectedly small (%d bytes); history fixture is ineffective", snapshotBytes)
	}
	if queuedBytes >= snapshotBytes/50 {
		t.Fatalf("projection copied %d bytes versus snapshot %d bytes", queuedBytes, snapshotBytes)
	}
	if queuedBytes > 16<<10 {
		t.Fatalf("projection allocated %d bytes for one queued render", queuedBytes)
	}
	t.Logf("history=%d renders, revisions=%d, snapshot=%d bytes, selection projection=%d bytes",
		history, revisions, snapshotBytes, queuedBytes)
}

const sheetIDForTest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

// TestPrivateAdmissionCapacityIsDecidedOncePerTransaction proves that two
// independent submissions racing for the last capacity slot cannot both win
// from the same pre-transaction view.
func TestPrivateAdmissionCapacityIsDecidedOncePerTransaction(t *testing.T) {
	c := testConfig(t)
	c.QueueLimit = 1
	s, err := NewWithSelector(c, queue.FIFO{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Store.Close()

	source := validSource()
	sheet := validSheet()
	recipe := validRecipe()
	mustUpdate(t, s, func(st *domain.State) error {
		st.Sources[source.ID] = source
		st.Sheets[sheet.ID] = []domain.Sheet{sheet}
		st.Recipes[recipe.ID] = []domain.Recipe{recipe}
		return nil
	})

	start := make(chan struct{})
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		key := fmt.Sprintf("capacity-race-%d", i)
		go func() {
			<-start
			_, err := s.Submit(key, Request{SheetID: sheet.ID, SheetRevision: 1, RecipeID: recipe.ID, RecipeRevision: 1})
			results <- err
		}()
	}
	close(start)
	accepted, rejected := 0, 0
	for i := 0; i < 2; i++ {
		err := <-results
		if err == nil {
			accepted++
			continue
		}
		status, code, _ := domain.Classify(err)
		if status != 429 || code != "capacity" {
			t.Fatalf("unexpected rejection: %v", err)
		}
		rejected++
	}
	if accepted != 1 || rejected != 1 {
		t.Fatalf("accepted=%d rejected=%d, want exactly one of each", accepted, rejected)
	}
	st, _ := s.Store.Snapshot()
	if got := len(st.Renders); got != 1 {
		t.Fatalf("renders=%d, want 1", got)
	}
}

// TestPrivateSubmitAndRetirementShareFrozenReferenceCheck races a submission
// against source retirement. Either serialization order is legal, but a frozen
// render whose source has been retired is not.
func TestPrivateSubmitAndRetirementShareFrozenReferenceCheck(t *testing.T) {
	for round := 0; round < 20; round++ {
		s, err := NewWithSelector(testConfig(t), queue.FIFO{})
		if err != nil {
			t.Fatal(err)
		}
		source := validSource()
		sheet := validSheet()
		recipe := validRecipe()
		mustUpdate(t, s, func(st *domain.State) error {
			st.Sources[source.ID] = source
			st.Sheets[sheet.ID] = []domain.Sheet{sheet}
			st.Recipes[recipe.ID] = []domain.Recipe{recipe}
			return nil
		})

		start := make(chan struct{})
		var submitErr, retireErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, submitErr = s.Submit(fmt.Sprintf("retire-race-%d", round), Request{SheetID: sheet.ID, SheetRevision: 1, RecipeID: recipe.ID, RecipeRevision: 1})
		}()
		go func() {
			defer wg.Done()
			<-start
			retireErr = s.RetireSource(source.ID)
		}()
		close(start)
		wg.Wait()

		st, err := s.Store.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		_, sourcePresent := st.Sources[source.ID]
		renderCount := len(st.Renders)
		switch {
		case sourcePresent:
			// Retirement lost to the frozen render or was otherwise refused.
			if renderCount != 1 || retireErr == nil {
				t.Fatalf("round %d: source kept, renders=%d retireErr=%v", round, renderCount, retireErr)
			}
		default:
			// Retirement won; no render may reference the retired source.
			if renderCount != 0 || submitErr == nil {
				t.Fatalf("round %d: source retired, renders=%d submitErr=%v", round, renderCount, submitErr)
			}
		}
		for _, r := range st.Renders {
			if _, frozen := r.Sources[source.ID]; frozen && !sourcePresent {
				t.Fatalf("round %d: render %s froze a retired source", round, r.ID)
			}
		}
		s.Store.Close()
	}
}

// TestPrivateStrategyCannotMutateState shows that a selector returning an id
// outside its projection grants no write authority.
func TestPrivateStrategyCannotMutateState(t *testing.T) {
	s, err := NewWithSelector(testConfig(t), queue.Func(func(queue.View) (string, bool) {
		return renderID(999), true
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Store.Close()
	seedQueued(t, s, []time.Time{testBase})
	before, _ := s.Store.Snapshot()
	if _, ok, err := s.claim(func() {}); ok || err != nil {
		t.Fatalf("claim = %v, %v; want no claim", ok, err)
	}
	after, _ := s.Store.Snapshot()
	if len(after.Events) != len(before.Events) {
		t.Fatalf("selector outside its view changed durable state")
	}
	for id, r := range after.Renders {
		if r.State != "queued" {
			t.Fatalf("render %s mutated to %s", id, r.State)
		}
	}
}

// TestPrivateRecoveryAfterClaimRetainsFrozenInput verifies that the claim
// refactor preserves the existing recovery contract: a render left running by
// a stopped process becomes failed/interrupted on the next open, with its
// frozen sources and plan intact and the same event vocabulary.
func TestPrivateRecoveryAfterClaimRetainsFrozenInput(t *testing.T) {
	c := testConfig(t)
	s, err := NewWithSelector(c, queue.FIFO{})
	if err != nil {
		t.Fatal(err)
	}
	seedQueued(t, s, []time.Time{testBase})
	claimed, ok, err := s.claim(func() {})
	if err != nil || !ok {
		t.Fatalf("claim = %v, %v", ok, err)
	}
	before, _ := s.Store.Snapshot()
	if err := s.Store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewWithSelector(c, queue.FIFO{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Store.Close()
	after, _ := reopened.Store.Snapshot()
	recovered := after.Renders[claimed.ID]
	if recovered.State != "failed" {
		t.Fatalf("state = %s, want failed", recovered.State)
	}
	if len(recovered.Attempts) != 1 || recovered.Attempts[0].Outcome != "interrupted" {
		t.Fatalf("attempt provenance = %+v", recovered.Attempts)
	}
	if recovered.Attempts[0].Detail != "process ended before artifact commit" {
		t.Fatalf("attempt detail = %q", recovered.Attempts[0].Detail)
	}
	if recovered.Sheet.Revision != before.Renders[claimed.ID].Sheet.Revision ||
		recovered.Recipe.Revision != before.Renders[claimed.ID].Recipe.Revision ||
		recovered.Plan.DurationMS != before.Renders[claimed.ID].Plan.DurationMS {
		t.Fatal("frozen revision, recipe or plan changed across recovery")
	}
	if _, ok := recovered.Sources[testSourceID]; !ok {
		t.Fatal("frozen source snapshot lost across recovery")
	}
	found := false
	for _, e := range after.Events {
		if e.Subject == claimed.ID && e.Kind == "render.interrupted" {
			found = true
		}
	}
	if !found {
		t.Fatal("render.interrupted event missing")
	}
	if _, err := reopened.Retry(claimed.ID); err != nil {
		t.Fatalf("retry after recovery: %v", err)
	}
	requeued, _ := reopened.Store.Snapshot()
	if requeued.Renders[claimed.ID].State != "queued" || len(requeued.Renders[claimed.ID].Attempts) != 1 {
		t.Fatalf("retry state = %+v", requeued.Renders[claimed.ID])
	}
}
