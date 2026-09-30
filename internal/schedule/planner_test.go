package schedule

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/admission"
	"github.com/wyw14/cry-96/internal/cycle"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/hydraulics"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/signal"
)

const chamber = "chamber-1"

type harness struct {
	store    journal.Store
	recorder *journal.Recorder
	gates    *gate.State
	queue    *admission.Queue
	adm      *admission.Service
	cycles   *cycle.Coordinator
	planner  *Planner
}

func newHarness(t *testing.T, store journal.Store, now func() time.Time) *harness {
	t.Helper()
	recorder := journal.NewRecorder(store, now)
	gateState := gate.NewState(chamber)
	signalStore := signal.NewStore()
	queue := admission.NewQueue()
	controller := admission.NewController(queue, gateState, signalStore, now)
	adm := admission.NewService(controller, recorder, now)
	cycles := cycle.NewCoordinator((*gate.Controller)(nil), (*hydraulics.Controller)(nil), recorder, now)
	planner := NewPlanner(adm, cycles, recorder, now)
	return &harness{
		store: store, recorder: recorder, gates: gateState, queue: queue,
		adm: adm, cycles: cycles, planner: planner,
	}
}

func vessels(n int) []model.Vessel {
	result := make([]model.Vessel, 0, n)
	for i := 0; i < n; i++ {
		vessel, err := model.NewVessel("vessel", "CALL", 10)
		if err != nil {
			panic(err)
		}
		result = append(result, vessel)
	}
	return result
}

func (h *harness) eventTypes(t *testing.T) []model.EventType {
	t.Helper()
	events, err := h.store.Events(context.Background(), chamber, 0)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	kinds := make([]model.EventType, 0, len(events))
	for _, event := range events {
		kinds = append(kinds, event.Type)
	}
	return kinds
}

func TestSubmitSucceedsOnSealedChamber(t *testing.T) {
	h := newHarness(t, journal.NewMemoryStore(), time.Now)
	plan, transit, err := h.planner.Submit(context.Background(), chamber, model.DirectionUpstream, vessels(2))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if transit.PlanID != plan.ID {
		t.Fatal("cycle is not linked to the plan")
	}
	if active := h.planner.Active(chamber); len(active) != 1 || active[0].ID != plan.ID {
		t.Fatalf("active plans = %v, want the submitted plan", active)
	}
	if !h.cycles.HasActive(chamber) {
		t.Fatal("chamber should own an active cycle")
	}
	entries := h.adm.List(chamber)
	if len(entries) != 2 {
		t.Fatalf("queue len = %d, want 2", len(entries))
	}
	for _, entry := range entries {
		if entry.PlanID != plan.ID {
			t.Fatal("queue contains an entry from another plan")
		}
	}
	kinds := h.eventTypes(t)
	want := []model.EventType{model.EventCycleCreated, model.EventPlanCreated, model.EventSignalChanged}
	if len(kinds) != 3 {
		t.Fatalf("events = %v, want %v", kinds, want)
	}
	for index, kind := range want {
		if kinds[index] != kind {
			t.Fatalf("events = %v, want %v", kinds, want)
		}
	}
}

func TestRejectedSubmitLeavesNothingBehind(t *testing.T) {
	h := newHarness(t, journal.NewMemoryStore(), time.Now)
	first, _, err := h.planner.Submit(context.Background(), chamber, model.DirectionUpstream, vessels(1))
	if err != nil {
		t.Fatalf("first submit: %v", err)
	}

	_, _, err = h.planner.Submit(context.Background(), chamber, model.DirectionDownstream, vessels(1))
	if err == nil {
		t.Fatal("second submit must conflict with the running transit")
	}

	if active := h.planner.Active(chamber); len(active) != 1 || active[0].ID != first.ID {
		t.Fatalf("active plans after rejection = %v, want only the first plan", active)
	}
	for _, entry := range h.adm.List(chamber) {
		if entry.PlanID != first.ID {
			t.Fatalf("rejected plan leaked into queue: %s", entry.PlanID)
		}
	}
	kinds := h.eventTypes(t)
	if len(kinds) != 3 {
		t.Fatalf("a rejected request must not add events, got %v", kinds)
	}
}

func TestUnsealedChamberRejectsWithoutSideEffects(t *testing.T) {
	h := newHarness(t, journal.NewMemoryStore(), time.Now)
	if err := h.gates.Observe(chamber, model.DirectionUpstream, model.GateOpen, time.Now()); err != nil {
		t.Fatalf("observe: %v", err)
	}
	_, _, err := h.planner.Submit(context.Background(), chamber, model.DirectionUpstream, vessels(2))
	if err == nil {
		t.Fatal("submit must be refused while gates are not sealed")
	}
	if len(h.planner.Active(chamber)) != 0 {
		t.Fatal("rejected plan became visible")
	}
	if len(h.adm.List(chamber)) != 0 {
		t.Fatal("rejected plan left queue entries")
	}
	if h.cycles.HasActive(chamber) {
		t.Fatal("rejected plan started a cycle")
	}
	if len(h.eventTypes(t)) != 0 {
		t.Fatalf("rejected plan recorded events: %v", h.eventTypes(t))
	}
}

func TestConcurrentSubmissionsHaveOneWinner(t *testing.T) {
	h := newHarness(t, journal.NewMemoryStore(), time.Now)
	const contenders = 32
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make(chan uuid.UUID, contenders)
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			plan, _, err := h.planner.Submit(context.Background(), chamber, model.DirectionUpstream, vessels(2))
			if err == nil {
				results <- plan.ID
			}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	var winners []uuid.UUID
	for id := range results {
		winners = append(winners, id)
	}
	if len(winners) != 1 {
		t.Fatalf("winners = %d, want exactly 1", len(winners))
	}
	winner := winners[0]
	if active := h.planner.Active(chamber); len(active) != 1 || active[0].ID != winner {
		t.Fatalf("active plans = %v, want only the winner", active)
	}
	entries := h.adm.List(chamber)
	if len(entries) != 2 {
		t.Fatalf("queue len = %d, want only the winner's 2 vessels", len(entries))
	}
	for _, entry := range entries {
		if entry.PlanID != winner {
			t.Fatal("loser plan leaked into the queue")
		}
	}
	current, ok := h.cycles.Current(chamber)
	if !ok || current.PlanID != winner {
		t.Fatal("current cycle is not the winner's")
	}
	kinds := h.eventTypes(t)
	counts := map[model.EventType]int{}
	for _, kind := range kinds {
		counts[kind]++
	}
	if counts[model.EventPlanCreated] != 1 || counts[model.EventCycleCreated] != 1 || counts[model.EventSignalChanged] != 1 {
		t.Fatalf("expected one success of each event, got %v", counts)
	}
}

// failingStore fails Append for a chosen event type while delegating the rest.
type failingStore struct {
	journal.Store
	fail model.EventType
}

func (s *failingStore) Append(ctx context.Context, event model.Event) (model.Event, error) {
	if event.Type == s.fail {
		return model.Event{}, errors.New("forced journal failure")
	}
	return s.Store.Append(ctx, event)
}

func TestCycleEventFailureRollsEverythingBack(t *testing.T) {
	store := &failingStore{Store: journal.NewMemoryStore(), fail: model.EventCycleCreated}
	h := newHarness(t, store, time.Now)
	_, _, err := h.planner.Submit(context.Background(), chamber, model.DirectionUpstream, vessels(1))
	if err == nil {
		t.Fatal("expected submit to fail")
	}
	if len(h.planner.Active(chamber)) != 0 || len(h.adm.List(chamber)) != 0 || h.cycles.HasActive(chamber) {
		t.Fatal("cycle journal failure left plan, queue or cycle state behind")
	}
}

func TestPlanEventFailureAbortsCycleAndQueue(t *testing.T) {
	store := &failingStore{Store: journal.NewMemoryStore(), fail: model.EventPlanCreated}
	h := newHarness(t, store, time.Now)
	_, _, err := h.planner.Submit(context.Background(), chamber, model.DirectionUpstream, vessels(1))
	if err == nil {
		t.Fatal("expected submit to fail")
	}
	if len(h.planner.Active(chamber)) != 0 {
		t.Fatal("failed plan must not be visible")
	}
	if len(h.adm.List(chamber)) != 0 {
		t.Fatal("failed plan left queue entries")
	}
	if h.cycles.HasActive(chamber) {
		t.Fatal("aborted cycle must be terminal and not active")
	}
	current, ok := h.cycles.Current(chamber)
	if !ok || !current.Stage.Terminal() {
		t.Fatal("aborted cycle should remain recorded in a terminal stage")
	}
}
