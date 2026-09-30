package schedule

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/admission"
	"github.com/wyw14/cry-96/internal/cycle"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// abortReason marks cycles created by a submission that could not commit.
const abortReason = "submission aborted before commit"

// Planner owns convoy plans and their admission queue membership.
type Planner struct {
	mu        sync.RWMutex
	dispatch  map[string]*sync.Mutex
	plans     map[uuid.UUID]model.ConvoyPlan
	admission *admission.Service
	cycles    *cycle.Coordinator
	recorder  *journal.Recorder
	now       func() time.Time
}

func NewPlanner(admissionService *admission.Service, cycles *cycle.Coordinator, recorder *journal.Recorder, now func() time.Time) *Planner {
	if now == nil {
		now = time.Now
	}
	return &Planner{
		dispatch:  make(map[string]*sync.Mutex),
		plans:     make(map[uuid.UUID]model.ConvoyPlan),
		admission: admissionService, cycles: cycles,
		recorder: recorder, now: now,
	}
}

// chamberLock serializes every submission for one chamber so concurrent
// requests can produce at most one committed plan and cycle.
func (p *Planner) chamberLock(chamberID string) *sync.Mutex {
	p.mu.Lock()
	defer p.mu.Unlock()
	lock, ok := p.dispatch[chamberID]
	if !ok {
		lock = &sync.Mutex{}
		p.dispatch[chamberID] = lock
	}
	return lock
}

// Submit creates a plan, reserves its queue position, starts the transit
// cycle and grants head-of-queue admission as a single observable boundary.
// On any failure the plan is never inserted, the tentative queue entries are
// removed and no success event is left behind; concurrent submissions for the
// same chamber are serialized so exactly one can succeed.
func (p *Planner) Submit(ctx context.Context, chamberID string, direction model.Direction, vessels []model.Vessel) (model.ConvoyPlan, model.TransitCycle, error) {
	plan, err := model.NewConvoyPlan(chamberID, direction, vessels, p.now())
	if err != nil {
		return model.ConvoyPlan{}, model.TransitCycle{}, err
	}

	lock := p.chamberLock(chamberID)
	lock.Lock()
	defer lock.Unlock()

	if p.cycles.HasActive(chamberID) {
		return model.ConvoyPlan{}, model.TransitCycle{}, errors.New("chamber already has an active transit")
	}

	// Tentative reservation: visible to the pre-check below, rolled back on
	// every failure path and never exposed as a committed plan.
	if err := p.admission.Enqueue(ctx, plan); err != nil {
		return model.ConvoyPlan{}, model.TransitCycle{}, err
	}
	rollbackQueue := func() { p.admission.RemovePlan(chamberID, plan) }

	probe := model.TransitCycle{ChamberID: chamberID, PlanID: plan.ID}
	if decision := p.admission.Ready(probe); !decision.Allowed {
		rollbackQueue()
		return model.ConvoyPlan{}, model.TransitCycle{}, errors.New(decision.Reason)
	}

	cycleValue, err := p.cycles.Start(ctx, plan)
	if err != nil {
		rollbackQueue()
		return model.ConvoyPlan{}, model.TransitCycle{}, err
	}

	if _, err := p.recorder.Record(ctx, chamberID, model.EventPlanCreated, plan.ID, 0, plan); err != nil {
		_ = p.cycles.Abort(context.WithoutCancel(ctx), cycleValue, abortReason)
		rollbackQueue()
		return model.ConvoyPlan{}, model.TransitCycle{}, err
	}

	decision, err := p.admission.Admit(ctx, cycleValue)
	if err != nil {
		_ = p.cycles.Abort(context.WithoutCancel(ctx), cycleValue, abortReason)
		rollbackQueue()
		return model.ConvoyPlan{}, model.TransitCycle{}, err
	}
	if !decision.Allowed {
		_ = p.cycles.Abort(context.WithoutCancel(ctx), cycleValue, abortReason)
		rollbackQueue()
		return model.ConvoyPlan{}, model.TransitCycle{}, errors.New(decision.Reason)
	}

	// Commit point: only now does the plan become visible in listings.
	p.mu.Lock()
	p.plans[plan.ID] = plan
	p.mu.Unlock()
	return plan.Clone(), cycleValue.Clone(), nil
}

func (p *Planner) Get(planID uuid.UUID) (model.ConvoyPlan, bool) {
	p.mu.RLock()
	plan, ok := p.plans[planID]
	p.mu.RUnlock()
	return plan.Clone(), ok
}

func (p *Planner) replace(plan model.ConvoyPlan) {
	p.mu.Lock()
	p.plans[plan.ID] = plan.Clone()
	p.mu.Unlock()
}

// Restore reinserts a plan recovered from the journal during startup.
func (p *Planner) Restore(plan model.ConvoyPlan) { p.replace(plan) }

func (p *Planner) Active(chamberID string) []model.ConvoyPlan {
	p.mu.RLock()
	var result []model.ConvoyPlan
	for _, plan := range p.plans {
		if plan.ChamberID == chamberID && !plan.Cancelled {
			result = append(result, plan.Clone())
		}
	}
	p.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result
}

func (p *Planner) require(planID uuid.UUID) (model.ConvoyPlan, error) {
	plan, ok := p.Get(planID)
	if !ok {
		return model.ConvoyPlan{}, errors.New("convoy plan not found")
	}
	return plan, nil
}
