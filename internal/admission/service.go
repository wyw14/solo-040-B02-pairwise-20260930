package admission

import (
	"context"
	"time"

	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// Service records queue changes and delegates safe release decisions.
type Service struct {
	controller *Controller
	recorder   *journal.Recorder
	now        func() time.Time
}

func NewService(controller *Controller, recorder *journal.Recorder, now func() time.Time) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{controller: controller, recorder: recorder, now: now}
}

// Enqueue reserves queue space tentatively. It neither records an event nor
// exposes a committed plan: the caller must either commit (record the
// plan.created event itself) or roll back with RemovePlan.
func (s *Service) Enqueue(_ context.Context, plan model.ConvoyPlan) error {
	return s.controller.queue.Add(plan.ChamberID, plan.ID, plan.Vessels)
}

// Ready is a non-mutating admission pre-check.
func (s *Service) Ready(cycle model.TransitCycle) Decision {
	return s.controller.Ready(cycle)
}

func (s *Service) Admit(ctx context.Context, cycle model.TransitCycle) (Decision, error) {
	decision, err := s.controller.Evaluate(ctx, cycle)
	if err != nil || !decision.Allowed {
		return decision, err
	}
	if _, err := s.recorder.Record(ctx, cycle.ChamberID, model.EventSignalChanged, cycle.ID, cycle.Generation, decision); err != nil {
		s.controller.Retract(cycle, decision.VesselID)
		return Decision{}, err
	}
	return decision, nil
}

func (s *Service) RemovePlan(chamberID string, plan model.ConvoyPlan) []QueueEntry {
	return s.controller.queue.RemovePlan(chamberID, plan.ID)
}

func (s *Service) Restore(chamberID string, entries []QueueEntry) {
	s.controller.queue.Restore(chamberID, entries)
}

func (s *Service) List(chamberID string) []QueueEntry {
	return s.controller.queue.List(chamberID)
}
