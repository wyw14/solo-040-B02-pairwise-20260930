package admission

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/signal"
)

type Decision struct {
	Allowed  bool      `json:"allowed"`
	Reason   string    `json:"reason"`
	VesselID uuid.UUID `json:"vessel_id,omitempty"`
}

// Controller grants admission only from observed sealed gate positions.
type Controller struct {
	queue   *Queue
	gates   *gate.State
	signals *signal.Store
	now     func() time.Time
}

func NewController(queue *Queue, gates *gate.State, signals *signal.Store, now func() time.Time) *Controller {
	if now == nil {
		now = time.Now
	}
	return &Controller{queue: queue, gates: gates, signals: signals, now: now}
}

// Ready reports whether a new cycle for the given plan could be admitted,
// without mutating any signal or queue state. It is the pre-flight gate used
// before a plan is allowed to become visible.
func (c *Controller) Ready(cycle model.TransitCycle) Decision {
	entry, ok := c.queue.Peek(cycle.ChamberID)
	if !ok {
		return Decision{Reason: "queue empty"}
	}
	if entry.PlanID != cycle.PlanID {
		return Decision{VesselID: entry.Vessel.ID, Reason: "head vessel belongs to another plan"}
	}
	if !c.gates.Sealed(cycle.ChamberID) {
		return Decision{VesselID: entry.Vessel.ID, Reason: "chamber gates are not sealed"}
	}
	return Decision{Allowed: true, VesselID: entry.Vessel.ID, Reason: "sealed chamber ready"}
}

func (c *Controller) Evaluate(ctx context.Context, cycle model.TransitCycle) (Decision, error) {
	if err := ctx.Err(); err != nil {
		return Decision{}, err
	}
	decision := c.Ready(cycle)
	if !decision.Allowed {
		return decision, nil
	}
	state := c.signals.Set(decision.VesselID, cycle.ID, cycle.Generation, model.SignalProceed, c.now())
	if state.Aspect != model.SignalProceed || !c.queue.MarkReleased(cycle.ChamberID, decision.VesselID) {
		return Decision{}, errors.New("failed to commit admission decision")
	}
	return decision, nil
}

// Retract reverses an admission decision whose event could not be recorded.
func (c *Controller) Retract(cycle model.TransitCycle, vesselID uuid.UUID) {
	c.signals.Remove(vesselID)
	c.queue.UnmarkReleased(cycle.ChamberID, vesselID)
}

func (c *Controller) OwnerAllowed(vesselID uuid.UUID, owner string, token uint64, lease model.Lease, now time.Time) bool {
	return lease.VesselID == vesselID && lease.Owner == owner &&
		lease.FencingToken == token && lease.Active(now)
}

func (c *Controller) Queue() *Queue { return c.queue }
