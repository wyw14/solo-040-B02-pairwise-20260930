package cycle

import (
	"github.com/wyw14/cry-96/internal/model"
)

// Service exposes transit state to the API. The submission boundary itself is
// owned by the schedule planner so plan, queue and cycle commit together.
type Service struct {
	coordinator *Coordinator
}

func NewService(coordinator *Coordinator) *Service {
	return &Service{coordinator: coordinator}
}

func (s *Service) Current(chamberID string) (model.TransitCycle, bool) {
	return s.coordinator.Current(chamberID)
}
