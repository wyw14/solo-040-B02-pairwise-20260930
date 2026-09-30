package admission

import (
	"testing"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

func TestPeekSkipsReleasedEntries(t *testing.T) {
	queue := NewQueue()
	first := uuid.New()
	second := uuid.New()
	vesselsA := []model.Vessel{{ID: uuid.New(), Name: "a", CallSign: "A", LengthM: 1}}
	vesselsB := []model.Vessel{{ID: uuid.New(), Name: "b", CallSign: "B", LengthM: 1}}
	if err := queue.Add("chamber-1", first, vesselsA); err != nil {
		t.Fatalf("add a: %v", err)
	}
	if err := queue.Add("chamber-1", second, vesselsB); err != nil {
		t.Fatalf("add b: %v", err)
	}

	head, ok := queue.Peek("chamber-1")
	if !ok || head.PlanID != first {
		t.Fatal("peek must start at the first convoy")
	}
	if !queue.MarkReleased("chamber-1", vesselsA[0].ID) {
		t.Fatal("mark released failed")
	}
	head, ok = queue.Peek("chamber-1")
	if !ok || head.PlanID != second {
		t.Fatal("peek must advance to the next convoy once the head is released")
	}

	queue.UnmarkReleased("chamber-1", vesselsA[0].ID)
	head, ok = queue.Peek("chamber-1")
	if !ok || head.PlanID != first {
		t.Fatal("unmarking must restore the head to the waiting position")
	}
}
