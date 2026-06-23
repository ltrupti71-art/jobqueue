package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jobqueue/api/internal/domain"
)

func testJob(id string, state domain.JobState) *domain.Job {
	now := time.Now().UTC()
	return &domain.Job{
		ID:                id,
		Type:              "echo",
		Payload:           json.RawMessage(`{}`),
		State:             state,
		MaxRetries:        3,
		TimeoutPerAttempt: time.Second,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
}

func TestCreateAndGet(t *testing.T) {
	st := NewMemoryStore()
	ctx := context.Background()
	job := testJob("job-1", domain.StateQueued)

	if err := st.Create(ctx, job); err != nil {
		t.Fatal(err)
	}

	got, err := st.Get(ctx, "job-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != job.ID || got.State != domain.StateQueued {
		t.Fatalf("got %+v", got)
	}
}

func TestCreateDuplicate(t *testing.T) {
	st := NewMemoryStore()
	ctx := context.Background()
	job := testJob("job-1", domain.StateQueued)

	if err := st.Create(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := st.Create(ctx, job); err == nil {
		t.Fatal("expected error on duplicate create")
	}
}

func TestGetNotFound(t *testing.T) {
	st := NewMemoryStore()
	_, err := st.Get(context.Background(), "missing")
	if err != ErrNotFound {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func TestUpdate(t *testing.T) {
	st := NewMemoryStore()
	ctx := context.Background()
	job := testJob("job-1", domain.StateQueued)
	_ = st.Create(ctx, job)

	job.State = domain.StateSucceeded
	if err := st.Update(ctx, job); err != nil {
		t.Fatal(err)
	}

	got, _ := st.Get(ctx, "job-1")
	if got.State != domain.StateSucceeded {
		t.Fatalf("state: got %s", got.State)
	}
}

func TestUpdateNotFound(t *testing.T) {
	st := NewMemoryStore()
	err := st.Update(context.Background(), testJob("missing", domain.StateQueued))
	if err != ErrNotFound {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func TestListByState(t *testing.T) {
	st := NewMemoryStore()
	ctx := context.Background()
	_ = st.Create(ctx, testJob("a", domain.StateQueued))
	_ = st.Create(ctx, testJob("b", domain.StateQueued))
	_ = st.Create(ctx, testJob("c", domain.StateSucceeded))

	queued, err := st.ListByState(ctx, domain.StateQueued)
	if err != nil {
		t.Fatal(err)
	}
	if len(queued) != 2 {
		t.Fatalf("queued count: got %d, want 2", len(queued))
	}
}

func TestCountByState(t *testing.T) {
	st := NewMemoryStore()
	ctx := context.Background()
	_ = st.Create(ctx, testJob("a", domain.StateDeadLettered))
	_ = st.Create(ctx, testJob("b", domain.StateDeadLettered))

	count, err := st.CountByState(ctx, domain.StateDeadLettered)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("count: got %d, want 2", count)
	}
}

func TestGetReturnsCopy(t *testing.T) {
	st := NewMemoryStore()
	ctx := context.Background()
	job := testJob("job-1", domain.StateQueued)
	_ = st.Create(ctx, job)

	got, _ := st.Get(ctx, "job-1")
	got.State = domain.StateRunning

	fresh, _ := st.Get(ctx, "job-1")
	if fresh.State != domain.StateQueued {
		t.Fatal("Get should return a copy, not a reference")
	}
}
