package queue

import (
	"context"
	"testing"
	"time"
)

func TestPriorityQueueOrder(t *testing.T) {
	q := New()
	now := time.Now()

	q.Enqueue("low", 1, now)
	q.Enqueue("high", 10, now)
	q.Enqueue("mid", 5, now)

	ctx := context.Background()
	id1, ok, _ := q.Dequeue(ctx)
	if !ok || id1 != "high" {
		t.Fatalf("first dequeue: got %q, want high", id1)
	}
	id2, ok, _ := q.Dequeue(ctx)
	if !ok || id2 != "mid" {
		t.Fatalf("second dequeue: got %q, want mid", id2)
	}
	id3, ok, _ := q.Dequeue(ctx)
	if !ok || id3 != "low" {
		t.Fatalf("third dequeue: got %q, want low", id3)
	}
}

func TestDelayedPromotion(t *testing.T) {
	q := New()
	future := time.Now().Add(200 * time.Millisecond)
	q.Enqueue("delayed", 1, future)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, ok, err := q.Dequeue(ctx)
	if ok {
		t.Fatal("expected no job before available_at")
	}
	if err == nil {
		t.Fatal("expected context deadline exceeded")
	}

	q.PromoteDue(time.Now().Add(300 * time.Millisecond))
	id, ok, _ := q.Dequeue(context.Background())
	if !ok || id != "delayed" {
		t.Fatalf("expected delayed job, got %q ok=%v", id, ok)
	}
}

func TestDrain(t *testing.T) {
	q := New()
	now := time.Now()
	q.Enqueue("a", 1, now)
	q.Enqueue("b", 2, now.Add(time.Minute))

	ids := q.Drain()
	if len(ids) != 2 {
		t.Fatalf("drain count: got %d, want 2", len(ids))
	}
	pending, delayed := q.Depth()
	if pending != 0 || delayed != 0 {
		t.Fatalf("queue not empty after drain: pending=%d delayed=%d", pending, delayed)
	}
}

func TestRemove(t *testing.T) {
	q := New()
	now := time.Now()
	q.Enqueue("keep", 1, now)
	q.Enqueue("remove", 1, now)

	if !q.Remove("remove") {
		t.Fatal("expected remove to succeed")
	}
	id, ok, _ := q.Dequeue(context.Background())
	if !ok || id != "keep" {
		t.Fatalf("got %q, want keep", id)
	}
}

func TestFIFOWithinSamePriority(t *testing.T) {
	q := New()
	base := time.Now()
	// Stagger available_at so heap ordering is deterministic for equal priority
	q.Enqueue("first", 5, base)
	q.Enqueue("second", 5, base.Add(time.Nanosecond))
	q.Enqueue("third", 5, base.Add(2*time.Nanosecond))

	for _, want := range []string{"first", "second", "third"} {
		id, ok, _ := q.Dequeue(context.Background())
		if !ok || id != want {
			t.Fatalf("got %q, want %q", id, want)
		}
	}
}

func TestRemoveFromDelayed(t *testing.T) {
	q := New()
	future := time.Now().Add(time.Minute)
	q.Enqueue("delayed", 1, future)

	if !q.Remove("delayed") {
		t.Fatal("expected remove from delayed queue")
	}
	pending, delayed := q.Depth()
	if pending != 0 || delayed != 0 {
		t.Fatalf("queue not empty: pending=%d delayed=%d", pending, delayed)
	}
}

func TestEnqueueOnClosedQueue(t *testing.T) {
	q := New()
	q.Close()
	q.Enqueue("ignored", 1, time.Now())

	id, ok, _ := q.Dequeue(context.Background())
	if ok {
		t.Fatalf("expected no job from closed queue, got %q", id)
	}
}

func TestDequeueAfterCloseWithPending(t *testing.T) {
	q := New()
	q.Enqueue("pending", 1, time.Now())
	q.Close()

	id, ok, _ := q.Dequeue(context.Background())
	if !ok || id != "pending" {
		t.Fatalf("expected pending job drained before close, got %q ok=%v", id, ok)
	}
	id, ok, _ = q.Dequeue(context.Background())
	if ok {
		t.Fatalf("expected empty after draining closed queue, got %q", id)
	}
}

func TestDequeueContextCancel(t *testing.T) {
	q := New()
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, ok, err := q.Dequeue(ctx)
		if ok {
			t.Error("expected no job")
		}
		if err == nil {
			t.Error("expected context error")
		}
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done
}
