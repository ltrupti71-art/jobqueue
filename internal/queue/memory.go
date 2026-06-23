package queue

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

type Item struct {
	JobID       string
	Priority    int
	AvailableAt time.Time
	index       int
}

type priorityQueue []*Item

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	if pq[i].Priority != pq[j].Priority {
		return pq[i].Priority > pq[j].Priority
	}
	return pq[i].AvailableAt.Before(pq[j].AvailableAt)
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *priorityQueue) Push(x any) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[:n-1]
	return item
}

// MemoryQueue is an in-process priority queue for local development.
type MemoryQueue struct {
	mu      sync.Mutex
	ready   priorityQueue
	delayed priorityQueue
	cond    *sync.Cond
	closed  bool
}

func NewMemory() *MemoryQueue {
	q := &MemoryQueue{}
	q.cond = sync.NewCond(&q.mu)
	heap.Init(&q.ready)
	heap.Init(&q.delayed)
	return q
}

// New returns an in-memory queue (alias for local dev).
func New() Queue {
	return NewMemory()
}

func (q *MemoryQueue) Enqueue(jobID string, priority int, availableAt time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	item := &Item{JobID: jobID, Priority: priority, AvailableAt: availableAt}
	if availableAt.After(time.Now()) {
		heap.Push(&q.delayed, item)
	} else {
		heap.Push(&q.ready, item)
		q.cond.Signal()
	}
}

func (q *MemoryQueue) PromoteDue(now time.Time) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	promoted := 0
	for q.delayed.Len() > 0 && !q.delayed[0].AvailableAt.After(now) {
		item := heap.Pop(&q.delayed).(*Item)
		item.AvailableAt = now
		heap.Push(&q.ready, item)
		promoted++
	}
	if promoted > 0 {
		q.cond.Broadcast()
	}
	return promoted
}

func (q *MemoryQueue) Dequeue(ctx context.Context) (string, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for {
		if q.closed && q.ready.Len() == 0 {
			return "", false, nil
		}
		if q.ready.Len() > 0 {
			item := heap.Pop(&q.ready).(*Item)
			return item.JobID, true, nil
		}
		if q.closed {
			return "", false, nil
		}

		done := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				q.cond.Broadcast()
			case <-done:
			}
		}()
		q.cond.Wait()
		close(done)

		if ctx.Err() != nil {
			return "", false, ctx.Err()
		}
	}
}

func (q *MemoryQueue) Remove(jobID string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if removed := q.removeFrom(&q.ready, jobID); removed {
		return true
	}
	return q.removeFrom(&q.delayed, jobID)
}

func (q *MemoryQueue) removeFrom(pq *priorityQueue, jobID string) bool {
	for i, item := range *pq {
		if item.JobID == jobID {
			heap.Remove(pq, i)
			return true
		}
	}
	return false
}

func (q *MemoryQueue) Depth() (pending, delayed int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.ready.Len(), q.delayed.Len()
}

func (q *MemoryQueue) Drain() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	var ids []string
	for q.ready.Len() > 0 {
		item := heap.Pop(&q.ready).(*Item)
		ids = append(ids, item.JobID)
	}
	for q.delayed.Len() > 0 {
		item := heap.Pop(&q.delayed).(*Item)
		ids = append(ids, item.JobID)
	}
	return ids
}

func (q *MemoryQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.cond.Broadcast()
}
