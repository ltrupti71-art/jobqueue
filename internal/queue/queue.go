package queue

import (
	"context"
	"time"
)

// Queue is a shared work queue backed by memory or PostgreSQL.
type Queue interface {
	Enqueue(jobID string, priority int, availableAt time.Time)
	PromoteDue(now time.Time) int
	Dequeue(ctx context.Context) (jobID string, ok bool, err error)
	Remove(jobID string) bool
	Depth() (pending, delayed int)
	Drain() []string
	Close()
}
