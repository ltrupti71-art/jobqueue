package store

import (
	"context"
	"time"

	"github.com/jobqueue/api/internal/domain"
)

type ScheduleStore interface {
	Create(ctx context.Context, schedule *domain.Schedule) error
	Get(ctx context.Context, id string) (*domain.Schedule, error)
	Update(ctx context.Context, schedule *domain.Schedule) error
	List(ctx context.Context) ([]*domain.Schedule, error)
	ListDue(ctx context.Context, now time.Time) ([]*domain.Schedule, error)
}
