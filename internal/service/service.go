package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jobqueue/api/internal/config"
	"github.com/jobqueue/api/internal/domain"
	"github.com/jobqueue/api/internal/handler"
	"github.com/jobqueue/api/internal/queue"
	"github.com/jobqueue/api/internal/store"
	"github.com/jobqueue/api/internal/worker"
)

var (
	ErrJobNotFound      = store.ErrNotFound
	ErrInvalidJobType   = errors.New("unknown job type")
	ErrJobNotCancellable = errors.New("job cannot be cancelled in current state")
)

type Service struct {
	cfg      config.Config
	store    store.Store
	queue    *queue.Queue
	handlers *handler.Registry
}

func New(cfg config.Config, st store.Store, q *queue.Queue, handlers *handler.Registry) *Service {
	return &Service{cfg: cfg, store: st, queue: q, handlers: handlers}
}

func (s *Service) Submit(ctx context.Context, req domain.SubmitJobRequest) (*domain.Job, error) {
	if _, ok := s.handlers.Get(req.Type); !ok {
		return nil, fmt.Errorf("%w: %s", ErrInvalidJobType, req.Type)
	}

	timeout := s.cfg.DefaultTimeout
	if req.TimeoutPerAttempt != "" {
		d, err := time.ParseDuration(req.TimeoutPerAttempt)
		if err != nil {
			return nil, fmt.Errorf("invalid timeout_per_attempt: %w", err)
		}
		timeout = d
	}

	maxRetries := s.cfg.DefaultMaxRetries
	if req.MaxRetries > 0 {
		maxRetries = req.MaxRetries
	}

	priority := s.cfg.DefaultPriority
	if req.Priority != 0 {
		priority = req.Priority
	}

	now := time.Now().UTC()
	job := &domain.Job{
		ID:                uuid.NewString(),
		Type:              req.Type,
		Payload:           req.Payload,
		Priority:          priority,
		MaxRetries:        maxRetries,
		TimeoutPerAttempt: timeout,
		State:             domain.StateQueued,
		AvailableAt:       now,
		CreatedAt:         now,
		UpdatedAt:         now,
	}

	if err := s.store.Create(ctx, job); err != nil {
		return nil, err
	}
	s.queue.Enqueue(job.ID, job.Priority, job.AvailableAt)
	return job, nil
}

func (s *Service) Get(ctx context.Context, id string) (*domain.Job, error) {
	return s.store.Get(ctx, id)
}

func (s *Service) QueueDepth(ctx context.Context) (domain.QueueDepth, error) {
	pending, delayed := s.queue.Depth()
	running, err := s.store.CountByState(ctx, domain.StateRunning)
	if err != nil {
		return domain.QueueDepth{}, err
	}
	dead, err := s.store.CountByState(ctx, domain.StateDeadLettered)
	if err != nil {
		return domain.QueueDepth{}, err
	}
	return domain.QueueDepth{
		Pending:      pending,
		Delayed:      delayed,
		Running:      running,
		DeadLettered: dead,
		TotalActive:  pending + delayed + running,
	}, nil
}

func (s *Service) Cancel(ctx context.Context, id string) (*domain.Job, error) {
	job, err := s.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if job.State != domain.StateQueued {
		return nil, ErrJobNotCancellable
	}
	if !s.queue.Remove(id) {
		// Job may have been picked up by a worker between check and remove.
		fresh, err := s.store.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		if fresh.State != domain.StateQueued {
			return nil, ErrJobNotCancellable
		}
	}
	now := time.Now().UTC()
	job.State = domain.StateCancelled
	job.UpdatedAt = now
	job.CompletedAt = &now
	if err := s.store.Update(ctx, job); err != nil {
		return nil, err
	}
	return job, nil
}

func (s *Service) Drain(ctx context.Context) (domain.DrainResult, error) {
	ids := s.queue.Drain()
	now := time.Now().UTC()
	for _, id := range ids {
		job, err := s.store.Get(ctx, id)
		if err != nil {
			continue
		}
		if job.State != domain.StateQueued {
			continue
		}
		job.State = domain.StateCancelled
		job.UpdatedAt = now
		job.CompletedAt = &now
		_ = s.store.Update(ctx, job)
	}
	return domain.DrainResult{Cancelled: len(ids), JobIDs: ids}, nil
}

func (s *Service) ProcessJob(ctx context.Context, jobID string) {
	job, err := s.store.Get(ctx, jobID)
	if err != nil {
		return
	}
	if job.State != domain.StateQueued {
		return
	}

	now := time.Now().UTC()
	job.State = domain.StateRunning
	job.AttemptCount++
	job.StartedAt = &now
	job.UpdatedAt = now
	if err := s.store.Update(ctx, job); err != nil {
		return
	}

	h, ok := s.handlers.Get(job.Type)
	if !ok {
		s.failJob(ctx, job, fmt.Sprintf("unknown handler: %s", job.Type), false)
		return
	}

	attemptCtx, cancel := context.WithTimeout(ctx, job.TimeoutPerAttempt)
	defer cancel()

	result, execErr := h(attemptCtx, job.Payload)
	completed := time.Now().UTC()

	if execErr == nil {
		job.State = domain.StateSucceeded
		job.Result = result
		job.LastError = ""
		job.CompletedAt = &completed
		job.UpdatedAt = completed
		_ = s.store.Update(ctx, job)
		return
	}

	s.handleFailure(ctx, job, execErr.Error(), completed)
}

func (s *Service) handleFailure(ctx context.Context, job *domain.Job, errMsg string, failedAt time.Time) {
	job.LastError = errMsg
	job.UpdatedAt = failedAt

	if job.AttemptCount >= job.MaxRetries {
		job.State = domain.StateDeadLettered
		job.CompletedAt = &failedAt
		_ = s.store.Update(ctx, job)
		return
	}

	delay := worker.ExponentialBackoff(job.AttemptCount, s.cfg.BackoffBase, s.cfg.BackoffMax)
	job.State = domain.StateQueued
	job.AvailableAt = failedAt.Add(delay)
	job.StartedAt = nil
	_ = s.store.Update(ctx, job)
	s.queue.Enqueue(job.ID, job.Priority, job.AvailableAt)
}

func (s *Service) failJob(ctx context.Context, job *domain.Job, errMsg string, retry bool) {
	now := time.Now().UTC()
	job.LastError = errMsg
	job.UpdatedAt = now
	if retry && job.AttemptCount < job.MaxRetries {
		s.handleFailure(ctx, job, errMsg, now)
		return
	}
	job.State = domain.StateDeadLettered
	job.CompletedAt = &now
	_ = s.store.Update(ctx, job)
}

func (s *Service) RunScheduler(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.SchedulerInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.queue.PromoteDue(time.Now().UTC())
		}
	}
}

func (s *Service) ListDeadLettered(ctx context.Context) ([]*domain.Job, error) {
	return s.store.ListByState(ctx, domain.StateDeadLettered)
}

func MarshalResult(v any) (json.RawMessage, error) {
	return json.Marshal(v)
}
