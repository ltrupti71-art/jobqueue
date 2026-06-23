package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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
	ErrJobNotFound       = store.ErrNotFound
	ErrInvalidJobType    = errors.New("unknown job type")
	ErrInvalidTimeout    = errors.New("invalid timeout_per_attempt")
	ErrJobNotCancellable = errors.New("job cannot be cancelled in current state")
)

type Service struct {
	cfg       config.Config
	store     store.Store
	queue     queue.Queue
	schedules store.ScheduleStore
	handlers  *handler.Registry
	logger    *slog.Logger
}

func New(cfg config.Config, st store.Store, q queue.Queue, schedules store.ScheduleStore, handlers *handler.Registry, logger *slog.Logger) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{cfg: cfg, store: st, queue: q, schedules: schedules, handlers: handlers, logger: logger}
}

func (s *Service) Submit(ctx context.Context, req domain.SubmitJobRequest) (*domain.Job, error) {
	if _, ok := s.handlers.Get(req.Type); !ok {
		s.logger.Warn("submit rejected: unknown job type", "type", req.Type)
		return nil, fmt.Errorf("%w: %s", ErrInvalidJobType, req.Type)
	}

	timeout := s.cfg.DefaultTimeout
	if req.TimeoutPerAttempt != "" {
		d, err := time.ParseDuration(req.TimeoutPerAttempt)
		if err != nil {
			s.logger.Warn("submit rejected: invalid timeout", "timeout", req.TimeoutPerAttempt, "error", err)
			return nil, fmt.Errorf("%w: %w", ErrInvalidTimeout, err)
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
	availableAt, err := resolveAvailableAt(now, req)
	if err != nil {
		s.logger.Warn("submit rejected: invalid schedule", "error", err)
		return nil, err
	}

	job := &domain.Job{
		ID:                uuid.NewString(),
		Type:              req.Type,
		Payload:           req.Payload,
		Priority:          priority,
		MaxRetries:        maxRetries,
		TimeoutPerAttempt: timeout,
		State:             domain.StateQueued,
		AvailableAt:       availableAt,
		CreatedAt:         now,
		UpdatedAt:         now,
	}

	if ac, ok := s.store.(store.AtomicCreator); ok {
		if err := ac.CreateAndEnqueue(ctx, job); err != nil {
			s.logger.Error("submit failed: atomic create", "job_id", job.ID, "type", job.Type, "error", err)
			return nil, fmt.Errorf("submit job: %w", err)
		}
		s.logger.Info("job submitted", "job_id", job.ID, "type", job.Type, "priority", priority, "max_retries", maxRetries, "available_at", availableAt)
		return job, nil
	}

	if err := s.store.Create(ctx, job); err != nil {
		s.logger.Error("submit failed: store create", "job_id", job.ID, "error", err)
		return nil, fmt.Errorf("create job: %w", err)
	}
	s.queue.Enqueue(job.ID, job.Priority, job.AvailableAt)
	s.logger.Info("job submitted", "job_id", job.ID, "type", job.Type, "priority", priority, "max_retries", maxRetries, "available_at", availableAt)
	return job, nil
}

func (s *Service) Get(ctx context.Context, id string) (*domain.Job, error) {
	job, err := s.store.Get(ctx, id)
	if err != nil {
		if errors.Is(err, ErrJobNotFound) {
			s.logger.Debug("job not found", "job_id", id)
		} else {
			s.logger.Error("get job failed", "job_id", id, "error", err)
		}
		return nil, err
	}
	return job, nil
}

func (s *Service) QueueDepth(ctx context.Context) (domain.QueueDepth, error) {
	pending, delayed := s.queue.Depth()
	running, err := s.store.CountByState(ctx, domain.StateRunning)
	if err != nil {
		s.logger.Error("queue depth: count running failed", "error", err)
		return domain.QueueDepth{}, fmt.Errorf("count running: %w", err)
	}
	dead, err := s.store.CountByState(ctx, domain.StateDeadLettered)
	if err != nil {
		s.logger.Error("queue depth: count dead-lettered failed", "error", err)
		return domain.QueueDepth{}, fmt.Errorf("count dead-lettered: %w", err)
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
		s.logger.Warn("cancel rejected: job not queued", "job_id", id, "state", job.State)
		return nil, ErrJobNotCancellable
	}
	if !s.queue.Remove(id) {
		fresh, err := s.store.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		if fresh.State != domain.StateQueued {
			s.logger.Warn("cancel rejected: job picked up by worker", "job_id", id, "state", fresh.State)
			return nil, ErrJobNotCancellable
		}
	}
	now := time.Now().UTC()
	job.State = domain.StateCancelled
	job.UpdatedAt = now
	job.CompletedAt = &now
	if err := s.store.Update(ctx, job); err != nil {
		s.logger.Error("cancel failed: store update", "job_id", id, "error", err)
		return nil, fmt.Errorf("cancel job: %w", err)
	}
	s.logger.Info("job cancelled", "job_id", id)
	return job, nil
}

func (s *Service) Drain(ctx context.Context) (domain.DrainResult, error) {
	ids := s.queue.Drain()
	now := time.Now().UTC()
	cancelled := 0
	for _, id := range ids {
		job, err := s.store.Get(ctx, id)
		if err != nil {
			s.logger.Warn("drain: skip job get failed", "job_id", id, "error", err)
			continue
		}
		if job.State != domain.StateQueued {
			s.logger.Debug("drain: skip non-queued job", "job_id", id, "state", job.State)
			continue
		}
		job.State = domain.StateCancelled
		job.UpdatedAt = now
		job.CompletedAt = &now
		if err := s.store.Update(ctx, job); err != nil {
			s.logger.Error("drain: store update failed", "job_id", id, "error", err)
			continue
		}
		cancelled++
	}
	s.logger.Info("queue drained", "removed_from_queue", len(ids), "cancelled", cancelled)
	return domain.DrainResult{Cancelled: cancelled, JobIDs: ids}, nil
}

func (s *Service) ProcessJob(ctx context.Context, jobID string) {
	job, err := s.store.Get(ctx, jobID)
	if err != nil {
		if errors.Is(err, ErrJobNotFound) {
			s.logger.Warn("process skipped: job not found", "job_id", jobID)
		} else {
			s.logger.Error("process failed: get job", "job_id", jobID, "error", err)
		}
		return
	}
	if job.State != domain.StateQueued {
		s.logger.Debug("process skipped: job not queued", "job_id", jobID, "state", job.State)
		return
	}

	now := time.Now().UTC()
	job.State = domain.StateRunning
	job.AttemptCount++
	job.StartedAt = &now
	job.UpdatedAt = now
	if err := s.store.Update(ctx, job); err != nil {
		s.logger.Error("process failed: mark running", "job_id", jobID, "error", err)
		return
	}

	s.logger.Info("job attempt started",
		"job_id", jobID,
		"type", job.Type,
		"attempt", job.AttemptCount,
		"max_retries", job.MaxRetries,
	)

	h, ok := s.handlers.Get(job.Type)
	if !ok {
		s.logger.Error("process failed: unknown handler", "job_id", jobID, "type", job.Type)
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
		if err := s.store.Update(ctx, job); err != nil {
			s.logger.Error("process failed: persist success", "job_id", jobID, "error", err)
			return
		}
		s.logger.Info("job succeeded", "job_id", jobID, "attempt", job.AttemptCount)
		return
	}

	s.logger.Warn("job attempt failed",
		"job_id", jobID,
		"attempt", job.AttemptCount,
		"error", execErr.Error(),
	)
	s.handleFailure(ctx, job, execErr.Error(), completed)
}

func (s *Service) handleFailure(ctx context.Context, job *domain.Job, errMsg string, failedAt time.Time) {
	job.LastError = errMsg
	job.UpdatedAt = failedAt

	if job.AttemptCount >= job.MaxRetries {
		job.State = domain.StateDeadLettered
		job.CompletedAt = &failedAt
		if err := s.store.Update(ctx, job); err != nil {
			s.logger.Error("dead-letter failed: store update", "job_id", job.ID, "error", err)
			return
		}
		s.logger.Error("job dead-lettered",
			"job_id", job.ID,
			"attempts", job.AttemptCount,
			"last_error", errMsg,
		)
		return
	}

	delay := worker.ExponentialBackoff(job.AttemptCount, s.cfg.BackoffBase, s.cfg.BackoffMax)
	job.State = domain.StateQueued
	job.AvailableAt = failedAt.Add(delay)
	job.StartedAt = nil
	if err := s.store.Update(ctx, job); err != nil {
		s.logger.Error("retry failed: store update", "job_id", job.ID, "error", err)
		return
	}
	s.queue.Enqueue(job.ID, job.Priority, job.AvailableAt)
	s.logger.Info("job scheduled for retry",
		"job_id", job.ID,
		"attempt", job.AttemptCount,
		"backoff", delay.String(),
		"available_at", job.AvailableAt,
	)
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
	if err := s.store.Update(ctx, job); err != nil {
		s.logger.Error("failJob: store update failed", "job_id", job.ID, "error", err)
		return
	}
	s.logger.Error("job dead-lettered", "job_id", job.ID, "last_error", errMsg)
}

func (s *Service) RunScheduler(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.SchedulerInterval)
	defer ticker.Stop()
	s.logger.Info("scheduler started", "interval", s.cfg.SchedulerInterval.String())
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("scheduler stopped")
			return
		case <-ticker.C:
			now := time.Now().UTC()
			if promoted := s.queue.PromoteDue(now); promoted > 0 {
				s.logger.Debug("scheduler promoted delayed jobs", "count", promoted)
			}
			if s.schedules != nil {
				s.processDueSchedules(ctx, now)
			}
		}
	}
}

func (s *Service) ListDeadLettered(ctx context.Context) ([]*domain.Job, error) {
	jobs, err := s.store.ListByState(ctx, domain.StateDeadLettered)
	if err != nil {
		s.logger.Error("list dead-lettered failed", "error", err)
		return nil, fmt.Errorf("list dead-lettered: %w", err)
	}
	return jobs, nil
}

func MarshalResult(v any) (json.RawMessage, error) {
	return json.Marshal(v)
}
