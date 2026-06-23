package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jobqueue/api/internal/domain"
	"github.com/jobqueue/api/internal/schedule"
	"github.com/jobqueue/api/internal/store"
)

var (
	ErrInvalidRunAt           = errors.New("invalid run_at")
	ErrInvalidDelay           = errors.New("invalid delay")
	ErrConflictingSchedule    = errors.New("run_at and delay are mutually exclusive")
	ErrInvalidCron            = errors.New("invalid cron expression")
	ErrInvalidTimezone        = errors.New("invalid timezone")
	ErrScheduleNotFound       = store.ErrNotFound
	ErrScheduleNotCancellable = errors.New("schedule cannot be cancelled in current state")
)

type scheduleFirer interface {
	FireDue(ctx context.Context, now time.Time) (*domain.Job, error)
}

func resolveAvailableAt(now time.Time, req domain.SubmitJobRequest) (time.Time, error) {
	hasRunAt := req.RunAt != ""
	hasDelay := req.Delay != ""
	if hasRunAt && hasDelay {
		return time.Time{}, ErrConflictingSchedule
	}
	if hasRunAt {
		t, err := parseRunAt(req.RunAt)
		if err != nil {
			return time.Time{}, fmt.Errorf("%w: %w", ErrInvalidRunAt, err)
		}
		return t.UTC(), nil
	}
	if hasDelay {
		d, err := time.ParseDuration(req.Delay)
		if err != nil {
			return time.Time{}, fmt.Errorf("%w: %w", ErrInvalidDelay, err)
		}
		if d < 0 {
			return time.Time{}, ErrInvalidDelay
		}
		return now.Add(d), nil
	}
	return now, nil
}

func parseRunAt(raw string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, raw)
}

func (s *Service) CreateSchedule(ctx context.Context, req domain.CreateScheduleRequest) (*domain.Schedule, error) {
	if _, ok := s.handlers.Get(req.Type); !ok {
		return nil, fmt.Errorf("%w: %s", ErrInvalidJobType, req.Type)
	}
	if req.Cron == "" {
		return nil, fmt.Errorf("%w: cron is required", ErrInvalidCron)
	}

	timeout, err := s.resolveTimeout(req.TimeoutPerAttempt)
	if err != nil {
		return nil, err
	}
	maxRetries := s.cfg.DefaultMaxRetries
	if req.MaxRetries > 0 {
		maxRetries = req.MaxRetries
	}
	priority := s.cfg.DefaultPriority
	if req.Priority != 0 {
		priority = req.Priority
	}
	tz := req.Timezone
	if tz == "" {
		tz = "UTC"
	}
	loc, err := schedule.LoadLocation(tz)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidTimezone, err)
	}
	if _, err := schedule.Parser.Parse(req.Cron); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCron, err)
	}

	now := time.Now().UTC()
	nextRun, err := schedule.FirstRun(req.Cron, loc, now)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCron, err)
	}

	if len(req.Payload) == 0 {
		req.Payload = json.RawMessage(`{}`)
	}

	sch := &domain.Schedule{
		ID:                uuid.NewString(),
		Type:              req.Type,
		Payload:           req.Payload,
		Priority:          priority,
		MaxRetries:        maxRetries,
		TimeoutPerAttempt: timeout,
		CronExpr:          req.Cron,
		Timezone:          tz,
		State:             domain.ScheduleActive,
		NextRunAt:         nextRun,
		CreatedAt:         now,
		UpdatedAt:         now,
	}

	if err := s.schedules.Create(ctx, sch); err != nil {
		s.logger.Error("create schedule failed", "error", err)
		return nil, fmt.Errorf("create schedule: %w", err)
	}
	s.logger.Info("schedule created",
		"schedule_id", sch.ID,
		"type", sch.Type,
		"cron", sch.CronExpr,
		"next_run_at", sch.NextRunAt,
	)
	return sch, nil
}

func (s *Service) GetSchedule(ctx context.Context, id string) (*domain.Schedule, error) {
	sch, err := s.schedules.Get(ctx, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, ErrScheduleNotFound
		}
		return nil, err
	}
	return sch, nil
}

func (s *Service) ListSchedules(ctx context.Context) ([]*domain.Schedule, error) {
	return s.schedules.List(ctx)
}

func (s *Service) CancelSchedule(ctx context.Context, id string) (*domain.Schedule, error) {
	sch, err := s.schedules.Get(ctx, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, ErrScheduleNotFound
		}
		return nil, err
	}
	if sch.State != domain.ScheduleActive {
		return nil, ErrScheduleNotCancellable
	}
	now := time.Now().UTC()
	sch.State = domain.ScheduleCancelled
	sch.UpdatedAt = now
	if err := s.schedules.Update(ctx, sch); err != nil {
		return nil, fmt.Errorf("cancel schedule: %w", err)
	}
	s.logger.Info("schedule cancelled", "schedule_id", id)
	return sch, nil
}

func (s *Service) processDueSchedules(ctx context.Context, now time.Time) {
	if firer, ok := s.schedules.(scheduleFirer); ok {
		for {
			job, err := firer.FireDue(ctx, now)
			if err != nil {
				s.logger.Error("fire due schedule failed", "error", err)
				return
			}
			if job == nil {
				return
			}
			s.logger.Info("schedule fired job",
				"schedule_id", job.ScheduleID,
				"job_id", job.ID,
				"available_at", job.AvailableAt,
			)
		}
	}

	due, err := s.schedules.ListDue(ctx, now)
	if err != nil {
		s.logger.Error("list due schedules failed", "error", err)
		return
	}
	for _, sch := range due {
		if err := s.fireSchedule(ctx, sch, now); err != nil {
			s.logger.Error("fire schedule failed", "schedule_id", sch.ID, "error", err)
		}
	}
}

func (s *Service) fireSchedule(ctx context.Context, sch *domain.Schedule, now time.Time) error {
	loc, err := schedule.LoadLocation(sch.Timezone)
	if err != nil {
		return err
	}
	nextRunAt, err := schedule.NextRun(sch.CronExpr, loc, sch.NextRunAt)
	if err != nil {
		return err
	}

	runAt := sch.NextRunAt
	job := &domain.Job{
		ID:                uuid.NewString(),
		Type:              sch.Type,
		Payload:           sch.Payload,
		Priority:          sch.Priority,
		MaxRetries:        sch.MaxRetries,
		TimeoutPerAttempt: sch.TimeoutPerAttempt,
		State:             domain.StateQueued,
		ScheduleID:        sch.ID,
		AvailableAt:       runAt,
		CreatedAt:         now,
		UpdatedAt:         now,
	}

	if ac, ok := s.store.(store.AtomicCreator); ok {
		if err := ac.CreateAndEnqueue(ctx, job); err != nil {
			return fmt.Errorf("enqueue scheduled job: %w", err)
		}
	} else {
		if err := s.store.Create(ctx, job); err != nil {
			return fmt.Errorf("create scheduled job: %w", err)
		}
		s.queue.Enqueue(job.ID, job.Priority, job.AvailableAt)
	}

	lastRun := runAt
	sch.LastRunAt = &lastRun
	sch.NextRunAt = nextRunAt
	sch.UpdatedAt = now
	if err := s.schedules.Update(ctx, sch); err != nil {
		return fmt.Errorf("advance schedule: %w", err)
	}

	s.logger.Info("schedule fired job",
		"schedule_id", sch.ID,
		"job_id", job.ID,
		"next_run_at", sch.NextRunAt,
	)
	return nil
}

func (s *Service) resolveTimeout(raw string) (time.Duration, error) {
	if raw == "" {
		return s.cfg.DefaultTimeout, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrInvalidTimeout, err)
	}
	return d, nil
}
