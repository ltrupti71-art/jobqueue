package store

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jobqueue/api/internal/domain"
)

type MemoryScheduleStore struct {
	mu        sync.RWMutex
	schedules map[string]*domain.Schedule
}

func NewMemoryScheduleStore() *MemoryScheduleStore {
	return &MemoryScheduleStore{schedules: make(map[string]*domain.Schedule)}
}

func (s *MemoryScheduleStore) Create(_ context.Context, schedule *domain.Schedule) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.schedules[schedule.ID]; exists {
		return errors.New("schedule already exists")
	}
	copy := *schedule
	s.schedules[schedule.ID] = &copy
	return nil
}

func (s *MemoryScheduleStore) Get(_ context.Context, id string) (*domain.Schedule, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sch, ok := s.schedules[id]
	if !ok {
		return nil, ErrNotFound
	}
	copy := *sch
	return &copy, nil
}

func (s *MemoryScheduleStore) Update(_ context.Context, schedule *domain.Schedule) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.schedules[schedule.ID]; !ok {
		return ErrNotFound
	}
	copy := *schedule
	s.schedules[schedule.ID] = &copy
	return nil
}

func (s *MemoryScheduleStore) List(_ context.Context) ([]*domain.Schedule, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*domain.Schedule, 0, len(s.schedules))
	for _, sch := range s.schedules {
		copy := *sch
		out = append(out, &copy)
	}
	return out, nil
}

func (s *MemoryScheduleStore) ListDue(_ context.Context, now time.Time) ([]*domain.Schedule, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*domain.Schedule
	for _, sch := range s.schedules {
		if sch.State == domain.ScheduleActive && !sch.NextRunAt.After(now) {
			copy := *sch
			out = append(out, &copy)
		}
	}
	return out, nil
}
