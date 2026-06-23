package store

import (
	"context"
	"errors"
	"sync"

	"github.com/jobqueue/api/internal/domain"
)

var ErrNotFound = errors.New("job not found")

type Store interface {
	Create(ctx context.Context, job *domain.Job) error
	Get(ctx context.Context, id string) (*domain.Job, error)
	Update(ctx context.Context, job *domain.Job) error
	ListByState(ctx context.Context, state domain.JobState) ([]*domain.Job, error)
	CountByState(ctx context.Context, state domain.JobState) (int, error)
}

type MemoryStore struct {
	mu   sync.RWMutex
	jobs map[string]*domain.Job
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{jobs: make(map[string]*domain.Job)}
}

func (s *MemoryStore) Create(_ context.Context, job *domain.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.jobs[job.ID]; exists {
		return errors.New("job already exists")
	}
	copy := *job
	s.jobs[job.ID] = &copy
	return nil
}

func (s *MemoryStore) Get(_ context.Context, id string) (*domain.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[id]
	if !ok {
		return nil, ErrNotFound
	}
	copy := *job
	return &copy, nil
}

func (s *MemoryStore) Update(_ context.Context, job *domain.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[job.ID]; !ok {
		return ErrNotFound
	}
	copy := *job
	s.jobs[job.ID] = &copy
	return nil
}

func (s *MemoryStore) ListByState(_ context.Context, state domain.JobState) ([]*domain.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*domain.Job
	for _, job := range s.jobs {
		if job.State == state {
			copy := *job
			out = append(out, &copy)
		}
	}
	return out, nil
}

func (s *MemoryStore) CountByState(_ context.Context, state domain.JobState) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := 0
	for _, job := range s.jobs {
		if job.State == state {
			count++
		}
	}
	return count, nil
}
