package worker

import (
	"context"
	"log/slog"
	"sync"

	"github.com/jobqueue/api/internal/queue"
)

type JobProcessor interface {
	ProcessJob(ctx context.Context, jobID string)
}

type Pool struct {
	count     int
	queue     queue.Queue
	processor JobProcessor
	logger    *slog.Logger
	wg        sync.WaitGroup
}

func NewPool(count int, q queue.Queue, processor JobProcessor, logger *slog.Logger) *Pool {
	if logger == nil {
		logger = slog.Default()
	}
	return &Pool{count: count, queue: q, processor: processor, logger: logger}
}

func (p *Pool) Start(ctx context.Context) {
	for i := 0; i < p.count; i++ {
		p.wg.Add(1)
		go p.run(ctx, i)
	}
}

func (p *Pool) run(ctx context.Context, id int) {
	defer p.wg.Done()
	p.logger.Info("worker started", "worker_id", id)
	for {
		jobID, ok, err := p.queue.Dequeue(ctx)
		if err != nil {
			if ctx.Err() != nil {
				p.logger.Info("worker stopping", "worker_id", id)
				return
			}
			p.logger.Error("dequeue error", "worker_id", id, "error", err)
			continue
		}
		if !ok {
			if ctx.Err() != nil {
				p.logger.Info("worker stopping", "worker_id", id)
				return
			}
			continue
		}
		p.logger.Info("processing job", "worker_id", id, "job_id", jobID)
		p.processor.ProcessJob(ctx, jobID)
	}
}

func (p *Pool) Wait() {
	p.wg.Wait()
}

func (p *Pool) Stop(q queue.Queue) {
	q.Close()
	p.wg.Wait()
}
