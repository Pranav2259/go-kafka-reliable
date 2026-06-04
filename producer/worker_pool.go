package producer

import (
	"github.com/Pranav2259/go-kafka-reliable/config"
	"github.com/Pranav2259/go-kafka-reliable/internal"
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type ProducerFunc func(context.Context, []internal.Message) error

type WorkerPool struct {
	cfg        *config.Config
	batchChan  chan []internal.Message
	resultChan chan WorkResult
	produceFn  ProducerFunc
	shutdownCh chan struct{}
	wg         sync.WaitGroup
	batchSeq   uint64
	logger     *internal.Logger
}

type WorkResult struct {
	Messages []internal.Message
	Err      error
	WorkerID int
	BatchID  uint64
}

func NewWorkerPool(cfg *config.Config, batchChan chan []internal.Message, produceFn ProducerFunc, logger *internal.Logger) *WorkerPool {
	return &WorkerPool{
		cfg:        cfg,
		batchChan:  batchChan,
		resultChan: make(chan WorkResult, cfg.BatchSize),
		produceFn:  produceFn,
		shutdownCh: make(chan struct{}),
		logger:     logger,
	}
}

func (wp *WorkerPool) Start(ctx context.Context) {
	for i := 0; i < wp.cfg.Workers; i++ {
		wp.wg.Add(1)
		go wp.worker(ctx, i+1)
	}
}

func (wp *WorkerPool) Stop() {
	close(wp.batchChan)
	wp.wg.Wait()
	close(wp.resultChan)
}

func (wp *WorkerPool) ResultChan() <-chan WorkResult {
	return wp.resultChan
}

func (wp *WorkerPool) worker(ctx context.Context, workerID int) {
	defer wp.wg.Done()

	for {
		select {
		case batch, ok := <-wp.batchChan:
			if !ok {
				return
			}

			batchID := atomic.AddUint64(&wp.batchSeq, 1)
			started := time.Now()
			wp.logger.Debug("worker=%d batch=%d processing messages=%d", workerID, batchID, len(batch))

			err := wp.produceFn(ctx, batch)
			if err == nil {
				wp.logger.Debug("worker=%d batch=%d done messages=%d duration=%s", workerID, batchID, len(batch), time.Since(started).Round(time.Millisecond))
				continue
			}

			wp.logger.Error("worker=%d batch=%d failed messages=%d duration=%s err=%v", workerID, batchID, len(batch), time.Since(started).Round(time.Millisecond), err)
			select {
			case wp.resultChan <- WorkResult{
				Messages: batch,
				Err:      err,
				WorkerID: workerID,
				BatchID:  batchID,
			}:
			case <-ctx.Done():
				return
			}

		case <-ctx.Done():
			return
		}
	}
}
