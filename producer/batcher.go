package producer

import (
	"github.com/Pranav2259/go-kafka-reliable/config"
	"github.com/Pranav2259/go-kafka-reliable/internal"
	"context"
	"sync"
	"time"
)

type Batcher struct {
	cfg       *config.Config
	msgChan   chan internal.Message
	batchChan chan []internal.Message
	done      chan struct{}
	doneWg    sync.WaitGroup
	mu        sync.RWMutex
	started   bool
	stopped   bool
}

func NewBatcher(cfg *config.Config, batchChan chan []internal.Message) *Batcher {
	b := &Batcher{
		cfg:       cfg,
		msgChan:   make(chan internal.Message, cfg.BatchSize),
		batchChan: batchChan,
		done:      make(chan struct{}),
	}
	return b
}

func (b *Batcher) Start(ctx context.Context) {
	b.mu.Lock()
	if b.started || b.stopped {
		b.mu.Unlock()
		return
	}
	b.started = true
	b.doneWg.Add(1)
	b.mu.Unlock()

	go b.run(ctx)
}

func (b *Batcher) Submit(ctx context.Context, msg internal.Message) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.stopped {
		return internal.ErrShutdown
	}

	select {
	case b.msgChan <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-b.done:
		return internal.ErrShutdown
	}
}

func (b *Batcher) Stop() {
	b.mu.Lock()
	if b.stopped {
		b.mu.Unlock()
		return
	}
	b.stopped = true
	close(b.msgChan)
	started := b.started
	b.mu.Unlock()

	if started {
		b.doneWg.Wait()
	}
	close(b.done)
}

func (b *Batcher) run(ctx context.Context) {
	defer b.doneWg.Done()

	batch := make([]internal.Message, 0, b.cfg.BatchSize)
	ticker := time.NewTicker(b.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case msg, ok := <-b.msgChan:
			if !ok {
				if len(batch) > 0 {
					b.sendBatch(ctx, batch)
				}
				return
			}

			batch = append(batch, msg)
			if len(batch) >= b.cfg.BatchSize {
				b.sendBatch(ctx, batch)
				batch = make([]internal.Message, 0, b.cfg.BatchSize)
				ticker.Reset(b.cfg.FlushInterval)
			}

		case <-ticker.C:
			if len(batch) > 0 {
				b.sendBatch(ctx, batch)
				batch = make([]internal.Message, 0, b.cfg.BatchSize)
			}
			ticker.Reset(b.cfg.FlushInterval)

		case <-ctx.Done():
			if len(batch) > 0 {
				b.sendBatch(ctx, batch)
			}
			return
		}
	}
}

func (b *Batcher) sendBatch(ctx context.Context, batch []internal.Message) {
	select {
	case b.batchChan <- batch:
	case <-ctx.Done():
	}
}
