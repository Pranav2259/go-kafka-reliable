package producer

import (
	"github.com/Pranav2259/go-kafka-reliable/config"
	fallbackstore "github.com/Pranav2259/go-kafka-reliable/fallback"
	"github.com/Pranav2259/go-kafka-reliable/health"
	"github.com/Pranav2259/go-kafka-reliable/internal"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type ReliableProducer struct {
	cfg           *config.Config
	batcher       *Batcher
	workerPool    *WorkerPool
	kafkaClient   *KafkaProducer
	fallback      internal.FallbackStore
	retryPolicy   *RetryPolicy
	healthChecker *health.HealthChecker
	replayEngine  *ReplayEngine
	logger        *internal.Logger
	wg            sync.WaitGroup
	mu            sync.Mutex
	isShutdown    bool
}

func New(cfg *config.Config) (*ReliableProducer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	logger := internal.NewLogger(cfg.Debug)

	kafkaClient, err := NewKafkaProducer(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}

	var fallbackStore internal.FallbackStore
	if cfg.FallbackEnabled {
		store, err := fallbackstore.NewSQLiteStore(cfg.FallbackDBPath)
		if err != nil {
			kafkaClient.Close()
			return nil, fmt.Errorf("failed to create fallback store: %w", err)
		}
		fallbackStore = store
	}

	batchChan := make(chan []internal.Message, cfg.Workers)
	batcher := NewBatcher(cfg, batchChan)
	retryPolicy := NewRetryPolicy(cfg)
	healthChecker := health.NewHealthChecker(cfg, kafkaClient, logger)
	replayEngine := NewReplayEngine(cfg, fallbackStore, kafkaClient, logger)

	p := &ReliableProducer{
		cfg:           cfg,
		batcher:       batcher,
		kafkaClient:   kafkaClient,
		fallback:      fallbackStore,
		retryPolicy:   retryPolicy,
		healthChecker: healthChecker,
		replayEngine:  replayEngine,
		logger:        logger,
	}

	workerPool := NewWorkerPool(cfg, batchChan, p.produceWithRetry, logger)
	p.workerPool = workerPool

	if fallbackStore != nil {
		healthChecker.SetRecoveryCallback(func() {
			replayEngine.Start(context.Background())
		})
	}

	return p, nil
}

func (p *ReliableProducer) Start(ctx context.Context) {
	p.batcher.Start(ctx)
	p.workerPool.Start(ctx)
	p.healthChecker.Start(ctx)

	p.wg.Add(1)
	go p.resultProcessor()
}

func (p *ReliableProducer) Send(ctx context.Context, msg internal.Message) error {
	p.mu.Lock()
	if p.isShutdown {
		p.mu.Unlock()
		return internal.ErrShutdown
	}
	p.mu.Unlock()

	if msg.ID == "" {
		msg.ID = generateMessageID()
	}

	return p.batcher.Submit(ctx, msg)
}

func (p *ReliableProducer) produceWithRetry(ctx context.Context, messages []internal.Message) error {
	return p.retryPolicy.Execute(ctx, func(retryCtx context.Context) error {
		return p.kafkaClient.SendMessages(retryCtx, messages)
	})
}

func (p *ReliableProducer) resultProcessor() {
	defer p.wg.Done()

	for result := range p.workerPool.ResultChan() {
		if result.Err == nil {
			continue
		}

		if p.fallback != nil && p.cfg.FallbackEnabled {
			retries := 3
			var lastErr error
			for attempt := 0; attempt < retries; attempt++ {
				fallbackCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				err := p.fallback.Save(fallbackCtx, result.Messages)
				cancel()

				if err == nil {
					lastErr = nil
					break
				}
				lastErr = err

				if attempt < retries-1 {
					time.Sleep(time.Duration((attempt+1)*100) * time.Millisecond)
				}
			}

			if lastErr != nil {
				p.logger.Error("worker=%d batch=%d failed to save %d messages to fallback after %d attempts: %v", result.WorkerID, result.BatchID, len(result.Messages), retries, lastErr)
			} else {
				p.logger.Info("worker=%d batch=%d saved %d messages to fallback (Kafka error: %v)", result.WorkerID, result.BatchID, len(result.Messages), result.Err)
			}
		} else {
			p.logger.Error("worker=%d batch=%d dropped %d messages, fallback disabled: %v", result.WorkerID, result.BatchID, len(result.Messages), result.Err)
		}
	}
}

func (p *ReliableProducer) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	if p.isShutdown {
		p.mu.Unlock()
		return nil
	}
	p.isShutdown = true
	p.mu.Unlock()

	p.healthChecker.Stop()

	p.batcher.Stop()
	p.workerPool.Stop()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}

	var errs error
	if err := p.kafkaClient.Close(); err != nil {
		errs = fmt.Errorf("kafka client close: %w", err)
	}

	if p.fallback != nil {
		if err := p.fallback.Close(); err != nil {
			errs = fmt.Errorf("%v; fallback close: %w", errs, err)
		}
	}

	return errs
}

func generateMessageID() string {
	return fmt.Sprintf("msg_%d_%d", time.Now().UnixNano(), rand.Intn(1000000))
}
