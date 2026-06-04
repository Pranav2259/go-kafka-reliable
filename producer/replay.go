package producer

import (
	"github.com/Pranav2259/go-kafka-reliable/config"
	"github.com/Pranav2259/go-kafka-reliable/internal"
	"context"
	"sync"
)

type ReplayEngine struct {
	cfg         *config.Config
	fallback    internal.FallbackStore
	kafkaClient *KafkaProducer
	retryPolicy *RetryPolicy
	mu          sync.Mutex
	isReplaying bool
	batchSize   int
	logger      *internal.Logger
}

func NewReplayEngine(cfg *config.Config, fallback internal.FallbackStore, kafkaClient *KafkaProducer, logger *internal.Logger) *ReplayEngine {
	return &ReplayEngine{
		cfg:         cfg,
		fallback:    fallback,
		kafkaClient: kafkaClient,
		retryPolicy: NewRetryPolicy(cfg),
		batchSize:   cfg.BatchSize,
		logger:      logger,
	}
}

func (re *ReplayEngine) Start(ctx context.Context) {
	if re.fallback == nil {
		return
	}

	re.mu.Lock()
	if re.isReplaying {
		re.mu.Unlock()
		return
	}
	re.isReplaying = true
	re.mu.Unlock()

	re.logger.Info("fallback replay started")
	go re.replay(ctx)
}

func (re *ReplayEngine) replay(ctx context.Context) {
	defer func() {
		re.mu.Lock()
		re.isReplaying = false
		re.mu.Unlock()
	}()

	totalReplayed := 0

	for {
		messages, err := re.fallback.FetchBatch(ctx, re.batchSize)
		if err != nil {
			re.logger.Error("fallback replay fetch failed: %v", err)
			return
		}

		if len(messages) == 0 {
			if totalReplayed > 0 {
				re.logger.Info("fallback replay complete, replayed %d messages", totalReplayed)
			} else {
				re.logger.Debug("fallback replay complete, no cached messages")
			}
			return
		}

		re.logger.Debug("fallback replay sending batch of %d messages", len(messages))

		err = re.retryPolicy.Execute(ctx, func(retryCtx context.Context) error {
			return re.kafkaClient.SendMessages(retryCtx, messages)
		})
		if err != nil {
			re.logger.Error("fallback replay paused, Kafka send failed for %d messages: %v", len(messages), err)
			return
		}

		successIDs := make([]string, 0, len(messages))
		for _, msg := range messages {
			successIDs = append(successIDs, msg.ID)
		}

		if err := re.fallback.Delete(ctx, successIDs); err != nil {
			re.logger.Error("fallback replay delete failed for %d messages: %v", len(successIDs), err)
			return
		}

		totalReplayed += len(successIDs)
		re.logger.Debug("fallback replay deleted %d messages from store", len(successIDs))
	}
}

func (re *ReplayEngine) IsReplaying() bool {
	re.mu.Lock()
	defer re.mu.Unlock()
	return re.isReplaying
}
