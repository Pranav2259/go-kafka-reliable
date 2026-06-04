package health

import (
	"github.com/Pranav2259/go-kafka-reliable/config"
	"github.com/Pranav2259/go-kafka-reliable/internal"
	"context"
	"sync"
	"time"
)

type HealthChecker struct {
	cfg            *config.Config
	kafkaChecker   KafkaChecker
	status         *internal.KafkaStatus
	mu             sync.RWMutex
	onRecovery     func()
	done           chan struct{}
	healthInterval time.Duration
	logger         *internal.Logger
}

type KafkaChecker interface {
	Ping(ctx context.Context) error
	Close() error
}

func NewHealthChecker(cfg *config.Config, checker KafkaChecker, logger *internal.Logger) *HealthChecker {
	return &HealthChecker{
		cfg:          cfg,
		kafkaChecker: checker,
		status: &internal.KafkaStatus{
			Connected: true,
		},
		done:           make(chan struct{}),
		healthInterval: 5 * time.Second,
		logger:         logger,
	}
}

func (hc *HealthChecker) Start(ctx context.Context) {
	go hc.healthLoop(ctx)
}

func (hc *HealthChecker) Stop() {
	close(hc.done)
}

func (hc *HealthChecker) SetRecoveryCallback(callback func()) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	hc.onRecovery = callback
}

func (hc *HealthChecker) Status() internal.KafkaStatus {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	return *hc.status
}

func (hc *HealthChecker) IsConnected() bool {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	return hc.status.Connected
}

func (hc *HealthChecker) healthLoop(ctx context.Context) {
	ticker := time.NewTicker(hc.healthInterval)
	defer ticker.Stop()

	hc.check(ctx)

	for {
		select {
		case <-ticker.C:
			hc.check(ctx)
		case <-hc.done:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (hc *HealthChecker) check(ctx context.Context) {
	checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	err := hc.kafkaChecker.Ping(checkCtx)

	hc.mu.Lock()
	defer hc.mu.Unlock()

	wasConnected := hc.status.Connected
	hc.status.Connected = (err == nil)
	hc.status.LastError = err

	if hc.status.Connected {
		hc.status.LastSuccess = time.Now()
		hc.logger.Debug("health check: ping OK")
		if !wasConnected {
			hc.logger.Info("Kafka connection restored, starting fallback replay")
			if hc.onRecovery != nil {
				go hc.onRecovery()
			}
		}
	} else if wasConnected {
		hc.logger.Info("Kafka connection lost, failed batches will be saved to fallback: %v", err)
	} else {
		hc.logger.Debug("health check: Kafka still unreachable: %v", err)
	}
}
