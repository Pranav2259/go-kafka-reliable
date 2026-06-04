package producer

import (
	"context"
	"math"
	"time"
	"github.com/Pranav2259/go-kafka-reliable/config"
)

type RetryPolicy struct {
	maxRetries int
	backoff    time.Duration
}

func NewRetryPolicy(cfg *config.Config) *RetryPolicy {
	return &RetryPolicy{
		maxRetries: cfg.RetryCount,
		backoff:    cfg.RetryBackoff,
	}
}

func (rp *RetryPolicy) Execute(ctx context.Context, fn func(context.Context) error) error {
	var lastErr error

	for attempt := 0; attempt <= rp.maxRetries; attempt++ {
		if attempt > 0 {
			backoffDuration := rp.exponentialBackoff(attempt)
			select {
			case <-time.After(backoffDuration):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		err := fn(ctx)
		if err == nil {
			return nil
		}

		lastErr = err
	}

	return lastErr
}

func (rp *RetryPolicy) exponentialBackoff(attempt int) time.Duration {
	multiplier := math.Pow(2, float64(attempt-1))
	return time.Duration(float64(rp.backoff) * multiplier)
}
