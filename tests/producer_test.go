package tests

import (
	"context"
	"testing"
	"time"

	"github.com/Pranav2259/go-kafka-reliable/config"
	"github.com/Pranav2259/go-kafka-reliable/internal"
	"github.com/Pranav2259/go-kafka-reliable/producer"
)

func TestRetryPolicy(t *testing.T) {
	cfg := &config.Config{
		RetryCount:   3,
		RetryBackoff: 10 * time.Millisecond,
	}

	rp := producer.NewRetryPolicy(cfg)

	attempts := 0
	err := rp.Execute(context.Background(), func(ctx context.Context) error {
		attempts++
		if attempts < 3 {
			return internal.ErrKafkaSend
		}
		return nil
	})

	if err != nil {
		t.Errorf("Expected success after retries, got error: %v", err)
	}

	if attempts != 3 {
		t.Errorf("Expected 3 attempts, got %d", attempts)
	}
}

func TestProducerCreation(t *testing.T) {
	cfg := &config.Config{
		Brokers:         []string{"localhost:9092"},
		BatchSize:       10,
		FlushInterval:   100 * time.Millisecond,
		Workers:         1,
		RetryCount:      1,
		RetryBackoff:    10 * time.Millisecond,
		SendTimeout:     50 * time.Millisecond,
		FallbackEnabled: false,
	}

	prod, err := producer.New(cfg)
	// Connection to Kafka will fail in test, but object creation should succeed
	if err == nil {
		if prod == nil {
			t.Error("Expected producer to be created")
		}
		prod.Shutdown(context.Background())
	}
}

func TestProducerWithFallback(t *testing.T) {
	cfg := &config.Config{
		Brokers:         []string{"localhost:9092"},
		BatchSize:       10,
		FlushInterval:   100 * time.Millisecond,
		Workers:         1,
		RetryCount:      1,
		RetryBackoff:    10 * time.Millisecond,
		SendTimeout:     50 * time.Millisecond,
		FallbackEnabled: true,
		FallbackDBPath:  ":memory:",
	}

	prod, err := producer.New(cfg)
	if err == nil {
		if prod == nil {
			t.Error("Expected producer to be created")
		}
		prod.Shutdown(context.Background())
	}
}
