package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Pranav2259/go-kafka-reliable/config"
	"github.com/Pranav2259/go-kafka-reliable/internal"
	"github.com/Pranav2259/go-kafka-reliable/producer"
)

func main() {
	cfg := &config.Config{
		Brokers:         []string{"localhost:9092"}, // replace with your broker addresses
		BatchSize:       100,
		FlushInterval:   1 * time.Second,
		Workers:         5,
		RetryCount:      1,
		RetryBackoff:    100 * time.Millisecond,
		FallbackEnabled: true,
		FallbackDBPath:  ".kafka.db",
		Debug:           true,
	}

	if err := cfg.Validate(); err != nil {
		log.Fatalf("invalid config: %v", err)
	}

	prod, err := producer.New(cfg)
	if err != nil {
		log.Fatalf("failed to create producer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	prod.Start(ctx)

	// Graceful shutdown on Ctrl+C
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		log.Println("shutting down...")
		cancel()
	}()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	batch := 0
	for {
		select {
		case <-ctx.Done():
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer shutdownCancel()
			if err := prod.Shutdown(shutdownCtx); err != nil {
				log.Printf("shutdown error: %v", err)
			}
			fmt.Println("done")
			return

		case <-ticker.C:
			batch++
			sent := 0
			for i := 0; i < 100; i++ {
				msg := internal.Message{
					Topic: "pranav-topic-new",
					Key:   []byte(fmt.Sprintf("batch-%d-key-%d", batch, i)),
					Value: []byte(fmt.Sprintf("batch-%d-value-%d", batch, i)),
				}
				if err := prod.Send(ctx, msg); err != nil {
					log.Printf("send error: %v", err)
					continue
				}
				sent++
			}
			log.Printf("batch=%d queued %d/100 messages", batch, sent)
		}
	}
}
