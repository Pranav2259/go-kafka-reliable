package producer

import (
	"github.com/Pranav2259/go-kafka-reliable/config"
	"github.com/Pranav2259/go-kafka-reliable/internal"
	"context"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/IBM/sarama"
)

type KafkaProducer struct {
	client sarama.SyncProducer
	cfg    *config.Config
	mu     sync.Mutex
}

func NewKafkaProducer(cfg *config.Config) (*KafkaProducer, error) {
	if !cfg.Debug {
		sarama.Logger = log.New(io.Discard, "", 0)
	}
	return &KafkaProducer{cfg: cfg}, nil
}

func newSaramaConfig(cfg *config.Config) *sarama.Config {
	saramaCfg := sarama.NewConfig()
	saramaCfg.Net.DialTimeout = cfg.SendTimeout
	saramaCfg.Net.ReadTimeout = cfg.SendTimeout
	saramaCfg.Net.WriteTimeout = cfg.SendTimeout
	saramaCfg.Metadata.Retry.Max = 1
	saramaCfg.Metadata.Retry.Backoff = cfg.RetryBackoff
	saramaCfg.Metadata.Timeout = cfg.SendTimeout
	saramaCfg.Producer.RequiredAcks = sarama.WaitForLocal
	saramaCfg.Producer.Retry.Max = cfg.RetryCount
	saramaCfg.Producer.Retry.Backoff = cfg.RetryBackoff
	saramaCfg.Producer.Timeout = cfg.SendTimeout
	saramaCfg.Producer.MaxMessageBytes = cfg.MaxMessageBytes
	saramaCfg.Producer.Return.Successes = true
	saramaCfg.Producer.Return.Errors = true
	saramaCfg.Version = sarama.V2_6_0_0

	return saramaCfg
}

func (kp *KafkaProducer) ensureClient() error {
	kp.mu.Lock()
	defer kp.mu.Unlock()

	if kp.client != nil {
		return nil
	}

	client, err := sarama.NewSyncProducer(kp.cfg.Brokers, newSaramaConfig(kp.cfg))
	if err != nil {
		return fmt.Errorf("failed to create kafka producer: %w", err)
	}

	kp.client = client
	return nil
}

func (kp *KafkaProducer) getClient() (sarama.SyncProducer, error) {
	if err := kp.ensureClient(); err != nil {
		return nil, err
	}

	kp.mu.Lock()
	defer kp.mu.Unlock()
	return kp.client, nil
}

func (kp *KafkaProducer) resetClient(client sarama.SyncProducer) {
	kp.mu.Lock()
	defer kp.mu.Unlock()

	if kp.client != client {
		return
	}

	kp.client = nil
	if client != nil {
		_ = client.Close()
	}
}

func (kp *KafkaProducer) SendMessage(ctx context.Context, msg internal.Message) error {
	return kp.SendMessages(ctx, []internal.Message{msg})
}

func (kp *KafkaProducer) SendMessages(ctx context.Context, messages []internal.Message) error {
	if len(messages) == 0 {
		return nil
	}

	client, err := kp.getClient()
	if err != nil {
		return err
	}

	saramaMessages := make([]*sarama.ProducerMessage, 0, len(messages))
	for _, msg := range messages {
		saramaMessages = append(saramaMessages, &sarama.ProducerMessage{
			Topic: msg.Topic,
			Key:   sarama.ByteEncoder(msg.Key),
			Value: sarama.ByteEncoder(msg.Value),
		})
	}

	sendCtx, cancel := context.WithTimeout(ctx, kp.cfg.SendTimeout)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- client.SendMessages(saramaMessages)
	}()

	select {
	case <-sendCtx.Done():
		kp.resetClient(client)
		return fmt.Errorf("kafka send timed out after %s: %w", kp.cfg.SendTimeout, sendCtx.Err())
	case err := <-errCh:
		if err != nil {
			kp.resetClient(client)
			return fmt.Errorf("failed to send messages to kafka: %w", err)
		}
	}

	return nil
}

func (kp *KafkaProducer) Close() error {
	kp.mu.Lock()
	defer kp.mu.Unlock()

	if kp.client == nil {
		return nil
	}

	err := kp.client.Close()
	kp.client = nil
	return err
}

func (kp *KafkaProducer) Ping(ctx context.Context) error {
	client, err := sarama.NewClient(kp.cfg.Brokers, newSaramaConfig(kp.cfg))
	if err != nil {
		return fmt.Errorf("failed to connect to kafka: %w", err)
	}
	defer client.Close()

	client.Brokers()
	return nil
}
