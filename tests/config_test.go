package tests

import (
	"testing"

	"github.com/Pranav2259/go-kafka-reliable/config"
	"github.com/Pranav2259/go-kafka-reliable/internal"
)

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *config.Config
		valid   bool
		message string
	}{
		{
			name:    "empty config",
			cfg:     &config.Config{},
			valid:   false,
			message: "should fail with no brokers",
		},
		{
			name: "valid config",
			cfg: &config.Config{
				Brokers: []string{"localhost:9092"},
			},
			valid:   true,
			message: "should pass with brokers",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.valid && err != nil {
				t.Errorf("Expected valid config, got error: %v", err)
			}
			if !tt.valid && err == nil {
				t.Errorf("Expected invalid config to fail")
			}
		})
	}
}

func TestMessageCreation(t *testing.T) {
	msg := internal.Message{
		Topic: "test-topic",
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
	}

	if msg.Topic != "test-topic" {
		t.Errorf("Expected topic 'test-topic', got '%s'", msg.Topic)
	}

	if string(msg.Key) != "test-key" {
		t.Errorf("Expected key 'test-key', got '%s'", string(msg.Key))
	}

	if string(msg.Value) != "test-value" {
		t.Errorf("Expected value 'test-value', got '%s'", string(msg.Value))
	}
}

func TestKafkaStatus(t *testing.T) {
	status := &internal.KafkaStatus{
		Connected: true,
	}

	if !status.Connected {
		t.Error("Expected Kafka to be connected")
	}

	status.Connected = false
	if status.Connected {
		t.Error("Expected Kafka to be disconnected")
	}
}
