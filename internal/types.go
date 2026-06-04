package internal

import (
	"context"
	"log"
	"os"
	"time"
)

type Message struct {
	ID    string
	Topic string
	Key   []byte
	Value []byte
}

type FallbackStore interface {
	Save(ctx context.Context, messages []Message) error
	FetchBatch(ctx context.Context, limit int) ([]Message, error)
	Delete(ctx context.Context, ids []string) error
	Close() error
}

type KafkaStatus struct {
	Connected   bool
	LastError   error
	LastSuccess time.Time
}

type Logger struct {
	debug bool
	l     *log.Logger
}

func NewLogger(debug bool) *Logger {
	return &Logger{
		debug: debug,
		l:     log.New(os.Stderr, "[ckafka] ", log.LstdFlags),
	}
}

func (l *Logger) Debug(format string, args ...interface{}) {
	if l.debug {
		l.l.Printf("DEBUG "+format, args...)
	}
}

func (l *Logger) Info(format string, args ...interface{}) {
	l.l.Printf(format, args...)
}

func (l *Logger) Error(format string, args ...interface{}) {
	l.l.Printf("ERROR "+format, args...)
}
