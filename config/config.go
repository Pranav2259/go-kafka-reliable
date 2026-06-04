package config

import (
	"time"
)

type Config struct {
	Brokers []string

	BatchSize     int
	FlushInterval time.Duration
	Workers       int

	RetryCount   int
	RetryBackoff time.Duration

	SendTimeout time.Duration

	FallbackEnabled bool
	FallbackDBPath  string

	MaxMessageBytes int

	Debug bool
}

func (c *Config) Validate() error {
	if len(c.Brokers) == 0 {
		return ErrNoBrokers
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = 3 * time.Second
	}
	if c.Workers <= 0 {
		c.Workers = 5
	}
	if c.RetryCount < 0 {
		c.RetryCount = 3
	}
	if c.RetryBackoff <= 0 {
		c.RetryBackoff = 500 * time.Millisecond
	}
	if c.SendTimeout <= 0 {
		c.SendTimeout = 5 * time.Second
	}
	if c.FallbackDBPath == "" {
		c.FallbackDBPath = ".ckafka_fallback.db"
	}
	if c.MaxMessageBytes <= 0 {
		c.MaxMessageBytes = 1 * 1024 * 1024 // 1MB
	}
	return nil
}
