package config

import "errors"

var (
	ErrNoBrokers = errors.New("brokers list cannot be empty")
)
