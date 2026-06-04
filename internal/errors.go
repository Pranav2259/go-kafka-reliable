package internal

import "errors"

var (
	ErrDBInit       = errors.New("failed to initialize database")
	ErrMessageSave  = errors.New("failed to save message to fallback store")
	ErrMessageFetch = errors.New("failed to fetch messages from fallback store")
	ErrMessageDelete = errors.New("failed to delete messages from fallback store")
	ErrKafkaSend    = errors.New("failed to send message to kafka")
	ErrShutdown     = errors.New("producer is shutting down")
)
