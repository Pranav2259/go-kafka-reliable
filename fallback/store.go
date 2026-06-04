package fallback

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/Pranav2259/go-kafka-reliable/internal"

	_ "github.com/mattn/go-sqlite3"
)

type SQLiteStore struct {
	db *sql.DB
}

func NewSQLiteStore(dbPath string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	store := &SQLiteStore{db: db}
	if err := store.initSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return store, nil
}

func (s *SQLiteStore) initSchema() error {
	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA busy_timeout=5000;",
	}
	for _, p := range pragmas {
		if _, err := s.db.Exec(p); err != nil {
			return fmt.Errorf("pragma %q: %w", p, err)
		}
	}

	schema := `
	CREATE TABLE IF NOT EXISTS failed_messages (
		id TEXT PRIMARY KEY,
		topic TEXT NOT NULL,
		key BLOB,
		value BLOB NOT NULL,
		created_at TIMESTAMP NOT NULL,
		retry_count INTEGER DEFAULT 0
	);
	CREATE INDEX IF NOT EXISTS idx_created_at ON failed_messages(created_at);
	`
	_, err := s.db.Exec(schema)
	return err
}

func (s *SQLiteStore) Save(ctx context.Context, messages []internal.Message) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT OR IGNORE INTO failed_messages (id, topic, key, value, created_at)
		VALUES (?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, msg := range messages {
		if msg.ID == "" {
			msg.ID = generateID()
		}
		_, err := stmt.ExecContext(ctx, msg.ID, msg.Topic, msg.Key, msg.Value, time.Now())
		if err != nil {
			return fmt.Errorf("insert message: %w", err)
		}
	}

	return tx.Commit()
}

func (s *SQLiteStore) FetchBatch(ctx context.Context, limit int) ([]internal.Message, error) {
	if limit <= 0 {
		limit = 100
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT id, topic, key, value FROM failed_messages
		ORDER BY created_at ASC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("query messages: %w", err)
	}
	defer rows.Close()

	var messages []internal.Message
	for rows.Next() {
		var msg internal.Message
		err := rows.Scan(&msg.ID, &msg.Topic, &msg.Key, &msg.Value)
		if err != nil {
			return nil, fmt.Errorf("scan message: %w", err)
		}
		messages = append(messages, msg)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return messages, nil
}

func (s *SQLiteStore) Delete(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	placeholders := strings.Repeat("?,", len(ids))
	placeholders = placeholders[:len(placeholders)-1]
	query := fmt.Sprintf("DELETE FROM failed_messages WHERE id IN (%s)", placeholders)

	args := make([]interface{}, len(ids))
	for i, id := range ids {
		args[i] = id
	}

	_, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("delete messages: %w", err)
	}
	return nil
}

func (s *SQLiteStore) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func generateID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
