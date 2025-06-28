package cache

import (
	"context"
	"io"
	"net/http"
	"os"
	"time"
)

// ItemMeta хранит метаданные о кешированном объекте.
type ItemMeta struct {
	Key         string      `json:"key"`
	UpstreamURL string      `json:"upstreamURL"`
	FetchedAt   time.Time   `json:"fetchedAt"`
	LastUsedAt  time.Time   `json:"lastUsedAt"`
	ExpiresAt   time.Time   `json:"expiresAt"`
	StatusCode  int         `json:"statusCode"`
	Headers     http.Header `json:"headers"`
	Size        int64       `json:"size"`
}

// IsStale проверяет, является ли элемент кеша устаревшим.
func (m *ItemMeta) IsStale(now time.Time) bool {
	return m.ExpiresAt.IsZero() || now.After(m.ExpiresAt)
}

// Manager - это интерфейс для управления кешем.
type Manager interface {
	Get(ctx context.Context, key string) (*ItemMeta, bool)
	Put(ctx context.Context, meta *ItemMeta) error
	Delete(ctx context.Context, key string) error
	GetContent(ctx context.Context, key string) (io.ReadCloser, error)
	PutContent(ctx context.Context, key string, r io.Reader) (int64, error)
	Close()
}

// noopManager - заглушка для случая, когда кеш отключен.
type noopManager struct{}

func (n *noopManager) Get(ctx context.Context, key string) (*ItemMeta, bool) { return nil, false }
func (n *noopManager) Put(ctx context.Context, meta *ItemMeta) error         { return nil }
func (n *noopManager) Delete(ctx context.Context, key string) error          { return nil }
func (n *noopManager) GetContent(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, os.ErrNotExist
}
func (n *noopManager) PutContent(ctx context.Context, key string, r io.Reader) (int64, error) {
	return io.Copy(io.Discard, r)
}
func (n *noopManager) Close() {}
