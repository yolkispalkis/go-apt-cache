package cache

import (
	"context"
	"io"
	"net/http"
	"os"
	"time"
)

const MetadataVersion = 2

var ErrNotFound = os.ErrNotExist

type ItemMeta struct {
	Version     int         `json:"version"`
	Key         string      `json:"key"`
	Path        string      `json:"-"`
	MetaPath    string      `json:"-"`
	UpstreamURL string      `json:"upstream_url"`
	FetchedAt   time.Time   `json:"fetched_at"`
	LastUsedAt  time.Time   `json:"last_used_at"`
	ValidatedAt time.Time   `json:"validated_at"`
	StatusCode  int         `json:"status_code"`
	Headers     http.Header `json:"headers"`
	Size        int64       `json:"size"`
	ExpiresAt   time.Time   `json:"expires_at,omitempty"`
}

func (m *ItemMeta) IsStale(now time.Time) bool {

	if m.StatusCode != http.StatusOK && m.StatusCode != http.StatusPartialContent {

		return true
	}
	if m.ExpiresAt.IsZero() {
		return false
	}
	return now.After(m.ExpiresAt)
}

type GetResult struct {
	Meta    *ItemMeta
	Content io.ReadCloser
	Hit     bool
}

type PutOptions struct {
	UpstreamURL string
	StatusCode  int
	Headers     http.Header
	Size        int64
	FetchedAt   time.Time
}

type Manager interface {
	Init(ctx context.Context) error
	Get(ctx context.Context, key string) (*GetResult, error)
	Put(ctx context.Context, key string, r io.Reader, opts PutOptions) (*ItemMeta, error)
	Delete(ctx context.Context, key string) error
	MarkUsed(ctx context.Context, key string) error
	UpdateValidatedAt(ctx context.Context, key string, validatedAt time.Time) error
	Purge(ctx context.Context) error
	Close() error
	CurrentSize() int64
	ItemCount() int64
}
