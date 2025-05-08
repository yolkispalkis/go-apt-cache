package cache

import (
	"context"
	"io"
	"net/http"
	"os"
	"time"
)

const (
	MetadataVersion = 2
)

type CacheItemMetadata struct {
	Version  int    `json:"version"`
	Key      string `json:"key"`
	Path     string `json:"-"`
	MetaPath string `json:"-"`

	UpstreamURL string    `json:"upstream_url"`
	FetchedAt   time.Time `json:"fetched_at"`
	LastUsedAt  time.Time `json:"last_used_at"`
	ValidatedAt time.Time `json:"validated_at"`

	StatusCode int         `json:"status_code"`
	Headers    http.Header `json:"headers"`
	Size       int64       `json:"size"`

	ExpiresAt time.Time `json:"expires_at,omitempty"`
}

func (m *CacheItemMetadata) IsStale(now time.Time) bool {
	if m.StatusCode != http.StatusOK && m.StatusCode != http.StatusPartialContent {

		return true
	}
	if m.ExpiresAt.IsZero() {
		return false
	}
	return now.After(m.ExpiresAt)
}

type CacheGetResult struct {
	Metadata *CacheItemMetadata
	Content  io.ReadCloser
	Hit      bool
}

type CachePutOptions struct {
	UpstreamURL string
	StatusCode  int
	Headers     http.Header
	Size        int64
	FetchedAt   time.Time
}

type CacheManager interface {
	Get(ctx context.Context, key string) (*CacheGetResult, error)

	Put(ctx context.Context, key string, reader io.Reader, opts CachePutOptions) (*CacheItemMetadata, error)

	Delete(ctx context.Context, key string) error

	MarkUsed(ctx context.Context, key string) error

	UpdateValidatedAt(ctx context.Context, key string, validatedAt time.Time) error

	Purge(ctx context.Context) error

	Init(ctx context.Context) error

	Close() error

	GetCurrentSize() int64

	GetItemCount() int64
}

var ErrNotFound = os.ErrNotExist
