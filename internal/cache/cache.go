package cache

import (
	"context"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type UnixTime time.Time

func (ut UnixTime) MarshalJSON() ([]byte, error) {
	t := time.Time(ut)
	return []byte(strconv.FormatInt(t.Unix(), 10)), nil
}

func (ut *UnixTime) UnmarshalJSON(b []byte) error {
	s := string(b)
	if s == "null" {
		*ut = UnixTime(time.Time{})
		return nil
	}

	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return err
	}

	if i == 0 {
		*ut = UnixTime(time.Time{})
		return nil
	}
	*ut = UnixTime(time.Unix(i, 0))
	return nil
}

func (ut UnixTime) Time() time.Time {
	return time.Time(ut)
}

const MetadataVersion = 3

var ErrNotFound = os.ErrNotExist

type ItemMeta struct {
	Version     int         `json:"version"`
	Key         string      `json:"key"`
	Path        string      `json:"-"`
	MetaPath    string      `json:"-"`
	UpstreamURL string      `json:"upstreamURL"`
	FetchedAt   UnixTime    `json:"fetchedAt"`
	LastUsedAt  UnixTime    `json:"lastUsedAt"`
	ValidatedAt UnixTime    `json:"validatedAt"`
	StatusCode  int         `json:"statusCode"`
	Headers     http.Header `json:"headers"`
	Size        int64       `json:"size"`
	ExpiresAt   UnixTime    `json:"expiresAt,omitempty"`
}

func (m *ItemMeta) IsStale(now time.Time) bool {
	if m.StatusCode != http.StatusOK && m.StatusCode != http.StatusPartialContent {
		return true
	}
	if m.ExpiresAt.Time().IsZero() {
		return true
	}
	return now.After(m.ExpiresAt.Time())
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
	UpdateAfterValidation(ctx context.Context, key string, validationTime time.Time, newHeaders http.Header, newStatusCode int, newSizeIfChanged int64) (*ItemMeta, error)
	Purge(ctx context.Context) error
	Close() error
	CurrentSize() int64
	ItemCount() int64
}
