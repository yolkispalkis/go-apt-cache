package storage

import (
	"io"
	"net/http"
	"sync"
	"time"
)

type Storage interface {
	Get(key string) (io.ReadCloser, int64, time.Time, error)
	Put(key string, content io.Reader, contentLength int64) error
	Exists(key string) (bool, error)
}

type Cache interface {
	Get(key string) (io.ReadCloser, int64, time.Time, error)
	Put(key string, content io.Reader, contentLength int64, lastModified time.Time) error
}

type LRUStatsProvider interface {
	GetCacheStats() (itemCount int, currentSize int64, maxSize int64)
}

type HeaderCache interface {
	GetHeaders(key string) (http.Header, error)
	PutHeaders(key string, headers http.Header) error
}

type ValidationCache interface {
	Get(key string) (bool, time.Time)
	Put(key string, lastValidated time.Time)
	SetTTL(ttl time.Duration)
}

type NoopCache struct{}

func NewNoopCache() *NoopCache {
	return &NoopCache{}
}

func (c *NoopCache) Get(key string) (io.ReadCloser, int64, time.Time, error) {
	return nil, 0, time.Time{}, io.EOF
}

func (c *NoopCache) Put(key string, content io.Reader, contentLength int64, lastModified time.Time) error {
	return nil
}

type NoopHeaderCache struct{}

func NewNoopHeaderCache() *NoopHeaderCache {
	return &NoopHeaderCache{}
}

func (c *NoopHeaderCache) GetHeaders(key string) (http.Header, error) {
	return nil, io.EOF
}

func (c *NoopHeaderCache) PutHeaders(key string, headers http.Header) error {
	return nil
}

type MemoryValidationCache struct {
	mu    sync.RWMutex
	cache map[string]time.Time
	ttl   time.Duration
}

func NewMemoryValidationCache(ttl time.Duration) *MemoryValidationCache {
	return &MemoryValidationCache{
		cache: make(map[string]time.Time),
		ttl:   ttl,
	}
}

func (c *MemoryValidationCache) Get(key string) (bool, time.Time) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	lastValidated, exists := c.cache[key]
	if !exists {
		return false, time.Time{}
	}

	if time.Since(lastValidated) > c.ttl {
		go func(k string) {
			c.mu.Lock()
			delete(c.cache, k)
			c.mu.Unlock()
		}(key)
		return false, lastValidated
	}

	return true, lastValidated
}

func (c *MemoryValidationCache) Put(key string, lastValidated time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache[key] = lastValidated
}

func (c *MemoryValidationCache) SetTTL(ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.ttl = ttl
}

type NoopValidationCache struct{}

func NewNoopValidationCache() *NoopValidationCache {
	return &NoopValidationCache{}
}

func (c *NoopValidationCache) Get(key string) (bool, time.Time) {
	return false, time.Time{}
}

func (c *NoopValidationCache) Put(key string, lastValidated time.Time) {
}

func (c *NoopValidationCache) SetTTL(ttl time.Duration) {
}
