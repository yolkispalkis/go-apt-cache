package storage

import (
	"io"
	"net/http"
	"sync"
	"time"
)

// Storage defines the interface for storing and retrieving APT repository files
type Storage interface {
	// Get retrieves a file from storage
	Get(key string) (io.ReadCloser, int64, time.Time, error)

	// Put stores a file in storage
	Put(key string, content io.Reader, contentLength int64) error

	// Exists checks if a file exists in storage
	Exists(key string) (bool, error)
}

// Cache defines the interface for caching APT repository files
type Cache interface {
	// Get retrieves a file from cache
	Get(key string) (io.ReadCloser, int64, time.Time, error)

	// Put stores a file in cache
	Put(key string, content io.Reader, contentLength int64, lastModified time.Time) error
}

// LRUStatsProvider defines the interface for getting LRU cache statistics
type LRUStatsProvider interface {
	// GetCacheStats returns the current cache statistics
	GetCacheStats() (itemCount int, currentSize int64, maxSize int64)
}

// HeaderCache defines the interface for caching HTTP headers
type HeaderCache interface {
	// GetHeaders retrieves headers for a key
	GetHeaders(key string) (http.Header, error)

	// PutHeaders stores headers for a key
	PutHeaders(key string, headers http.Header) error
}

// ValidationCache defines the interface for caching validation results
type ValidationCache interface {
	// Get checks if a validation result is cached
	Get(key string) (bool, time.Time)

	// Put stores a validation result
	Put(key string, lastValidated time.Time)

	// SetTTL updates the TTL for cached validations
	SetTTL(ttl time.Duration)
}

// NoopCache is a cache implementation that does nothing
type NoopCache struct{}

// NewNoopCache creates a new NoopCache
func NewNoopCache() *NoopCache {
	return &NoopCache{}
}

// Get always returns an error
func (c *NoopCache) Get(key string) (io.ReadCloser, int64, time.Time, error) {
	return nil, 0, time.Time{}, io.EOF
}

// Put does nothing
func (c *NoopCache) Put(key string, content io.Reader, contentLength int64, lastModified time.Time) error {
	return nil
}

// NoopHeaderCache is a header cache implementation that does nothing
type NoopHeaderCache struct{}

// NewNoopHeaderCache creates a new NoopHeaderCache
func NewNoopHeaderCache() *NoopHeaderCache {
	return &NoopHeaderCache{}
}

// GetHeaders always returns an error
func (c *NoopHeaderCache) GetHeaders(key string) (http.Header, error) {
	return nil, io.EOF
}

// PutHeaders does nothing
func (c *NoopHeaderCache) PutHeaders(key string, headers http.Header) error {
	return nil
}

// MemoryValidationCache is an in-memory implementation of ValidationCache
type MemoryValidationCache struct {
	mu    sync.RWMutex
	cache map[string]time.Time
	ttl   time.Duration
}

// NewMemoryValidationCache creates a new MemoryValidationCache
func NewMemoryValidationCache(ttl time.Duration) *MemoryValidationCache {
	return &MemoryValidationCache{
		cache: make(map[string]time.Time),
		ttl:   ttl,
	}
}

// Get checks if a validation result is cached and still valid
func (c *MemoryValidationCache) Get(key string) (bool, time.Time) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	lastValidated, exists := c.cache[key]
	if !exists {
		return false, time.Time{}
	}

	// Check if the cached validation is still valid
	if time.Since(lastValidated) > c.ttl {
		return false, lastValidated
	}

	return true, lastValidated
}

// Put stores a validation result
func (c *MemoryValidationCache) Put(key string, lastValidated time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache[key] = lastValidated
}

// SetTTL updates the TTL for cached validations
func (c *MemoryValidationCache) SetTTL(ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.ttl = ttl
}

// NoopValidationCache is a validation cache implementation that does nothing
type NoopValidationCache struct{}

// NewNoopValidationCache creates a new NoopValidationCache
func NewNoopValidationCache() *NoopValidationCache {
	return &NoopValidationCache{}
}

// Get always returns false
func (c *NoopValidationCache) Get(key string) (bool, time.Time) {
	return false, time.Time{}
}

// Put does nothing
func (c *NoopValidationCache) Put(key string, lastValidated time.Time) {
	// Do nothing
}

// SetTTL does nothing
func (c *NoopValidationCache) SetTTL(ttl time.Duration) {
	// Do nothing
}
