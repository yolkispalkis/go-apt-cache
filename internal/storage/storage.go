package storage

import (
	"io"
	"net/http"
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
