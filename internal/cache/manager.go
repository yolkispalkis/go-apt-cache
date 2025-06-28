package cache

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

// ItemMeta хранит метаданные о кешированном объекте.
type ItemMeta struct {
	Key         string
	UpstreamURL string
	FetchedAt   time.Time
	ExpiresAt   time.Time
	StatusCode  int
	Headers     http.Header
	Size        int64
}

// IsStale проверяет, является ли элемент кеша устаревшим.
func (m *ItemMeta) IsStale(now time.Time) bool {
	return m.ExpiresAt.IsZero() || now.After(m.ExpiresAt)
}

// Manager - это интерфейс для управления кешем.
type Manager interface {
	Get(ctx context.Context, key string) (*ItemMeta, bool)
	Put(ctx context.Context, key string, meta *ItemMeta)
	Delete(ctx context.Context, key string)
	GetContent(ctx context.Context, key string) (io.ReadCloser, error)
	PutContent(ctx context.Context, key string, r io.Reader) (int64, error)
	DeleteContent(ctx context.Context, key string) error
	Close()
	Stats() ristretto.Metrics
}

// cacheManager координирует работу кеша метаданных в памяти и дискового кеша для файлов.
type cacheManager struct {
	cfg       config.CacheConfig
	log       *logging.Logger
	memCache  *ristretto.Cache
	diskStore *diskStore
}

// NewManager создает новый менеджер кеша.
func NewManager(cfg config.CacheConfig, logger *logging.Logger) (Manager, error) {
	if !cfg.Enabled {
		return &noopManager{}, nil
	}

	log := logger.WithComponent("cacheManager")

	maxSizeBytes, err := util.ParseSize(cfg.MaxSize)
	if err != nil {
		return nil, fmt.Errorf("invalid cache max size: %w", err)
	}

	// Ristretto требует оценки максимального количества ключей.
	// Предположим, что средний размер метаданных ~1KB.
	numCounters := maxSizeBytes / 1024
	if numCounters < 1000 {
		numCounters = 1000
	}

	memCache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: numCounters * 10, // Рекомендация Ristretto
		MaxCost:     1 << 30,          // 1GB - максимальный "вес" кеша, мы не используем его для ограничения.
		BufferItems: 64,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create in-memory cache: %w", err)
	}

	diskStore, err := newDiskStore(cfg.Dir, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create disk store: %w", err)
	}

	log.Info("Cache manager initialized", "dir", cfg.Dir, "max_size", cfg.MaxSize)

	return &cacheManager{
		cfg:       cfg,
		log:       log,
		memCache:  memCache,
		diskStore: diskStore,
	}, nil
}

// Get получает метаданные из кеша в памяти.
func (m *cacheManager) Get(ctx context.Context, key string) (*ItemMeta, bool) {
	value, found := m.memCache.Get(key)
	if !found {
		return nil, false
	}
	meta, ok := value.(*ItemMeta)
	if !ok {
		m.log.Warn("Invalid type in memory cache, deleting", "key", key)
		m.memCache.Del(key)
		return nil, false
	}
	return meta, true
}

// Put помещает метаданные в кеш.
func (m *cacheManager) Put(ctx context.Context, key string, meta *ItemMeta) {
	ttl := time.Until(meta.ExpiresAt)
	if ttl <= 0 {
		// Не кешируем уже протухшие элементы в памяти
		return
	}
	m.memCache.SetWithTTL(key, meta, 1, ttl)
}

// Delete удаляет метаданные из кеша.
func (m *cacheManager) Delete(ctx context.Context, key string) {
	m.memCache.Del(key)
}

// GetContent открывает файл контента с диска.
func (m *cacheManager) GetContent(ctx context.Context, key string) (io.ReadCloser, error) {
	return m.diskStore.Get(key)
}

// PutContent сохраняет контент на диск.
func (m *cacheManager) PutContent(ctx context.Context, key string, r io.Reader) (int64, error) {
	// TODO: Реализовать логику вытеснения (eviction) на основе LRU или другого критерия, если размер диска превышен.
	return m.diskStore.Put(key, r)
}

// DeleteContent удаляет файл контента с диска.
func (m *cacheManager) DeleteContent(ctx context.Context, key string) error {
	return m.diskStore.Delete(key)
}

func (m *cacheManager) Close() {
	m.memCache.Close()
	m.log.Info("Cache manager closed.")
}

func (m *cacheManager) Stats() ristretto.Metrics {
	return *m.memCache.Metrics
}

// noopManager - заглушка для случая, когда кеш отключен.
type noopManager struct{}

func (n *noopManager) Get(ctx context.Context, key string) (*ItemMeta, bool) { return nil, false }
func (n *noopManager) Put(ctx context.Context, key string, meta *ItemMeta)   {}
func (n *noopManager) Delete(ctx context.Context, key string)                {}
func (n *noopManager) GetContent(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, os.ErrNotExist
}
func (n *noopManager) PutContent(ctx context.Context, key string, r io.Reader) (int64, error) {
	return io.Copy(io.Discard, r)
}
func (n *noopManager) DeleteContent(ctx context.Context, key string) error { return nil }
func (n *noopManager) Close()                                              {}
func (n *noopManager) Stats() ristretto.Metrics                            { return ristretto.Metrics{} }
