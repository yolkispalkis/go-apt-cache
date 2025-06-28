// internal/cache/disk_lru.go
package cache

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

// lruEntry - элемент двусвязного списка для реализации LRU.
type lruEntry struct {
	key  string
	meta *ItemMeta
}

// metadataUpdate - структура для асинхронного обновления метаданных.
type metadataUpdate struct {
	Key  string
	Meta *ItemMeta
}

// metadataBatcher собирает обновления метаданных и периодически записывает их на диск.
type metadataBatcher struct {
	updates chan metadataUpdate
	ticker  *time.Ticker
	store   *diskStore
	log     *logging.Logger
	done    chan struct{}
	wg      sync.WaitGroup
}

func newMetadataBatcher(store *diskStore, logger *logging.Logger, interval time.Duration) *metadataBatcher {
	mb := &metadataBatcher{
		updates: make(chan metadataUpdate, 1000),
		ticker:  time.NewTicker(interval),
		store:   store,
		log:     logger,
		done:    make(chan struct{}),
	}
	mb.wg.Add(1)
	go mb.run()
	return mb
}

func (mb *metadataBatcher) run() {
	defer mb.wg.Done()
	updates := make(map[string]*ItemMeta)

	for {
		select {
		case update := <-mb.updates:
			if oldMeta, exists := updates[update.Key]; exists {
				util.ReturnHeader(oldMeta.Headers)
			}
			updates[update.Key] = update.Meta
		case <-mb.ticker.C:
			if len(updates) > 0 {
				mb.flush(updates)
				updates = make(map[string]*ItemMeta)
			}
		case <-mb.done:
			if len(updates) > 0 {
				mb.flush(updates)
			}
			return
		}
	}
}

func (mb *metadataBatcher) flush(updates map[string]*ItemMeta) {
	for _, meta := range updates {
		if err := mb.store.WriteMetadata(meta); err != nil {
			mb.log.Warn().Err(err).Str("key", meta.Key).Msg("Failed to batch update metadata")
		}
		util.ReturnHeader(meta.Headers)
	}
	mb.log.Debug().Int("count", len(updates)).Msg("Batch updated metadata files")
}

func (mb *metadataBatcher) schedule(key string, meta *ItemMeta) {
	select {
	case mb.updates <- metadataUpdate{Key: key, Meta: meta}:
	default:
		mb.log.Warn().Str("key", key).Msg("Metadata update queue full, dropping update")
		util.ReturnHeader(meta.Headers)
	}
}

func (mb *metadataBatcher) close() {
	close(mb.done)
	mb.ticker.Stop()
	mb.wg.Wait()
}

// DiskLRU - реализация дискового кеша с LRU-стратегией вытеснения.
type DiskLRU struct {
	cfg      config.CacheConfig
	log      *logging.Logger
	store    *diskStore
	maxBytes int64

	mu           sync.RWMutex
	items        map[string]*list.Element
	lruList      *list.List
	currentBytes atomic.Int64

	initOnce    sync.Once
	initErr     error
	ready       bool
	metaBatcher *metadataBatcher
}

// NewDiskLRU создает новый экземпляр DiskLRU.
func NewDiskLRU(cfg config.CacheConfig, logger *logging.Logger) (Manager, error) {
	if !cfg.Enabled {
		logger.Info().Msg("Cache is disabled in configuration.")
		return &noopManager{}, nil
	}

	log := logger.WithComponent("DiskLRU")

	maxBytes, err := util.ParseSize(cfg.MaxSize)
	if err != nil {
		return nil, fmt.Errorf("invalid cache MaxSize %q: %w", cfg.MaxSize, err)
	}

	store, err := newDiskStore(cfg.Dir, log)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize disk store at %s: %w", cfg.Dir, err)
	}

	batchInterval := 30 * time.Second // Можно вынести в конфиг

	cache := &DiskLRU{
		cfg:         cfg,
		log:         log,
		store:       store,
		maxBytes:    maxBytes,
		items:       make(map[string]*list.Element),
		lruList:     list.New(),
		metaBatcher: newMetadataBatcher(store, log, batchInterval),
	}

	go cache.initialScan()

	return cache, nil
}

func (c *DiskLRU) initialScan() {
	c.initOnce.Do(func() {
		c.log.Info().Msg("Starting initial disk scan...")
		startTime := time.Now()

		metaFiles, err := c.store.ScanMetaFiles()
		if err != nil {
			c.initErr = err
			c.log.Error().Err(err).Msg("Failed during initial scan")
			c.ready = true // Помечаем как готовый, но с ошибкой
			return
		}

		itemsToSort := make([]*ItemMeta, 0, len(metaFiles))
		var totalSize int64

		for _, mPath := range metaFiles {
			meta, err := c.store.ReadMeta(mPath)
			if err != nil {
				c.log.Warn().Err(err).Str("path", mPath).Msg("Failed to read metadata, removing artifact")
				// Ключ извлекаем из имени файла, т.к. прочитать его из JSON не удалось
				key := strings.TrimSuffix(filepath.Base(mPath), ".meta")
				c.store.Delete(key)
				continue
			}
			itemsToSort = append(itemsToSort, meta)
			totalSize += meta.Size
		}

		// ИСПРАВЛЕНО: Сортируем только по LastUsedAt из метаданных.
		sort.Slice(itemsToSort, func(i, j int) bool {
			return itemsToSort[i].LastUsedAt.Before(itemsToSort[j].LastUsedAt)
		})

		c.mu.Lock()
		for _, meta := range itemsToSort {
			entry := &lruEntry{key: meta.Key, meta: meta}
			elem := c.lruList.PushFront(entry)
			c.items[meta.Key] = elem
		}
		c.mu.Unlock()
		c.currentBytes.Store(totalSize)

		c.log.Info().
			Int("items", len(itemsToSort)).
			Str("size", util.FormatSize(totalSize)).
			Dur("duration", time.Since(startTime)).
			Msg("Initial disk scan complete")

		c.ready = true
	})
}

func (c *DiskLRU) checkReady() error {
	if !c.ready {
		return errors.New("cache is not ready (initial scan in progress)")
	}
	return c.initErr
}

// Get получает элемент из кеша.
func (c *DiskLRU) Get(ctx context.Context, key string) (*ItemMeta, bool) {
	if err := c.checkReady(); err != nil {
		c.log.Error().Err(err).Msg("Cache is not ready")
		return nil, false
	}

	c.mu.RLock()
	elem, exists := c.items[key]
	c.mu.RUnlock()

	if !exists {
		return nil, false
	}

	go c.MarkUsed(ctx, key)

	return elem.Value.(*lruEntry).meta, true
}

// Put добавляет или обновляет метаданные элемента в кеше.
func (c *DiskLRU) Put(ctx context.Context, meta *ItemMeta) error {
	if err := c.checkReady(); err != nil {
		return err
	}

	if err := c.store.WriteMetadata(meta); err != nil {
		return fmt.Errorf("disk store write for key %s: %w", meta.Key, err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if oldElem, exists := c.items[meta.Key]; exists {
		oldMeta := oldElem.Value.(*lruEntry).meta
		c.currentBytes.Add(-oldMeta.Size)
		c.lruList.Remove(oldElem)
	}

	entry := &lruEntry{key: meta.Key, meta: meta}
	elem := c.lruList.PushFront(entry)
	c.items[meta.Key] = elem
	c.currentBytes.Add(meta.Size)

	c.log.Debug().Str("key", meta.Key).Str("size", util.FormatSize(meta.Size)).Msg("Item metadata added/updated in cache")
	return nil
}

func (c *DiskLRU) MarkUsed(ctx context.Context, key string) {
	c.mu.Lock()
	elem, exists := c.items[key]
	if !exists {
		c.mu.Unlock()
		return
	}

	meta := elem.Value.(*lruEntry).meta
	meta.LastUsedAt = time.Now()
	c.lruList.MoveToFront(elem)

	metaCopy := *meta
	metaCopy.Headers = util.CopyHeader(meta.Headers)
	c.mu.Unlock()

	c.metaBatcher.schedule(key, &metaCopy)
}

func (c *DiskLRU) Delete(ctx context.Context, key string) error {
	c.mu.Lock()
	if elem, exists := c.items[key]; exists {
		meta := elem.Value.(*lruEntry).meta
		c.currentBytes.Add(-meta.Size)
		c.lruList.Remove(elem)
		delete(c.items, key)
	}
	c.mu.Unlock()

	return c.store.Delete(key)
}

func (c *DiskLRU) GetContent(ctx context.Context, key string) (io.ReadCloser, error) {
	// ИСПРАВЛЕНО: Убрали обновление atime
	return c.store.GetContent(key)
}

func (c *DiskLRU) PutContent(ctx context.Context, key string, r io.Reader) (int64, error) {
	// Перед записью проверим, хватит ли места.
	// Для простоты, предполагаем максимальный размер файла, если он неизвестен.
	// В реальности, лучше сначала записать во временный файл, узнать размер, а потом вытеснять.
	// Но для упрощения сделаем так.
	c.ensureSpace(0) // Запускаем вытеснение, если уже переполнены
	return c.store.PutContent(key, r)
}

func (c *DiskLRU) Close() {
	if c.metaBatcher != nil {
		c.metaBatcher.close()
	}
	c.log.Info().Msg("Cache manager closed.")
}

func (c *DiskLRU) ensureSpace(requiredSize int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for c.currentBytes.Load()+requiredSize > c.maxBytes && c.lruList.Len() > 0 {
		elem := c.lruList.Back()
		if elem == nil {
			return
		}
		entry := c.lruList.Remove(elem).(*lruEntry)
		delete(c.items, entry.key)
		c.currentBytes.Add(-entry.meta.Size)

		c.log.Info().
			Str("key", entry.key).
			Str("size", util.FormatSize(entry.meta.Size)).
			Time("last_used", entry.meta.LastUsedAt).
			Msg("Evicting item")

		go c.store.Delete(entry.key)
	}
}
