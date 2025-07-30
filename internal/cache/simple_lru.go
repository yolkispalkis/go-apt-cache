package cache

import (
	"container/list"
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

type lruEntry struct {
	key  string
	meta *ItemMeta
}

type SimpleLRU struct {
	mu           sync.RWMutex
	maxBytes     int64
	currentBytes int64
	lruList      *list.List
	items        map[string]*list.Element
	store        *diskStore
	log          *logging.Logger
}

func NewSimpleLRU(cfg config.CacheConfig, logger *logging.Logger) (Manager, error) {
	if !cfg.Enabled {
		logger.Info().Msg("Cache is disabled in configuration.")
		return &noopManager{}, nil
	}

	log := logger.WithComponent("SimpleLRU")

	maxBytes, err := util.ParseSize(cfg.MaxSize)
	if err != nil {
		return nil, fmt.Errorf("invalid cache MaxSize %q: %w", cfg.MaxSize, err)
	}

	store, err := newDiskStore(cfg.Dir, log)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize disk store at %s: %w", cfg.Dir, err)
	}

	lru := &SimpleLRU{
		maxBytes: maxBytes,
		lruList:  list.New(),
		items:    make(map[string]*list.Element),
		store:    store,
		log:      log,
	}

	if err := lru.initialScan(); err != nil {
		log.Error().Err(err).Msg("Initial cache scan failed, starting with an empty cache")
	}

	return lru, nil
}

func (lru *SimpleLRU) initialScan() error {
	lru.log.Info().Msg("Starting initial disk scan...")
	startTime := time.Now()

	metaFiles, err := lru.store.ScanMetaFiles()
	if err != nil {
		return fmt.Errorf("failed to scan meta files: %w", err)
	}

	metas := make([]*ItemMeta, 0, len(metaFiles))
	for _, mPath := range metaFiles {
		meta, err := lru.store.ReadMeta(mPath)
		if err != nil {
			lru.log.Warn().Err(err).Str("path", mPath).Msg("Could not read meta file, deleting artifact")
			_ = os.Remove(mPath)
			_ = os.Remove(strings.TrimSuffix(mPath, metaSuffix) + contentSuffix)
			continue
		}
		metas = append(metas, meta)
	}

	sort.Slice(metas, func(i, j int) bool {
		return metas[i].LastUsedAt.After(metas[j].LastUsedAt)
	})

	for _, meta := range metas {
		entry := &lruEntry{key: meta.Key, meta: meta}
		elem := lru.lruList.PushFront(entry)
		lru.items[meta.Key] = elem
		lru.currentBytes += meta.Size
	}

	lru.log.Info().
		Dur("duration", time.Since(startTime)).
		Int("files", len(metas)).
		Str("size", util.FormatSize(lru.currentBytes)).
		Msg("Initial disk scan complete.")

	return nil
}

func (lru *SimpleLRU) Get(ctx context.Context, key string) (*ItemMeta, bool) {
	lru.mu.Lock()
	elem, exists := lru.items[key]
	if !exists {
		lru.mu.Unlock()
		return nil, false
	}

	lru.lruList.MoveToFront(elem)
	entry := elem.Value.(*lruEntry)
	entry.meta.LastUsedAt = time.Now()
	metaPtr := entry.meta // беглый указатель для асинхронной записи
	lru.mu.Unlock()

	// Persist updated LastUsedAt – асинхронно, чтобы не держать лок
	go func(m *ItemMeta) {
		if err := lru.store.WriteMetadata(m); err != nil {
			lru.log.Warn().Err(err).Str("key", m.Key).Msg("Failed to persist LastUsedAt")
		}
	}(metaPtr)

	metaCopy := *metaPtr
	metaCopy.Headers = util.CopyHeader(metaPtr.Headers)
	return &metaCopy, true
}

func (lru *SimpleLRU) Put(ctx context.Context, meta *ItemMeta) error {
	if err := lru.store.WriteMetadata(meta); err != nil {
		return fmt.Errorf("disk store write for key %s: %w", meta.Key, err)
	}

	lru.mu.Lock()

	if oldElem, exists := lru.items[meta.Key]; exists {
		oldMeta := oldElem.Value.(*lruEntry).meta
		lru.currentBytes -= oldMeta.Size
		lru.lruList.Remove(oldElem)
		util.ReturnHeader(oldMeta.Headers)
	}

	var keysToEvict []string
	if meta.Size > 0 {
		keysToEvict = lru.ensureSpaceLocked(meta.Size)
	}

	entry := &lruEntry{key: meta.Key, meta: meta}
	elem := lru.lruList.PushFront(entry)
	lru.items[meta.Key] = elem
	lru.currentBytes += meta.Size

	lru.mu.Unlock()

	for _, key := range keysToEvict {
		go func(k string) {
			if err := lru.store.Delete(k); err != nil {
				lru.log.Error().Err(err).Str("key", k).Msg("Failed to delete evicted item files")
			}
		}(key)
	}

	lru.log.Debug().Str("key", meta.Key).Str("size", util.FormatSize(meta.Size)).Msg("Item metadata added/updated")
	return nil
}

func (lru *SimpleLRU) Delete(ctx context.Context, key string) error {
	lru.mu.Lock()
	if elem, exists := lru.items[key]; exists {
		meta := elem.Value.(*lruEntry).meta
		lru.currentBytes -= meta.Size
		lru.lruList.Remove(elem)
		delete(lru.items, key)
		util.ReturnHeader(meta.Headers)
	}
	lru.mu.Unlock()

	return lru.store.Delete(key)
}

func (lru *SimpleLRU) GetContent(ctx context.Context, key string) (io.ReadCloser, error) {
	return lru.store.GetContent(key)
}

func (lru *SimpleLRU) PutContent(ctx context.Context, key string, r io.Reader) (int64, error) {
	return lru.store.PutContent(key, r)
}

func (lru *SimpleLRU) Close() {
	lru.log.Info().Msg("SimpleLRU cache closed.")
}

func (lru *SimpleLRU) ensureSpaceLocked(needed int64) []string {
	var keysToEvict []string
	for lru.maxBytes > 0 && (lru.currentBytes+needed) > lru.maxBytes && lru.lruList.Len() > 0 {
		key := lru.evictOneLocked()
		if key != "" {
			keysToEvict = append(keysToEvict, key)
		}
	}
	return keysToEvict
}

func (lru *SimpleLRU) evictOneLocked() string {
	elem := lru.lruList.Back()
	if elem == nil {
		return ""
	}
	entry := lru.lruList.Remove(elem).(*lruEntry)
	delete(lru.items, entry.key)
	lru.currentBytes -= entry.meta.Size
	util.ReturnHeader(entry.meta.Headers)

	lru.log.Info().
		Str("key", entry.key).
		Str("size", util.FormatSize(entry.meta.Size)).
		Time("last_used", entry.meta.LastUsedAt).
		Msg("Evicting item")

	return entry.key
}
