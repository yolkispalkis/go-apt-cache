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
	start := time.Now()

	metaFiles, err := lru.store.ScanMetaFiles()
	if err != nil {
		return fmt.Errorf("scan meta files: %w", err)
	}

	var metas []*ItemMeta
	for _, mPath := range metaFiles {
		m, err := lru.store.ReadMeta(mPath)
		if err != nil {
			lru.log.Warn().Err(err).Str("path", mPath).Msg("Corrupted meta; deleting")
			_ = os.Remove(mPath)
			_ = os.Remove(strings.TrimSuffix(mPath, metaSuffix) + contentSuffix)
			continue
		}
		metas = append(metas, m)
	}

	sort.Slice(metas, func(i, j int) bool { return metas[i].LastUsedAt.After(metas[j].LastUsedAt) })

	for _, m := range metas {
		elem := lru.lruList.PushFront(&lruEntry{key: m.Key, meta: m})
		lru.items[m.Key] = elem
		lru.currentBytes += m.Size
	}

	lru.log.Info().
		Dur("duration", time.Since(start)).
		Int("files", len(metas)).
		Str("size", util.FormatSize(lru.currentBytes)).
		Msg("Initial disk scan complete")

	return nil
}

func (lru *SimpleLRU) Get(ctx context.Context, key string) (*ItemMeta, bool) {
	lru.mu.Lock()
	elem, ok := lru.items[key]
	if !ok {
		lru.mu.Unlock()
		return nil, false
	}

	lru.lruList.MoveToFront(elem)
	entry := elem.Value.(*lruEntry)
	entry.meta.LastUsedAt = time.Now()
	metaPtr := entry.meta
	lru.mu.Unlock()

	// persist asynchronously
	go func(m *ItemMeta) {
		if err := lru.store.WriteMetadata(m); err != nil {
			lru.log.Warn().Err(err).Str("key", m.Key).Msg("Failed to persist LastUsedAt")
		}
	}(metaPtr)

	copy := *metaPtr
	copy.Headers = util.CopyHeader(metaPtr.Headers)
	return &copy, true
}

func (lru *SimpleLRU) Put(ctx context.Context, meta *ItemMeta) error {
	if err := lru.store.WriteMetadata(meta); err != nil {
		return fmt.Errorf("write meta %s: %w", meta.Key, err)
	}

	lru.mu.Lock()

	if oldElem, exists := lru.items[meta.Key]; exists {
		oldMeta := oldElem.Value.(*lruEntry).meta
		lru.currentBytes -= oldMeta.Size
		lru.lruList.Remove(oldElem)
	}

	var evictKeys []string
	if meta.Size > 0 {
		evictKeys = lru.ensureSpaceLocked(meta.Size)
	}

	elem := lru.lruList.PushFront(&lruEntry{key: meta.Key, meta: meta})
	lru.items[meta.Key] = elem
	lru.currentBytes += meta.Size

	lru.mu.Unlock()

	// delete files of evicted keys outside lock
	for _, k := range evictKeys {
		k := k
		go func() {
			if err := lru.store.Delete(k); err != nil {
				lru.log.Error().Err(err).Str("key", k).Msg("Delete evicted item failed")
			}
		}()
	}
	return nil
}

func (lru *SimpleLRU) Delete(ctx context.Context, key string) error {
	lru.mu.Lock()
	if elem, ok := lru.items[key]; ok {
		meta := elem.Value.(*lruEntry).meta
		lru.currentBytes -= meta.Size
		lru.lruList.Remove(elem)
		delete(lru.items, key)
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
	lru.log.Info().Msg("SimpleLRU cache closed")
}

func (lru *SimpleLRU) ensureSpaceLocked(need int64) []string {
	var keys []string
	for lru.maxBytes > 0 && (lru.currentBytes+need) > lru.maxBytes && lru.lruList.Len() > 0 {
		if k := lru.evictOneLocked(); k != "" {
			keys = append(keys, k)
		}
	}
	return keys
}

func (lru *SimpleLRU) evictOneLocked() string {
	back := lru.lruList.Back()
	if back == nil {
		return ""
	}
	entry := lru.lruList.Remove(back).(*lruEntry)
	delete(lru.items, entry.key)
	lru.currentBytes -= entry.meta.Size

	lru.log.Info().
		Str("key", entry.key).
		Str("size", util.FormatSize(entry.meta.Size)).
		Time("last_used", entry.meta.LastUsedAt).
		Msg("Evicting item")
	return entry.key
}
