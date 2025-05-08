package cache

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

type lruEntry struct {
	key  string
	size int64
	elem *list.Element
}

type DiskLRU struct {
	cfg      config.CacheConfig
	log      zerolog.Logger
	store    *diskStore
	maxBytes int64
	defTTL   time.Duration
	negTTL   time.Duration

	mu           sync.RWMutex
	items        map[string]*ItemMeta
	lruList      *list.List
	currentBytes atomic.Int64

	initOnce sync.Once
	initErr  error
	ready    bool
}

func NewDiskLRU(cfg config.CacheConfig, logger zerolog.Logger) (*DiskLRU, error) {
	if !cfg.Enabled {
		logger.Info().Msg("Cache disabled in configuration.")

		return &DiskLRU{cfg: cfg, log: logger, ready: true}, nil
	}

	maxBytes, err := util.ParseSize(cfg.MaxSize)
	if err != nil {
		return nil, fmt.Errorf("invalid cache MaxSize %q: %w", cfg.MaxSize, err)
	}
	if maxBytes <= 0 {
		return nil, fmt.Errorf("cache MaxSize must be > 0 if enabled: %s", cfg.MaxSize)
	}

	absCacheDir, err := filepath.Abs(cfg.Dir)
	if err != nil {
		return nil, fmt.Errorf("abs path for cache dir %s: %w", cfg.Dir, err)
	}

	effectiveCfg := cfg
	effectiveCfg.Dir = absCacheDir

	store, err := newDiskStore(effectiveCfg.Dir, logger)
	if err != nil {
		return nil, fmt.Errorf("init disk store at %s: %w", effectiveCfg.Dir, err)
	}

	return &DiskLRU{
		cfg:      effectiveCfg,
		log:      logger.With().Str("component", "DiskLRU").Logger(),
		store:    store,
		maxBytes: maxBytes,
		defTTL:   cfg.DefaultTTL.StdDuration(),
		negTTL:   cfg.NegativeTTL.StdDuration(),
		items:    make(map[string]*ItemMeta),
		lruList:  list.New(),
	}, nil
}

func (c *DiskLRU) Init(ctx context.Context) error {
	if !c.cfg.Enabled {
		c.ready = true
		return nil
	}

	c.initOnce.Do(func() {
		c.log.Info().Str("dir", c.cfg.Dir).Msg("Initializing cache...")
		startTime := time.Now()

		if c.cfg.CleanOnStart {
			c.log.Info().Msg("Cleaning cache directory on startup as configured.")
			if err := c.store.cleanDir(); err != nil {
				c.initErr = fmt.Errorf("clean cache directory: %w", err)
				c.log.Error().Err(c.initErr).Msg("Cache clean failed.")
				return
			}
			c.log.Info().Msg("Cache directory cleaned.")
			c.ready = true
			return
		}

		metaFiles, err := c.store.scanMetaFiles()
		if err != nil {
			c.initErr = fmt.Errorf("scan cache directory: %w", err)
			c.log.Error().Err(c.initErr).Msg("Cache scan failed.")
			return
		}

		type itemToSort struct{ meta *ItemMeta }
		var itemsToLoad []itemToSort

		var currentSizeOnDisk int64
		loadedCount := 0

		for _, mPath := range metaFiles {
			if ctx.Err() != nil {
				c.initErr = ctx.Err()
				return
			}

			meta, err := c.store.readMeta(mPath)
			if err != nil || meta.Key == "" {
				c.log.Warn().Str("path", mPath).Err(err).Msg("Failed to read/validate metadata, skipping/removing.")
				_ = os.Remove(mPath)
				_ = os.Remove(strings.TrimSuffix(mPath, metadataSuffix) + contentSuffix)
				continue
			}

			meta.MetaPath = mPath
			meta.Path = c.store.contentPath(meta.Key)

			if meta.StatusCode != http.StatusNotFound {
				info, err := os.Stat(meta.Path)
				if err != nil {
					c.log.Warn().Str("key", meta.Key).Str("path", meta.Path).Err(err).Msg("Content file missing/unreadable, removing metadata.")
					_ = os.Remove(mPath)
					continue
				}
				if info.Size() != meta.Size {
					c.log.Warn().Str("key", meta.Key).Int64("meta_size", meta.Size).Int64("file_size", info.Size()).Msg("Content file size mismatch, removing both.")
					_ = os.Remove(mPath)
					_ = os.Remove(meta.Path)
					continue
				}
			}
			itemsToLoad = append(itemsToLoad, itemToSort{meta: meta})
			currentSizeOnDisk += meta.Size
			loadedCount++
		}

		sort.Slice(itemsToLoad, func(i, j int) bool {
			return itemsToLoad[i].meta.LastUsedAt.After(itemsToLoad[j].meta.LastUsedAt)
		})

		for _, item := range itemsToLoad {
			c.items[item.meta.Key] = item.meta
			entry := &lruEntry{key: item.meta.Key, size: item.meta.Size}
			entry.elem = c.lruList.PushFront(entry)
		}
		c.currentBytes.Store(currentSizeOnDisk)

		c.log.Info().
			Int("loaded_items", loadedCount).
			Str("total_size", util.FormatSize(c.currentBytes.Load())).
			Dur("duration", time.Since(startTime)).
			Msg("Cache initialization complete.")

		if c.currentBytes.Load() > c.maxBytes {
			c.log.Info().
				Str("current_size", util.FormatSize(c.currentBytes.Load())).
				Str("max_size", util.FormatSize(c.maxBytes)).
				Msg("Cache size exceeds maximum after init, performing eviction.")

			c.evictItems(c.currentBytes.Load()-c.maxBytes, false)
		}
		c.ready = true
	})
	return c.initErr
}

func (c *DiskLRU) checkReady() error {
	if !c.ready && c.cfg.Enabled {
		return errors.New("cache not initialized or initialization failed")
	}
	return nil
}

func (c *DiskLRU) Get(ctx context.Context, key string) (*GetResult, error) {
	if !c.cfg.Enabled {
		return nil, ErrNotFound
	}
	if err := c.checkReady(); err != nil {
		return nil, err
	}

	c.mu.RLock()
	meta, exists := c.items[key]
	c.mu.RUnlock()

	if !exists {
		return nil, ErrNotFound
	}

	if meta.StatusCode == http.StatusNotFound {
		if c.negTTL > 0 && time.Since(meta.FetchedAt) > c.negTTL {
			c.log.Debug().Str("key", key).Msg("Negative cache entry expired.")
			go c.Delete(context.Background(), key)
			return nil, ErrNotFound
		}
		_ = c.MarkUsed(ctx, key)
		return &GetResult{Meta: meta, Hit: true}, nil
	}

	contentFile, err := c.store.openContentFile(meta.Path)
	if err != nil {
		c.log.Error().Err(err).Str("key", key).Str("path", meta.Path).Msg("Failed to open content file for cached item.")
		go c.Delete(context.Background(), key)
		return nil, ErrNotFound
	}

	_ = c.MarkUsed(ctx, key)
	return &GetResult{Meta: meta, Content: contentFile, Hit: true}, nil
}

func (c *DiskLRU) Put(ctx context.Context, key string, r io.Reader, opts PutOptions) (*ItemMeta, error) {
	if !c.cfg.Enabled {
		if r != nil {
			if rc, ok := r.(io.Closer); ok {
				defer rc.Close()
			}
			_, _ = io.Copy(io.Discard, r)
		}
		return nil, errors.New("cache disabled, item not stored")
	}
	if err := c.checkReady(); err != nil {
		return nil, err
	}

	if opts.StatusCode == http.StatusNotFound && c.negTTL > 0 {
		return c.putNegativeEntry(ctx, key, opts)
	}

	if opts.StatusCode != http.StatusOK && opts.StatusCode != http.StatusPartialContent {
		if r != nil {
			if rc, ok := r.(io.Closer); ok {
				defer rc.Close()
			}
			_, _ = io.Copy(io.Discard, r)
		}
		return nil, fmt.Errorf("will not cache response with status %d for key %s", opts.StatusCode, key)
	}

	now := time.Now()
	meta := &ItemMeta{
		Version:     MetadataVersion,
		Key:         key,
		UpstreamURL: opts.UpstreamURL,
		FetchedAt:   opts.FetchedAt,
		LastUsedAt:  now,
		ValidatedAt: now,
		StatusCode:  opts.StatusCode,
		Headers:     util.CopyHeader(opts.Headers),
		Size:        opts.Size,
	}
	meta.ExpiresAt = c.calculateExpiresAt(meta.Headers, meta.FetchedAt)

	writtenSize, cPath, mPath, err := c.store.write(key, r, meta)
	if err != nil {
		return nil, fmt.Errorf("disk store write for key %s: %w", key, err)
	}

	meta.Size = writtenSize
	meta.Path = cPath
	meta.MetaPath = mPath

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.items[key]; exists {
		c.log.Debug().Str("key", key).Msg("Replacing existing item in cache.")
		c.removeItemLocked(key, false)
	}

	if c.maxBytes > 0 && meta.Size > c.maxBytes {
		c.log.Warn().Str("key", key).
			Str("item_size", util.FormatSize(meta.Size)).
			Str("max_cache_size", util.FormatSize(c.maxBytes)).
			Msg("Item size exceeds max cache size, not caching.")
		_ = c.store.deleteFiles(meta.Path, meta.MetaPath)
		return nil, errors.New("item size exceeds maximum cache size")
	}

	if c.currentBytes.Load()+meta.Size > c.maxBytes {
		c.evictItemsLocked((c.currentBytes.Load()+meta.Size)-c.maxBytes, true)
	}

	c.items[key] = meta
	entry := &lruEntry{key: key, size: meta.Size}
	entry.elem = c.lruList.PushFront(entry)
	c.currentBytes.Add(meta.Size)

	c.log.Debug().Str("key", key).Str("size", util.FormatSize(meta.Size)).Msg("Item added to cache.")
	return meta, nil
}

func (c *DiskLRU) putNegativeEntry(_ context.Context, key string, opts PutOptions) (*ItemMeta, error) {
	c.log.Debug().Str("key", key).Dur("ttl", c.negTTL).Msg("Caching negative (404) entry.")
	now := time.Now()
	meta := &ItemMeta{
		Version:     MetadataVersion,
		Key:         key,
		UpstreamURL: opts.UpstreamURL,
		FetchedAt:   opts.FetchedAt,
		LastUsedAt:  now,
		ValidatedAt: now,
		StatusCode:  http.StatusNotFound,
		Headers:     util.CopyHeader(opts.Headers),
		Size:        0,
	}

	if c.negTTL > 0 {
		meta.ExpiresAt = meta.FetchedAt.Add(c.negTTL)
	}

	_, _, mPath, err := c.store.write(key, nil, meta)
	if err != nil {
		return nil, fmt.Errorf("disk store write negative meta for %s: %w", key, err)
	}
	meta.MetaPath = mPath

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.items[key]; exists {
		c.log.Debug().Str("key", key).Msg("Replacing existing item (was not 404 or was different 404) with new negative entry.")
		c.removeItemLocked(key, true)
	}

	c.items[key] = meta
	entry := &lruEntry{key: key, size: 0}
	entry.elem = c.lruList.PushFront(entry)

	return meta, nil
}

func (c *DiskLRU) calculateExpiresAt(headers http.Header, fetchedAt time.Time) time.Time {
	if ccHeader := headers.Get("Cache-Control"); ccHeader != "" {
		directives := util.ParseCacheControl(ccHeader)
		if maxAgeStr, ok := directives["max-age"]; ok {
			if maxAgeSec, err := strconv.ParseInt(maxAgeStr, 10, 64); err == nil && maxAgeSec > 0 {
				return fetchedAt.Add(time.Duration(maxAgeSec) * time.Second)
			}
		}

		if _, ok := directives["no-store"]; ok {
			return time.Time{}
		}
		if _, ok := directives["no-cache"]; ok {
			return fetchedAt
		}
	}

	if expiresHeader := headers.Get("Expires"); expiresHeader != "" {
		if expiresTime, err := http.ParseTime(expiresHeader); err == nil {
			if expiresTime.Before(fetchedAt) || expiresTime.Equal(fetchedAt) {
				return fetchedAt
			}
			return expiresTime
		}
	}

	if c.defTTL > 0 {
		return fetchedAt.Add(c.defTTL)
	}
	return time.Time{}
}

func (c *DiskLRU) Delete(ctx context.Context, key string) error {
	if !c.cfg.Enabled {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.items[key]; !exists {
		return nil
	}
	c.removeItemLocked(key, true)
	c.log.Debug().Str("key", key).Msg("Item removed from cache.")
	return nil
}

func (c *DiskLRU) removeItemLocked(key string, deleteFilesFromDisk bool) {
	meta, exists := c.items[key]
	if !exists {
		return
	}

	var targetElement *list.Element
	for e := c.lruList.Front(); e != nil; e = e.Next() {
		entry, ok := e.Value.(*lruEntry)
		if ok && entry.key == key {
			targetElement = e
			break
		}
	}

	if targetElement != nil {
		c.lruList.Remove(targetElement)
	} else {
		c.log.Warn().Str("key", key).Msg("Item in map but not in LRU list during remove. Potential inconsistency.")
	}

	delete(c.items, key)
	if meta.StatusCode != http.StatusNotFound {
		c.currentBytes.Add(-meta.Size)
	}

	if deleteFilesFromDisk {
		if err := c.store.deleteFiles(meta.Path, meta.MetaPath); err != nil {
			c.log.Error().Err(err).Str("key", key).Msg("Failed to delete cache files from disk.")
		}
	}
}

func (c *DiskLRU) MarkUsed(ctx context.Context, key string) error {
	if !c.cfg.Enabled {
		return nil
	}
	if err := c.checkReady(); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	meta, exists := c.items[key]
	if !exists {
		return ErrNotFound
	}

	meta.LastUsedAt = time.Now()

	if err := c.store.writeMetadata(meta.MetaPath, meta); err != nil {
		c.log.Warn().Err(err).Str("key", key).Msg("Failed to update metadata file on MarkUsed")
	}

	foundInLRU := false
	for e := c.lruList.Front(); e != nil; e = e.Next() {
		if entry, ok := e.Value.(*lruEntry); ok && entry.key == key {
			c.lruList.MoveToFront(e)
			foundInLRU = true
			break
		}
	}
	if !foundInLRU {
		c.log.Warn().Str("key", key).Msg("Item marked used but not found in LRU list (potentially new or inconsistent state).")
	}
	return nil
}

func (c *DiskLRU) UpdateValidatedAt(ctx context.Context, key string, validatedAt time.Time) error {
	if !c.cfg.Enabled {
		return nil
	}
	if err := c.checkReady(); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	meta, exists := c.items[key]
	if !exists {
		return ErrNotFound
	}

	meta.ValidatedAt = validatedAt
	meta.LastUsedAt = time.Now()

	if err := c.store.writeMetadata(meta.MetaPath, meta); err != nil {
		c.log.Warn().Err(err).Str("key", key).Msg("Failed to update metadata file on UpdateValidatedAt")
	}

	for e := c.lruList.Front(); e != nil; e = e.Next() {
		if entry, ok := e.Value.(*lruEntry); ok && entry.key == key {
			c.lruList.MoveToFront(e)
			break
		}
	}
	return nil
}

func (c *DiskLRU) evictItemsLocked(requiredSpaceToFree int64, deleteFiles bool) {
	if requiredSpaceToFree <= 0 && c.currentBytes.Load() <= c.maxBytes {
		return
	}

	c.log.Info().
		Str("required_space", util.FormatSize(requiredSpaceToFree)).
		Str("current_size", util.FormatSize(c.currentBytes.Load())).
		Msg("Starting eviction process.")

	freedSpace := int64(0)
	itemsEvicted := 0

	for c.lruList.Len() > 0 && (freedSpace < requiredSpaceToFree || c.currentBytes.Load() > c.maxBytes) {
		tailElement := c.lruList.Back()
		if tailElement == nil {
			break
		}

		entry, ok := tailElement.Value.(*lruEntry)
		if !ok {
			c.lruList.Remove(tailElement)
			continue
		}

		meta, metaExists := c.items[entry.key]
		if !metaExists {
			c.log.Warn().Str("key", entry.key).Msg("Item in LRU list but not in metadata map during eviction. Removing from LRU.")
			c.lruList.Remove(tailElement)
			continue
		}

		c.log.Debug().Str("key", entry.key).Str("size", util.FormatSize(entry.size)).Msg("Evicting item.")
		c.removeItemLocked(entry.key, deleteFiles)

		if meta.StatusCode != http.StatusNotFound {
			freedSpace += entry.size
		}
		itemsEvicted++
	}
	c.log.Info().
		Int("items_evicted", itemsEvicted).
		Str("space_freed", util.FormatSize(freedSpace)).
		Str("new_size", util.FormatSize(c.currentBytes.Load())).
		Msg("Eviction process finished.")
}

func (c *DiskLRU) evictItems(requiredSpaceToFree int64, deleteFiles bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.evictItemsLocked(requiredSpaceToFree, deleteFiles)
}

func (c *DiskLRU) Purge(ctx context.Context) error {
	if !c.cfg.Enabled {
		return nil
	}
	c.log.Info().Msg("Purging all items from cache.")

	c.mu.Lock()
	defer c.mu.Unlock()

	keysToPurge := make([]string, 0, len(c.items))
	for k := range c.items {
		keysToPurge = append(keysToPurge, k)
	}
	for _, key := range keysToPurge {
		c.removeItemLocked(key, true)
	}

	c.items = make(map[string]*ItemMeta)
	c.lruList = list.New()
	c.currentBytes.Store(0)

	c.log.Info().Msg("Cache purge complete.")
	return nil
}

func (c *DiskLRU) Close() error {
	if !c.cfg.Enabled {
		return nil
	}
	c.log.Info().Msg("Closing cache manager.")
	return nil
}

func (c *DiskLRU) CurrentSize() int64 {
	if !c.cfg.Enabled {
		return 0
	}
	return c.currentBytes.Load()
}

func (c *DiskLRU) ItemCount() int64 {
	if !c.cfg.Enabled {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return int64(len(c.items))
}
