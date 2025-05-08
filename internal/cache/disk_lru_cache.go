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
	key         string
	size        int64
	filePath    string
	metaPath    string
	listElement *list.Element
}

type DiskLRUCache struct {
	baseDir          string
	maxSizeBytes     int64
	defaultTTL       time.Duration
	negativeCacheTTL time.Duration

	mu          sync.RWMutex
	items       map[string]*CacheItemMetadata
	lruList     *list.List
	currentSize int64

	store       *diskStore
	logger      zerolog.Logger
	cfg         config.CacheConfig
	initialized bool
	initOnce    sync.Once
	initErr     error
}

func NewDiskLRUCache(cfg config.CacheConfig, logger zerolog.Logger) (*DiskLRUCache, error) {
	if !cfg.Enabled {
		logger.Info().Msg("Cache is disabled in configuration.")
		return &DiskLRUCache{cfg: cfg, logger: logger, initialized: true}, nil
	}

	maxSize, err := util.ParseSize(cfg.MaxSize)
	if err != nil {
		return nil, fmt.Errorf("invalid cache max size %q: %w", cfg.MaxSize, err)
	}
	if maxSize <= 0 && cfg.Enabled {
		return nil, fmt.Errorf("cache max size must be positive if cache is enabled: got %s", cfg.MaxSize)
	}

	absBaseDir, err := filepath.Abs(cfg.Directory)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path for cache directory %s: %w", cfg.Directory, err)
	}

	ds, err := newDiskStore(absBaseDir, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize disk store at %s: %w", absBaseDir, err)
	}

	lc := &DiskLRUCache{
		baseDir:          absBaseDir,
		maxSizeBytes:     maxSize,
		defaultTTL:       cfg.DefaultTTL.Duration(),
		negativeCacheTTL: cfg.NegativeCacheTTL.Duration(),
		items:            make(map[string]*CacheItemMetadata),
		lruList:          list.New(),
		currentSize:      0,
		store:            ds,
		logger:           logger.With().Str("component", "DiskLRUCache").Logger(),
		cfg:              cfg,
	}
	return lc, nil
}

func (c *DiskLRUCache) Init(ctx context.Context) error {
	if !c.cfg.Enabled {
		c.initialized = true
		return nil
	}

	c.initOnce.Do(func() {
		c.logger.Info().Str("directory", c.baseDir).Msg("Initializing cache...")
		startTime := time.Now()

		if c.cfg.CleanOnStart {
			c.logger.Info().Msg("Cleaning cache directory on startup as per configuration...")
			if err := c.store.cleanDirectory(); err != nil {
				c.initErr = fmt.Errorf("failed to clean cache directory: %w", err)
				c.logger.Error().Err(c.initErr).Msg("Cache clean failed.")
				return
			}
			c.logger.Info().Msg("Cache directory cleaned.")

			c.initialized = true
			return
		}

		metaFiles, err := c.store.scanMetadataFiles()
		if err != nil {
			c.initErr = fmt.Errorf("failed to scan cache directory: %w", err)
			c.logger.Error().Err(c.initErr).Msg("Cache scan failed.")
			return
		}

		loadedCount := 0
		var totalSize int64
		tempLruEntries := make([]*lruEntry, 0, len(metaFiles))

		for _, metaPath := range metaFiles {
			select {
			case <-ctx.Done():
				c.initErr = ctx.Err()
				return
			default:
			}

			meta, err := c.store.readMetadataFromFile(metaPath)
			if err != nil {
				c.logger.Warn().Str("path", metaPath).Err(err).Msg("Failed to read metadata file during init, skipping.")

				contentPath := strings.TrimSuffix(metaPath, metadataSuffix) + contentSuffix
				_ = os.Remove(metaPath)
				_ = os.Remove(contentPath)
				continue
			}

			if meta.Key == "" {
				c.logger.Warn().Str("path", metaPath).Msg("Metadata file has empty key, skipping.")
				continue
			}
			contentPath := c.store.contentPath(meta.Key)
			info, err := os.Stat(contentPath)
			if err != nil {
				c.logger.Warn().Str("key", meta.Key).Str("content_path", contentPath).Err(err).Msg("Content file missing or unreadable for metadata, removing metadata.")
				_ = os.Remove(metaPath)
				continue
			}
			if info.Size() != meta.Size {
				c.logger.Warn().Str("key", meta.Key).Int64("meta_size", meta.Size).Int64("file_size", info.Size()).Msg("Content file size mismatch with metadata, removing.")
				_ = os.Remove(metaPath)
				_ = os.Remove(contentPath)
				continue
			}

			meta.Path = contentPath
			meta.MetaPath = metaPath

			c.items[meta.Key] = meta
			totalSize += meta.Size

			tempLruEntries = append(tempLruEntries, &lruEntry{
				key:      meta.Key,
				size:     meta.Size,
				filePath: meta.Path,
				metaPath: meta.MetaPath,
			})
			loadedCount++
		}

		sort.Slice(tempLruEntries, func(i, j int) bool {
			return c.items[tempLruEntries[i].key].LastUsedAt.After(c.items[tempLruEntries[j].key].LastUsedAt)
		})

		for _, entryData := range tempLruEntries {
			el := c.lruList.PushFront(&lruEntry{
				key:      entryData.key,
				size:     entryData.size,
				filePath: entryData.filePath,
				metaPath: entryData.metaPath,
			})

			(el.Value.(*lruEntry)).listElement = el
		}

		c.currentSize = totalSize
		c.logger.Info().
			Int("loaded_items", loadedCount).
			Str("total_size", util.FormatSize(c.currentSize)).
			Dur("duration", time.Since(startTime)).
			Msg("Cache initialization complete.")

		if c.currentSize > c.maxSizeBytes {
			c.logger.Info().
				Str("current_size", util.FormatSize(c.currentSize)).
				Str("max_size", util.FormatSize(c.maxSizeBytes)).
				Msg("Cache size exceeds maximum after init, performing eviction.")
			c.evict(c.currentSize - c.maxSizeBytes)
		}
		c.initialized = true
	})
	return c.initErr
}

func (c *DiskLRUCache) checkInitialized() error {
	if !c.initialized && c.cfg.Enabled {
		return errors.New("cache not initialized or initialization failed")
	}
	return nil
}

func (c *DiskLRUCache) Get(ctx context.Context, key string) (*CacheGetResult, error) {
	if !c.cfg.Enabled {
		return nil, ErrNotFound
	}
	if err := c.checkInitialized(); err != nil {
		return nil, err
	}

	c.mu.RLock()
	meta, exists := c.items[key]
	c.mu.RUnlock()

	if !exists {
		return nil, ErrNotFound
	}

	if meta.StatusCode == http.StatusNotFound {
		if c.negativeCacheTTL > 0 && time.Since(meta.FetchedAt) > c.negativeCacheTTL {
			c.logger.Debug().Str("key", key).Msg("Negative cache entry expired.")

			go func() {
				if err := c.Delete(context.Background(), key); err != nil {
					c.logger.Warn().Err(err).Str("key", key).Msg("Failed to delete expired negative cache entry")
				}
			}()
			return nil, ErrNotFound
		}

		c.MarkUsed(ctx, key)
		return &CacheGetResult{
			Metadata: meta,
			Content:  nil,
			Hit:      true,
		}, nil
	}

	contentFile, err := c.store.openContentFile(meta.Path)
	if err != nil {
		c.logger.Error().Err(err).Str("key", key).Str("path", meta.Path).Msg("Failed to open content file for cached item.")

		go func() {
			if delErr := c.Delete(context.Background(), key); delErr != nil {
				c.logger.Warn().Err(delErr).Str("key", key).Msg("Failed to delete inconsistent cache entry")
			}
		}()
		return nil, ErrNotFound
	}

	c.MarkUsed(ctx, key)

	return &CacheGetResult{
		Metadata: meta,
		Content:  contentFile,
		Hit:      true,
	}, nil
}

func (c *DiskLRUCache) Put(ctx context.Context, key string, reader io.Reader, opts CachePutOptions) (*CacheItemMetadata, error) {
	if !c.cfg.Enabled {

		_, _ = io.Copy(io.Discard, reader)
		if rc, ok := reader.(io.Closer); ok {
			_ = rc.Close()
		}
		return nil, errors.New("cache disabled, item not stored")
	}
	if err := c.checkInitialized(); err != nil {
		return nil, err
	}

	if opts.StatusCode == http.StatusNotFound && c.negativeCacheTTL > 0 {
		return c.putNegativeEntry(ctx, key, opts)
	}

	if opts.StatusCode != http.StatusOK && opts.StatusCode != http.StatusPartialContent {
		c.logger.Debug().Str("key", key).Int("status", opts.StatusCode).Msg("Not caching non-200/206/404 response.")
		_, _ = io.Copy(io.Discard, reader)
		if rc, ok := reader.(io.Closer); ok {
			_ = rc.Close()
		}
		return nil, fmt.Errorf("will not cache response with status %d", opts.StatusCode)
	}

	now := time.Now()
	meta := &CacheItemMetadata{
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

	writtenSize, contentPath, metaPath, err := c.store.write(key, reader, meta)
	if err != nil {
		return nil, fmt.Errorf("failed to write content to disk store for key %s: %w", key, err)
	}
	meta.Size = writtenSize
	meta.Path = contentPath
	meta.MetaPath = metaPath

	c.mu.Lock()
	defer c.mu.Unlock()

	if oldMeta, exists := c.items[key]; exists {
		c.logger.Debug().Str("key", key).Msg("Replacing existing item in cache.")
		c.removeItemLocked(key, oldMeta, false)
	}

	if c.maxSizeBytes > 0 && c.currentSize+meta.Size > c.maxSizeBytes {
		c.evictLocked((c.currentSize + meta.Size) - c.maxSizeBytes)
	}

	if c.maxSizeBytes > 0 && meta.Size > c.maxSizeBytes {
		c.logger.Warn().Str("key", key).
			Str("item_size", util.FormatSize(meta.Size)).
			Str("max_cache_size", util.FormatSize(c.maxSizeBytes)).
			Msg("Item size exceeds max cache size, not caching.")

		_ = c.store.deleteFiles(contentPath, metaPath)
		return nil, errors.New("item size exceeds maximum cache size")
	}

	c.items[key] = meta
	lruVal := &lruEntry{
		key:      key,
		size:     meta.Size,
		filePath: meta.Path,
		metaPath: meta.MetaPath,
	}
	element := c.lruList.PushFront(lruVal)
	lruVal.listElement = element

	atomic.AddInt64(&c.currentSize, meta.Size)

	c.logger.Debug().Str("key", key).Str("size", util.FormatSize(meta.Size)).Msg("Item added to cache.")
	return meta, nil
}

func (c *DiskLRUCache) putNegativeEntry(ctx context.Context, key string, opts CachePutOptions) (*CacheItemMetadata, error) {
	c.logger.Debug().Str("key", key).Dur("ttl", c.negativeCacheTTL).Msg("Caching negative (404) entry.")
	now := time.Now()
	meta := &CacheItemMetadata{
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

	meta.MetaPath = c.store.metadataPath(key)

	if err := c.store.writeMetadata(meta.MetaPath, meta); err != nil {
		return nil, fmt.Errorf("failed to write metadata for negative cache entry %s: %w", key, err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if oldMeta, exists := c.items[key]; exists {
		c.removeItemLocked(key, oldMeta, true)
	}

	c.items[key] = meta
	lruVal := &lruEntry{
		key:      key,
		size:     0,
		filePath: "",
		metaPath: meta.MetaPath,
	}
	element := c.lruList.PushFront(lruVal)
	lruVal.listElement = element

	return meta, nil
}

func (c *DiskLRUCache) calculateExpiresAt(headers http.Header, fetchedAt time.Time) time.Time {

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

	if c.defaultTTL > 0 {
		return fetchedAt.Add(c.defaultTTL)
	}

	return time.Time{}
}

func (c *DiskLRUCache) Delete(ctx context.Context, key string) error {
	if !c.cfg.Enabled {
		return nil
	}
	if err := c.checkInitialized(); err != nil {

		c.logger.Warn().Err(err).Str("key", key).Msg("Attempting delete on uninitialized cache.")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	meta, exists := c.items[key]
	if !exists {
		return nil
	}

	c.removeItemLocked(key, meta, true)
	c.logger.Debug().Str("key", key).Msg("Item removed from cache.")
	return nil
}

func (c *DiskLRUCache) removeItemLocked(key string, meta *CacheItemMetadata, deleteFiles bool) {

	var lruVal *lruEntry
	var targetElement *list.Element
	for e := c.lruList.Front(); e != nil; e = e.Next() {
		entry := e.Value.(*lruEntry)
		if entry.key == key {
			lruVal = entry
			targetElement = e
			break
		}
	}

	if targetElement != nil {
		c.lruList.Remove(targetElement)
	} else {

		c.logger.Warn().Str("key", key).Msg("Item found in map but not in LRU list during remove. Inconsistency?")
	}

	delete(c.items, key)
	if meta.StatusCode != http.StatusNotFound {
		atomic.AddInt64(&c.currentSize, -meta.Size)
	}

	if deleteFiles {

		contentPathToDelete := meta.Path
		metaPathToDelete := meta.MetaPath

		if lruVal != nil {
			if lruVal.filePath != "" {
				contentPathToDelete = lruVal.filePath
			}
			if lruVal.metaPath != "" {
				metaPathToDelete = lruVal.metaPath
			}
		}

		if err := c.store.deleteFiles(contentPathToDelete, metaPathToDelete); err != nil {
			c.logger.Error().Err(err).Str("key", key).Msg("Failed to delete cache files.")
		}
	}
}

func (c *DiskLRUCache) MarkUsed(ctx context.Context, key string) error {
	if !c.cfg.Enabled {
		return nil
	}
	if err := c.checkInitialized(); err != nil {
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
		c.logger.Warn().Err(err).Str("key", key).Msg("Failed to update metadata file on MarkUsed")

	}

	var targetElement *list.Element
	for e := c.lruList.Front(); e != nil; e = e.Next() {
		if e.Value.(*lruEntry).key == key {
			targetElement = e
			break
		}
	}
	if targetElement != nil {
		c.lruList.MoveToFront(targetElement)
	} else {

		c.logger.Debug().Str("key", key).Msg("Item marked used but not found in LRU list (potentially negative entry or new item).")
	}
	return nil
}

func (c *DiskLRUCache) UpdateValidatedAt(ctx context.Context, key string, validatedAt time.Time) error {
	if !c.cfg.Enabled {
		return nil
	}
	if err := c.checkInitialized(); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	meta, exists := c.items[key]
	if !exists {
		return ErrNotFound
	}
	meta.ValidatedAt = validatedAt

	if err := c.store.writeMetadata(meta.MetaPath, meta); err != nil {
		c.logger.Warn().Err(err).Str("key", key).Msg("Failed to update metadata file on UpdateValidatedAt")

	}
	return nil
}

func (c *DiskLRUCache) evictLocked(requiredSpace int64) {
	if requiredSpace <= 0 {
		return
	}
	c.logger.Info().
		Str("required_space", util.FormatSize(requiredSpace)).
		Str("current_size", util.FormatSize(c.currentSize)).
		Msg("Starting eviction process.")

	freedSpace := int64(0)
	itemsEvicted := 0

	for c.lruList.Len() > 0 && (freedSpace < requiredSpace || c.currentSize > c.maxSizeBytes) {
		tailElement := c.lruList.Back()
		if tailElement == nil {
			break
		}
		entry := tailElement.Value.(*lruEntry)

		meta, metaExists := c.items[entry.key]
		if !metaExists {

			c.logger.Warn().Str("key", entry.key).Msg("Item in LRU list but not in metadata map during eviction. Removing from LRU.")
			c.lruList.Remove(tailElement)
			continue
		}

		c.logger.Debug().Str("key", entry.key).Str("size", util.FormatSize(entry.size)).Msg("Evicting item.")
		c.removeItemLocked(entry.key, meta, true)

		if meta.StatusCode != http.StatusNotFound {
			freedSpace += entry.size
		}
		itemsEvicted++
	}
	c.logger.Info().
		Int("items_evicted", itemsEvicted).
		Str("space_freed", util.FormatSize(freedSpace)).
		Str("new_size", util.FormatSize(c.currentSize)).
		Msg("Eviction process finished.")
}

func (c *DiskLRUCache) evict(requiredSpace int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.evictLocked(requiredSpace)
}

func (c *DiskLRUCache) Purge(ctx context.Context) error {
	if !c.cfg.Enabled {
		return nil
	}
	c.logger.Info().Msg("Purging all items from cache.")

	c.mu.Lock()
	defer c.mu.Unlock()

	keysToPurge := make([]string, 0, len(c.items))
	for k := range c.items {
		keysToPurge = append(keysToPurge, k)
	}

	for _, key := range keysToPurge {
		if meta, exists := c.items[key]; exists {
			c.removeItemLocked(key, meta, true)
		}
	}

	c.items = make(map[string]*CacheItemMetadata)
	c.lruList = list.New()
	atomic.StoreInt64(&c.currentSize, 0)

	c.logger.Info().Msg("Cache purge complete.")
	return nil
}

func (c *DiskLRUCache) Close() error {
	if !c.cfg.Enabled {
		return nil
	}
	c.logger.Info().Msg("Closing cache manager.")

	return nil
}

func (c *DiskLRUCache) GetCurrentSize() int64 {
	if !c.cfg.Enabled {
		return 0
	}
	return atomic.LoadInt64(&c.currentSize)
}

func (c *DiskLRUCache) GetItemCount() int64 {
	if !c.cfg.Enabled {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return int64(len(c.items))
}
