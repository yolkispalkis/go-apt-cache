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
	meta *ItemMeta
}

type DiskLRU struct {
	cfg      config.CacheConfig
	log      zerolog.Logger
	store    *diskStore
	maxBytes int64

	negTTL time.Duration

	mu           sync.RWMutex
	items        map[string]*list.Element
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

		negTTL:  cfg.NegativeTTL.StdDuration(),
		items:   make(map[string]*list.Element),
		lruList: list.New(),
	}, nil
}

func (c *DiskLRU) calculateExpiresAt(headers http.Header, dateValueTime time.Time) time.Time {
	ageSec := int64(0)
	if ageHeader := headers.Get("Age"); ageHeader != "" {
		if ageVal, err := strconv.ParseInt(ageHeader, 10, 64); err == nil && ageVal >= 0 {
			ageSec = ageVal
		} else {
			c.log.Warn().Str("age_header", ageHeader).Err(err).Msg("Invalid Age header, ignoring.")
		}
	}

	var freshnessLifetimeSec int64 = -1

	if ccHeader := headers.Get("Cache-Control"); ccHeader != "" {
		directives := util.ParseCacheControl(ccHeader)

		if _, ok := directives["no-store"]; ok {
			return time.Unix(0, 0)
		}

		if sMaxAgeStr, ok := directives["s-maxage"]; ok {
			if sMaxAgeVal, err := strconv.ParseInt(sMaxAgeStr, 10, 64); err == nil && sMaxAgeVal >= 0 {
				freshnessLifetimeSec = sMaxAgeVal
			}
		}

		if freshnessLifetimeSec == -1 {
			if maxAgeStr, ok := directives["max-age"]; ok {
				if maxAgeVal, err := strconv.ParseInt(maxAgeStr, 10, 64); err == nil && maxAgeVal >= 0 {
					freshnessLifetimeSec = maxAgeVal
				}
			}
		}

		if _, ok := directives["no-cache"]; ok {
			return dateValueTime
		}
	}

	if freshnessLifetimeSec == -1 {
		if expiresHeader := headers.Get("Expires"); expiresHeader != "" {
			if expiresTime, err := http.ParseTime(expiresHeader); err == nil {
				if expiresTime.After(dateValueTime) {
					freshnessLifetimeSec = int64(expiresTime.Sub(dateValueTime).Seconds())
				} else {
					freshnessLifetimeSec = 0
				}
			}
		}
	}

	if freshnessLifetimeSec != -1 {
		currentFreshnessSec := freshnessLifetimeSec - ageSec
		if currentFreshnessSec < 0 {
			currentFreshnessSec = 0
		}
		return dateValueTime.Add(time.Duration(currentFreshnessSec) * time.Second)
	}

	if lmStr := headers.Get("Last-Modified"); lmStr != "" {
		if lastModTime, err := http.ParseTime(lmStr); err == nil {
			if dateValueTime.After(lastModTime) {
				heuristicLifetime := dateValueTime.Sub(lastModTime) / 10
				heuristicLifetimeSec := int64(heuristicLifetime.Seconds()) - ageSec
				if heuristicLifetimeSec < 0 {
					heuristicLifetimeSec = 0
				}
				keyForLog := ""
				if headers != nil {
					keyForLog = headers.Get("X-Cache-Key-Debug")
				}
				c.log.Debug().Str("key", keyForLog).Dur("heuristic_ttl", time.Duration(heuristicLifetimeSec)*time.Second).Msg("Applying heuristic caching TTL")
				return dateValueTime.Add(time.Duration(heuristicLifetimeSec) * time.Second)
			}
		}
	}
	return dateValueTime
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

		type itemToSort struct {
			key  string
			meta *ItemMeta
		}
		var itemsToLoad []itemToSort
		var currentSizeOnDisk int64
		loadedCount := 0

		for _, mPath := range metaFiles {
			if ctx.Err() != nil {
				c.initErr = ctx.Err()
				return
			}

			meta, err := c.store.readMeta(mPath)
			if err != nil {
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
			itemsToLoad = append(itemsToLoad, itemToSort{key: meta.Key, meta: meta})
			if meta.StatusCode != http.StatusNotFound {
				currentSizeOnDisk += meta.Size
			}
			loadedCount++
		}

		sort.Slice(itemsToLoad, func(i, j int) bool {
			return itemsToLoad[i].meta.LastUsedAt.Time().Before(itemsToLoad[j].meta.LastUsedAt.Time())
		})

		for _, item := range itemsToLoad {
			entry := &lruEntry{key: item.key, meta: item.meta}
			listElement := c.lruList.PushFront(entry)
			c.items[item.key] = listElement
		}
		c.currentBytes.Store(currentSizeOnDisk)

		c.log.Info().
			Int("loaded_items", loadedCount).
			Str("total_size", util.FormatSize(c.currentBytes.Load())).
			Dur("duration", time.Since(startTime)).
			Msg("Cache initialization complete.")

		if c.currentBytes.Load() > c.maxBytes && c.maxBytes > 0 {
			c.log.Info().
				Str("current_size", util.FormatSize(c.currentBytes.Load())).
				Str("max_size", util.FormatSize(c.maxBytes)).
				Msg("Cache size exceeds maximum after init, performing eviction.")

			c.evictItems(c.currentBytes.Load()-c.maxBytes, true)
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
	listElement, exists := c.items[key]
	c.mu.RUnlock()

	if !exists {
		return nil, ErrNotFound
	}

	entry := listElement.Value.(*lruEntry)
	meta := entry.meta

	if meta.StatusCode == http.StatusNotFound {
		if c.negTTL > 0 && time.Since(meta.FetchedAt.Time()) > c.negTTL {
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

	prelimMetaForWrite := &ItemMeta{
		Version:     MetadataVersion,
		Key:         key,
		UpstreamURL: opts.UpstreamURL,
		FetchedAt:   UnixTime(opts.FetchedAt),
		StatusCode:  opts.StatusCode,
		Headers:     util.CopyHeader(opts.Headers),
		Size:        opts.Size,
	}
	if prelimMetaForWrite.Headers == nil {
		prelimMetaForWrite.Headers = make(http.Header)
	}
	prelimMetaForWrite.Headers.Set("X-Cache-Key-Debug", key)

	writtenSize, cPath, mPath, err := c.store.write(key, r, prelimMetaForWrite)
	if err != nil {
		return nil, fmt.Errorf("disk store write for key %s: %w", key, err)
	}

	now := time.Now()
	finalNewMeta := &ItemMeta{
		Version:     MetadataVersion,
		Key:         key,
		Path:        cPath,
		MetaPath:    mPath,
		UpstreamURL: opts.UpstreamURL,
		FetchedAt:   UnixTime(opts.FetchedAt),
		LastUsedAt:  UnixTime(now),
		ValidatedAt: UnixTime(now),
		StatusCode:  opts.StatusCode,
		Headers:     util.CopyHeader(prelimMetaForWrite.Headers),
		Size:        writtenSize,
	}

	var dateValueTime time.Time
	if dateStr := finalNewMeta.Headers.Get("Date"); dateStr != "" {
		if t, errDate := http.ParseTime(dateStr); errDate == nil {
			dateValueTime = t
		}
	}
	if dateValueTime.IsZero() {
		dateValueTime = opts.FetchedAt
	}
	finalNewMeta.ExpiresAt = UnixTime(c.calculateExpiresAt(finalNewMeta.Headers, dateValueTime))

	c.mu.Lock()
	defer c.mu.Unlock()

	if oldElement, exists := c.items[key]; exists {
		c.log.Debug().Str("key", key).Msg("Replacing existing item in cache.")
		c.removeItemLocked(oldElement.Value.(*lruEntry).key, true)
	}

	if c.maxBytes > 0 && finalNewMeta.Size > c.maxBytes {
		c.log.Warn().Str("key", key).
			Str("item_size", util.FormatSize(finalNewMeta.Size)).
			Str("max_cache_size", util.FormatSize(c.maxBytes)).
			Msg("Item size exceeds max cache size, not caching. Cleaning up written files.")
		_ = c.store.deleteFiles(finalNewMeta.Path, finalNewMeta.MetaPath)
		return nil, errors.New("item size exceeds maximum cache size")
	}

	if c.currentBytes.Load()+finalNewMeta.Size > c.maxBytes && c.maxBytes > 0 {
		spaceToFree := (c.currentBytes.Load() + finalNewMeta.Size) - c.maxBytes
		c.evictItemsLocked(spaceToFree, true)
	}

	if c.currentBytes.Load()+finalNewMeta.Size > c.maxBytes && c.maxBytes > 0 {
		c.log.Warn().Str("key", key).
			Str("item_size", util.FormatSize(finalNewMeta.Size)).
			Str("current_cache_size_after_evict", util.FormatSize(c.currentBytes.Load())).
			Str("max_cache_size", util.FormatSize(c.maxBytes)).
			Msg("Not enough space for item even after eviction attempt, not caching. Cleaning up written files.")
		_ = c.store.deleteFiles(finalNewMeta.Path, finalNewMeta.MetaPath)
		return nil, errors.New("item size exceeds maximum cache size after eviction attempt")
	}

	entry := &lruEntry{key: key, meta: finalNewMeta}
	listElement := c.lruList.PushFront(entry)
	c.items[key] = listElement
	if finalNewMeta.StatusCode != http.StatusNotFound {
		c.currentBytes.Add(finalNewMeta.Size)
	}

	c.log.Debug().Str("key", key).Str("size", util.FormatSize(finalNewMeta.Size)).Msg("Item added to cache.")
	return finalNewMeta, nil
}

func (c *DiskLRU) putNegativeEntry(_ context.Context, key string, opts PutOptions) (*ItemMeta, error) {
	c.log.Debug().Str("key", key).Dur("ttl", c.negTTL).Msg("Caching negative (404) entry.")
	now := time.Now()
	meta := &ItemMeta{
		Version:     MetadataVersion,
		Key:         key,
		UpstreamURL: opts.UpstreamURL,
		FetchedAt:   UnixTime(opts.FetchedAt),
		LastUsedAt:  UnixTime(now),
		ValidatedAt: UnixTime(now),
		StatusCode:  http.StatusNotFound,
		Headers:     util.CopyHeader(opts.Headers),
		Size:        0,
	}
	if meta.Headers == nil {
		meta.Headers = make(http.Header)
	}
	meta.Headers.Set("X-Cache-Key-Debug", key)

	if c.negTTL > 0 {
		meta.ExpiresAt = UnixTime(meta.FetchedAt.Time().Add(c.negTTL))
	}

	_, _, mPath, err := c.store.write(key, nil, meta)
	if err != nil {
		return nil, fmt.Errorf("disk store write negative meta for %s: %w", key, err)
	}
	meta.MetaPath = mPath

	c.mu.Lock()
	defer c.mu.Unlock()

	if oldElement, exists := c.items[key]; exists {
		c.log.Debug().Str("key", key).Msg("Replacing existing item with new negative entry.")
		c.removeItemLocked(oldElement.Value.(*lruEntry).key, true)
	}

	entry := &lruEntry{key: key, meta: meta}
	listElement := c.lruList.PushFront(entry)
	c.items[key] = listElement

	return meta, nil
}

func (c *DiskLRU) Delete(ctx context.Context, key string) error {
	if !c.cfg.Enabled {
		return nil
	}
	if err := c.checkReady(); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.items[key]; !exists {
		_ = c.store.deleteFiles(c.store.contentPath(key), c.store.metadataPath(key))
		return nil
	}
	c.removeItemLocked(key, true)
	c.log.Debug().Str("key", key).Msg("Item removed from cache.")
	return nil
}

func (c *DiskLRU) removeItemLocked(key string, deleteFilesFromDisk bool) {
	listElement, exists := c.items[key]
	if !exists {
		if deleteFilesFromDisk {
			_ = c.store.deleteFiles(c.store.contentPath(key), c.store.metadataPath(key))
		}
		return
	}

	entry := listElement.Value.(*lruEntry)
	meta := entry.meta

	c.lruList.Remove(listElement)
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
	listElement, exists := c.items[key]
	if !exists {
		c.mu.Unlock()
		return ErrNotFound
	}

	entry := listElement.Value.(*lruEntry)
	entry.meta.LastUsedAt = UnixTime(time.Now())
	c.lruList.MoveToFront(listElement)
	metaSnapshot := *entry.meta
	c.mu.Unlock()

	go func(mSnap ItemMeta) {
		if err := c.store.writeMetadata(mSnap.MetaPath, &mSnap); err != nil {
			c.log.Warn().Err(err).Str("key", mSnap.Key).Msg("Failed to update metadata file on MarkUsed (async)")
		}
	}(metaSnapshot)

	return nil
}

func (c *DiskLRU) UpdateAfterValidation(ctx context.Context, key string, validationTime time.Time, newHeaders http.Header, newStatusCode int, newSizeIfChanged int64) (*ItemMeta, error) {
	if !c.cfg.Enabled {
		return nil, errors.New("cache disabled")
	}
	if err := c.checkReady(); err != nil {
		return nil, err
	}

	c.mu.Lock()
	listElement, exists := c.items[key]
	if !exists {
		c.mu.Unlock()
		return nil, ErrNotFound
	}

	entry := listElement.Value.(*lruEntry)
	metaToUpdate := entry.meta

	if val := newHeaders.Get("Date"); val != "" {
		if _, err := http.ParseTime(val); err == nil {
			metaToUpdate.Headers.Set("Date", val)
		} else {
			c.log.Warn().Str("key", key).Str("date_header", val).Err(err).Msg("Invalid Date header in validation response, using current time.")
			metaToUpdate.Headers.Set("Date", validationTime.UTC().Format(http.TimeFormat))
		}
	} else {
		metaToUpdate.Headers.Set("Date", validationTime.UTC().Format(http.TimeFormat))
		c.log.Warn().Str("key", key).Msg("Date header missing in validation response, using current time.")
	}

	if val := newHeaders.Get("Cache-Control"); val != "" {
		metaToUpdate.Headers.Set("Cache-Control", val)
	}
	if val := newHeaders.Get("ETag"); val != "" {
		metaToUpdate.Headers.Set("ETag", val)
	}
	if val := newHeaders.Get("Expires"); val != "" {
		metaToUpdate.Headers.Set("Expires", val)
	}
	if val := newHeaders.Get("Last-Modified"); val != "" {
		metaToUpdate.Headers.Set("Last-Modified", val)
	}
	if val := newHeaders.Get("Age"); val != "" {
		metaToUpdate.Headers.Set("Age", val)
	}
	metaToUpdate.Headers.Set("X-Cache-Key-Debug", key)

	var baseTimeForExpiryCalc time.Time
	if dateStr := metaToUpdate.Headers.Get("Date"); dateStr != "" {
		if t, err := http.ParseTime(dateStr); err == nil {
			baseTimeForExpiryCalc = t
		}
	}
	if baseTimeForExpiryCalc.IsZero() {
		baseTimeForExpiryCalc = validationTime
	}
	metaToUpdate.ExpiresAt = UnixTime(c.calculateExpiresAt(metaToUpdate.Headers, baseTimeForExpiryCalc))

	metaToUpdate.ValidatedAt = UnixTime(validationTime)
	metaToUpdate.LastUsedAt = UnixTime(validationTime)

	c.lruList.MoveToFront(listElement)

	metaSnapshot := *metaToUpdate
	c.mu.Unlock()

	go func(mSnap ItemMeta) {
		if err := c.store.writeMetadata(mSnap.MetaPath, &mSnap); err != nil {
			c.log.Warn().Err(err).Str("key", mSnap.Key).Msg("Failed to update metadata file on UpdateAfterValidation (async)")
		} else {
			c.log.Debug().Str("key", mSnap.Key).Time("new_expires_at", mSnap.ExpiresAt.Time()).Msg("Cache metadata updated and written after validation.")
		}
	}(metaSnapshot)

	return &metaSnapshot, nil
}

func (c *DiskLRU) evictItemsLocked(requiredSpaceToFree int64, deleteFiles bool) {
	if requiredSpaceToFree <= 0 && c.currentBytes.Load() <= c.maxBytes {
		return
	}

	c.log.Info().
		Str("required_space_to_free", util.FormatSize(requiredSpaceToFree)).
		Str("current_size", util.FormatSize(c.currentBytes.Load())).
		Str("max_size", util.FormatSize(c.maxBytes)).
		Msg("Starting eviction process.")

	freedSpace := int64(0)
	itemsEvicted := 0

	for c.lruList.Len() > 0 && (freedSpace < requiredSpaceToFree || c.currentBytes.Load() > c.maxBytes) {
		tailElement := c.lruList.Back()
		if tailElement == nil {
			break
		}

		entryToEvict, ok := tailElement.Value.(*lruEntry)
		if !ok {
			c.log.Error().Interface("element_value", tailElement.Value).Msg("LRU list element has unexpected type, removing.")
			c.lruList.Remove(tailElement)
			continue
		}

		c.log.Debug().Str("key", entryToEvict.key).Str("size", util.FormatSize(entryToEvict.meta.Size)).Time("last_used", entryToEvict.meta.LastUsedAt.Time()).Msg("Evicting item.")
		c.removeItemLocked(entryToEvict.key, deleteFiles)

		if entryToEvict.meta.StatusCode != http.StatusNotFound {
			freedSpace += entryToEvict.meta.Size
		}
		itemsEvicted++
	}
	c.log.Info().
		Int("items_evicted", itemsEvicted).
		Str("space_freed", util.FormatSize(freedSpace)).
		Str("new_cache_size", util.FormatSize(c.currentBytes.Load())).
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

	c.items = make(map[string]*list.Element)
	c.lruList.Init()
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
