package cache

import (
	"container/heap"
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
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

type MetadataUpdate struct {
	Key  string
	Meta *ItemMeta
}

type MetadataBatcher struct {
	updates chan MetadataUpdate
	ticker  *time.Ticker
	store   *diskStore
	log     zerolog.Logger
	done    chan struct{}
}

func newMetadataBatcher(store *diskStore, logger zerolog.Logger, interval time.Duration) *MetadataBatcher {
	mb := &MetadataBatcher{
		updates: make(chan MetadataUpdate, 1000),
		ticker:  time.NewTicker(interval),
		store:   store,
		log:     logger,
		done:    make(chan struct{}),
	}
	go mb.batchUpdates()
	return mb
}

func (mb *MetadataBatcher) batchUpdates() {
	updates := make(map[string]*ItemMeta, 100)

	for {
		select {
		case update := <-mb.updates:
			updates[update.Key] = update.Meta

		case <-mb.ticker.C:
			if len(updates) > 0 {
				mb.flushUpdates(updates)
				updates = make(map[string]*ItemMeta, 100)
			}

		case <-mb.done:
			// Финальная очистка
			mb.flushUpdates(updates)
			return
		}
	}
}

func (mb *MetadataBatcher) flushUpdates(updates map[string]*ItemMeta) {
	for _, meta := range updates {
		if err := mb.store.writeMetadata(meta.MetaPath, meta); err != nil {
			mb.log.Warn().Err(err).Str("key", meta.Key).Msg("Failed to batch update metadata")
		}
	}
	mb.log.Debug().Int("count", len(updates)).Msg("Batch updated metadata files")
}

func (mb *MetadataBatcher) scheduleUpdate(key string, meta *ItemMeta) {
	select {
	case mb.updates <- MetadataUpdate{Key: key, Meta: meta}:
	default:
		mb.log.Warn().Str("key", key).Msg("Metadata update queue full, dropping update")
	}
}

func (mb *MetadataBatcher) close() {
	close(mb.done)
	mb.ticker.Stop()
}

type ItemHeap []*ItemMeta

func (h ItemHeap) Len() int { return len(h) }
func (h ItemHeap) Less(i, j int) bool {
	return h[i].LastUsedAt.Time().Before(h[j].LastUsedAt.Time())
}
func (h ItemHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *ItemHeap) Push(x interface{}) {
	*h = append(*h, x.(*ItemMeta))
}

func (h *ItemHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
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

	initOnce    sync.Once
	initErr     error
	ready       bool
	metaBatcher *MetadataBatcher
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

	batchInterval := cfg.MetadataBatchInterval.StdDuration()
	if batchInterval <= 0 {
		batchInterval = 30 * time.Second
	}

	cache := &DiskLRU{
		cfg:      effectiveCfg,
		log:      logger.With().Str("component", "DiskLRU").Logger(),
		store:    store,
		maxBytes: maxBytes,

		negTTL:      cfg.NegativeTTL.StdDuration(),
		items:       make(map[string]*list.Element),
		lruList:     list.New(),
		metaBatcher: newMetadataBatcher(store, logger, batchInterval),
	}

	return cache, nil
}

func (c *DiskLRU) calculateExpiresAt(cacheKey string, headers http.Header, dateValueTime time.Time) time.Time {
	ageSec := int64(0)
	if ageHeader := headers.Get("Age"); ageHeader != "" {
		if ageVal, err := strconv.ParseInt(ageHeader, 10, 64); err == nil && ageVal >= 0 {
			ageSec = ageVal
		} else {
			c.log.Warn().Str("key", cacheKey).Str("age_header", ageHeader).Err(err).Msg("Invalid Age header, ignoring.")
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
				c.log.Debug().Str("key", cacheKey).Dur("heuristic_ttl", time.Duration(heuristicLifetimeSec)*time.Second).Msg("Applying heuristic caching TTL")
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

		// Используем heap для эффективной сортировки больших объемов
		itemHeap := make(ItemHeap, 0, len(metaFiles))
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

			heap.Push(&itemHeap, meta)
			if meta.StatusCode != http.StatusNotFound {
				currentSizeOnDisk += meta.Size
			}
			loadedCount++
		}

		// Извлекаем элементы из heap в правильном порядке (от старых к новым)
		for itemHeap.Len() > 0 {
			meta := heap.Pop(&itemHeap).(*ItemMeta)
			entry := &lruEntry{key: meta.Key, meta: meta}
			listElement := c.lruList.PushFront(entry)
			c.items[meta.Key] = listElement
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
	return c.GetWithOptions(ctx, key, GetOptions{WithContent: false})
}

func (c *DiskLRU) GetWithOptions(ctx context.Context, key string, opts GetOptions) (*GetResult, error) {
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
		return &GetResult{Meta: meta, Hit: true, KeepOpen: false}, nil
	}

	var contentFile *os.File
	var err error

	if opts.WithContent {
		contentFile, err = c.store.openContentFile(meta.Path)
		if err != nil {
			c.log.Error().Err(err).Str("key", key).Str("path", meta.Path).Msg("Failed to open content file for cached item.")
			go c.Delete(context.Background(), key)
			return nil, ErrNotFound
		}
	}

	_ = c.MarkUsed(ctx, key)
	return &GetResult{Meta: meta, Content: contentFile, Hit: true, KeepOpen: opts.WithContent}, nil
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
	finalNewMeta.ExpiresAt = UnixTime(c.calculateExpiresAt(finalNewMeta.Key, finalNewMeta.Headers, dateValueTime))

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

	// Используем батчер вместо горутины
	c.metaBatcher.scheduleUpdate(key, &metaSnapshot)
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

	var baseTimeForExpiryCalc time.Time
	if dateStr := metaToUpdate.Headers.Get("Date"); dateStr != "" {
		if t, err := http.ParseTime(dateStr); err == nil {
			baseTimeForExpiryCalc = t
		}
	}
	if baseTimeForExpiryCalc.IsZero() {
		baseTimeForExpiryCalc = validationTime
	}
	metaToUpdate.ExpiresAt = UnixTime(c.calculateExpiresAt(key, metaToUpdate.Headers, baseTimeForExpiryCalc))

	metaToUpdate.ValidatedAt = UnixTime(validationTime)
	metaToUpdate.LastUsedAt = UnixTime(validationTime)

	c.lruList.MoveToFront(listElement)

	metaSnapshot := *metaToUpdate
	c.mu.Unlock()

	// Используем батчер для обновления метаданных
	c.metaBatcher.scheduleUpdate(key, &metaSnapshot)
	c.log.Debug().Str("key", key).Time("new_expires_at", metaSnapshot.ExpiresAt.Time()).Msg("Cache metadata updated after validation.")

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

	// Собираем список для удаления под блокировкой
	type evictionItem struct {
		key      string
		size     int64
		metaPath string
		dataPath string
	}

	var itemsToEvict []evictionItem
	freedSpace := int64(0)

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

		// Собираем информацию для удаления
		item := evictionItem{
			key:      entryToEvict.key,
			size:     entryToEvict.meta.Size,
			metaPath: entryToEvict.meta.MetaPath,
			dataPath: entryToEvict.meta.Path,
		}
		itemsToEvict = append(itemsToEvict, item)

		c.log.Debug().Str("key", entryToEvict.key).Str("size", util.FormatSize(entryToEvict.meta.Size)).Time("last_used", entryToEvict.meta.LastUsedAt.Time()).Msg("Evicting item.")

		// Удаляем из памяти
		c.lruList.Remove(tailElement)
		delete(c.items, entryToEvict.key)

		if entryToEvict.meta.StatusCode != http.StatusNotFound {
			c.currentBytes.Add(-entryToEvict.meta.Size)
			freedSpace += entryToEvict.meta.Size
		}
	}

	// Удаляем файлы без блокировки (если нужно)
	if deleteFiles && len(itemsToEvict) > 0 {
		go func(items []evictionItem) {
			for _, item := range items {
				if err := c.store.deleteFiles(item.dataPath, item.metaPath); err != nil {
					c.log.Error().Err(err).Str("key", item.key).Msg("Failed to delete cache files from disk during eviction.")
				}
			}
		}(itemsToEvict)
	}

	c.log.Info().
		Int("items_evicted", len(itemsToEvict)).
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
	if c.metaBatcher != nil {
		c.metaBatcher.close()
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
