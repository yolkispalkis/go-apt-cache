package cache

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

const (
	MetadataSuffix  = ".meta"
	ContentSuffix   = ".cache"
	MetadataVersion = 1
)

type CacheMetadata struct {
	Version   int         `json:"version"`
	FetchTime time.Time   `json:"fetchTime"`
	ModTime   time.Time   `json:"modTime"`
	Size      int64       `json:"size"`
	Headers   http.Header `json:"headers"`
	Key       string      `json:"-"`
	FilePath  string      `json:"-"`
	MetaPath  string      `json:"-"`
}

type CacheManager interface {
	Get(ctx context.Context, key string) (content io.ReadCloser, metadata *CacheMetadata, err error)
	Put(ctx context.Context, key string, reader io.Reader, metadata CacheMetadata) error
	Delete(ctx context.Context, key string) error
	Stats() CacheStats
	Close() error
	GetValidation(key string) (validationTime time.Time, ok bool)
	PutValidation(key string, validationTime time.Time)
	RecordHit()
	RecordMiss()
	RecordValidationError()
}

type CacheStats struct {
	ItemCount                       int    `json:"item_count"`
	CurrentSize                     int64  `json:"current_size_bytes"`
	MaxSize                         int64  `json:"max_size_bytes"`
	Hits                            uint64 `json:"hits"`
	Misses                          uint64 `json:"misses"`
	ValidationHits                  uint64 `json:"validation_hits"`
	ValidationErrors                uint64 `json:"validation_errors"`
	ValidationItemCount             int    `json:"validation_item_count"`
	CacheDirectory                  string `json:"cache_directory"`
	CacheEnabled                    bool   `json:"cache_enabled"`
	ValidationTTLEnabled            bool   `json:"validation_ttl_enabled"`
	ValidationTTL                   string `json:"validation_ttl"`
	InconsistencyMetaWithoutContent uint64 `json:"inconsistency_meta_without_content"`
	InconsistencyDuringScan         uint64 `json:"inconsistency_during_scan"`
	InitTimeMs                      int64  `json:"init_time_ms"`
	InitError                       string `json:"init_error,omitempty"`
	CleanOnStart                    bool   `json:"clean_on_start"`
}

type cacheEntry struct {
	key     string
	size    int64
	element *list.Element
}

type validationEntry struct {
	key       string
	validated time.Time
}

type DiskLRUCache struct {
	maxSizeBytes  int64
	validationTTL time.Duration
	enabled       bool
	cleanOnStart  bool

	mu          sync.RWMutex
	currentSize int64
	items       map[string]*cacheEntry
	lruList     *list.List

	valMu       sync.RWMutex
	validations map[string]*list.Element
	valLruList  *list.List

	initWg       sync.WaitGroup
	initErr      error
	initDuration time.Duration
	closed       chan struct{}
	store        *diskStore

	statsHits                            atomic.Uint64
	statsMisses                          atomic.Uint64
	statsValidationHits                  atomic.Uint64
	statsValidationErrors                atomic.Uint64
	statsInconsistencyMetaWithoutContent atomic.Uint64
	statsInconsistencyDuringScan         atomic.Uint64
}

func NewDiskLRUCache(cfg config.CacheConfig) (*DiskLRUCache, error) {
	if !cfg.Enabled {
		logging.Warn("Cache is disabled in configuration.")
		return &DiskLRUCache{enabled: false, closed: make(chan struct{})}, nil
	}

	maxSize, err := util.ParseSize(cfg.MaxSize)
	if err != nil {
		return nil, fmt.Errorf("invalid cache max size %q: %w", cfg.MaxSize, err)
	}
	if maxSize <= 0 {
		return nil, fmt.Errorf("cache max size must be positive (%s resulted in %d bytes)", cfg.MaxSize, maxSize)
	}

	store, err := newDiskStore(cfg.Directory)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize disk store: %w", err)
	}

	cache := &DiskLRUCache{
		maxSizeBytes:  maxSize,
		validationTTL: cfg.ValidationTTL.Duration(),
		enabled:       true,
		cleanOnStart:  cfg.CleanOnStart,
		items:         make(map[string]*cacheEntry),
		lruList:       list.New(),
		validations:   make(map[string]*list.Element),
		valLruList:    list.New(),
		closed:        make(chan struct{}),
		store:         store,
	}

	cache.initWg.Add(1)
	go cache.initialize()

	return cache, nil
}

func (c *DiskLRUCache) initialize() {
	startTime := time.Now()
	defer func() {
		c.initDuration = time.Since(startTime)
		c.initWg.Done()
	}()

	if !c.enabled {
		c.initErr = errors.New("cache is disabled, skipping initialization")
		return
	}

	if c.cleanOnStart {
		logging.Info("Cleaning cache directory on startup...", "directory", c.store.baseDir)
		if err := c.store.cleanDirectory(); err != nil {
			c.initErr = fmt.Errorf("cache clean failed: %w", err)
			logging.ErrorE("Cache clean failed", c.initErr)
		}
		c.mu.Lock()
		c.currentSize = 0
		c.items = make(map[string]*cacheEntry)
		c.lruList = list.New()
		c.mu.Unlock()
		logging.Info("Cache directory cleaned.")
		return
	}

	logging.Info("Scanning cache directory to rebuild state...", "directory", c.store.baseDir)
	discoveredEntries, scannedFiles, scanErr := c.store.scanDirectory()
	if scanErr != nil {
		c.initErr = fmt.Errorf("failed during cache directory scan: %w", scanErr)
		logging.ErrorE("Cache scan failed", c.initErr)

		c.mu.Lock()
		c.currentSize = 0
		c.items = make(map[string]*cacheEntry)
		c.lruList = list.New()
		c.mu.Unlock()
		return
	}

	c.rebuildLRUState(discoveredEntries)
	c.statsInconsistencyDuringScan.Store(uint64(len(discoveredEntries)))

	logging.Info("Cache scan reconstruction complete",
		"scanned_files", scannedFiles,
		"loaded_items", len(discoveredEntries),
		"current_cache_size", util.FormatSize(c.currentSize),
		"max_cache_size_bytes", c.maxSizeBytes)

	c.mu.Lock()
	c.evictLocked(0)
	c.mu.Unlock()
}

func (c *DiskLRUCache) rebuildLRUState(discoveredEntries []*cacheEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]*cacheEntry, len(discoveredEntries))
	c.lruList = list.New()
	c.currentSize = 0

	for _, entry := range discoveredEntries {
		if _, exists := c.items[entry.key]; !exists {
			element := c.lruList.PushFront(entry)
			entry.element = element
			c.items[entry.key] = entry
			c.currentSize += entry.size
		} else {
			logging.Warn("Duplicate cache key detected during LRU reconstruction, skipping.", "key", entry.key)
			c.statsInconsistencyDuringScan.Add(1)
		}
	}

	if c.currentSize < 0 {
		logging.Error("Internal error: current cache size negative after LRU rebuild", "size", c.currentSize)
		c.currentSize = 0
	}
}

func (c *DiskLRUCache) waitInit() error {
	c.initWg.Wait()
	return c.initErr
}

func (c *DiskLRUCache) Get(ctx context.Context, key string) (io.ReadCloser, *CacheMetadata, error) {
	if !c.enabled {
		return nil, nil, os.ErrNotExist
	}

	if err := c.waitInit(); err != nil {
		return nil, nil, fmt.Errorf("cache initialization failed: %w", err)
	}

	c.mu.Lock()
	entry, exists := c.items[key]
	if !exists {
		c.mu.Unlock()
		return nil, nil, os.ErrNotExist
	}
	c.lruList.MoveToFront(entry.element)
	c.mu.Unlock()

	metadata, err := c.store.readMetadata(key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			c.handleInconsistency(key, "meta_missing_on_get", fmt.Errorf("metadata file missing for key in memory: %w", err))
			return nil, nil, os.ErrNotExist
		}
		logging.ErrorE("Failed to read metadata for cache hit", err, "key", key)
		c.handleInconsistency(key, "meta_read_error", err)
		return nil, nil, os.ErrNotExist
	}

	contentFile, err := c.store.openContentFile(key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			c.handleInconsistency(key, "content_missing_on_get", fmt.Errorf("content file missing for key in memory: %w", err))
			return nil, nil, os.ErrNotExist
		}
		logging.ErrorE("Failed to open content file for cache hit", err, "key", key)
		c.handleInconsistency(key, "content_open_error", err)
		return nil, nil, os.ErrNotExist
	}

	contentInfo, statErr := contentFile.Stat()
	if statErr != nil {
		_ = contentFile.Close()
		c.handleInconsistency(key, "content_stat_error", fmt.Errorf("failed to stat content file: %w", statErr))
		return nil, nil, os.ErrNotExist
	}

	if metadata.Size >= 0 && contentInfo.Size() != metadata.Size {
		_ = contentFile.Close()
		c.handleInconsistency(key, "size_mismatch_on_get", fmt.Errorf("metadata size %d != file size %d", metadata.Size, contentInfo.Size()))
		return nil, nil, os.ErrNotExist
	} else if metadata.Size < 0 {
		metadata.Size = contentInfo.Size()
	}

	return contentFile, metadata, nil
}

func (c *DiskLRUCache) handleInconsistency(key, reason string, err error) {
	logReason := reason
	if err != nil {
		logReason = fmt.Sprintf("%s: %v", reason, err)
	}
	logging.Warn("Cache inconsistency detected. Removing entry.", "key", key, "reason", logReason)

	if strings.Contains(reason, "meta") {
		c.statsInconsistencyMetaWithoutContent.Add(1)
	}

	c.mu.Lock()
	c.deleteInternalLocked(key)
	c.mu.Unlock()

	c.deleteFilesAsync(key)
}

func (c *DiskLRUCache) Put(ctx context.Context, key string, reader io.Reader, metadata CacheMetadata) (err error) {
	if !c.enabled {
		_, _ = io.Copy(io.Discard, reader)
		return errors.New("cache is disabled, item not stored")
	}
	if initErr := c.waitInit(); initErr != nil {
		_, _ = io.Copy(io.Discard, reader)
		return fmt.Errorf("cache initialization failed, cannot put item %s: %w", key, initErr)
	}

	dirPath, err := c.store.preparePutDirectories(key)
	if err != nil {
		_, _ = io.Copy(io.Discard, reader)
		return fmt.Errorf("failed preparing cache directory for %s: %w", key, err)
	}

	tempContentPath, tempMetaPath, cleanupTempFiles, err := c.store.createTemporaryFiles(key, dirPath)
	if err != nil {
		_, _ = io.Copy(io.Discard, reader)
		return fmt.Errorf("failed creating temporary files for %s: %w", key, err)
	}
	defer cleanupTempFiles()

	finalSize, err := c.store.writeToTemporaryFiles(reader, metadata, tempContentPath, tempMetaPath)
	if err != nil {
		return fmt.Errorf("failed writing temporary files for %s: %w", key, err)
	}

	oldElement, oldSize, err := c.updateLRUForPut(key, finalSize)
	if err != nil {
		return fmt.Errorf("failed updating LRU state for %s: %w", key, err)
	}

	err = c.store.commitTemporaryFiles(key, tempContentPath, tempMetaPath)
	if err != nil {
		c.rollbackLRUUpdate(key, oldElement, oldSize, finalSize)
		return fmt.Errorf("failed committing cache files for %s: %w", key, err)
	}

	tempContentPath = ""
	tempMetaPath = ""

	c.PutValidation(key, time.Now())
	return nil
}

func (c *DiskLRUCache) updateLRUForPut(key string, finalSize int64) (oldElement *list.Element, oldSize int64, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	oldSize = -1

	if oldEntry, exists := c.items[key]; exists {
		oldSize = oldEntry.size
		oldElement = oldEntry.element

		c.currentSize -= oldSize
		c.lruList.Remove(oldElement)
		delete(c.items, key)
	}

	spaceNeeded := finalSize
	spaceToFree := (c.currentSize + spaceNeeded) - c.maxSizeBytes
	if spaceToFree > 0 {
		c.evictLocked(spaceToFree)
	}

	newEntry := &cacheEntry{key: key, size: finalSize}
	element := c.lruList.PushFront(newEntry)
	newEntry.element = element
	c.items[key] = newEntry
	c.currentSize += finalSize

	if c.currentSize < 0 {
		logging.Error("Internal error: current cache size negative after Put update", "size", c.currentSize, "key", key)
		c.currentSize = 0
		return oldElement, oldSize, fmt.Errorf("internal cache state error: negative size for key %s", key)
	}

	return oldElement, oldSize, nil
}

func (c *DiskLRUCache) rollbackLRUUpdate(key string, oldElement *list.Element, oldSize int64, finalSize int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	logging.Warn("Rolling back LRU update due to commit failure", "key", key)

	if entry, exists := c.items[key]; exists && entry.size == finalSize {
		c.lruList.Remove(entry.element)
		delete(c.items, key)
		c.currentSize -= finalSize
	}

	if oldElement != nil && oldSize != -1 {
		oldEntry := oldElement.Value.(*cacheEntry)
		newOldElement := c.lruList.PushFront(oldEntry)
		oldEntry.element = newOldElement
		c.items[key] = oldEntry
		c.currentSize += oldSize
	}

	if c.currentSize < 0 {
		logging.Error("Internal error: current cache size negative after Put rollback", "size", c.currentSize, "key", key)
		c.currentSize = 0
	}
}

func (c *DiskLRUCache) deleteInternalLocked(key string) (size int64, exists bool) {
	entry, exists := c.items[key]
	if !exists {
		return 0, false
	}

	c.lruList.Remove(entry.element)
	delete(c.items, key)
	c.currentSize -= entry.size

	if c.currentSize < 0 {
		logging.Error("Internal error: current cache size negative after deleteInternalLocked", "size", c.currentSize, "key", key, "deleted_size", entry.size)
		c.currentSize = 0
	}

	return entry.size, true
}

func (c *DiskLRUCache) deleteFilesAsync(key string) {
	go func(k string) {
		err := c.store.deleteFiles(k)
		if err != nil {
			logging.ErrorE("Cache delete files async failed", err, "key", k)
		} else {
			logging.Debug("Cache delete files async completed", "key", k)
		}
	}(key)
}

func (c *DiskLRUCache) Delete(ctx context.Context, key string) error {
	if !c.enabled {
		return errors.New("cache is disabled, cannot delete")
	}
	if initErr := c.waitInit(); initErr != nil {
		logging.Warn("Cannot delete, cache init failed", "error", initErr, "key", key)
	}

	c.mu.Lock()
	deletedSize, exists := c.deleteInternalLocked(key)
	c.mu.Unlock()

	if !exists {
		return nil
	}

	c.deleteFilesAsync(key)
	logging.Debug("Cache delete initiated", "key", key, "freed_memory_bytes", deletedSize)
	return nil
}

func (c *DiskLRUCache) evictLocked(spaceToFree int64) {
	if !c.enabled || c.maxSizeBytes <= 0 {
		return
	}

	targetSize := c.maxSizeBytes
	if spaceToFree > 0 {
		targetSize = c.currentSize - spaceToFree
		if targetSize < 0 {
			targetSize = 0
		}
		if targetSize > c.maxSizeBytes {
			targetSize = c.maxSizeBytes
		}
	}

	if c.currentSize <= targetSize {
		return
	}

	freedSpace := int64(0)
	itemsEvicted := 0

	for c.currentSize > targetSize {
		element := c.lruList.Back()
		if element == nil {
			break
		}

		entry := element.Value.(*cacheEntry)
		size, existed := c.deleteInternalLocked(entry.key)

		if existed {
			freedSpace += size
			itemsEvicted++
			c.deleteFilesAsync(entry.key)
		} else {
			c.lruList.Remove(element)
		}
	}

	if itemsEvicted > 0 {
		logging.Debug("Eviction finished", "items_evicted", itemsEvicted, "total_freed_space", util.FormatSize(freedSpace), "new_current_size", util.FormatSize(c.currentSize))
	}

	if c.currentSize < 0 {
		logging.Error("Internal error: current cache size negative after eviction", "size", c.currentSize)
		c.currentSize = 0
	}
}

func (c *DiskLRUCache) RecordHit() {
	if c.enabled {
		c.statsHits.Add(1)
	}
}
func (c *DiskLRUCache) RecordMiss() {
	if c.enabled {
		c.statsMisses.Add(1)
	}
}
func (c *DiskLRUCache) RecordValidationError() {
	if c.enabled {
		c.statsValidationErrors.Add(1)
	}
}

func (c *DiskLRUCache) Stats() CacheStats {
	_ = c.waitInit()

	c.mu.RLock()
	itemCount := len(c.items)
	currentSize := c.currentSize
	maxSize := c.maxSizeBytes
	baseDir := ""
	if c.store != nil {
		baseDir = c.store.baseDir
	}
	enabled := c.enabled
	c.mu.RUnlock()

	c.valMu.RLock()
	validationCount := len(c.validations)
	c.valMu.RUnlock()

	initErrStr := ""
	if c.initErr != nil {
		initErrStr = c.initErr.Error()
	}

	return CacheStats{
		ItemCount:                       itemCount,
		CurrentSize:                     currentSize,
		MaxSize:                         maxSize,
		Hits:                            c.statsHits.Load(),
		Misses:                          c.statsMisses.Load(),
		ValidationHits:                  c.statsValidationHits.Load(),
		ValidationErrors:                c.statsValidationErrors.Load(),
		ValidationItemCount:             validationCount,
		CacheDirectory:                  baseDir,
		CacheEnabled:                    enabled,
		ValidationTTLEnabled:            c.validationTTL > 0,
		ValidationTTL:                   c.validationTTL.String(),
		InconsistencyMetaWithoutContent: c.statsInconsistencyMetaWithoutContent.Load(),
		InconsistencyDuringScan:         c.statsInconsistencyDuringScan.Load(),
		InitTimeMs:                      c.initDuration.Milliseconds(),
		InitError:                       initErrStr,
		CleanOnStart:                    c.cleanOnStart,
	}
}

func (c *DiskLRUCache) Close() error {
	select {
	case <-c.closed:
		return errors.New("cache already closed")
	default:
		close(c.closed)
	}

	if c.enabled {
		c.initWg.Wait()
	}
	logging.Info("DiskLRUCache closed.")
	return nil
}

func (c *DiskLRUCache) GetValidation(key string) (validationTime time.Time, ok bool) {
	ttl := c.validationTTL
	if ttl <= 0 || !c.enabled {
		return time.Time{}, false
	}

	if initErr := c.waitInit(); initErr != nil {
		return time.Time{}, false
	}

	c.valMu.RLock()
	element, exists := c.validations[key]
	if !exists {
		c.valMu.RUnlock()
		return time.Time{}, false
	}
	entry := element.Value.(*validationEntry)
	validatedAt := entry.validated
	isExpired := time.Since(validatedAt) > ttl
	c.valMu.RUnlock()

	if isExpired {
		c.deleteValidationAsync(key)
		return time.Time{}, false
	}

	c.valMu.Lock()
	defer c.valMu.Unlock()

	element, stillExists := c.validations[key]
	if !stillExists {
		return time.Time{}, false
	}

	currentEntry := element.Value.(*validationEntry)
	if time.Since(currentEntry.validated) > ttl {
		c.valLruList.Remove(element)
		delete(c.validations, key)
		return time.Time{}, false
	}

	c.valLruList.MoveToFront(element)
	c.statsValidationHits.Add(1)
	validationTime = currentEntry.validated
	ok = true

	return validationTime, ok
}

func (c *DiskLRUCache) PutValidation(key string, validationTime time.Time) {
	ttl := c.validationTTL
	if ttl <= 0 || !c.enabled {
		return
	}
	if initErr := c.waitInit(); initErr != nil {
		return
	}

	c.valMu.Lock()
	defer c.valMu.Unlock()

	if element, exists := c.validations[key]; exists {
		entry := element.Value.(*validationEntry)
		entry.validated = validationTime
		c.valLruList.MoveToFront(element)
	} else {
		entry := &validationEntry{key: key, validated: validationTime}
		element := c.valLruList.PushFront(entry)
		c.validations[key] = element
	}
}

func (c *DiskLRUCache) deleteValidationAsync(key string) {
	go func(k string) {
		c.valMu.Lock()
		if element, exists := c.validations[k]; exists {
			c.valLruList.Remove(element)
			delete(c.validations, k)
		}
		c.valMu.Unlock()
	}(key)
}
