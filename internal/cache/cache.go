package cache

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

const (
	DirectoryIndexFilename = ".dirindex"
	MetadataSuffix         = ".meta"
	MetadataVersion        = 1
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
}

type CacheStats struct {
	ItemCount            int
	CurrentSize          int64
	MaxSize              int64
	ValidationItemCount  int
	CacheDirectory       string
	CacheEnabled         bool
	ValidationTTLEnabled bool
	ValidationTTL        time.Duration
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
	baseDir       string
	maxSizeBytes  int64
	validationTTL time.Duration
	enabled       bool

	mu          sync.RWMutex
	currentSize int64
	items       map[string]*cacheEntry
	lruList     *list.List

	valMu       sync.RWMutex
	validations map[string]*list.Element
	valLruList  *list.List

	initWg  sync.WaitGroup
	initErr error
	closed  chan struct{}
}

func NewDiskLRUCache(cfg config.CacheConfig) (*DiskLRUCache, error) {
	if !cfg.Enabled {
		logging.Warn("Cache is disabled in configuration.")
	}

	maxSize, err := util.ParseSize(cfg.MaxSize)
	if err != nil {
		return nil, fmt.Errorf("invalid cache max size %q: %w", cfg.MaxSize, err)
	}
	if maxSize <= 0 && cfg.Enabled {
		return nil, errors.New("cache max size must be positive when enabled")
	}

	baseDir := util.CleanPath(cfg.Directory)
	if cfg.Enabled {
		if err := os.MkdirAll(baseDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create cache directory %s: %w", baseDir, err)
		}
	}

	cache := &DiskLRUCache{
		baseDir:       baseDir,
		maxSizeBytes:  maxSize,
		validationTTL: cfg.ValidationTTL.Duration(),
		enabled:       cfg.Enabled,
		items:         make(map[string]*cacheEntry),
		lruList:       list.New(),
		validations:   make(map[string]*list.Element),
		valLruList:    list.New(),
		closed:        make(chan struct{}),
	}

	if cfg.Enabled {
		cache.initWg.Add(1)
		go cache.initialize(cfg.CleanOnStart)
	} else {
		cache.initErr = errors.New("cache is disabled")
	}

	return cache, nil
}

func (c *DiskLRUCache) getContentFilePath(key string) string {
	parts := strings.SplitN(key, "/", 2)
	if len(parts) == 0 {
		return filepath.Join(c.baseDir, util.SanitizeFilename(key))
	}
	repoName := parts[0]
	filePathPart := ""
	if len(parts) > 1 {
		filePathPart = parts[1]
	}
	safeRepoName := util.SanitizeFilename(repoName)
	safeFilePath := util.SanitizePath(filePathPart)
	if safeFilePath == "" || safeFilePath == "." {
		safeFilePath = DirectoryIndexFilename
	}
	return filepath.Join(c.baseDir, safeRepoName, safeFilePath)
}

func (c *DiskLRUCache) getMetaFilePath(key string) string {
	return c.getContentFilePath(key) + MetadataSuffix
}

func (c *DiskLRUCache) initialize(cleanOnStart bool) {
	defer c.initWg.Done()

	if !c.enabled {
		c.initErr = errors.New("cache is disabled, skipping initialization")
		return
	}

	if cleanOnStart {
		logging.Info("Cleaning cache directory %s on startup...", c.baseDir)

		dirEntries, err := os.ReadDir(c.baseDir)
		if err != nil {
			c.initErr = fmt.Errorf("failed to read cache directory for cleaning %s: %w", c.baseDir, err)
			logging.Error("Cache clean failed: %v", c.initErr)
			return
		}
		for _, entry := range dirEntries {
			path := filepath.Join(c.baseDir, entry.Name())
			if err := os.RemoveAll(path); err != nil {
				logging.Warn("Failed to remove item during clean %s: %v", path, err)
			}
		}
		logging.Info("Cache directory cleaned.")
		return
	}

	logging.Debug("Scanning cache directory %s to rebuild state...", c.baseDir)
	startTime := time.Now()
	var scannedItems int
	var totalSize int64
	entries := []*cacheEntry{}

	err := filepath.Walk(c.baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			logging.Warn("Error accessing path %s during cache scan: %v", path, err)
			if info != nil && info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if info.IsDir() || strings.HasSuffix(path, ".tmp") || strings.HasSuffix(path, MetadataSuffix) {

			return nil
		}
		if path == c.baseDir {
			return nil
		}

		relPath, err := filepath.Rel(c.baseDir, path)
		if err != nil {
			logging.Warn("Failed to get relative path for %s: %v", path, err)
			return nil
		}

		key := filepath.ToSlash(relPath)
		keyParts := strings.SplitN(key, string(filepath.Separator), 2)
		if len(keyParts) == 2 && keyParts[1] == DirectoryIndexFilename {
			key = keyParts[0] + "/."
		} else if len(keyParts) == 1 && keyParts[0] == DirectoryIndexFilename {
			key = "."
		}

		metaPath := path + MetadataSuffix
		var entrySize int64 = info.Size()

		metaFile, metaErr := os.Open(metaPath)
		if metaErr == nil {
			defer metaFile.Close()
			decoder := json.NewDecoder(metaFile)
			var meta CacheMetadata
			if decodeErr := decoder.Decode(&meta); decodeErr == nil && meta.Size >= 0 {
				entrySize = meta.Size
			} else {
				logging.Warn("Failed to read/decode metadata %s or size invalid, using file size for item %s", metaPath, key)
			}
		} else if !os.IsNotExist(metaErr) {
			logging.Warn("Error opening metadata file %s: %v", metaPath, metaErr)
		}

		entry := &cacheEntry{
			key:  key,
			size: entrySize,
		}
		entries = append(entries, entry)
		scannedItems++
		totalSize += entry.size

		return nil
	})

	if err != nil {
		c.initErr = fmt.Errorf("failed during cache directory scan: %w", err)
		logging.Error("Cache scan failed: %v", c.initErr)
		return
	}

	c.mu.Lock()
	for _, entry := range entries {

		if _, exists := c.items[entry.key]; !exists {
			element := c.lruList.PushFront(entry)
			entry.element = element
			c.items[entry.key] = entry
		} else {
			logging.Warn("Duplicate key detected during scan, skipping: %s", entry.key)
			totalSize -= entry.size
			scannedItems--
		}
	}
	c.currentSize = totalSize
	c.mu.Unlock()

	logging.Info("Cache scan complete in %s. Rebuilt state for %d items, total size %s.",
		time.Since(startTime), scannedItems, util.FormatSize(totalSize))

	c.evict(0)
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

	filePath := c.getContentFilePath(key)
	metaPath := c.getMetaFilePath(key)

	metaFile, err := os.Open(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			logging.Warn("Cache inconsistency: item %s in memory but metadata %s not found. Removing.", key, metaPath)
			c.deleteInternal(key)
			go c.deleteFilesAsync(key)
			return nil, nil, os.ErrNotExist
		}
		return nil, nil, fmt.Errorf("failed to open cache metadata file %s: %w", metaPath, err)
	}
	defer metaFile.Close()

	var metadata CacheMetadata
	decoder := json.NewDecoder(metaFile)
	if err := decoder.Decode(&metadata); err != nil {
		logging.Error("Failed to decode metadata file %s for key %s: %v. Removing corrupted entry.", metaPath, key, err)
		c.deleteInternal(key)
		go c.deleteFilesAsync(key)
		return nil, nil, os.ErrNotExist
	}

	contentFile, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			logging.Warn("Cache inconsistency: item %s metadata %s exists but content file %s not found. Removing.", key, metaPath, filePath)
			c.deleteInternal(key)
			go c.deleteFilesAsync(key)
			return nil, nil, os.ErrNotExist
		}
		return nil, nil, fmt.Errorf("failed to open cache content file %s: %w", filePath, err)
	}

	contentInfo, err := contentFile.Stat()
	if err != nil {
		contentFile.Close()
		logging.Warn("Failed to stat content file %s for item %s. Removing entry.", filePath, key)
		c.deleteInternal(key)
		go c.deleteFilesAsync(key)
		return nil, nil, fmt.Errorf("failed to stat content file: %w", err)
	}

	if metadata.Size >= 0 && contentInfo.Size() != metadata.Size {
		contentFile.Close()
		logging.Warn("Cache file size mismatch for %s: meta(%d) vs file(%d). Removing corrupted entry.", key, metadata.Size, contentInfo.Size())
		c.deleteInternal(key)
		go c.deleteFilesAsync(key)
		return nil, nil, os.ErrNotExist
	}

	logging.Debug("Cache hit: %s (Size: %d)", key, metadata.Size)
	metadata.Key = key
	metadata.FilePath = filePath
	metadata.MetaPath = metaPath
	return contentFile, &metadata, nil
}

func (c *DiskLRUCache) Put(ctx context.Context, key string, reader io.Reader, metadata CacheMetadata) error {
	if !c.enabled {
		_, _ = io.Copy(io.Discard, reader)
		return errors.New("cache is disabled, item not stored")
	}
	if err := c.waitInit(); err != nil {
		return fmt.Errorf("cache initialization failed, cannot put item: %w", err)
	}

	filePath := c.getContentFilePath(key)
	metaPath := c.getMetaFilePath(key)
	dirPath := filepath.Dir(filePath)

	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s for cache item: %w", dirPath, err)
	}

	tempContentFile, err := os.CreateTemp(dirPath, filepath.Base(filePath)+".*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temporary content file in %s: %w", dirPath, err)
	}
	tempContentPath := tempContentFile.Name()
	defer func() {
		tempContentFile.Close()
		if _, statErr := os.Stat(tempContentPath); statErr == nil {
			logging.Debug("Cleaning up temporary content file: %s", tempContentPath)
			os.Remove(tempContentPath)
		}
	}()

	tempMetaFile, err := os.CreateTemp(dirPath, filepath.Base(metaPath)+".*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temporary metadata file in %s: %w", dirPath, err)
	}
	tempMetaPath := tempMetaFile.Name()
	defer func() {
		tempMetaFile.Close()
		if _, statErr := os.Stat(tempMetaPath); statErr == nil {
			logging.Debug("Cleaning up temporary metadata file: %s", tempMetaPath)
			os.Remove(tempMetaPath)
		}
	}()

	writtenSize, err := io.Copy(tempContentFile, reader)
	if err != nil {
		return fmt.Errorf("failed to write to temporary content file %s: %w", tempContentPath, err)
	}
	if err := tempContentFile.Close(); err != nil {
		return fmt.Errorf("failed to close temporary content file %s: %w", tempContentPath, err)
	}

	metadata.Version = MetadataVersion
	if metadata.FetchTime.IsZero() {
		metadata.FetchTime = time.Now().UTC()
	}
	if metadata.Size < 0 {
		metadata.Size = writtenSize
	} else if writtenSize != metadata.Size {
		return fmt.Errorf("cache write size mismatch for %s: expected %d, wrote %d", key, metadata.Size, writtenSize)
	}

	encoder := json.NewEncoder(tempMetaFile)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(&metadata); err != nil {
		return fmt.Errorf("failed to encode metadata to %s: %w", tempMetaPath, err)
	}
	if err := tempMetaFile.Close(); err != nil {
		return fmt.Errorf("failed to close temporary metadata file %s: %w", tempMetaPath, err)
	}

	finalSize := metadata.Size
	c.mu.Lock()
	if oldEntry, exists := c.items[key]; exists {
		sizeDiff := finalSize - oldEntry.size
		c.currentSize -= oldEntry.size
		c.lruList.Remove(oldEntry.element)
		delete(c.items, key)
		logging.Debug("Updating existing cache entry %s, size diff %d", key, sizeDiff)
		c.evict(sizeDiff)
	} else {
		logging.Debug("Adding new cache entry %s, size %d", key, finalSize)
		c.evict(finalSize)
	}

	newEntry := &cacheEntry{
		key:  key,
		size: finalSize,
	}
	element := c.lruList.PushFront(newEntry)
	newEntry.element = element
	c.items[key] = newEntry
	c.currentSize += finalSize
	c.mu.Unlock()

	if err := os.Rename(tempMetaPath, metaPath); err != nil {
		logging.Error("Failed to rename temp metadata file %s to %s: %v. Reverting LRU changes.", tempMetaPath, metaPath, err)

		c.mu.Lock()
		c.currentSize -= finalSize
		c.lruList.Remove(element)
		delete(c.items, key)
		c.mu.Unlock()

		return fmt.Errorf("failed to rename temp metadata file: %w", err)
	}

	if err := os.Rename(tempContentPath, filePath); err != nil {
		logging.Error("Failed to rename temp content file %s to %s: %v. Reverting LRU changes and removing meta file.", tempContentPath, filePath, err)

		c.mu.Lock()
		c.currentSize -= finalSize
		c.lruList.Remove(element)
		delete(c.items, key)
		c.mu.Unlock()

		if removeMetaErr := os.Remove(metaPath); removeMetaErr != nil && !os.IsNotExist(removeMetaErr) {
			logging.Error("Failed to remove committed meta file %s during content rename failure: %v", metaPath, removeMetaErr)
		}
		return fmt.Errorf("failed to rename temp content file: %w", err)
	}

	tempContentPath = ""
	tempMetaPath = ""

	logging.Debug("Cache put successful: %s (Size: %d)", key, finalSize)
	return nil
}

func (c *DiskLRUCache) deleteInternal(key string) (size int64, exists bool) {
	if !c.enabled {
		return 0, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.items[key]
	if !exists {
		return 0, false
	}

	c.lruList.Remove(entry.element)
	delete(c.items, key)
	c.currentSize -= entry.size
	if c.currentSize < 0 {
		c.currentSize = 0
	}

	return entry.size, true
}

func (c *DiskLRUCache) deleteFilesAsync(key string) {
	contentPath := c.getContentFilePath(key)
	metaPath := c.getMetaFilePath(key)

	go func(cPath, mPath, k string) {
		contentErr := os.Remove(cPath)
		metaErr := os.Remove(mPath)

		if contentErr != nil && !errors.Is(contentErr, os.ErrNotExist) {
			logging.Error("Failed to delete cache content file %s for key %s: %v", cPath, k, contentErr)
		}
		if metaErr != nil && !errors.Is(metaErr, os.ErrNotExist) {
			logging.Error("Failed to delete cache metadata file %s for key %s: %v", mPath, k, metaErr)
		}
		if contentErr == nil && metaErr == nil {
			logging.Debug("Cache deleted files (content+meta) for key: %s", k)
		} else if errors.Is(contentErr, os.ErrNotExist) && errors.Is(metaErr, os.ErrNotExist) {

			logging.Debug("Attempted to delete already non-existent cache files for key: %s", k)
		}
	}(contentPath, metaPath, key)
}

func (c *DiskLRUCache) Delete(ctx context.Context, key string) error {
	if !c.enabled {
		return errors.New("cache is disabled, cannot delete")
	}
	if err := c.waitInit(); err != nil {
		return fmt.Errorf("cache initialization failed, cannot delete item: %w", err)
	}

	size, exists := c.deleteInternal(key)
	if !exists {
		return nil
	}

	c.deleteFilesAsync(key)

	logging.Debug("Cache delete initiated for: %s (Size: %d)", key, size)
	return nil
}

func (c *DiskLRUCache) evict(requiredSpace int64) {

	if !c.enabled || c.maxSizeBytes <= 0 {
		return
	}

	spaceToFree := (c.currentSize + requiredSpace) - c.maxSizeBytes
	if spaceToFree <= 0 {
		return
	}

	logging.Debug("Eviction triggered: currentSize=%d, requiredSpace=%d, maxSizeBytes=%d, spaceToFree=%d",
		c.currentSize, requiredSpace, c.maxSizeBytes, spaceToFree)

	var freedSpace int64
	itemsEvicted := 0
	for freedSpace < spaceToFree {
		element := c.lruList.Back()
		if element == nil {
			logging.Warn("Eviction stopped: LRU list empty but need to free %d more bytes (currentSize=%d)",
				spaceToFree-freedSpace, c.currentSize)
			break
		}

		entry := element.Value.(*cacheEntry)

		c.lruList.Remove(element)
		delete(c.items, entry.key)
		c.currentSize -= entry.size
		freedSpace += entry.size
		itemsEvicted++

		logging.Debug("Evicting LRU item: %s (Size: %d, Freed: %d / %d)", entry.key, entry.size, freedSpace, spaceToFree)

		c.deleteFilesAsync(entry.key)
	}
	if c.currentSize < 0 {
		c.currentSize = 0
	}
	logging.Debug("Eviction finished: %d items evicted, freedSpace=%d, new currentSize=%d", itemsEvicted, freedSpace, c.currentSize)
}

func (c *DiskLRUCache) Stats() CacheStats {
	c.mu.RLock()
	itemCount := len(c.items)
	currentSize := c.currentSize
	maxSize := c.maxSizeBytes
	baseDir := c.baseDir
	enabled := c.enabled
	c.mu.RUnlock()

	c.valMu.RLock()
	validationCount := len(c.validations)
	c.valMu.RUnlock()

	return CacheStats{
		ItemCount:            itemCount,
		CurrentSize:          currentSize,
		MaxSize:              maxSize,
		ValidationItemCount:  validationCount,
		CacheDirectory:       baseDir,
		CacheEnabled:         enabled,
		ValidationTTLEnabled: c.validationTTL > 0,
		ValidationTTL:        c.validationTTL,
	}
}

func (c *DiskLRUCache) Close() error {
	close(c.closed)
	if c.enabled {
		c.initWg.Wait()
	}
	logging.Info("DiskLRUCache closed.")
	return nil
}

func (c *DiskLRUCache) GetValidation(key string) (validationTime time.Time, ok bool) {

	if c.validationTTL <= 0 {
		return time.Time{}, false
	}
	c.valMu.RLock()
	element, exists := c.validations[key]
	if !exists {
		c.valMu.RUnlock()
		return time.Time{}, false
	}
	entry := element.Value.(*validationEntry)
	if time.Since(entry.validated) > c.validationTTL {
		c.valMu.RUnlock()
		go c.deleteValidation(key)
		return time.Time{}, false
	}
	c.valMu.RUnlock()

	c.valMu.Lock()

	if element, stillExists := c.validations[key]; stillExists {
		currentEntry := element.Value.(*validationEntry)
		if time.Since(currentEntry.validated) <= c.validationTTL {
			c.valLruList.MoveToFront(element)
			validationTime = currentEntry.validated
			ok = true
		} else {
			ok = false
		}
	} else {
		ok = false
	}
	c.valMu.Unlock()

	if ok {
		logging.Debug("Validation cache hit: %s (Validated: %s)", key, validationTime)
	} else {
		logging.Debug("Validation cache miss or expired: %s", key)
	}
	return validationTime, ok
}

func (c *DiskLRUCache) PutValidation(key string, validationTime time.Time) {

	if c.validationTTL <= 0 {
		return
	}
	c.valMu.Lock()
	defer c.valMu.Unlock()
	if element, exists := c.validations[key]; exists {
		entry := element.Value.(*validationEntry)
		entry.validated = validationTime
		c.valLruList.MoveToFront(element)
		logging.Debug("Validation cache updated: %s", key)
	} else {
		entry := &validationEntry{key: key, validated: validationTime}
		element := c.valLruList.PushFront(entry)
		c.validations[key] = element
		logging.Debug("Validation cache put: %s", key)
	}
}

func (c *DiskLRUCache) deleteValidation(key string) {

	c.valMu.Lock()
	defer c.valMu.Unlock()
	if element, exists := c.validations[key]; exists {
		c.valLruList.Remove(element)
		delete(c.validations, key)
		logging.Debug("Validation cache deleted entry: %s", key)
	}
}
