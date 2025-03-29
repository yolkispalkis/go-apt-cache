package cache

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

type CacheManager interface {
	Get(ctx context.Context, key string) (io.ReadCloser, int64, time.Time, error)
	Put(ctx context.Context, key string, reader io.Reader, expectedSize int64, modTime time.Time) error
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
	modTime time.Time

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

	items map[string]*cacheEntry

	lruList *list.List

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

func (c *DiskLRUCache) getFilePath(key string) string {

	parts := strings.SplitN(key, "/", 2)
	repoName := parts[0]
	filePathPart := ""
	if len(parts) > 1 {
		filePathPart = parts[1]
	}

	safeRepoName := util.SanitizeFilename(repoName)
	safeFilePath := util.SanitizePath(filePathPart)

	fullPath := filepath.Join(c.baseDir, safeRepoName, safeFilePath)

	return fullPath
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
			if entry.IsDir() {
				if err := os.RemoveAll(path); err != nil {
					logging.Warn("Failed to remove directory during clean %s: %v", path, err)
				}
			} else {
				if err := os.Remove(path); err != nil {
					logging.Warn("Failed to remove file during clean %s: %v", path, err)
				}
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

		if info.IsDir() || strings.HasSuffix(path, ".tmp") {
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

		entry := &cacheEntry{
			key:     key,
			size:    info.Size(),
			modTime: info.ModTime(),
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
		element := c.lruList.PushFront(entry)
		entry.element = element
		c.items[entry.key] = entry
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

func (c *DiskLRUCache) Get(ctx context.Context, key string) (io.ReadCloser, int64, time.Time, error) {
	if !c.enabled {
		return nil, 0, time.Time{}, os.ErrNotExist
	}
	if err := c.waitInit(); err != nil {
		return nil, 0, time.Time{}, fmt.Errorf("cache initialization failed: %w", err)
	}

	c.mu.Lock()
	entry, exists := c.items[key]
	if !exists {
		c.mu.Unlock()
		return nil, 0, time.Time{}, os.ErrNotExist
	}

	c.lruList.MoveToFront(entry.element)
	c.mu.Unlock()

	filePath := c.getFilePath(key)
	file, err := os.Open(filePath)
	if err != nil {

		if os.IsNotExist(err) {
			logging.Warn("Cache inconsistency: item %s in memory but not found on disk at %s. Removing from cache.", key, filePath)
			c.deleteInternal(key)
			return nil, 0, time.Time{}, os.ErrNotExist
		}

		return nil, 0, time.Time{}, fmt.Errorf("failed to open cache file %s: %w", filePath, err)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		logging.Warn("Failed to stat cache file %s for item %s. Removing from cache.", filePath, key)
		c.deleteInternal(key)
		return nil, 0, time.Time{}, fmt.Errorf("failed to stat cache file: %w", err)
	}
	if info.Size() != entry.size {
		file.Close()
		logging.Warn("Cache file size mismatch for %s (%d) vs expected (%d). Removing corrupted entry.", key, info.Size(), entry.size)
		c.deleteInternal(key)
		if removeErr := os.Remove(filePath); removeErr != nil && !os.IsNotExist(removeErr) {
			logging.Error("Failed to remove corrupted cache file %s: %v", filePath, removeErr)
		}
		return nil, 0, time.Time{}, os.ErrNotExist
	}

	logging.Debug("Cache hit: %s", key)

	return file, entry.size, entry.modTime, nil
}

func (c *DiskLRUCache) Put(ctx context.Context, key string, reader io.Reader, expectedSize int64, modTime time.Time) error {
	if !c.enabled {

		_, _ = io.Copy(io.Discard, reader)
		return errors.New("cache is disabled, item not stored")
	}
	if err := c.waitInit(); err != nil {
		return fmt.Errorf("cache initialization failed, cannot put item: %w", err)
	}

	filePath := c.getFilePath(key)
	dirPath := filepath.Dir(filePath)

	if fileInfo, err := os.Stat(dirPath); err == nil {
		if !fileInfo.IsDir() {
			logging.Warn("Path %s exists but is not a directory, removing it", dirPath)
			if err := os.Remove(dirPath); err != nil {
				return fmt.Errorf("failed to remove file at directory path %s: %w", dirPath, err)
			}
		}
	}
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s for cache item: %w", dirPath, err)
	}

	tempFile, err := os.CreateTemp(dirPath, filepath.Base(filePath)+".*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temporary cache file in %s: %w", dirPath, err)
	}
	tempFilePath := tempFile.Name()

	cleanupTemp := true
	defer func() {
		tempFile.Close()
		if cleanupTemp {
			if _, statErr := os.Stat(tempFilePath); statErr == nil {
				logging.Debug("Cleaning up temporary file: %s", tempFilePath)
				os.Remove(tempFilePath)
			}
		}
	}()

	writtenSize, err := io.Copy(tempFile, reader)
	if err != nil {

		return fmt.Errorf("failed to write to temporary cache file %s: %w", tempFilePath, err)
	}

	if err := tempFile.Close(); err != nil {

		return fmt.Errorf("failed to close temporary cache file %s: %w", tempFilePath, err)
	}

	if expectedSize > 0 && writtenSize != expectedSize {

		return fmt.Errorf("cache write size mismatch for %s: expected %d, wrote %d", key, expectedSize, writtenSize)
	}
	finalSize := writtenSize

	if modTime.IsZero() {
		modTime = time.Now()
	}

	if err := os.Chtimes(tempFilePath, modTime, modTime); err != nil {
		logging.Warn("Failed to set modification time for temp cache file %s: %v", tempFilePath, err)

	}

	c.mu.Lock()

	if oldEntry, exists := c.items[key]; exists {

		sizeDiff := finalSize - oldEntry.size

		c.currentSize -= oldEntry.size
		c.lruList.Remove(oldEntry.element)
		delete(c.items, key)

		c.evict(sizeDiff)
	} else {

		c.evict(finalSize)
	}

	entry := &cacheEntry{
		key:     key,
		size:    finalSize,
		modTime: modTime,
	}

	element := c.lruList.PushFront(entry)
	entry.element = element
	c.items[key] = entry
	c.currentSize += finalSize
	c.mu.Unlock()

	if err := os.Rename(tempFilePath, filePath); err != nil {

		c.mu.Lock()
		c.currentSize -= finalSize
		c.lruList.Remove(element)
		delete(c.items, key)
		c.mu.Unlock()

		return fmt.Errorf("failed to rename temp cache file %s to %s: %w", tempFilePath, filePath, err)
	}

	cleanupTemp = false
	logging.Debug("Cache put: %s (Size: %d)", key, finalSize)

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

	filePath := c.getFilePath(key)
	go func(path string, key string, size int64) {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			logging.Error("Failed to delete cache file %s for key %s: %v", path, key, err)

		} else {
			logging.Debug("Cache deleted file: %s (Size: %d)", key, size)
		}
	}(filePath, key, size)

	logging.Debug("Cache delete initiated for: %s", key)
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
	for freedSpace < spaceToFree {

		element := c.lruList.Back()
		if element == nil {
			logging.Warn("Eviction stopped: LRU list is empty but need to free %d more bytes (currentSize=%d)",
				spaceToFree-freedSpace, c.currentSize)
			break
		}

		entry := element.Value.(*cacheEntry)

		c.lruList.Remove(element)
		delete(c.items, entry.key)
		c.currentSize -= entry.size
		freedSpace += entry.size

		logging.Debug("Evicting LRU item: %s (Size: %d, Freed: %d / %d)", entry.key, entry.size, freedSpace, spaceToFree)

		filePath := c.getFilePath(entry.key)
		go func(path string, key string) {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				logging.Error("Eviction: Failed to delete cache file %s for key %s: %v", path, key, err)
			}
		}(filePath, entry.key)
	}
	if c.currentSize < 0 {
		c.currentSize = 0
	}
	logging.Debug("Eviction finished: freedSpace=%d, new currentSize=%d", freedSpace, c.currentSize)
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
