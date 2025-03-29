// internal/cache/cache.go
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
	ItemCount   int
	CurrentSize int64
	MaxSize     int64
}

type cacheEntry struct {
	key     string
	size    int64
	modTime time.Time
}

type validationEntry struct {
	key       string
	validated time.Time
}

type DiskLRUCache struct {
	baseDir       string
	maxSizeBytes  int64
	validationTTL time.Duration

	mu          sync.RWMutex
	currentSize int64
	items       map[string]*list.Element
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
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create cache directory %s: %w", baseDir, err)
	}

	cache := &DiskLRUCache{
		baseDir:       baseDir,
		maxSizeBytes:  maxSize,
		validationTTL: cfg.ValidationTTL.Duration(),
		items:         make(map[string]*list.Element),
		lruList:       list.New(),
		validations:   make(map[string]*list.Element),
		valLruList:    list.New(),
		closed:        make(chan struct{}),
	}

	cache.initWg.Add(1)
	go cache.initialize(cfg.CleanOnStart)

	return cache, nil
}

func (c *DiskLRUCache) getFilePath(key string) string {
	parts := strings.SplitN(key, "/", 2)
	repoName := parts[0]
	filePath := ""
	if len(parts) > 1 {
		filePath = parts[1]
	}

	safeRepoName := util.SanitizeFilename(repoName)
	safeFilePath := util.SanitizePath(filePath)

	fullPath := filepath.Join(c.baseDir, safeRepoName, safeFilePath)

	return fullPath
}

func (c *DiskLRUCache) initialize(cleanOnStart bool) {
	defer c.initWg.Done()

	if cleanOnStart {
		logging.Info("Cleaning cache directory %s on startup...", c.baseDir)
		err := filepath.Walk(c.baseDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if path == c.baseDir {
				return nil
			}
			if info.IsDir() {
				return nil
			}
			return os.Remove(path)
		})
		if err != nil {
			c.initErr = fmt.Errorf("failed during cache clean: %w", err)
			logging.Error("Cache clean failed: %v", c.initErr)
			return
		}
		logging.Info("Cache directory cleaned.")
	}

	logging.Debug("Scanning cache directory %s to rebuild state...", c.baseDir)
	startTime := time.Now()
	var scannedItems int
	var totalSize int64

	err := filepath.Walk(c.baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			logging.Warn("Error accessing path %s during cache scan: %v", path, err)
			return filepath.SkipDir
		}
		if info.IsDir() || strings.HasSuffix(path, ".tmp") {
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

		c.mu.Lock()
		if _, exists := c.items[key]; !exists {
			element := c.lruList.PushFront(entry)
			c.items[key] = element
			c.currentSize += entry.size
			scannedItems++
			totalSize += entry.size
		} else {
			logging.Warn("Duplicate key found during cache scan: %s", key)
		}
		c.mu.Unlock()

		return nil
	})

	if err != nil {
		c.initErr = fmt.Errorf("failed during cache directory scan: %w", err)
		logging.Error("Cache scan failed: %v", c.initErr)
		return
	}

	logging.Info("Cache scan complete in %s. Found %d items, total size %s.",
		time.Since(startTime), scannedItems, util.FormatSize(totalSize))

	c.evict(0)
}

func (c *DiskLRUCache) waitInit() error {
	c.initWg.Wait()
	return c.initErr
}

func (c *DiskLRUCache) Get(ctx context.Context, key string) (io.ReadCloser, int64, time.Time, error) {
	if err := c.waitInit(); err != nil {
		return nil, 0, time.Time{}, fmt.Errorf("cache initialization failed: %w", err)
	}

	c.mu.RLock()
	element, exists := c.items[key]
	c.mu.RUnlock()

	if !exists {
		return nil, 0, time.Time{}, os.ErrNotExist
	}

	entry := element.Value.(*cacheEntry)
	filePath := c.getFilePath(key)

	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			logging.Warn("Cache inconsistency: item %s in memory but not found on disk at %s. Removing from cache.", key, filePath)
			c.deleteInternal(key, element, entry.size)
			return nil, 0, time.Time{}, os.ErrNotExist
		}
		return nil, 0, time.Time{}, fmt.Errorf("failed to open cache file %s: %w", filePath, err)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		logging.Warn("Failed to stat cache file %s for item %s. Removing from cache.", filePath, key)
		c.deleteInternal(key, element, entry.size)
		return nil, 0, time.Time{}, fmt.Errorf("failed to stat cache file: %w", err)
	}
	if info.Size() != entry.size {
		file.Close()
		logging.Warn("Cache file size mismatch for %s (%d) vs expected (%d). Removing corrupted entry.", key, info.Size(), entry.size)
		c.deleteInternal(key, element, entry.size)
		if removeErr := os.Remove(filePath); removeErr != nil && !os.IsNotExist(removeErr) {
			logging.Error("Failed to remove corrupted cache file %s: %v", filePath, removeErr)
		}
		return nil, 0, time.Time{}, os.ErrNotExist
	}

	c.mu.Lock()
	if _, stillExists := c.items[key]; stillExists {
		c.lruList.MoveToFront(element)
	} else {
		c.mu.Unlock()
		file.Close()
		return nil, 0, time.Time{}, os.ErrNotExist
	}
	c.mu.Unlock()

	logging.Debug("Cache hit: %s", key)
	return file, entry.size, entry.modTime, nil
}

func (c *DiskLRUCache) Put(ctx context.Context, key string, reader io.Reader, expectedSize int64, modTime time.Time) error {
	if err := c.waitInit(); err != nil {
		return fmt.Errorf("cache initialization failed, cannot put item: %w", err)
	}

	filePath := c.getFilePath(key)
	dirPath := filepath.Dir(filePath)

	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s for cache item: %w", dirPath, err)
	}

	tempFile, err := os.CreateTemp(dirPath, filepath.Base(filePath)+".*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temporary cache file in %s: %w", dirPath, err)
	}
	tempFilePath := tempFile.Name()
	defer func() {
		if tempFile != nil {
			tempFile.Close()
			os.Remove(tempFilePath)
		}
	}()

	writtenSize, err := io.Copy(tempFile, reader)
	if err != nil {
		return fmt.Errorf("failed to write to temporary cache file %s: %w", tempFilePath, err)
	}

	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("failed to close temporary cache file %s: %w", tempFilePath, err)
	}
	tempFile = nil

	if expectedSize > 0 && writtenSize != expectedSize {
		os.Remove(tempFilePath)
		return fmt.Errorf("cache write size mismatch for %s: expected %d, wrote %d", key, expectedSize, writtenSize)
	}

	if !modTime.IsZero() {
		if err := os.Chtimes(tempFilePath, modTime, modTime); err != nil {
			logging.Warn("Failed to set modification time for temp cache file %s: %v", tempFilePath, err)
		}
	}

	c.mu.Lock()
	c.mu.Unlock()

	if err := os.Rename(tempFilePath, filePath); err != nil {
		os.Remove(tempFilePath)
		return fmt.Errorf("failed to rename temp cache file %s to %s: %w", tempFilePath, filePath, err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	finalExistingSize := int64(0)
	if element, exists := c.items[key]; exists {
		finalExistingSize = element.Value.(*cacheEntry).size
		c.lruList.Remove(element)
		c.currentSize -= finalExistingSize
	}

	entry := &cacheEntry{
		key:     key,
		size:    writtenSize,
		modTime: modTime,
	}
	element := c.lruList.PushFront(entry)
	c.items[key] = element
	c.currentSize += writtenSize

	logging.Debug("Cache put: %s (Size: %d)", key, writtenSize)

	c.evict(0)

	return nil
}

func (c *DiskLRUCache) deleteInternal(key string, element *list.Element, size int64) {
	if element != nil {
		c.lruList.Remove(element)
	}
	delete(c.items, key)
	c.currentSize -= size

	go func(filePath string) {
		if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
			logging.Error("Failed to delete cache file %s: %v", filePath, err)
		} else {
			logging.Debug("Cache deleted file: %s", filePath)
		}
	}(c.getFilePath(key))
}

func (c *DiskLRUCache) Delete(ctx context.Context, key string) error {
	if err := c.waitInit(); err != nil {
		return fmt.Errorf("cache initialization failed, cannot delete item: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	element, exists := c.items[key]
	if !exists {
		return nil
	}

	entry := element.Value.(*cacheEntry)
	c.deleteInternal(key, element, entry.size)

	logging.Debug("Cache delete: %s", key)
	return nil
}

func (c *DiskLRUCache) evict(requiredSpace int64) {
	neededSize := c.maxSizeBytes - requiredSpace
	if c.currentSize <= neededSize {
		return
	}

	amountToFree := c.currentSize - neededSize
	logging.Debug("Evicting cache items. Current: %s, Max: %s, Need to free: %s",
		util.FormatSize(c.currentSize), util.FormatSize(c.maxSizeBytes), util.FormatSize(amountToFree))

	freed := int64(0)
	for c.currentSize > neededSize {
		element := c.lruList.Back()
		if element == nil {
			logging.Warn("Eviction stopped: LRU list is empty but size still exceeds limit (%d > %d)", c.currentSize, neededSize)
			break
		}

		entry := element.Value.(*cacheEntry)
		logging.Debug("Evicting LRU item: %s (Size: %d)", entry.key, entry.size)

		c.deleteInternal(entry.key, element, entry.size)
		freed += entry.size
	}
	logging.Debug("Eviction complete. Freed: %s", util.FormatSize(freed))
}

func (c *DiskLRUCache) Stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return CacheStats{
		ItemCount:   c.lruList.Len(),
		CurrentSize: c.currentSize,
		MaxSize:     c.maxSizeBytes,
	}
}

func (c *DiskLRUCache) Close() error {
	close(c.closed)
	c.initWg.Wait()
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
		c.valLruList.MoveToFront(element)
	} else {
		exists = false
	}
	c.valMu.Unlock()

	if !exists {
		return time.Time{}, false
	}

	logging.Debug("Validation cache hit: %s (Validated: %s)", key, entry.validated)
	return entry.validated, true
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
		logging.Debug("Validation cache deleted expired/invalid entry: %s", key)
	}
}
