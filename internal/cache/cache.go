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
	// Keep track of the list element for quick removal/promotion
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

	mu          sync.RWMutex
	currentSize int64
	// Map from key to cacheEntry pointer
	items map[string]*cacheEntry
	// Doubly linked list for LRU order. Stores *cacheEntry.
	lruList *list.List

	// Separate LRU for validation cache
	valMu       sync.RWMutex
	validations map[string]*list.Element // key -> *list.Element
	valLruList  *list.List               // Stores *validationEntry

	initWg  sync.WaitGroup
	initErr error
	closed  chan struct{}
}

func NewDiskLRUCache(cfg config.CacheConfig) (*DiskLRUCache, error) {
	if !cfg.Enabled {
		logging.Warn("Cache is disabled in configuration.")
		// Consider returning a no-op CacheManager implementation instead
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
		items:         make(map[string]*cacheEntry),
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
	// Extract repository name and the rest of the path
	parts := strings.SplitN(key, "/", 2)
	repoName := parts[0]
	filePathPart := ""
	if len(parts) > 1 {
		filePathPart = parts[1]
	}

	// Sanitize components to prevent path traversal and invalid characters
	safeRepoName := util.SanitizeFilename(repoName)
	safeFilePath := util.SanitizePath(filePathPart)

	// Construct the full path within the cache base directory
	fullPath := filepath.Join(c.baseDir, safeRepoName, safeFilePath)

	return fullPath
}

func (c *DiskLRUCache) initialize(cleanOnStart bool) {
	defer c.initWg.Done()

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
		// No need to rebuild state if cleaned
		return
	}

	logging.Debug("Scanning cache directory %s to rebuild state...", c.baseDir)
	startTime := time.Now()
	var scannedItems int
	var totalSize int64
	entries := []*cacheEntry{} // Collect entries first, then sort

	err := filepath.Walk(c.baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			logging.Warn("Error accessing path %s during cache scan: %v", path, err)
			// Decide whether to skip directory or just the file
			if info != nil && info.IsDir() {
				return filepath.SkipDir
			}
			return nil // Skip this file/path on error
		}
		// Skip directories and temporary files
		if info.IsDir() || strings.HasSuffix(path, ".tmp") {
			return nil
		}
		// Skip the base directory itself if Walk includes it
		if path == c.baseDir {
			return nil
		}

		// Derive cache key from relative path
		relPath, err := filepath.Rel(c.baseDir, path)
		if err != nil {
			logging.Warn("Failed to get relative path for %s: %v", path, err)
			return nil
		}
		// Convert to canonical key format (forward slashes)
		key := filepath.ToSlash(relPath)

		// Create entry (element will be set later)
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

	// --- Rebuild LRU list and items map ---
	// No reliable access time from Walk, using ModTime as proxy for initial order
	// Sort entries by ModTime (oldest first) to approximate LRU
	// sort.Slice(entries, func(i, j int) bool {
	// 	return entries[i].modTime.Before(entries[j].modTime)
	// })
	// Actually, adding in Walk order might be closer to access if files aren't modified
	// Let's add them in the order they were found by Walk.

	c.mu.Lock()
	for _, entry := range entries {
		// Add to front of list (assuming recent files are found later by Walk?)
		// Or add to back? Let's add to front for consistency with Get/Put logic.
		element := c.lruList.PushFront(entry)
		entry.element = element // Store pointer to list element
		c.items[entry.key] = entry
	}
	c.currentSize = totalSize
	c.mu.Unlock()

	logging.Info("Cache scan complete in %s. Rebuilt state for %d items, total size %s.",
		time.Since(startTime), scannedItems, util.FormatSize(totalSize))

	// Evict if oversized after rebuild
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

	c.mu.Lock() // Lock for read and potential list move
	entry, exists := c.items[key]
	if !exists {
		c.mu.Unlock()
		return nil, 0, time.Time{}, os.ErrNotExist
	}

	// Promote item to front of LRU list
	c.lruList.MoveToFront(entry.element)
	c.mu.Unlock() // Unlock after map access and list move

	// --- Open and verify file ---
	filePath := c.getFilePath(key)
	file, err := os.Open(filePath)
	if err != nil {
		// Handle case where file is missing despite being in the map (inconsistency)
		if os.IsNotExist(err) {
			logging.Warn("Cache inconsistency: item %s in memory but not found on disk at %s. Removing from cache.", key, filePath)
			c.deleteInternal(key) // Use internal delete which handles map/list removal
			return nil, 0, time.Time{}, os.ErrNotExist
		}
		// Other file opening errors
		return nil, 0, time.Time{}, fmt.Errorf("failed to open cache file %s: %w", filePath, err)
	}

	// Optional: Verify size against stored metadata
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
		return nil, 0, time.Time{}, os.ErrNotExist // Treat size mismatch as not found
	}

	logging.Debug("Cache hit: %s", key)
	// Return the file handle, size, and modTime from the entry
	return file, entry.size, entry.modTime, nil
}

func (c *DiskLRUCache) Put(ctx context.Context, key string, reader io.Reader, expectedSize int64, modTime time.Time) error {
	if err := c.waitInit(); err != nil {
		return fmt.Errorf("cache initialization failed, cannot put item: %w", err)
	}

	filePath := c.getFilePath(key)
	dirPath := filepath.Dir(filePath)

	// --- File Writing Logic (same as before) ---
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
	defer func() { // Ensure cleanup on error
		if _, err := os.Stat(tempFilePath); err == nil {
			tempFile.Close()
			os.Remove(tempFilePath)
		}
	}()

	writtenSize, err := io.Copy(tempFile, reader)
	if err != nil {
		tempFile.Close()
		os.Remove(tempFilePath)
		return fmt.Errorf("failed to write to temporary cache file %s: %w", tempFilePath, err)
	}

	if err := tempFile.Close(); err != nil {
		os.Remove(tempFilePath)
		return fmt.Errorf("failed to close temporary cache file %s: %w", tempFilePath, err)
	}

	if expectedSize > 0 && writtenSize != expectedSize {
		os.Remove(tempFilePath)
		return fmt.Errorf("cache write size mismatch for %s: expected %d, wrote %d", key, expectedSize, writtenSize)
	}
	finalSize := writtenSize // Use actual written size

	// Use provided modTime, or current time if zero
	if modTime.IsZero() {
		modTime = time.Now()
	}
	// Attempt to set file modification time
	if err := os.Chtimes(tempFilePath, modTime, modTime); err != nil {
		logging.Warn("Failed to set modification time for temp cache file %s: %v", tempFilePath, err)
	}

	// --- Update Cache Metadata (Map and LRU List) ---
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if item already exists
	if oldEntry, exists := c.items[key]; exists {
		// Remove old entry from list and update size
		c.currentSize -= oldEntry.size
		c.lruList.Remove(oldEntry.element)
		delete(c.items, key) // Delete old map entry
	}

	// Create new entry
	entry := &cacheEntry{
		key:     key,
		size:    finalSize,
		modTime: modTime,
	}
	// Add to front of LRU list and store element pointer
	element := c.lruList.PushFront(entry)
	entry.element = element
	c.items[key] = entry
	c.currentSize += finalSize

	// --- Finalize File Rename (after metadata update) ---
	if err := os.Rename(tempFilePath, filePath); err != nil {
		// Rename failed: Rollback metadata changes
		c.currentSize -= finalSize
		c.lruList.Remove(element)
		delete(c.items, key)
		// Don't remove tempFilePath here, defer will handle it
		return fmt.Errorf("failed to rename temp cache file %s to %s: %w", tempFilePath, filePath, err)
	}

	logging.Debug("Cache put: %s (Size: %d)", key, finalSize)

	// --- Trigger Eviction ---
	c.evict(finalSize)

	return nil
}

// deleteInternal removes item from map and LRU list. Caller handles file removal.
// Must be called with c.mu held or acquire lock itself. Let's have it acquire lock.
func (c *DiskLRUCache) deleteInternal(key string) (size int64, exists bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.items[key]
	if !exists {
		return 0, false
	}

	// Remove from list using stored element pointer
	c.lruList.Remove(entry.element)
	// Remove from map
	delete(c.items, key)
	// Update size
	c.currentSize -= entry.size
	if c.currentSize < 0 {
		c.currentSize = 0
	} // Prevent negative size

	return entry.size, true
}

func (c *DiskLRUCache) Delete(ctx context.Context, key string) error {
	if err := c.waitInit(); err != nil {
		return fmt.Errorf("cache initialization failed, cannot delete item: %w", err)
	}

	// Remove from metadata first
	size, exists := c.deleteInternal(key)
	if !exists {
		return nil // Already gone
	}

	// Remove the actual file (asynchronously)
	filePath := c.getFilePath(key)
	go func(path string, key string, size int64) {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			logging.Error("Failed to delete cache file %s for key %s: %v", path, key, err)
			// Potential inconsistency: file exists but metadata is gone.
			// Re-initializing might fix it, or manual cleanup needed.
		} else {
			logging.Debug("Cache deleted file: %s (Size: %d)", key, size)
		}
	}(filePath, key, size)

	logging.Debug("Cache delete initiated for: %s", key)
	return nil
}

// evict removes items based on LRU until size constraint is met.
// Must be called with c.mu held.
func (c *DiskLRUCache) evict(requiredSpace int64) {
	if c.maxSizeBytes <= 0 {
		return // Eviction disabled
	}

	targetSize := c.maxSizeBytes - requiredSpace
	if targetSize < 0 {
		targetSize = 0
	}

	for c.currentSize > targetSize {
		// Get least recently used item (tail of the list)
		element := c.lruList.Back()
		if element == nil {
			logging.Warn("Eviction stopped: LRU list is empty but size still exceeds limit (%d > %d)", c.currentSize, targetSize)
			break // Should not happen if size calculation is correct
		}

		entry := element.Value.(*cacheEntry)

		// Remove from list and map (within the lock)
		c.lruList.Remove(element)
		delete(c.items, entry.key)
		c.currentSize -= entry.size

		logging.Debug("Evicting LRU item: %s (Size: %d)", entry.key, entry.size)

		// Remove file asynchronously
		filePath := c.getFilePath(entry.key)
		go func(path string, key string) {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				logging.Error("Eviction: Failed to delete cache file %s for key %s: %v", path, key, err)
			}
		}(filePath, entry.key)
	}
	if c.currentSize < 0 {
		c.currentSize = 0
	} // Sanity check
}

func (c *DiskLRUCache) Stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	// Ensure itemCount matches map length for consistency
	itemCount := len(c.items)
	if itemCount != c.lruList.Len() {
		logging.Warn("Cache inconsistency: item count mismatch (map: %d, list: %d)", itemCount, c.lruList.Len())
		// Use map length as potentially more accurate?
	}
	return CacheStats{
		ItemCount:   itemCount,
		CurrentSize: c.currentSize,
		MaxSize:     c.maxSizeBytes,
	}
}

func (c *DiskLRUCache) Close() error {
	close(c.closed)
	// Wait for initialization to complete if it hasn't
	c.initWg.Wait()
	logging.Info("DiskLRUCache closed.")
	// No DB to close here
	return nil
}

// --- Validation Cache (Memory LRU) ---

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
		go c.deleteValidation(key) // Delete expired async
		return time.Time{}, false
	}
	c.valMu.RUnlock() // Release read lock before taking write lock

	// Promote in LRU list
	c.valMu.Lock()
	// Re-check existence after acquiring write lock
	if element, stillExists := c.validations[key]; stillExists {
		c.valLruList.MoveToFront(element)
		// Re-read entry value in case it changed? Unlikely here.
		validationTime = element.Value.(*validationEntry).validated
		ok = true
	} else {
		ok = false // Removed between RUnlock and Lock
	}
	c.valMu.Unlock()

	if ok {
		logging.Debug("Validation cache hit: %s (Validated: %s)", key, validationTime)
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
		logging.Debug("Validation cache deleted expired/invalid entry: %s", key)
	}
}
