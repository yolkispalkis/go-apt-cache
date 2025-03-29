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

// CacheManager defines the interface for cache operations.
type CacheManager interface {
	// Get retrieves an item from the cache. Returns a reader, size, mod time, and error.
	// The caller is responsible for closing the reader.
	// Returns os.ErrNotExist if the item is not found.
	Get(ctx context.Context, key string) (io.ReadCloser, int64, time.Time, error)

	// Put stores an item in the cache, streaming from the provided reader.
	// The expectedSize is used for pre-allocation checks if > 0.
	// The modTime is stored as the file's modification time.
	Put(ctx context.Context, key string, reader io.Reader, expectedSize int64, modTime time.Time) error

	// Delete removes an item from the cache.
	Delete(ctx context.Context, key string) error

	// Stats returns current cache statistics.
	Stats() CacheStats

	// Close performs any necessary cleanup.
	Close() error

	// Validation Cache operations
	GetValidation(key string) (validationTime time.Time, ok bool)
	PutValidation(key string, validationTime time.Time)
}

// CacheStats provides information about the cache state.
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

// DiskLRUCache implements CacheManager using disk storage and an LRU policy.
type DiskLRUCache struct {
	baseDir       string
	maxSizeBytes  int64
	validationTTL time.Duration

	mu          sync.RWMutex
	currentSize int64
	items       map[string]*list.Element // map[key]*list.Element{&cacheEntry{}}
	lruList     *list.List               // list.List{&cacheEntry{}}

	valMu       sync.RWMutex
	validations map[string]*list.Element // map[key]*list.Element{&validationEntry{}}
	valLruList  *list.List               // list.List{&validationEntry{}}

	initWg  sync.WaitGroup
	initErr error
	closed  chan struct{}
}

// NewDiskLRUCache creates a new disk-based LRU cache.
func NewDiskLRUCache(cfg config.CacheConfig) (*DiskLRUCache, error) {
	if !cfg.Enabled {
		// Return a disabled cache implementation if needed, or handle upstream
		logging.Warn("Cache is disabled in configuration.")
		// For simplicity, continue initialization but note it's disabled logic (won't store/retrieve)
		// A better approach might be a NullCache implementation.
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
	go cache.initialize(cfg.CleanOnStart) // Initialize asynchronously

	return cache, nil
}

func (c *DiskLRUCache) getFilePath(key string) string {
	// Use repository name as the first part of the path
	parts := strings.SplitN(key, "/", 2)
	repoName := parts[0]
	filePath := ""
	if len(parts) > 1 {
		filePath = parts[1]
	}

	// Ensure the key components are safe for filesystem paths
	safeRepoName := util.SanitizeFilename(repoName)
	safeFilePath := util.SanitizePath(filePath)

	// Combine ensuring no leading/trailing slashes are misinterpreted by Join
	fullPath := filepath.Join(c.baseDir, safeRepoName, safeFilePath)

	// Add a suffix to distinguish cache files easily if desired, but simple path is fine
	// fullPath += ".cache"
	return fullPath
}

func (c *DiskLRUCache) initialize(cleanOnStart bool) {
	defer c.initWg.Done()

	if cleanOnStart {
		logging.Info("Cleaning cache directory %s on startup...", c.baseDir)
		// Be cautious with RemoveAll
		// A safer approach might be to iterate and remove contents
		err := filepath.Walk(c.baseDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if path == c.baseDir {
				return nil // Skip root directory itself
			}
			if info.IsDir() {
				// Remove directory later after its contents are gone
				return nil
			}
			return os.Remove(path) // Remove file
		})
		if err != nil {
			c.initErr = fmt.Errorf("failed during cache clean: %w", err)
			logging.Error("Cache clean failed: %v", c.initErr)
			return
		}
		// Now remove empty subdirectories if any (optional)
		logging.Info("Cache directory cleaned.")
	}

	// Walk the directory to rebuild the LRU state
	logging.Debug("Scanning cache directory %s to rebuild state...", c.baseDir)
	startTime := time.Now()
	var scannedItems int
	var totalSize int64

	err := filepath.Walk(c.baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// Log error but try to continue scanning other parts
			logging.Warn("Error accessing path %s during cache scan: %v", path, err)
			return filepath.SkipDir // Skip problematic directory/file
		}
		if info.IsDir() || strings.HasSuffix(path, ".tmp") {
			return nil // Skip directories and temp files
		}

		relPath, err := filepath.Rel(c.baseDir, path)
		if err != nil {
			logging.Warn("Failed to get relative path for %s: %v", path, err)
			return nil
		}

		key := filepath.ToSlash(relPath) // Use forward slashes for keys internally

		entry := &cacheEntry{
			key:     key,
			size:    info.Size(),
			modTime: info.ModTime(),
		}

		c.mu.Lock()
		if _, exists := c.items[key]; !exists {
			element := c.lruList.PushFront(entry) // Add to front (most recent) initially
			c.items[key] = element
			c.currentSize += entry.size
			scannedItems++
			totalSize += entry.size
		} else {
			// This shouldn't happen if keys are derived correctly from paths
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

	// Optionally, enforce max size limit immediately after scan
	c.evict(0) // Evict if currentSize exceeds maxSize
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
		return nil, 0, time.Time{}, os.ErrNotExist // Use standard error for not found
	}

	entry := element.Value.(*cacheEntry)
	filePath := c.getFilePath(key)

	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			// File missing on disk but present in memory state - inconsistent
			logging.Warn("Cache inconsistency: item %s in memory but not found on disk at %s. Removing from cache.", key, filePath)
			c.deleteInternal(key, element, entry.size) // Use internal delete
			return nil, 0, time.Time{}, os.ErrNotExist
		}
		return nil, 0, time.Time{}, fmt.Errorf("failed to open cache file %s: %w", filePath, err)
	}

	// Check if file size matches expected size (optional, for integrity)
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
		c.deleteInternal(key, element, entry.size) // Delete corrupted entry
		// Also delete the physical file
		if removeErr := os.Remove(filePath); removeErr != nil && !os.IsNotExist(removeErr) {
			logging.Error("Failed to remove corrupted cache file %s: %v", filePath, removeErr)
		}
		return nil, 0, time.Time{}, os.ErrNotExist // Treat as not found now
	}

	// Promote the item in the LRU list
	c.mu.Lock()
	// Check again if exists, might have been deleted between RUnlock and Lock
	if _, stillExists := c.items[key]; stillExists {
		c.lruList.MoveToFront(element)
	} else {
		// Item was deleted concurrently
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

	// Ensure target directory exists
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s for cache item: %w", dirPath, err)
	}

	// Create a temporary file in the same directory
	tempFile, err := os.CreateTemp(dirPath, filepath.Base(filePath)+".*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temporary cache file in %s: %w", dirPath, err)
	}
	tempFilePath := tempFile.Name()
	// Ensure temp file is cleaned up on error
	defer func() {
		if tempFile != nil {
			tempFile.Close()        // Close if still open
			os.Remove(tempFilePath) // Remove on any error path
		}
	}()

	// Stream content to the temporary file
	writtenSize, err := io.Copy(tempFile, reader)
	if err != nil {
		return fmt.Errorf("failed to write to temporary cache file %s: %w", tempFilePath, err)
	}

	// Close the file handle before renaming
	if err := tempFile.Close(); err != nil {
		// Keep defer cleanup active
		return fmt.Errorf("failed to close temporary cache file %s: %w", tempFilePath, err)
	}
	tempFile = nil // Prevent deferred cleanup from removing the successfully written file

	// Verify size if expectedSize was provided
	if expectedSize > 0 && writtenSize != expectedSize {
		os.Remove(tempFilePath) // Clean up incomplete file
		return fmt.Errorf("cache write size mismatch for %s: expected %d, wrote %d", key, expectedSize, writtenSize)
	}

	// Set modification time
	if !modTime.IsZero() {
		if err := os.Chtimes(tempFilePath, modTime, modTime); err != nil {
			// Log as warning, proceed with rename
			logging.Warn("Failed to set modification time for temp cache file %s: %v", tempFilePath, err)
		}
	}

	// Make room before adding the item to the cache state
	// We add writtenSize and potentially remove size of existing item with same key
	c.mu.Lock()
	existingSize := int64(0)
	if element, exists := c.items[key]; exists {
		existingSize = element.Value.(*cacheEntry).size
	}
	// Calculate required size *after* removing the potential old item
	c.mu.Unlock() // Evict can take time, unlock first

	// Evict might need to happen *before* rename to avoid exceeding limit briefly
	// It's complex. Let's evict after knowing the final size, accepting brief over-limit state.
	// Alternative: evict(writtenSize - existingSize) before adding state.

	// Rename temporary file to the final path, overwriting if exists
	if err := os.Rename(tempFilePath, filePath); err != nil {
		os.Remove(tempFilePath) // Clean up temp file if rename failed
		return fmt.Errorf("failed to rename temp cache file %s to %s: %w", tempFilePath, filePath, err)
	}

	// Update cache state (LRU list and size)
	c.mu.Lock()
	defer c.mu.Unlock()

	// Recalculate size difference *after* successful rename
	finalExistingSize := int64(0)
	if element, exists := c.items[key]; exists {
		finalExistingSize = element.Value.(*cacheEntry).size
		// Remove old entry from list *before* adding new/updated one
		c.lruList.Remove(element)
		// Adjust size downward first
		c.currentSize -= finalExistingSize
	}

	entry := &cacheEntry{
		key:     key,
		size:    writtenSize,
		modTime: modTime, // Use modTime passed in, or file's modTime if zero?
	}
	element := c.lruList.PushFront(entry)
	c.items[key] = element
	c.currentSize += writtenSize // Add new size

	logging.Debug("Cache put: %s (Size: %d)", key, writtenSize)

	// Evict items if cache size exceeds the limit *after* adding the new item
	c.evict(0) // Call evict with 0 ensures it only removes if over limit

	return nil
}

// deleteInternal removes item state and file without acquiring the main lock itself.
// Assumes caller holds the lock or handles concurrency.
func (c *DiskLRUCache) deleteInternal(key string, element *list.Element, size int64) {
	// Remove from LRU list and map
	if element != nil {
		c.lruList.Remove(element)
	}
	delete(c.items, key)
	c.currentSize -= size // Adjust size

	// Asynchronously remove the file from disk to avoid blocking lock
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
		return nil // Deleting a non-existent item is not an error
	}

	entry := element.Value.(*cacheEntry)
	c.deleteInternal(key, element, entry.size) // Use internal delete

	logging.Debug("Cache delete: %s", key)
	return nil
}

// evict removes least recently used items until the cache size is below the limit.
// `requiredSpace` indicates extra space needed beyond just fitting under max size.
// Caller must hold the mutex.
func (c *DiskLRUCache) evict(requiredSpace int64) {
	neededSize := c.maxSizeBytes - requiredSpace
	if c.currentSize <= neededSize {
		return // Already within limits
	}

	amountToFree := c.currentSize - neededSize
	logging.Debug("Evicting cache items. Current: %s, Max: %s, Need to free: %s",
		util.FormatSize(c.currentSize), util.FormatSize(c.maxSizeBytes), util.FormatSize(amountToFree))

	freed := int64(0)
	for c.currentSize > neededSize {
		element := c.lruList.Back()
		if element == nil {
			logging.Warn("Eviction stopped: LRU list is empty but size still exceeds limit (%d > %d)", c.currentSize, neededSize)
			break // Should not happen if sizes are tracked correctly
		}

		entry := element.Value.(*cacheEntry)
		logging.Debug("Evicting LRU item: %s (Size: %d)", entry.key, entry.size)

		// Use internal delete, passing known info
		c.deleteInternal(entry.key, element, entry.size) // Deletes state & schedules file removal
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
	close(c.closed) // Signal background tasks to stop if any were added
	// Wait for initialization if it hasn't finished
	c.initWg.Wait()
	// No other specific close actions needed for this simple implementation
	logging.Info("DiskLRUCache closed.")
	return nil
}

// --- Validation Cache ---

func (c *DiskLRUCache) GetValidation(key string) (validationTime time.Time, ok bool) {
	if c.validationTTL <= 0 {
		return time.Time{}, false // Validation caching disabled
	}

	c.valMu.RLock()
	element, exists := c.validations[key]
	if !exists {
		c.valMu.RUnlock()
		return time.Time{}, false
	}

	entry := element.Value.(*validationEntry)
	// Check TTL
	if time.Since(entry.validated) > c.validationTTL {
		c.valMu.RUnlock()
		// Expired, delete asynchronously
		go c.deleteValidation(key)
		return time.Time{}, false
	}
	c.valMu.RUnlock()

	// Promote in LRU list (acquire write lock briefly)
	c.valMu.Lock()
	// Check existence again after acquiring write lock
	if element, stillExists := c.validations[key]; stillExists {
		c.valLruList.MoveToFront(element)
	} else {
		exists = false // It was deleted concurrently
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
		return // Validation caching disabled
	}

	c.valMu.Lock()
	defer c.valMu.Unlock()

	if element, exists := c.validations[key]; exists {
		// Update existing entry
		entry := element.Value.(*validationEntry)
		entry.validated = validationTime
		c.valLruList.MoveToFront(element)
		logging.Debug("Validation cache updated: %s", key)
	} else {
		// Add new entry
		entry := &validationEntry{key: key, validated: validationTime}
		element := c.valLruList.PushFront(entry)
		c.validations[key] = element
		logging.Debug("Validation cache put: %s", key)
	}

	// Optional: Clean up old validation entries if map grows too large
	// (Simpler: rely on TTL check during Get)
}

// deleteValidation removes an entry from the validation cache.
func (c *DiskLRUCache) deleteValidation(key string) {
	c.valMu.Lock()
	defer c.valMu.Unlock()
	if element, exists := c.validations[key]; exists {
		c.valLruList.Remove(element)
		delete(c.validations, key)
		logging.Debug("Validation cache deleted expired/invalid entry: %s", key)
	}
}
