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
	"sync/atomic"
	"syscall"
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
}

type CacheStats struct {
	ItemCount                       int
	CurrentSize                     int64
	MaxSize                         int64
	ValidationItemCount             int
	CacheDirectory                  string
	CacheEnabled                    bool
	ValidationTTLEnabled            bool
	ValidationTTL                   time.Duration
	InconsistencyMetaWithoutContent uint64
	InconsistencyContentWithoutMeta uint64
	InconsistencySizeMismatch       uint64
	InconsistencyCorruptMetadata    uint64
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

	statsInconsistencyMetaWithoutContent atomic.Uint64
	statsInconsistencyContentWithoutMeta atomic.Uint64
	statsInconsistencySizeMismatch       atomic.Uint64
	statsInconsistencyCorruptMetadata    atomic.Uint64
}

func NewDiskLRUCache(cfg config.CacheConfig) (*DiskLRUCache, error) {
	if !cfg.Enabled {
		logging.Warn("Cache is disabled in configuration.")
		return &DiskLRUCache{enabled: false, closed: make(chan struct{})}, nil
	}

	maxSize, err := util.ParseSize(cfg.MaxSize)
	if err != nil {
		logging.ErrorE("Invalid cache max size string", err, "max_size_config", cfg.MaxSize)
		return nil, fmt.Errorf("invalid cache max size %q: %w", cfg.MaxSize, err)
	}
	if maxSize <= 0 {
		logging.Error("Cache max size must be positive when cache is enabled", "max_size_config", cfg.MaxSize)
		return nil, fmt.Errorf("cache max size must be positive (%s resulted in %d bytes)", cfg.MaxSize, maxSize)
	}

	baseDir := util.CleanPath(cfg.Directory)
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		logging.ErrorE("Failed to create cache directory", err, "directory", baseDir)
		return nil, fmt.Errorf("failed to create cache directory %s: %w", baseDir, err)
	}

	cache := &DiskLRUCache{
		baseDir:       baseDir,
		maxSizeBytes:  maxSize,
		validationTTL: cfg.ValidationTTL.Duration(),
		enabled:       true,
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

func (c *DiskLRUCache) getBaseFilePath(key string) string {
	safeRelativePath := util.SanitizePath(key)

	if safeRelativePath == "" || safeRelativePath == "." {
		logging.Warn("Generated empty or dot cache path from key, potential issue", "key", key)
		safeRelativePath = "_invalid_key_path_"
	}

	return filepath.Join(c.baseDir, safeRelativePath)
}

func (c *DiskLRUCache) getContentFilePath(key string) string {
	return c.getBaseFilePath(key) + ContentSuffix
}

func (c *DiskLRUCache) getMetaFilePath(key string) string {
	return c.getBaseFilePath(key) + MetadataSuffix
}

func (c *DiskLRUCache) initialize(cleanOnStart bool) {
	defer c.initWg.Done()

	if !c.enabled {
		c.initErr = errors.New("cache is disabled, skipping initialization")
		logging.Warn("Skipping cache initialization", "reason", "cache disabled")
		return
	}

	if cleanOnStart {
		logging.Info("Cleaning cache directory on startup...", "directory", c.baseDir)
		dirEntries, err := os.ReadDir(c.baseDir)
		if err != nil {
			c.initErr = fmt.Errorf("failed to read cache directory for cleaning %s: %w", c.baseDir, err)
			logging.ErrorE("Cache clean failed: cannot read directory", c.initErr, "directory", c.baseDir)
			return
		}
		for _, entry := range dirEntries {
			path := filepath.Join(c.baseDir, entry.Name())
			logging.Debug("Removing cache item during clean", "path", path)
			if err := os.RemoveAll(path); err != nil {
				logging.Warn("Failed to remove item during cache clean", "error", err, "path", path)
			}
		}
		logging.Info("Cache directory cleaned successfully.")
		c.mu.Lock()
		c.currentSize = 0
		c.items = make(map[string]*cacheEntry)
		c.lruList = list.New()
		c.mu.Unlock()
		return
	}

	logging.Info("Scanning cache directory to rebuild state...", "directory", c.baseDir)
	startTime := time.Now()
	var scannedItems int64
	var totalSize int64
	discoveredEntries := []*cacheEntry{}

	err := filepath.Walk(c.baseDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			logging.Warn("Error accessing path during cache scan", "error", walkErr, "path", path)
			if info != nil && info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		if path == c.baseDir || info.IsDir() {
			return nil
		}

		if strings.HasSuffix(path, ".tmp") {
			logging.Debug("Skipping temporary file during scan", "path", path)
			return nil
		}

		if !strings.HasSuffix(path, ContentSuffix) {
			return nil
		}

		basePath := strings.TrimSuffix(path, ContentSuffix)
		relBasePath, err := filepath.Rel(c.baseDir, basePath)
		if err != nil {
			logging.Warn("Failed to get relative base path during scan, skipping item", "error", err, "basePath", basePath, "fullPath", path)
			return nil
		}
		cacheKey := filepath.ToSlash(relBasePath)

		if cacheKey == "" || cacheKey == "." {
			logging.Warn("Generated invalid cache key during scan, skipping item.", "path", path, "relative_base", relBasePath)
			return nil
		}

		metaPath := basePath + MetadataSuffix
		entrySize := info.Size()

		metaFile, metaErr := os.Open(metaPath)
		if metaErr == nil {
			func() {
				defer metaFile.Close()
				decoder := json.NewDecoder(metaFile)
				var meta CacheMetadata
				if decodeErr := decoder.Decode(&meta); decodeErr == nil {
					if meta.Size >= 0 {
						entrySize = meta.Size
					} else {
						logging.Warn("Metadata has invalid size, using file size", "meta_size", meta.Size, "meta_path", metaPath, "file_size", info.Size(), "key", cacheKey)
					}
				} else {
					logging.Warn("Failed to decode metadata file, using file stats", "error", decodeErr, "meta_path", metaPath, "key", cacheKey)
				}
			}()
		} else if !os.IsNotExist(metaErr) {
			logging.Warn("Error opening metadata file, using file stats", "error", metaErr, "meta_path", metaPath, "key", cacheKey)
		} else {
			logging.Warn("Metadata file not found for content file, using file stats", "meta_path", metaPath, "content_path", path, "key", cacheKey)
		}

		entry := &cacheEntry{
			key:  cacheKey,
			size: entrySize,
		}
		discoveredEntries = append(discoveredEntries, entry)
		atomic.AddInt64(&scannedItems, 1)
		atomic.AddInt64(&totalSize, entry.size)

		return nil
	})

	if err != nil {
		c.initErr = fmt.Errorf("failed during cache directory scan: %w", err)
		logging.ErrorE("Cache scan failed", c.initErr)
		return
	}

	c.mu.Lock() // Lock before modifying shared state
	c.items = make(map[string]*cacheEntry, len(discoveredEntries))
	c.lruList = list.New()
	c.currentSize = 0

	loadedCount := 0
	for _, entry := range discoveredEntries {
		if _, exists := c.items[entry.key]; !exists {
			element := c.lruList.PushFront(entry)
			entry.element = element
			c.items[entry.key] = entry
			c.currentSize += entry.size
			loadedCount++
		} else {
			logging.Warn("Duplicate cache key detected during scan reconstruction, skipping.", "key", entry.key)
		}
	}
	c.mu.Unlock() // Unlock after modifying shared state

	logging.Info("Cache scan reconstruction complete",
		"duration", util.FormatDuration(time.Since(startTime)),
		"scanned_content_files", scannedItems,
		"loaded_items", loadedCount,
		"total_discovered_size", util.FormatSize(totalSize),
		"current_cache_size", util.FormatSize(c.currentSize),
		"current_cache_size_bytes", c.currentSize)

	// Perform initial eviction check *after* loading items
	c.mu.Lock()      // Lock before calling evictLocked
	c.evictLocked(0) // Call the locked version with 0 to check current size against max
	c.mu.Unlock()    // Unlock after check

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
		logging.ErrorE("Cache initialization failed, cannot Get item", err, "key", key)
		return nil, nil, fmt.Errorf("cache initialization failed: %w", err)
	}

	c.mu.Lock()
	entry, exists := c.items[key]
	if !exists {
		c.mu.Unlock()
		logging.Debug("Cache miss [GET]: key not found in memory map.", "key", key)
		return nil, nil, os.ErrNotExist
	}
	c.lruList.MoveToFront(entry.element)
	c.mu.Unlock()

	filePath := c.getContentFilePath(key)
	metaPath := c.getMetaFilePath(key)
	logging.Debug("Cache hit [GET]: checking files", "key", key, "content_file", filePath, "meta_file", metaPath)

	metaFile, err := os.Open(metaPath)
	if err != nil {
		c.statsInconsistencyContentWithoutMeta.Add(1)
		if os.IsNotExist(err) {
			logging.Warn("Cache inconsistency: item in memory but metadata file not found. Removing entry.", "key", key, "meta_path", metaPath)
		} else {
			logging.ErrorE("Cache inconsistency: failed to open existing metadata file. Removing entry.", err, "key", key, "meta_path", metaPath)
		}
		c.mu.Lock()
		c.deleteInternalLocked(key)
		c.mu.Unlock()
		go c.deleteFilesAsync(key)
		return nil, nil, os.ErrNotExist
	}
	defer metaFile.Close()

	var metadata CacheMetadata
	decoder := json.NewDecoder(metaFile)
	if err := decoder.Decode(&metadata); err != nil {
		c.statsInconsistencyCorruptMetadata.Add(1)
		logging.ErrorE("Cache inconsistency: failed to decode metadata file. Removing entry.", err, "meta_path", metaPath, "key", key)
		c.mu.Lock()
		c.deleteInternalLocked(key)
		c.mu.Unlock()
		go c.deleteFilesAsync(key)
		return nil, nil, os.ErrNotExist
	}
	metaFile.Close()

	contentFile, err := os.Open(filePath)
	if err != nil {
		c.statsInconsistencyMetaWithoutContent.Add(1)
		if os.IsNotExist(err) {
			logging.Warn("Cache inconsistency: metadata exists but content file not found. Removing entry.", "key", key, "content_path", filePath)
		} else {
			logging.ErrorE("Cache inconsistency: failed to open existing content file. Removing entry.", err, "key", key, "content_path", filePath)
		}
		c.mu.Lock()
		c.deleteInternalLocked(key)
		c.mu.Unlock()
		go c.deleteFilesAsync(key)
		return nil, nil, os.ErrNotExist
	}

	contentInfo, err := contentFile.Stat()
	if err != nil {
		_ = contentFile.Close()
		logging.Warn("Cache inconsistency: failed to stat content file. Removing entry.", "error", err, "content_path", filePath, "key", key)
		c.mu.Lock()
		c.deleteInternalLocked(key)
		c.mu.Unlock()
		go c.deleteFilesAsync(key)

		return nil, nil, os.ErrNotExist

	}

	if metadata.Size >= 0 && contentInfo.Size() != metadata.Size {
		c.statsInconsistencySizeMismatch.Add(1)
		_ = contentFile.Close()
		logging.Warn("Cache inconsistency: file size mismatch. Removing corrupted entry.", "key", key, "metadata_size", metadata.Size, "file_size", contentInfo.Size())
		c.mu.Lock()
		c.deleteInternalLocked(key)
		c.mu.Unlock()
		go c.deleteFilesAsync(key)
		return nil, nil, os.ErrNotExist
	} else if metadata.Size < 0 {
		metadata.Size = contentInfo.Size()
		logging.Debug("Updated metadata size based on file size", "key", key, "new_size", metadata.Size)
	}

	logging.Debug("Cache hit, returning content", "key", key, "size", metadata.Size)
	metadata.Key = key
	metadata.FilePath = filePath
	metadata.MetaPath = metaPath
	return contentFile, &metadata, nil
}

func (c *DiskLRUCache) Put(ctx context.Context, key string, reader io.Reader, metadata CacheMetadata) (err error) {
	if !c.enabled {
		_, _ = io.Copy(io.Discard, reader)
		logging.Warn("Cache is disabled, item not stored", "key", key)
		return errors.New("cache is disabled, item not stored")
	}
	if initErr := c.waitInit(); initErr != nil {
		_, _ = io.Copy(io.Discard, reader)
		logging.ErrorE("Cache initialization failed, cannot Put item", initErr, "key", key)
		return fmt.Errorf("cache initialization failed, cannot put item %s: %w", key, initErr)
	}

	basePath := c.getBaseFilePath(key)
	filePath := basePath + ContentSuffix
	metaPath := basePath + MetadataSuffix
	dirPath := filepath.Dir(basePath)

	if mkdirErr := os.MkdirAll(dirPath, 0755); mkdirErr != nil {
		if pathErr, ok := mkdirErr.(*os.PathError); ok && errors.Is(pathErr.Err, syscall.ENOTDIR) {
			logging.ErrorE("Failed to create directory for cache item: part of the path is a file", mkdirErr, "directory", dirPath, "key", key, "conflicting_path", pathErr.Path)
			conflictingKey := c.getKeyFromPathConflict(pathErr.Path)
			if conflictingKey != "" {
				logging.Warn("Attempting to remove conflicting cache entry due to path conflict", "conflicting_key", conflictingKey, "new_key", key)
				c.mu.Lock()
				_, deleted := c.deleteInternalLocked(conflictingKey)
				c.mu.Unlock()
				if deleted {
					go c.deleteFilesAsync(conflictingKey)
				}
			}
		} else {
			logging.ErrorE("Failed to create directory for cache item", mkdirErr, "directory", dirPath, "key", key)
		}
		_, _ = io.Copy(io.Discard, reader)
		return fmt.Errorf("failed to create directory %s for cache item %s: %w", dirPath, key, mkdirErr)
	}

	var tempContentFile, tempMetaFile *os.File
	var tempContentPath, tempMetaPath string
	defer func() {
		if tempContentFile != nil {
			_ = tempContentFile.Close()
		}
		if tempContentPath != "" {
			logging.Debug("Cleaning up temporary content file", "path", tempContentPath)
			if remErr := os.Remove(tempContentPath); remErr != nil && !errors.Is(remErr, os.ErrNotExist) {
				logging.ErrorE("Failed to remove temporary content file", remErr, "temp_path", tempContentPath)
			}
		}
		if tempMetaFile != nil {
			_ = tempMetaFile.Close()
		}
		if tempMetaPath != "" {
			logging.Debug("Cleaning up temporary metadata file", "path", tempMetaPath)
			if remErr := os.Remove(tempMetaPath); remErr != nil && !errors.Is(remErr, os.ErrNotExist) {
				logging.ErrorE("Failed to remove temporary metadata file", remErr, "temp_path", tempMetaPath)
			}
		}
	}()

	baseFilename := filepath.Base(basePath)
	tempContentFile, err = os.CreateTemp(dirPath, baseFilename+".*.cache.tmp")
	if err != nil {
		_, _ = io.Copy(io.Discard, reader)
		logging.ErrorE("Failed to create temporary content file", err, "directory", dirPath, "key", key)
		return fmt.Errorf("failed to create temporary content file in %s for key %s: %w", dirPath, key, err)
	}
	tempContentPath = tempContentFile.Name()
	logging.Debug("Created temporary content file", "path", tempContentPath)

	tempMetaFile, err = os.CreateTemp(dirPath, baseFilename+".*.meta.tmp")
	if err != nil {
		_, _ = io.Copy(io.Discard, reader)
		logging.ErrorE("Failed to create temporary metadata file", err, "directory", dirPath, "key", key)
		return fmt.Errorf("failed to create temporary metadata file in %s for key %s: %w", dirPath, key, err)
	}
	tempMetaPath = tempMetaFile.Name()
	logging.Debug("Created temporary metadata file", "path", tempMetaPath)

	writtenSize, err := io.Copy(tempContentFile, reader)
	if err != nil {
		logging.ErrorE("Failed to write to temporary content file", err, "temp_path", tempContentPath, "key", key)
		_ = tempContentFile.Close()
		tempContentFile = nil
		return fmt.Errorf("failed to write content for key %s: %w", key, err)
	}
	if err = tempContentFile.Close(); err != nil {
		logging.ErrorE("Failed to close temporary content file after writing", err, "temp_path", tempContentPath, "key", key)
		tempContentFile = nil
		return fmt.Errorf("failed to close temporary content file for key %s: %w", key, err)
	}
	tempContentFile = nil

	metadata.Version = MetadataVersion
	if metadata.FetchTime.IsZero() {
		metadata.FetchTime = time.Now().UTC()
	}
	if metadata.Size >= 0 {
		if writtenSize != metadata.Size {
			logging.Error("Cache write size mismatch", "key", key, "expected_size", metadata.Size, "written_size", writtenSize)
			return fmt.Errorf("cache write size mismatch for %s: expected %d, wrote %d", key, metadata.Size, writtenSize)
		}
	} else {
		metadata.Size = writtenSize
	}

	encoder := json.NewEncoder(tempMetaFile)
	encoder.SetIndent("", "  ")
	if err = encoder.Encode(&metadata); err != nil {
		logging.ErrorE("Failed to encode metadata to temporary file", err, "temp_path", tempMetaPath, "key", key)
		_ = tempMetaFile.Close()
		tempMetaFile = nil
		return fmt.Errorf("failed to encode metadata for key %s: %w", key, err)
	}
	if err = tempMetaFile.Close(); err != nil {
		logging.ErrorE("Failed to close temporary metadata file after writing", err, "temp_path", tempMetaPath, "key", key)
		tempMetaFile = nil
		return fmt.Errorf("failed to close temporary metadata file for key %s: %w", key, err)
	}
	tempMetaFile = nil

	finalSize := metadata.Size
	c.mu.Lock()

	var oldSize int64 = -1
	var element *list.Element
	if oldEntry, exists := c.items[key]; exists {
		oldSize = oldEntry.size
		c.currentSize -= oldSize
		c.lruList.Remove(oldEntry.element)
		logging.Debug("Updating existing cache entry", "key", key, "old_size", oldSize, "new_size", finalSize)
	} else {
		logging.Debug("Adding new cache entry", "key", key, "size", finalSize)
	}

	spaceToFree := (c.currentSize + finalSize) - c.maxSizeBytes
	if spaceToFree > 0 {
		c.evictLocked(spaceToFree)
	}

	newEntry := &cacheEntry{
		key:  key,
		size: finalSize,
	}
	element = c.lruList.PushFront(newEntry)
	newEntry.element = element
	c.items[key] = newEntry
	c.currentSize += finalSize

	if c.currentSize < 0 {
		logging.Error("Internal error: current cache size negative after Put", "size", c.currentSize, "key", key)
		c.currentSize = 0
	}

	c.mu.Unlock()

	if err = os.Rename(tempMetaPath, metaPath); err != nil {
		logging.ErrorE("Failed to rename temporary metadata file. Reverting LRU changes.", err, "temp_path", tempMetaPath, "final_path", metaPath, "key", key)
		c.mu.Lock()
		c.currentSize -= finalSize
		if element != nil {
			c.lruList.Remove(element)
		}
		delete(c.items, key)
		if oldSize >= 0 {
			logging.Warn("Could not fully revert LRU state after failed metadata rename for updated key.", "key", key, "old_size", oldSize)
		}
		c.mu.Unlock()
		return fmt.Errorf("failed to commit metadata for key %s: %w", key, err)
	}
	logging.Debug("Renamed temporary metadata file", "from", tempMetaPath, "to", metaPath)
	tempMetaPath = ""

	if err = os.Rename(tempContentPath, filePath); err != nil {
		logging.ErrorE("Failed to rename temporary content file. Reverting LRU and removing committed meta file.", err, "temp_path", tempContentPath, "final_path", filePath, "key", key)
		c.mu.Lock()
		c.currentSize -= finalSize
		if element != nil {
			c.lruList.Remove(element)
		}
		delete(c.items, key)
		if removeMetaErr := os.Remove(metaPath); removeMetaErr != nil && !os.IsNotExist(removeMetaErr) {
			logging.ErrorE("Failed to remove committed meta file during content rename failure rollback", removeMetaErr, "meta_path", metaPath, "key", key)
		}
		if oldSize >= 0 {
			logging.Warn("Could not fully revert LRU state after failed content rename for updated key.", "key", key, "old_size", oldSize)
		}
		c.mu.Unlock()
		return fmt.Errorf("failed to commit content for key %s: %w", key, err)
	}
	logging.Debug("Renamed temporary content file", "from", tempContentPath, "to", filePath)
	tempContentPath = ""

	logging.Debug("Cache put successful", "key", key, "size", finalSize)
	return nil
}

func (c *DiskLRUCache) getKeyFromPathConflict(conflictPath string) string {
	relConflictPath, err := filepath.Rel(c.baseDir, conflictPath)
	if err != nil {
		return ""
	}
	key := ""
	if strings.HasSuffix(relConflictPath, ContentSuffix) {
		key = filepath.ToSlash(strings.TrimSuffix(relConflictPath, ContentSuffix))
	} else if strings.HasSuffix(relConflictPath, MetadataSuffix) {
		key = filepath.ToSlash(strings.TrimSuffix(relConflictPath, MetadataSuffix))
	}
	return key
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
	contentPath := c.getContentFilePath(key)
	metaPath := c.getMetaFilePath(key)

	go func(cPath, mPath, k string) {
		logging.Debug("Async deleting cache files", "key", k, "content_path", cPath, "meta_path", mPath)
		contentErr := os.Remove(cPath)
		metaErr := os.Remove(mPath)

		logFields := map[string]interface{}{
			"key":          k,
			"content_path": cPath,
			"meta_path":    mPath,
		}
		hasError := false

		if contentErr != nil && !errors.Is(contentErr, os.ErrNotExist) {
			logFields["content_error"] = contentErr.Error()
			logging.Error("Cache delete files async: content deletion failed", logFields)
			hasError = true
		} else {
			if contentErr != nil {
				logFields["content_status"] = "not_found"
			} else {
				logFields["content_status"] = "deleted"
			}
		}

		if metaErr != nil && !errors.Is(metaErr, os.ErrNotExist) {
			logFields["meta_error"] = metaErr.Error()
			if !hasError {
				logging.Error("Cache delete files async: metadata deletion failed", logFields)
			}
			hasError = true
		} else {
			if metaErr != nil {
				logFields["meta_status"] = "not_found"
			} else {
				logFields["meta_status"] = "deleted"
			}
		}

		if !hasError {
			logging.Debug("Cache delete files async: completed", logFields)
		}
	}(contentPath, metaPath, key)
}

func (c *DiskLRUCache) Delete(ctx context.Context, key string) error {
	if !c.enabled {
		logging.Warn("Cache is disabled, cannot delete", "key", key)
		return errors.New("cache is disabled, cannot delete")
	}

	c.mu.Lock()
	deletedSize, exists := c.deleteInternalLocked(key)
	c.mu.Unlock()

	if !exists {
		logging.Debug("Cache delete: key not found in memory map, nothing to delete.", "key", key)
		return nil
	}

	c.deleteFilesAsync(key)

	logging.Debug("Cache delete initiated", "key", key, "freed_memory_bytes", deletedSize)
	return nil
}

func (c *DiskLRUCache) evictLocked(spaceToFree int64) {
	if !c.enabled || c.maxSizeBytes <= 0 || spaceToFree <= 0 {
		return
	}

	logging.Debug("Eviction required", "current_size", util.FormatSize(c.currentSize), "max_size", util.FormatSize(c.maxSizeBytes), "need_to_free", util.FormatSize(spaceToFree))

	freedSpace := int64(0)
	itemsEvicted := 0

	for freedSpace < spaceToFree {
		element := c.lruList.Back()
		if element == nil {
			logging.Error("Eviction stopped: LRU list empty but still need to free space. Cache state might be inconsistent.",
				"freed_so_far", util.FormatSize(freedSpace),
				"still_need_to_free", util.FormatSize(spaceToFree-freedSpace),
				"current_size", util.FormatSize(c.currentSize))
			break
		}

		entry := element.Value.(*cacheEntry)

		deletedSize, existed := c.deleteInternalLocked(entry.key)

		if existed {
			freedSpace += deletedSize
			itemsEvicted++
			logging.Debug("Evicted LRU item", "key", entry.key, "size", deletedSize, "total_freed", util.FormatSize(freedSpace), "needed", util.FormatSize(spaceToFree))
			c.deleteFilesAsync(entry.key)
		} else {
			c.lruList.Remove(element)
			logging.Error("Eviction inconsistency: Element found in LRU list but not in map. Removed list element.", "key", entry.key)
		}
	}

	logging.Debug("Eviction finished", "items_evicted", itemsEvicted, "total_freed_space", util.FormatSize(freedSpace), "new_current_size", util.FormatSize(c.currentSize))

	if c.currentSize < 0 {
		logging.Error("Internal error: current cache size negative after eviction", "size", c.currentSize)
		c.currentSize = 0
	}
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

	inconsistMetaNoContent := c.statsInconsistencyMetaWithoutContent.Load()
	inconsistContentNoMeta := c.statsInconsistencyContentWithoutMeta.Load()
	inconsistSizeMismatch := c.statsInconsistencySizeMismatch.Load()
	inconsistCorruptMeta := c.statsInconsistencyCorruptMetadata.Load()

	return CacheStats{
		ItemCount:                       itemCount,
		CurrentSize:                     currentSize,
		MaxSize:                         maxSize,
		ValidationItemCount:             validationCount,
		CacheDirectory:                  baseDir,
		CacheEnabled:                    enabled,
		ValidationTTLEnabled:            c.validationTTL > 0,
		ValidationTTL:                   c.validationTTL,
		InconsistencyMetaWithoutContent: inconsistMetaNoContent,
		InconsistencyContentWithoutMeta: inconsistContentNoMeta,
		InconsistencySizeMismatch:       inconsistSizeMismatch,
		InconsistencyCorruptMetadata:    inconsistCorruptMeta,
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
		logging.Debug("Waiting for cache initialization goroutine to complete...")
		c.initWg.Wait()
		logging.Debug("Cache initialization goroutine finished.")
	}
	logging.Info("DiskLRUCache closed.")
	return nil
}

func (c *DiskLRUCache) GetValidation(key string) (validationTime time.Time, ok bool) {
	ttl := c.validationTTL
	if ttl <= 0 {
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
		logging.Debug("Validation cache entry expired, removing", "key", key, "ttl", ttl)
		go c.deleteValidationAsync(key)
		return time.Time{}, false
	}

	c.valMu.Lock()
	element, stillExists := c.validations[key]
	if !stillExists {
		c.valMu.Unlock()
		logging.Debug("Validation cache entry disappeared between RLock and Lock", "key", key)
		return time.Time{}, false
	}
	currentEntry := element.Value.(*validationEntry)
	if time.Since(currentEntry.validated) > ttl {
		logging.Debug("Validation cache entry expired between RLock and Lock, removing", "key", key)
		c.valLruList.Remove(element)
		delete(c.validations, key)
		c.valMu.Unlock()
		return time.Time{}, false
	}

	c.valLruList.MoveToFront(element)
	validationTime = currentEntry.validated
	ok = true
	c.valMu.Unlock()

	logging.Debug("Validation cache hit", "key", key, "validated_at", validationTime.Format(time.RFC3339))
	return validationTime, ok
}

func (c *DiskLRUCache) PutValidation(key string, validationTime time.Time) {
	ttl := c.validationTTL
	if ttl <= 0 {
		return
	}

	c.valMu.Lock()
	defer c.valMu.Unlock()

	if element, exists := c.validations[key]; exists {
		entry := element.Value.(*validationEntry)
		entry.validated = validationTime
		c.valLruList.MoveToFront(element)
		logging.Debug("Validation cache updated", "key", key, "new_time", validationTime.Format(time.RFC3339))
	} else {
		entry := &validationEntry{key: key, validated: validationTime}
		element := c.valLruList.PushFront(entry)
		c.validations[key] = element
		logging.Debug("Validation cache put", "key", key, "time", validationTime.Format(time.RFC3339))
	}
}

func (c *DiskLRUCache) deleteValidationAsync(key string) {
	go func(k string) {
		c.valMu.Lock()
		if element, exists := c.validations[k]; exists {
			c.valLruList.Remove(element)
			delete(c.validations, k)
			logging.Debug("Validation cache deleted entry async", "key", k)
		}
		c.valMu.Unlock()
	}(key)
}
