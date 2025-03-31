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
	InconsistencyContentWithoutMeta uint64 `json:"inconsistency_content_without_meta"`
	InconsistencySizeMismatch       uint64 `json:"inconsistency_size_mismatch"`
	InconsistencyCorruptMetadata    uint64 `json:"inconsistency_corrupt_metadata"`
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
	baseDir       string
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

	statsHits                            atomic.Uint64
	statsMisses                          atomic.Uint64
	statsValidationHits                  atomic.Uint64
	statsValidationErrors                atomic.Uint64
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

		logging.ErrorE("Invalid cache max size string during init", err, "max_size_config", cfg.MaxSize)
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
		cleanOnStart:  cfg.CleanOnStart,
		items:         make(map[string]*cacheEntry),
		lruList:       list.New(),
		validations:   make(map[string]*list.Element),
		valLruList:    list.New(),
		closed:        make(chan struct{}),
	}

	cache.initWg.Add(1)
	go cache.initialize()

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

func (c *DiskLRUCache) initialize() {
	startTime := time.Now()
	defer func() {
		c.initDuration = time.Since(startTime)
		c.initWg.Done()
	}()

	if !c.enabled {
		c.initErr = errors.New("cache is disabled, skipping initialization")
		logging.Warn("Skipping cache initialization", "reason", "cache disabled")
		return
	}

	if c.cleanOnStart {
		logging.Info("Cleaning cache directory on startup...", "directory", c.baseDir)
		dirEntries, err := os.ReadDir(c.baseDir)
		if err != nil {
			c.initErr = fmt.Errorf("failed to read cache directory for cleaning %s: %w", c.baseDir, err)
			logging.ErrorE("Cache clean failed: cannot read directory", c.initErr, "directory", c.baseDir)
			return
		}
		cleanedCount := 0
		for _, entry := range dirEntries {
			path := filepath.Join(c.baseDir, entry.Name())
			logging.Debug("Removing cache item during clean", "path", path)
			if err := os.RemoveAll(path); err != nil {
				logging.Warn("Failed to remove item during cache clean", "error", err, "path", path)
			} else {
				cleanedCount++
			}
		}
		logging.Info("Cache directory cleaned successfully.", "items_removed", cleanedCount)
		c.mu.Lock()
		c.currentSize = 0
		c.items = make(map[string]*cacheEntry)
		c.lruList = list.New()
		c.mu.Unlock()
		return
	}

	logging.Info("Scanning cache directory to rebuild state...", "directory", c.baseDir)
	var scannedFiles int64
	var totalDiscoveredSize int64
	var loadedItems int64
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

		atomic.AddInt64(&scannedFiles, 1)
		basePath := strings.TrimSuffix(path, ContentSuffix)
		metaPath := basePath + MetadataSuffix
		relBasePath, relErr := filepath.Rel(c.baseDir, basePath)
		if relErr != nil {
			logging.Warn("Failed to get relative base path during scan, skipping item", "error", relErr, "basePath", basePath, "fullPath", path)
			return nil
		}
		cacheKey := filepath.ToSlash(relBasePath)

		if cacheKey == "" || cacheKey == "." {
			logging.Warn("Generated invalid cache key during scan, skipping item.", "path", path, "relative_base", relBasePath)
			return nil
		}

		entrySize := int64(-1)
		realFileSize := info.Size()

		metaFile, metaErr := os.Open(metaPath)
		if metaErr == nil {

			var meta CacheMetadata
			decoder := json.NewDecoder(metaFile)
			decodeErr := decoder.Decode(&meta)
			metaFile.Close()

			if decodeErr == nil {

				if meta.Size >= 0 {

					if meta.Size != realFileSize {
						logging.Warn("Cache inconsistency detected during scan: metadata size mismatch. Removing entry.",
							"key", cacheKey, "meta_size", meta.Size, "file_size", realFileSize, "content_path", path, "meta_path", metaPath)
						c.statsInconsistencySizeMismatch.Add(1)
						_ = os.Remove(path)
						_ = os.Remove(metaPath)
						return nil
					}
					entrySize = meta.Size
				} else {

					entrySize = realFileSize
					logging.Debug("Metadata size was -1, using actual file size", "key", cacheKey, "size", entrySize)
				}
			} else {

				logging.Warn("Cache inconsistency detected during scan: failed to decode metadata file. Removing entry.",
					"error", decodeErr, "meta_path", metaPath, "key", cacheKey, "content_path", path)
				c.statsInconsistencyCorruptMetadata.Add(1)
				_ = os.Remove(path)
				_ = os.Remove(metaPath)
				return nil
			}
		} else if os.IsNotExist(metaErr) {

			logging.Warn("Cache inconsistency detected during scan: content file exists without metadata. Removing content.",
				"key", cacheKey, "content_path", path, "meta_path", metaPath)
			c.statsInconsistencyContentWithoutMeta.Add(1)
			_ = os.Remove(path)
			return nil
		} else {

			logging.Warn("Error opening metadata file during scan, cannot validate. Skipping item.",
				"error", metaErr, "meta_path", metaPath, "key", cacheKey, "content_path", path)

			return nil
		}

		entry := &cacheEntry{
			key:  cacheKey,
			size: entrySize,
		}
		discoveredEntries = append(discoveredEntries, entry)
		atomic.AddInt64(&loadedItems, 1)
		atomic.AddInt64(&totalDiscoveredSize, entry.size)

		return nil
	})

	if err != nil {
		c.initErr = fmt.Errorf("failed during cache directory scan: %w", err)
		logging.ErrorE("Cache scan failed", c.initErr)
		return
	}

	c.mu.Lock()
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

			logging.Warn("Duplicate cache key detected during scan reconstruction, skipping.", "key", entry.key)
		}
	}
	c.mu.Unlock()

	logging.Info("Cache scan reconstruction complete",
		"scanned_files", scannedFiles,
		"loaded_items", loadedItems,
		"total_discovered_size", util.FormatSize(totalDiscoveredSize),
		"current_cache_size", util.FormatSize(c.currentSize),
		"current_cache_size_bytes", c.currentSize)

	c.mu.Lock()
	c.evictLocked(0)
	c.mu.Unlock()
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

	var metadata CacheMetadata
	decoder := json.NewDecoder(metaFile)
	decodeErr := decoder.Decode(&metadata)
	metaFile.Close()

	if decodeErr != nil {
		c.statsInconsistencyCorruptMetadata.Add(1)
		logging.ErrorE("Cache inconsistency: failed to decode metadata file. Removing entry.", decodeErr, "meta_path", metaPath, "key", key)
		c.mu.Lock()
		c.deleteInternalLocked(key)
		c.mu.Unlock()
		go c.deleteFilesAsync(key)

		return nil, nil, os.ErrNotExist
	}

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
		logging.Warn("Cache inconsistency: file size mismatch during Get. Removing corrupted entry.", "key", key, "metadata_size", metadata.Size, "file_size", contentInfo.Size())
		c.mu.Lock()
		c.deleteInternalLocked(key)
		c.mu.Unlock()
		go c.deleteFilesAsync(key)

		return nil, nil, os.ErrNotExist
	} else if metadata.Size < 0 {

		metadata.Size = contentInfo.Size()
		logging.Debug("Updated metadata size based on file size during Get", "key", key, "new_size", metadata.Size)
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

	dirInfo, dirStatErr := os.Stat(dirPath)
	if dirStatErr != nil {
		if errors.Is(dirStatErr, os.ErrNotExist) {
			if mkdirErr := os.MkdirAll(dirPath, 0755); mkdirErr != nil {
				logging.ErrorE("Failed to create directory for cache item", mkdirErr, "directory", dirPath, "key", key)
				_, _ = io.Copy(io.Discard, reader)
				return fmt.Errorf("failed to create directory %s for cache item %s: %w", dirPath, key, mkdirErr)
			}
		} else {

			logging.ErrorE("Failed to stat directory for cache item", dirStatErr, "directory", dirPath, "key", key)
			_, _ = io.Copy(io.Discard, reader)
			return fmt.Errorf("failed to stat directory %s for cache item %s: %w", dirPath, key, dirStatErr)
		}
	} else if !dirInfo.IsDir() {

		logging.Error("Cache path conflict: a file exists where a directory is needed.", "conflicting_path", dirPath, "key", key)

		_, _ = io.Copy(io.Discard, reader)
		return fmt.Errorf("path conflict: cannot create directory %s because a file exists", dirPath)
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
	if err = tempContentFile.Sync(); err != nil {
		logging.Warn("Failed to sync temporary content file", "error", err, "temp_path", tempContentPath)
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

	if metadata.Headers == nil {
		metadata.Headers = make(http.Header)
	}
	if metadata.Headers.Get("Content-Type") == "" {
		logging.Warn("Content-Type was not set in metadata provided to Put, setting to octet-stream", "key", key)
		metadata.Headers.Set("Content-Type", "application/octet-stream")
	}

	encoder := json.NewEncoder(tempMetaFile)
	encoder.SetIndent("", "  ")
	if err = encoder.Encode(&metadata); err != nil {
		logging.ErrorE("Failed to encode metadata to temporary file", err, "temp_path", tempMetaPath, "key", key)
		_ = tempMetaFile.Close()
		tempMetaFile = nil
		return fmt.Errorf("failed to encode metadata for key %s: %w", key, err)
	}
	if err = tempMetaFile.Sync(); err != nil {
		logging.Warn("Failed to sync temporary metadata file", "error", err, "temp_path", tempMetaPath)
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
	var oldElement *list.Element
	if oldEntry, exists := c.items[key]; exists {
		oldSize = oldEntry.size
		oldElement = oldEntry.element
		c.currentSize -= oldSize
		c.lruList.Remove(oldElement)
		logging.Debug("Updating existing cache entry", "key", key, "old_size", oldSize, "new_size", finalSize)
	} else {
		logging.Debug("Adding new cache entry", "key", key, "size", finalSize)
	}

	spaceNeeded := finalSize
	if oldSize == -1 {
		spaceToFree := (c.currentSize + spaceNeeded) - c.maxSizeBytes
		if spaceToFree > 0 {
			c.evictLocked(spaceToFree)
		}
	} else if finalSize > oldSize {
		spaceToFree := (c.currentSize + finalSize) - c.maxSizeBytes
		if spaceToFree > 0 {
			c.evictLocked(spaceToFree)
		}
	}

	newEntry := &cacheEntry{key: key, size: finalSize}
	element := c.lruList.PushFront(newEntry)
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
		if oldElement != nil {

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
		if oldElement != nil {
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

		logFields := map[string]interface{}{"key": k, "content_path": cPath, "meta_path": mPath}
		hasError := false

		if contentErr != nil && !errors.Is(contentErr, os.ErrNotExist) {
			logFields["content_error"] = contentErr.Error()
			logging.Error("Cache delete files async: content deletion failed", logFields)
			hasError = true
		}

		if metaErr != nil && !errors.Is(metaErr, os.ErrNotExist) {
			logFields["meta_error"] = metaErr.Error()
			logging.Error("Cache delete files async: metadata deletion failed", logFields)
			hasError = true
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

		if c.currentSize <= c.maxSizeBytes && spaceToFree == 0 {
			return
		}
	}

	logging.Debug("Eviction required/check", "current_size", util.FormatSize(c.currentSize), "max_size", util.FormatSize(c.maxSizeBytes), "target_free_space", util.FormatSize(spaceToFree))

	freedSpace := int64(0)
	itemsEvicted := 0
	targetSize := c.maxSizeBytes - spaceToFree

	for c.currentSize > targetSize {
		element := c.lruList.Back()
		if element == nil {
			if c.currentSize > targetSize {
				logging.Error("Eviction stopped: LRU list empty but still need to free space. Cache state might be inconsistent.",
					"current_size", util.FormatSize(c.currentSize), "target_size", util.FormatSize(targetSize))
			}
			break
		}

		entry := element.Value.(*cacheEntry)
		deletedSize, existed := c.deleteInternalLocked(entry.key)

		if existed {
			freedSpace += deletedSize
			itemsEvicted++
			logging.Debug("Evicted LRU item", "key", entry.key, "size", deletedSize)
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
	baseDir := c.baseDir
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
		InconsistencyContentWithoutMeta: c.statsInconsistencyContentWithoutMeta.Load(),
		InconsistencySizeMismatch:       c.statsInconsistencySizeMismatch.Load(),
		InconsistencyCorruptMetadata:    c.statsInconsistencyCorruptMetadata.Load(),
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
		logging.Debug("Waiting for cache initialization goroutine to complete...")
		c.initWg.Wait()
		logging.Debug("Cache initialization goroutine finished.")
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
		logging.Warn("Cannot get validation, cache init failed", "error", initErr, "key", key)
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
		c.deleteValidationAsync(key)
		return time.Time{}, false
	}

	c.valMu.Lock()
	defer c.valMu.Unlock()

	element, stillExists := c.validations[key]
	if !stillExists {

		logging.Debug("Validation cache entry disappeared between RLock and Lock", "key", key)
		return time.Time{}, false
	}

	currentEntry := element.Value.(*validationEntry)
	if time.Since(currentEntry.validated) > ttl {

		logging.Debug("Validation cache entry expired between RLock and Lock, removing", "key", key)
		c.valLruList.Remove(element)
		delete(c.validations, key)
		return time.Time{}, false
	}

	c.valLruList.MoveToFront(element)
	c.statsValidationHits.Add(1)
	validationTime = currentEntry.validated
	ok = true

	logging.Debug("Validation cache hit", "key", key, "validated_at", validationTime.Format(time.RFC3339))
	return validationTime, ok
}

func (c *DiskLRUCache) PutValidation(key string, validationTime time.Time) {
	ttl := c.validationTTL
	if ttl <= 0 || !c.enabled {
		return
	}

	if initErr := c.waitInit(); initErr != nil {
		logging.Warn("Cannot put validation, cache init failed", "error", initErr, "key", key)
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
