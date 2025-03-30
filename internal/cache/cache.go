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
	dirIndexKeySuffix      = "/."
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

		logging.ErrorE("Invalid cache max size", err, "max_size_config", cfg.MaxSize)
		return nil, fmt.Errorf("invalid cache max size %q: %w", cfg.MaxSize, err)
	}
	if maxSize <= 0 && cfg.Enabled {
		logging.Error("Cache max size must be positive when enabled", "max_size_config", cfg.MaxSize)
		return nil, errors.New("cache max size must be positive when enabled")
	}

	baseDir := util.CleanPath(cfg.Directory)
	if cfg.Enabled {
		if err := os.MkdirAll(baseDir, 0755); err != nil {

			logging.ErrorE("Failed to create cache directory", err, "directory", baseDir)
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
	isDirIndex := strings.HasSuffix(key, dirIndexKeySuffix)
	baseKey := key
	if isDirIndex {
		baseKey = strings.TrimSuffix(key, dirIndexKeySuffix)
	}

	parts := strings.SplitN(baseKey, "/", 2)
	repoName := ""
	filePathPart := baseKey

	if len(parts) > 0 {
		repoName = parts[0]
		if len(parts) > 1 {
			filePathPart = parts[1]
		} else if repoName == baseKey {
			filePathPart = ""
		}
	}

	safeRepoName := util.SanitizeFilename(repoName)
	safeFilePath := util.SanitizePath(filePathPart)

	targetPath := filepath.Join(c.baseDir, safeRepoName, safeFilePath)

	if isDirIndex {
		if filePathPart == "" && repoName != baseKey {
			targetPath = filepath.Join(c.baseDir, safeRepoName)
		} else if safeFilePath == "." || safeFilePath == "" {
			targetPath = filepath.Join(c.baseDir, safeRepoName)
		}
		return filepath.Join(targetPath, DirectoryIndexFilename)
	} else if safeFilePath == "" || safeFilePath == "." {
		logging.Warn("Request for potentially ambiguous file key treated as file in cache root", "key", key, "cache_file", safeRepoName)
		return filepath.Join(c.baseDir, safeRepoName)
	}

	return targetPath
}

func (c *DiskLRUCache) getMetaFilePath(key string) string {
	return c.getContentFilePath(key) + MetadataSuffix
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
			if err := os.RemoveAll(path); err != nil {
				logging.Warn("Failed to remove item during clean", "error", err, "path", path)
			}
		}
		logging.Info("Cache directory cleaned.")
		return
	}

	logging.Info("Scanning cache directory to rebuild state...", "directory", c.baseDir)
	startTime := time.Now()
	var scannedItems int
	var totalSize int64
	entries := []*cacheEntry{}

	err := filepath.Walk(c.baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			logging.Warn("Error accessing path during cache scan", "error", err, "path", path)
			if info != nil && info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		if path == c.baseDir || info.IsDir() || strings.HasSuffix(path, ".tmp") || strings.HasSuffix(path, MetadataSuffix) {
			return nil
		}

		relPath, err := filepath.Rel(c.baseDir, path)
		if err != nil {
			logging.Warn("Failed to get relative path", "error", err, "path", path)
			return nil
		}

		cacheKey := ""
		isDirIndex := info.Name() == DirectoryIndexFilename

		if isDirIndex {
			dirPath := filepath.Dir(relPath)
			if dirPath == "." {
				parts := strings.Split(filepath.ToSlash(path), string(filepath.Separator))
				if len(parts) > 1 && parts[len(parts)-2] != filepath.Base(c.baseDir) {
					cacheKey = parts[len(parts)-2] + dirIndexKeySuffix
				} else {
					logging.Warn("Found directory index directly in cache base directory, skipping.", "filename", DirectoryIndexFilename, "directory", c.baseDir)
					return nil
				}
			} else {
				cacheKey = filepath.ToSlash(dirPath) + dirIndexKeySuffix
			}
		} else {
			cacheKey = filepath.ToSlash(relPath)
		}

		if cacheKey == "" || cacheKey == dirIndexKeySuffix {
			logging.Warn("Generated invalid cache key for path, skipping.", "path", path)
			return nil
		}

		metaPath := path + MetadataSuffix
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
						logging.Warn("Invalid size in metadata, using file size", "metadata_size", meta.Size, "meta_path", metaPath, "file_size", info.Size(), "key", cacheKey)
					}
				} else {
					logging.Warn("Failed to decode metadata, using file stats", "error", decodeErr, "meta_path", metaPath, "key", cacheKey)
				}
			}()
		} else if !os.IsNotExist(metaErr) {
			logging.Warn("Error opening metadata file, using file stats", "error", metaErr, "meta_path", metaPath, "key", cacheKey)
		}

		entry := &cacheEntry{
			key:  cacheKey,
			size: entrySize,
		}
		entries = append(entries, entry)
		scannedItems++
		totalSize += entry.size

		return nil
	})

	if err != nil {
		c.initErr = fmt.Errorf("failed during cache directory scan: %w", err)
		logging.ErrorE("Cache scan failed", c.initErr)
		return
	}

	c.mu.Lock()
	c.items = make(map[string]*cacheEntry, len(entries))
	c.lruList = list.New()
	c.currentSize = 0

	for _, entry := range entries {
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
		"duration", util.FormatDuration(time.Since(startTime)),
		"scanned_items", scannedItems,
		"loaded_items", len(c.items),
		"total_size", util.FormatSize(c.currentSize),
		"total_size_bytes", c.currentSize)

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
		logging.ErrorE("Cache initialization failed on Get", err)
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
	logging.Debug("Cache hit [GET]: key maps to content file", "key", key, "content_file", filePath)

	metaFile, err := os.Open(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			logging.Warn("Cache inconsistency: item in memory but metadata not found. Removing entry.", "key", key, "meta_path", metaPath)

			c.mu.Lock()
			c.deleteInternalLocked(key)
			c.mu.Unlock()

			go c.deleteFilesAsync(key)
			return nil, nil, os.ErrNotExist
		}
		logging.ErrorE("Failed to open cache metadata file", err, "meta_path", metaPath, "key", key)
		return nil, nil, fmt.Errorf("failed to open cache metadata file %s for key %s: %w", metaPath, key, err)
	}
	defer metaFile.Close()

	var metadata CacheMetadata
	decoder := json.NewDecoder(metaFile)
	if err := decoder.Decode(&metadata); err != nil {
		logging.ErrorE("Failed to decode metadata file. Removing corrupted entry.", err, "meta_path", metaPath, "key", key)

		c.mu.Lock()
		c.deleteInternalLocked(key)
		c.mu.Unlock()

		go c.deleteFilesAsync(key)
		return nil, nil, os.ErrNotExist
	}

	contentFile, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			logging.Warn("Cache inconsistency: metadata exists but content file not found. Removing entry.", "key", key, "meta_path", metaPath, "content_path", filePath)

			c.mu.Lock()
			c.deleteInternalLocked(key)
			c.mu.Unlock()

			go c.deleteFilesAsync(key)
			return nil, nil, os.ErrNotExist
		}
		logging.ErrorE("Failed to open cache content file", err, "content_path", filePath, "key", key)
		return nil, nil, fmt.Errorf("failed to open cache content file %s for key %s: %w", filePath, key, err)
	}

	contentInfo, err := contentFile.Stat()
	if err != nil {
		contentFile.Close()
		logging.Warn("Failed to stat content file. Removing entry.", "error", err, "content_path", filePath, "key", key)

		c.mu.Lock()
		c.deleteInternalLocked(key)
		c.mu.Unlock()

		go c.deleteFilesAsync(key)
		return nil, nil, fmt.Errorf("failed to stat content file %s: %w", filePath, err)
	}

	if metadata.Size >= 0 && contentInfo.Size() != metadata.Size {
		contentFile.Close()
		logging.Warn("Cache file size mismatch. Removing corrupted entry.", "key", key, "metadata_size", metadata.Size, "file_size", contentInfo.Size())

		c.mu.Lock()
		c.deleteInternalLocked(key)
		c.mu.Unlock()

		go c.deleteFilesAsync(key)
		return nil, nil, os.ErrNotExist
	} else if metadata.Size < 0 {
		metadata.Size = contentInfo.Size()
		logging.Debug("Updated metadata size based on file size", "key", key, "new_size", metadata.Size)
	}

	logging.Debug("Cache hit", "key", key, "size", metadata.Size)
	metadata.Key = key
	metadata.FilePath = filePath
	metadata.MetaPath = metaPath
	return contentFile, &metadata, nil
}

func (c *DiskLRUCache) Put(ctx context.Context, key string, reader io.Reader, metadata CacheMetadata) error {
	if !c.enabled {
		_, _ = io.Copy(io.Discard, reader)
		logging.Warn("Cache is disabled, item not stored", "key", key)
		return errors.New("cache is disabled, item not stored")
	}
	if err := c.waitInit(); err != nil {
		logging.ErrorE("Cache initialization failed, cannot put item", err, "key", key)
		return fmt.Errorf("cache initialization failed, cannot put item %s: %w", key, err)
	}

	filePath := c.getContentFilePath(key)
	metaPath := c.getMetaFilePath(key)
	dirPath := filepath.Dir(filePath)

	if err := os.MkdirAll(dirPath, 0755); err != nil {
		logging.ErrorE("Failed to create directory for cache item", err, "directory", dirPath, "key", key)
		return fmt.Errorf("failed to create directory %s for cache item %s: %w", dirPath, key, err)
	}

	tempContentFile, err := os.CreateTemp(dirPath, filepath.Base(filePath)+".*.tmp")
	if err != nil {
		logging.ErrorE("Failed to create temporary content file", err, "directory", dirPath, "key", key)
		return fmt.Errorf("failed to create temporary content file in %s for key %s: %w", dirPath, key, err)
	}
	tempContentPath := tempContentFile.Name()
	cleanTempContent := func() {
		_ = tempContentFile.Close()
		if _, statErr := os.Stat(tempContentPath); statErr == nil {
			logging.Debug("Cleaning up temporary content file", "temp_path", tempContentPath)
			if remErr := os.Remove(tempContentPath); remErr != nil && !errors.Is(remErr, os.ErrNotExist) {
				logging.ErrorE("Failed to remove temporary content file", remErr, "temp_path", tempContentPath)
			}
		}
	}
	defer cleanTempContent()

	tempMetaFile, err := os.CreateTemp(dirPath, filepath.Base(metaPath)+".*.tmp")
	if err != nil {
		logging.ErrorE("Failed to create temporary metadata file", err, "directory", dirPath, "key", key)
		return fmt.Errorf("failed to create temporary metadata file in %s for key %s: %w", dirPath, key, err)
	}
	tempMetaPath := tempMetaFile.Name()
	cleanTempMeta := func() {
		_ = tempMetaFile.Close()
		if _, statErr := os.Stat(tempMetaPath); statErr == nil {
			logging.Debug("Cleaning up temporary metadata file", "temp_path", tempMetaPath)
			if remErr := os.Remove(tempMetaPath); remErr != nil && !errors.Is(remErr, os.ErrNotExist) {
				logging.ErrorE("Failed to remove temporary metadata file", remErr, "temp_path", tempMetaPath)
			}
		}
	}
	defer cleanTempMeta()

	writtenSize, err := io.Copy(tempContentFile, reader)
	if err != nil {
		logging.ErrorE("Failed to write to temporary content file", err, "temp_path", tempContentPath, "key", key)
		return fmt.Errorf("failed to write to temporary content file %s for key %s: %w", tempContentPath, key, err)
	}
	if err := tempContentFile.Close(); err != nil {
		logging.ErrorE("Failed to close temporary content file after writing", err, "temp_path", tempContentPath, "key", key)
		return fmt.Errorf("failed to close temporary content file %s after writing for key %s: %w", tempContentPath, key, err)
	}

	metadata.Version = MetadataVersion
	if metadata.FetchTime.IsZero() {
		metadata.FetchTime = time.Now().UTC()
	}
	if metadata.Size < 0 {
		metadata.Size = writtenSize
	} else if writtenSize != metadata.Size {
		logging.Error("Cache write size mismatch", "key", key, "expected_size", metadata.Size, "written_size", writtenSize)
		return fmt.Errorf("cache write size mismatch for %s: expected %d, wrote %d", key, metadata.Size, writtenSize)
	}

	encoder := json.NewEncoder(tempMetaFile)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(&metadata); err != nil {
		logging.ErrorE("Failed to encode metadata", err, "temp_path", tempMetaPath, "key", key)
		return fmt.Errorf("failed to encode metadata to %s for key %s: %w", tempMetaPath, key, err)
	}
	if err := tempMetaFile.Close(); err != nil {
		logging.ErrorE("Failed to close temporary metadata file after writing", err, "temp_path", tempMetaPath, "key", key)
		return fmt.Errorf("failed to close temporary metadata file %s after writing for key %s: %w", tempMetaPath, key, err)
	}

	finalSize := metadata.Size

	c.mu.Lock()
	var oldSize int64 = 0
	if oldEntry, exists := c.items[key]; exists {
		oldSize = oldEntry.size
		sizeDiff := finalSize - oldSize
		c.currentSize -= oldSize
		c.lruList.Remove(oldEntry.element)
		logging.Debug("Updating existing cache entry", "key", key, "old_size", oldSize, "new_size", finalSize, "size_diff", sizeDiff)
		c.evict(sizeDiff)
	} else {
		logging.Debug("Adding new cache entry", "key", key, "size", finalSize)
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
		logging.ErrorE("Failed to rename temp metadata file. Reverting LRU changes.", err, "temp_path", tempMetaPath, "final_path", metaPath, "key", key)
		c.mu.Lock()
		c.currentSize -= finalSize
		if element != nil {
			c.lruList.Remove(element)
		}
		delete(c.items, key)
		if oldSize > 0 {
			logging.Warn("Could not revert LRU state completely after failed metadata rename for updated key.", "key", key)
		}
		c.mu.Unlock()
		return fmt.Errorf("failed to rename temp metadata file for key %s: %w", key, err)
	}
	tempMetaPath = ""

	if err := os.Rename(tempContentPath, filePath); err != nil {
		logging.ErrorE("Failed to rename temp content file. Reverting LRU changes and removing committed meta file.", err, "temp_path", tempContentPath, "final_path", filePath, "key", key)
		c.mu.Lock()
		c.currentSize -= finalSize
		if element != nil {
			c.lruList.Remove(element)
		}
		delete(c.items, key)
		c.mu.Unlock()
		if removeMetaErr := os.Remove(metaPath); removeMetaErr != nil && !os.IsNotExist(removeMetaErr) {
			logging.ErrorE("Failed to remove committed meta file during content rename failure", removeMetaErr, "meta_path", metaPath, "key", key)
		}
		return fmt.Errorf("failed to rename temp content file for key %s: %w", key, err)
	}
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
		logging.Warn("Current cache size became negative after deleting key. Resetting to zero.", "key", key, "deleted_size", entry.size, "resulting_size", c.currentSize)
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

		logFields := map[string]interface{}{
			"key":          k,
			"content_path": cPath,
			"meta_path":    mPath,
		}

		if contentErr != nil && !errors.Is(contentErr, os.ErrNotExist) {
			logFields["content_error"] = contentErr.Error()
			logging.Error("Cache delete files: content deletion failed", logFields)
		} else {
			delete(logFields, "content_error")
		}

		if metaErr != nil && !errors.Is(metaErr, os.ErrNotExist) {
			logFields["meta_error"] = metaErr.Error()
			logging.Error("Cache delete files: metadata deletion failed", logFields)
		} else {
			delete(logFields, "meta_error")
		}

		if (contentErr == nil || errors.Is(contentErr, os.ErrNotExist)) && (metaErr == nil || errors.Is(metaErr, os.ErrNotExist)) {
			msg := "Cache delete files: successful or files already gone."
			if contentErr != nil && metaErr != nil {
				msg = "Cache delete files: files already did not exist."
			}
			logging.Debug(msg, logFields)
		}
	}(contentPath, metaPath, key)
}

func (c *DiskLRUCache) Delete(ctx context.Context, key string) error {
	if !c.enabled {
		logging.Warn("Cache is disabled, cannot delete", "key", key)
		return errors.New("cache is disabled, cannot delete")
	}

	c.mu.Lock()
	size, exists := c.deleteInternalLocked(key)
	c.mu.Unlock()

	if !exists {
		logging.Debug("Cache delete: key not found in memory map, nothing to delete.", "key", key)
		return nil
	}

	c.deleteFilesAsync(key)

	logging.Debug("Cache delete initiated", "key", key, "freed_memory_bytes", size)
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

	logging.Debug("Eviction triggered",
		"current_size", util.FormatSize(c.currentSize),
		"required_space", requiredSpace,
		"max_size", util.FormatSize(c.maxSizeBytes),
		"need_to_free", util.FormatSize(spaceToFree))

	var freedSpace int64
	itemsEvicted := 0
	for freedSpace < spaceToFree {
		element := c.lruList.Back()
		if element == nil {
			logging.Error("Eviction stopped: LRU list empty but need to free more bytes. Cache state might be inconsistent.",
				"need_to_free_more", util.FormatSize(spaceToFree-freedSpace),
				"current_size", util.FormatSize(c.currentSize))
			break
		}

		entry := element.Value.(*cacheEntry)
		freedSize, existed := c.deleteInternalLocked(entry.key)

		if existed {
			freedSpace += freedSize
			itemsEvicted++
			logging.Debug("Evicting LRU item", "key", entry.key, "size", freedSize, "freed_total", util.FormatSize(freedSpace), "needed", util.FormatSize(spaceToFree))
			c.deleteFilesAsync(entry.key)
		} else {

			c.lruList.Remove(element)
			logging.Error("Eviction inconsistency: Element found in LRU list but not in map.", "key", entry.key)
		}
	}

	if c.currentSize < 0 {
		logging.Warn("Current cache size became negative after eviction. Resetting to zero.", "final_size", c.currentSize)
		c.currentSize = 0
	}

	logging.Debug("Eviction finished",
		"items_evicted", itemsEvicted,
		"freed_space", util.FormatSize(freedSpace),
		"new_current_size", util.FormatSize(c.currentSize))
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

			c.valLruList.Remove(element)
			delete(c.validations, key)
			ok = false
			logging.Debug("Validation cache entry expired between RLock and Lock.", "key", key)
		}
	} else {

		ok = false
	}
	c.valMu.Unlock()

	if ok {
		logging.Debug("Validation cache hit", "key", key, "validated_at", validationTime.Format(time.RFC3339))
	} else {
		logging.Debug("Validation cache miss or expired", "key", key)
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
		logging.Debug("Validation cache updated", "key", key)
	} else {
		entry := &validationEntry{key: key, validated: validationTime}
		element := c.valLruList.PushFront(entry)
		c.validations[key] = element
		logging.Debug("Validation cache put", "key", key)
	}

}

func (c *DiskLRUCache) deleteValidation(key string) {
	c.valMu.Lock()
	defer c.valMu.Unlock()
	if element, exists := c.validations[key]; exists {
		c.valLruList.Remove(element)
		delete(c.validations, key)
		logging.Debug("Validation cache deleted entry", "key", key)
	}
}
