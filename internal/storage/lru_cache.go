package storage

import (
	"container/list"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/yolkispalkis/go-apt-cache/internal/utils"
)

// FileOperations contains common file operations
type FileOperations struct {
	basePath string
}

// FileType represents the type of file to operate on
type FileType int

const (
	RegularFile FileType = iota
	CacheFile
)

// NewFileOperations creates a new FileOperations instance
func NewFileOperations(basePath string) *FileOperations {
	return &FileOperations{
		basePath: basePath,
	}
}

// EnsureDirectoryExists ensures a directory exists
func (f *FileOperations) EnsureDirectoryExists(relativePath string) error {
	dirPath := filepath.Join(f.basePath, relativePath)
	return utils.CreateDirectory(dirPath)
}

// getFilePath returns the full path for a file based on the file type
func (f *FileOperations) getFilePath(key string, fileType FileType) string {
	// Convert key to safe filename
	safePath := safeFilename(key)

	// Add extension for cache files
	if fileType == CacheFile {
		safePath += ".filecache"
	}

	return filepath.Join(f.basePath, safePath)
}

// GetFilePath returns the full path for a regular file
func (f *FileOperations) GetFilePath(key string) string {
	return f.getFilePath(key, RegularFile)
}

// GetCacheFilePath returns the full path for a cache file with the .filecache extension
func (f *FileOperations) GetCacheFilePath(key string) string {
	return f.getFilePath(key, CacheFile)
}

// ReadFile reads a regular file and returns its contents
func (f *FileOperations) ReadFile(key string) ([]byte, error) {
	filePath := f.GetFilePath(key)
	return os.ReadFile(filePath)
}

// ReadCacheFile reads a cache file and returns its contents
func (f *FileOperations) ReadCacheFile(key string) ([]byte, error) {
	filePath := f.GetCacheFilePath(key)
	return os.ReadFile(filePath)
}

// writeFileWithTemp is a helper function to write data to a file with atomic operations
func (f *FileOperations) writeFileWithTemp(filePath string, data []byte) error {
	// Ensure directory exists
	dirPath := filepath.Dir(filePath)
	if err := utils.CreateDirectory(dirPath); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Create a temporary file first to ensure atomic writes
	tempFilePath := filePath + ".tmp"
	if err := os.WriteFile(tempFilePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temporary file: %w", err)
	}

	// Rename the temporary file to the target file (atomic operation)
	if err := os.Rename(tempFilePath, filePath); err != nil {
		return fmt.Errorf("failed to rename temporary file: %w", err)
	}

	return nil
}

// WriteFile writes data to a regular file
func (f *FileOperations) WriteFile(key string, data []byte) error {
	filePath := f.GetFilePath(key)
	return f.writeFileWithTemp(filePath, data)
}

// WriteCacheFile writes data to a cache file
func (f *FileOperations) WriteCacheFile(key string, data []byte) error {
	filePath := f.GetCacheFilePath(key)
	return f.writeFileWithTemp(filePath, data)
}

// FileExists checks if a regular file exists
func (f *FileOperations) FileExists(key string) bool {
	filePath := f.GetFilePath(key)
	_, err := os.Stat(filePath)
	return err == nil
}

// CacheFileExists checks if a cache file exists
func (f *FileOperations) CacheFileExists(key string) bool {
	filePath := f.GetCacheFilePath(key)
	_, err := os.Stat(filePath)
	return err == nil
}

// DeleteFile deletes a regular file
func (f *FileOperations) DeleteFile(key string) error {
	filePath := f.GetFilePath(key)
	return os.Remove(filePath)
}

// DeleteCacheFile deletes a cache file
func (f *FileOperations) DeleteCacheFile(key string) error {
	filePath := f.GetCacheFilePath(key)
	return os.Remove(filePath)
}

// LRUCacheOptions contains options for creating a new LRU cache
type LRUCacheOptions struct {
	BasePath     string // Base directory for the cache
	MaxSizeBytes int64  // Maximum size of the cache in bytes
	CleanOnStart bool   // Whether to clean the cache on startup
}

// LRUCache implements a Least Recently Used cache
type LRUCache struct {
	basePath     string
	maxSizeBytes int64
	currentSize  int64
	items        map[string]*list.Element
	lruList      *list.List
	mutex        sync.RWMutex
	fileOps      *FileOperations
}

// cacheItem represents an item in the LRU cache
type cacheItem struct {
	key          string
	size         int64
	lastModified time.Time
}

// NewLRUCache creates a new LRU cache
func NewLRUCache(basePath string, maxSizeBytes int64) (*LRUCache, error) {
	return NewLRUCacheWithOptions(LRUCacheOptions{
		BasePath:     basePath,
		MaxSizeBytes: maxSizeBytes,
		CleanOnStart: false,
	})
}

// NewLRUCacheWithOptions creates a new LRU cache with the given options
func NewLRUCacheWithOptions(options LRUCacheOptions) (*LRUCache, error) {
	// Create base directory if it doesn't exist
	if err := utils.CreateDirectory(options.BasePath); err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %w", err)
	}

	fileOps := NewFileOperations(options.BasePath)

	cache := &LRUCache{
		basePath:     options.BasePath,
		maxSizeBytes: options.MaxSizeBytes,
		items:        make(map[string]*list.Element),
		lruList:      list.New(),
		fileOps:      fileOps,
	}

	// Clean cache if requested
	if options.CleanOnStart {
		if err := cache.Clean(); err != nil {
			return nil, fmt.Errorf("failed to clean cache: %w", err)
		}
	}

	// Initialize cache size and items from existing files
	if err := cache.initialize(); err != nil {
		return nil, fmt.Errorf("failed to initialize cache: %w", err)
	}

	return cache, nil
}

// Clean removes all files from the cache
func (c *LRUCache) Clean() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Clear in-memory cache
	c.items = make(map[string]*list.Element)
	c.lruList = list.New()
	c.currentSize = 0

	// Remove all files from cache directory
	entries, err := os.ReadDir(c.basePath)
	if err != nil {
		return fmt.Errorf("failed to read cache directory: %w", err)
	}

	for _, entry := range entries {
		entryPath := filepath.Join(c.basePath, entry.Name())
		if entry.IsDir() {
			// Remove all files in the directory
			if err := cleanDirectory(entryPath); err != nil {
				log.Printf("Warning: failed to clean directory %s: %v", entryPath, err)
			}
		} else {
			// Remove the file
			if err := os.Remove(entryPath); err != nil {
				log.Printf("Warning: failed to remove file %s: %v", entryPath, err)
			}
		}
	}

	return nil
}

// cleanDirectory removes all files in a directory recursively
func cleanDirectory(dirPath string) error {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	for _, entry := range entries {
		entryPath := filepath.Join(dirPath, entry.Name())
		if entry.IsDir() {
			// Clean subdirectory first
			if err := cleanDirectory(entryPath); err != nil {
				log.Printf("Warning: failed to clean subdirectory %s: %v", entryPath, err)
			}
			// Then remove the directory itself
			if err := os.Remove(entryPath); err != nil {
				log.Printf("Warning: failed to remove directory %s: %v", entryPath, err)
			}
		} else {
			// Remove the file
			if err := os.Remove(entryPath); err != nil {
				log.Printf("Warning: failed to remove file %s: %v", entryPath, err)
			}
		}
	}

	return nil
}

// initialize scans the cache directory and builds the initial cache state
func (c *LRUCache) initialize() error {
	// Walk the cache directory
	return filepath.Walk(c.basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Skip header cache files
		if strings.HasSuffix(path, ".headercache") {
			return nil
		}

		// Skip files that don't have .filecache extension
		if !strings.HasSuffix(path, ".filecache") {
			return nil
		}

		// Skip temporary files
		if strings.HasSuffix(path, ".tmp") {
			// Remove any leftover temporary files
			if err := os.Remove(path); err != nil {
				log.Printf("Warning: failed to remove temporary file %s: %v", path, err)
			}
			return nil
		}

		// Get relative path as key
		relPath, err := filepath.Rel(c.basePath, path)
		if err != nil {
			return err
		}

		// Convert path separators to forward slashes for consistency
		key := filepath.ToSlash(relPath)

		// Remove .filecache extension to get the original key
		key = strings.TrimSuffix(key, ".filecache")

		// Add leading slash if the key contains directory structure
		// This is important for repository paths which are absolute
		if strings.Contains(key, "/") {
			key = "/" + key
		}

		// Add to cache
		item := &cacheItem{
			key:          key,
			size:         info.Size(),
			lastModified: info.ModTime(),
		}
		element := c.lruList.PushFront(item)
		c.items[key] = element
		c.currentSize += info.Size()

		return nil
	})
}

// Get retrieves a file from the cache
func (c *LRUCache) Get(key string) (io.ReadCloser, int64, time.Time, error) {
	c.mutex.RLock()
	element, exists := c.items[key]
	c.mutex.RUnlock()

	if !exists {
		return nil, 0, time.Time{}, fmt.Errorf("item not found in cache: %s", key)
	}

	// Move to front of LRU list
	c.mutex.Lock()
	c.lruList.MoveToFront(element)
	item := element.Value.(*cacheItem)
	c.mutex.Unlock()

	// Get file path
	filePath := c.fileOps.GetCacheFilePath(key)

	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		// Remove from cache if file doesn't exist
		if os.IsNotExist(err) {
			c.mutex.Lock()
			c.lruList.Remove(element)
			delete(c.items, key)
			c.currentSize -= item.size
			c.mutex.Unlock()
		}
		return nil, 0, time.Time{}, fmt.Errorf("failed to open file: %w", err)
	}

	// Get file info
	info, err := file.Stat()
	if err != nil {
		file.Close()
		c.mutex.Lock()
		c.lruList.Remove(element)
		delete(c.items, key)
		c.currentSize -= item.size
		c.mutex.Unlock()
		return nil, 0, time.Time{}, fmt.Errorf("failed to get file info: %w", err)
	}

	// Enhanced validation: Check if file is empty or has unexpected size
	if info.Size() == 0 {
		file.Close()
		// Remove corrupted file from cache
		c.mutex.Lock()
		c.lruList.Remove(element)
		delete(c.items, key)
		c.currentSize -= item.size
		c.mutex.Unlock()
		// Also remove the physical file
		os.Remove(filePath)
		return nil, 0, time.Time{}, fmt.Errorf("corrupted file in cache (zero size): %s", key)
	}

	// Update item size if it has changed
	if info.Size() != item.size {
		// If the size difference is significant, consider the file corrupted
		if float64(info.Size())/float64(item.size) < 0.9 || float64(info.Size())/float64(item.size) > 1.1 {
			file.Close()
			// Remove corrupted file from cache
			c.mutex.Lock()
			c.lruList.Remove(element)
			delete(c.items, key)
			c.currentSize -= item.size
			c.mutex.Unlock()
			// Also remove the physical file
			os.Remove(filePath)
			return nil, 0, time.Time{}, fmt.Errorf("corrupted file in cache (size mismatch): expected %d bytes, got %d bytes", item.size, info.Size())
		}

		// Update size if within acceptable range
		c.mutex.Lock()
		c.currentSize = c.currentSize - item.size + info.Size()
		item.size = info.Size()
		c.mutex.Unlock()
	}

	return file, info.Size(), info.ModTime(), nil
}

// Put stores a file in the cache
func (c *LRUCache) Put(key string, content io.Reader, contentLength int64, lastModified time.Time) error {
	// Make room for new item if needed
	c.makeRoom(contentLength)

	// Get file path
	filePath := c.fileOps.GetCacheFilePath(key)

	// Create directory if it doesn't exist
	dirPath := filepath.Dir(filePath)
	if err := utils.CreateDirectory(dirPath); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Create a temporary file first
	tempFilePath := filePath + ".tmp"
	file, err := os.Create(tempFilePath)
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}

	// Copy content to file
	written, err := io.Copy(file, content)
	if err != nil {
		// Close and remove file if copy failed
		file.Close()
		os.Remove(tempFilePath)
		return fmt.Errorf("failed to write file: %w", err)
	}

	// Close the file
	if err := file.Close(); err != nil {
		os.Remove(tempFilePath)
		return fmt.Errorf("failed to close file: %w", err)
	}

	// Enhanced validation:
	// 1. Check file size if contentLength is provided
	if contentLength > 0 && written != contentLength {
		// Remove file if size validation failed
		os.Remove(tempFilePath)
		return fmt.Errorf("file size validation failed: expected %d bytes, got %d bytes", contentLength, written)
	}

	// 2. Additional validation - check if file is readable and has expected size
	validateFile, err := os.Open(tempFilePath)
	if err != nil {
		os.Remove(tempFilePath)
		return fmt.Errorf("file validation failed - cannot open file: %w", err)
	}

	fileInfo, err := validateFile.Stat()
	validateFile.Close()
	if err != nil {
		os.Remove(tempFilePath)
		return fmt.Errorf("file validation failed - cannot stat file: %w", err)
	}

	if fileInfo.Size() != written {
		os.Remove(tempFilePath)
		return fmt.Errorf("file validation failed - file size mismatch: expected %d bytes, got %d bytes", written, fileInfo.Size())
	}

	// Set file modification time
	if err := os.Chtimes(tempFilePath, lastModified, lastModified); err != nil {
		log.Printf("Warning: failed to set file modification time: %v", err)
	}

	// Rename the temporary file to the target file (atomic operation)
	if err := os.Rename(tempFilePath, filePath); err != nil {
		// If rename fails, try to remove the temporary file
		os.Remove(tempFilePath)
		return fmt.Errorf("failed to rename temporary file: %w", err)
	}

	// Update cache
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Check if item already exists
	if element, exists := c.items[key]; exists {
		// Update existing item
		item := element.Value.(*cacheItem)
		c.currentSize -= item.size
		item.size = written
		item.lastModified = lastModified
		c.lruList.MoveToFront(element)
	} else {
		// Add new item
		item := &cacheItem{
			key:          key,
			size:         written,
			lastModified: lastModified,
		}
		element := c.lruList.PushFront(item)
		c.items[key] = element
	}

	c.currentSize += written

	return nil
}

// makeRoom removes least recently used items to make room for a new item
func (c *LRUCache) makeRoom(size int64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// If cache is empty or size is 0, nothing to do
	if c.lruList.Len() == 0 || size <= 0 {
		return
	}

	// If max size is 0, no limit
	if c.maxSizeBytes <= 0 {
		return
	}

	// If there's enough room, nothing to do
	if c.currentSize+size <= c.maxSizeBytes {
		return
	}

	// Calculate how much space we need to free
	spaceToFree := (c.currentSize + size) - c.maxSizeBytes

	// Add 10% buffer to avoid frequent evictions
	spaceToFree += spaceToFree / 10

	// Keep track of freed space
	freedSpace := int64(0)

	// Remove items until there's enough room
	for c.lruList.Len() > 0 && freedSpace < spaceToFree {
		// Get least recently used item
		element := c.lruList.Back()
		if element == nil {
			break
		}

		item := element.Value.(*cacheItem)

		// Remove from list and map
		c.lruList.Remove(element)
		delete(c.items, item.key)

		// Update current size and freed space
		c.currentSize -= item.size
		freedSpace += item.size

		// Remove file
		if err := c.fileOps.DeleteCacheFile(item.key); err != nil && !os.IsNotExist(err) {
			log.Printf("Warning: failed to remove file %s: %v", item.key, err)
		}
	}
}

// GetCacheStats returns statistics about the cache
func (c *LRUCache) GetCacheStats() (int, int64, int64) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.lruList.Len(), c.currentSize, c.maxSizeBytes
}

// FileHeaderCache implements a header cache using files
type FileHeaderCache struct {
	basePath string
	fileOps  *FileOperations
	mutex    sync.RWMutex
}

// NewFileHeaderCache creates a new file-based header cache
func NewFileHeaderCache(basePath string) (*FileHeaderCache, error) {
	return &FileHeaderCache{
		basePath: basePath,
		fileOps:  NewFileOperations(basePath),
	}, nil
}

// GetHeaders retrieves headers for a key
func (c *FileHeaderCache) GetHeaders(key string) (http.Header, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	// Get the file path using the same pattern as file cache
	filePath := filepath.Join(c.basePath, safeFilename(key)+".headercache")

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("header cache not found: %w", err)
	}

	// Parse JSON
	var headers http.Header
	if err := json.Unmarshal(data, &headers); err != nil {
		return nil, fmt.Errorf("failed to parse header cache: %w", err)
	}
	return headers, nil
}

// PutHeaders stores headers for a key
func (c *FileHeaderCache) PutHeaders(key string, headers http.Header) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Convert headers to JSON
	data, err := json.Marshal(headers)
	if err != nil {
		return fmt.Errorf("failed to marshal headers: %w", err)
	}

	// Get the file path using the same pattern as file cache
	filePath := filepath.Join(c.basePath, safeFilename(key)+".headercache")

	// Ensure directory exists
	dirPath := filepath.Dir(filePath)
	if err := utils.CreateDirectory(dirPath); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Create a temporary file first to ensure atomic writes
	tempFilePath := filePath + ".tmp"
	if err := os.WriteFile(tempFilePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temporary file: %w", err)
	}

	// Rename the temporary file to the target file (atomic operation)
	if err := os.Rename(tempFilePath, filePath); err != nil {
		// If rename fails, try to remove the temporary file
		os.Remove(tempFilePath)
		return fmt.Errorf("failed to rename temporary file: %w", err)
	}

	return nil
}

// safeFilename converts a key to a safe filename while preserving directory structure
func safeFilename(key string) string {
	// For long keys, use MD5 hash
	if len(key) > 255 {
		hash := md5.Sum([]byte(key))
		return fmt.Sprintf("%x", hash)
	}

	// Normalize path separators to forward slashes
	key = filepath.ToSlash(key)

	// Special case for root path "/"
	if key == "/" {
		return "root"
	}

	// Remove leading slash if present
	key = strings.TrimPrefix(key, "/")

	// Split path into components
	components := strings.Split(key, "/")

	// Process each component to make it safe
	for i, component := range components {
		// Replace unsafe characters in each component
		safe := strings.ReplaceAll(component, ":", "_")
		safe = strings.ReplaceAll(safe, "?", "_")
		safe = strings.ReplaceAll(safe, "*", "_")
		safe = strings.ReplaceAll(safe, "\"", "_")
		safe = strings.ReplaceAll(safe, "<", "_")
		safe = strings.ReplaceAll(safe, ">", "_")
		safe = strings.ReplaceAll(safe, "|", "_")
		safe = strings.ReplaceAll(safe, "\\", "_")

		components[i] = safe
	}

	// Rejoin with proper path separators
	return filepath.Join(components...)
}
