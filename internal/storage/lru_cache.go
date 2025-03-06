package storage

import (
	"container/list"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/utils"
)

type FileOperations struct {
	basePath string
}

type FileType int

const (
	RegularFile FileType = iota
	CacheFile
)

func NewFileOperations(basePath string) *FileOperations {
	return &FileOperations{
		basePath: basePath,
	}
}

func (f *FileOperations) EnsureDirectoryExists(relativePath string) error {
	dirPath := filepath.Join(f.basePath, relativePath)
	return utils.CreateDirectory(dirPath)
}

func (f *FileOperations) getFilePath(key string, fileType FileType) string {
	safePath := utils.SafeFilename(key)

	if fileType == CacheFile {
		safePath += ".filecache"
	}

	fullPath := filepath.Join(f.basePath, safePath)

	// Ensure directory exists
	dir := filepath.Dir(fullPath)
	if err := utils.CreateDirectory(dir); err != nil {
		logging.Error("Failed to create directory for cache file: %v", err)
	}

	return fullPath
}

func (f *FileOperations) GetFilePath(key string) string {
	return f.getFilePath(key, RegularFile)
}

func (f *FileOperations) GetCacheFilePath(key string) string {
	return f.getFilePath(key, CacheFile)
}

func (f *FileOperations) ReadFile(key string) ([]byte, error) {
	filePath := f.GetFilePath(key)
	return os.ReadFile(filePath)
}

func (f *FileOperations) ReadCacheFile(key string) ([]byte, error) {
	filePath := f.GetCacheFilePath(key)
	return os.ReadFile(filePath)
}

func (f *FileOperations) writeFileWithTemp(filePath string, data []byte) error {
	dirPath := filepath.Dir(filePath)
	if err := utils.CreateDirectory(dirPath); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	tempFilePath := filePath + ".tmp"
	if err := os.WriteFile(tempFilePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temporary file: %w", err)
	}

	if err := os.Rename(tempFilePath, filePath); err != nil {
		return fmt.Errorf("failed to rename temporary file: %w", err)
	}

	return nil
}

func (f *FileOperations) WriteFile(key string, data []byte) error {
	filePath := f.GetFilePath(key)
	return f.writeFileWithTemp(filePath, data)
}

func (f *FileOperations) WriteCacheFile(key string, data []byte) error {
	filePath := f.GetCacheFilePath(key)
	return f.writeFileWithTemp(filePath, data)
}

func (f *FileOperations) FileExists(key string) bool {
	filePath := f.GetFilePath(key)
	_, err := os.Stat(filePath)
	return err == nil
}

func (f *FileOperations) CacheFileExists(key string) bool {
	filePath := f.GetCacheFilePath(key)
	_, err := os.Stat(filePath)
	return err == nil
}

func (f *FileOperations) DeleteFile(key string) error {
	filePath := f.GetFilePath(key)
	return os.Remove(filePath)
}

func (f *FileOperations) DeleteCacheFile(key string) error {
	filePath := f.GetCacheFilePath(key)
	return os.Remove(filePath)
}

type LRUCacheOptions struct {
	BasePath     string
	MaxSizeBytes int64
	CleanOnStart bool
}

type LRUCache struct {
	basePath     string
	maxSizeBytes int64
	currentSize  int64
	items        map[string]*list.Element
	lruList      *list.List
	mutex        sync.RWMutex
	fileOps      *FileOperations
}

type cacheItem struct {
	key          string
	size         int64
	lastModified time.Time
}

func NewLRUCache(basePath string, maxSizeBytes int64) (*LRUCache, error) {
	return NewLRUCacheWithOptions(LRUCacheOptions{
		BasePath:     basePath,
		MaxSizeBytes: maxSizeBytes,
		CleanOnStart: false,
	})
}

func NewLRUCacheWithOptions(options LRUCacheOptions) (*LRUCache, error) {
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

	if options.CleanOnStart {
		if err := cache.Clean(); err != nil {
			return nil, fmt.Errorf("failed to clean cache: %w", err)
		}
	}

	if err := cache.initialize(); err != nil {
		return nil, fmt.Errorf("failed to initialize cache: %w", err)
	}

	return cache, nil
}

func (c *LRUCache) Clean() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.items = make(map[string]*list.Element)
	c.lruList = list.New()
	c.currentSize = 0

	entries, err := os.ReadDir(c.basePath)
	if err != nil {
		return fmt.Errorf("failed to read cache directory: %w", err)
	}

	for _, entry := range entries {
		entryPath := filepath.Join(c.basePath, entry.Name())
		if entry.IsDir() {
			if err := cleanDirectory(entryPath); err != nil {
				logging.Warning("failed to clean directory %s: %v", entryPath, err)
			}
		} else {
			if err := os.Remove(entryPath); err != nil {
				logging.Warning("failed to remove file %s: %v", entryPath, err)
			}
		}
	}

	return nil
}

func cleanDirectory(dirPath string) error {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	for _, entry := range entries {
		entryPath := filepath.Join(dirPath, entry.Name())
		if entry.IsDir() {
			if err := cleanDirectory(entryPath); err != nil {
				logging.Warning("failed to clean subdirectory %s: %v", entryPath, err)
			}
			if err := os.Remove(entryPath); err != nil {
				logging.Warning("failed to remove directory %s: %v", entryPath, err)
			}
		} else {
			if err := os.Remove(entryPath); err != nil {
				logging.Warning("failed to remove file %s: %v", entryPath, err)
			}
		}
	}

	return nil
}

func (c *LRUCache) initialize() error {
	return filepath.Walk(c.basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if strings.HasSuffix(path, ".headercache") {
			return nil
		}

		if !strings.HasSuffix(path, ".filecache") {
			return nil
		}

		if strings.HasSuffix(path, ".tmp") {
			if err := os.Remove(path); err != nil {
				logging.Warning("failed to remove temporary file %s: %v", path, err)
			}
			return nil
		}

		relPath, err := filepath.Rel(c.basePath, path)
		if err != nil {
			return err
		}

		key := filepath.ToSlash(relPath)

		key = strings.TrimSuffix(key, ".filecache")

		if strings.Contains(key, "/") {
			key = "/" + key
		}

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

func (c *LRUCache) Get(key string) (io.ReadCloser, int64, time.Time, error) {
	c.mutex.RLock()
	element, exists := c.items[key]
	c.mutex.RUnlock()

	logging.Debug("LRUCache: Get key=%s (exists=%v)", key, exists)

	if !exists {
		return nil, 0, time.Time{}, fmt.Errorf("item not found in cache: %s", key)
	}

	c.mutex.Lock()
	c.lruList.MoveToFront(element)
	item := element.Value.(*cacheItem)
	logging.Debug("LRUCache: Item last modified=%v", item.lastModified)
	c.mutex.Unlock()

	filePath := c.fileOps.GetCacheFilePath(key)
	logging.Debug("LRUCache: File path=%s", filePath)

	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			c.mutex.Lock()
			c.lruList.Remove(element)
			delete(c.items, key)
			c.currentSize -= item.size
			c.mutex.Unlock()
		}
		logging.Error("LRUCache: Failed to open file - %v", err)
		return nil, 0, time.Time{}, fmt.Errorf("failed to open file: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		c.mutex.Lock()
		c.lruList.Remove(element)
		delete(c.items, item.key)
		c.currentSize -= item.size
		c.mutex.Unlock()
		logging.Error("LRUCache: Failed to get file info - %v", err)
		return nil, 0, time.Time{}, fmt.Errorf("failed to get file info: %w", err)
	}
	logging.Debug("LRUCache: File size=%d bytes", info.Size())
	if info.Size() == 0 {
		file.Close()
		c.mutex.Lock()
		c.lruList.Remove(element)
		delete(c.items, key)
		c.currentSize -= item.size
		c.mutex.Unlock()
		os.Remove(filePath)
		return nil, 0, time.Time{}, fmt.Errorf("corrupted file in cache (zero size): %s", key)
	}

	if info.Size() != item.size {
		if float64(info.Size())/float64(item.size) < 0.9 || float64(info.Size())/float64(item.size) > 1.1 {
			file.Close()
			c.mutex.Lock()
			c.lruList.Remove(element)
			delete(c.items, key)
			c.currentSize -= item.size
			c.mutex.Unlock()
			os.Remove(filePath)
			return nil, 0, time.Time{}, fmt.Errorf("corrupted file in cache (size mismatch): expected %d bytes, got %d bytes", item.size, info.Size())
		}

		c.mutex.Lock()
		c.currentSize = c.currentSize - item.size + info.Size()
		item.size = info.Size()
		c.mutex.Unlock()
	}

	return file, info.Size(), info.ModTime(), nil
}

func (c *LRUCache) Put(key string, content io.Reader, contentLength int64, lastModified time.Time) error {
	c.makeRoom(contentLength)

	filePath := c.fileOps.GetCacheFilePath(key)

	dirPath := filepath.Dir(filePath)
	if err := utils.CreateDirectory(dirPath); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	tempFilePath := filePath + ".tmp"
	file, err := os.Create(tempFilePath)
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}

	written, err := io.Copy(file, content)
	if err != nil {
		file.Close()
		os.Remove(tempFilePath)
		return fmt.Errorf("failed to write file: %w", err)
	}

	if err := file.Close(); err != nil {
		os.Remove(tempFilePath)
		return fmt.Errorf("failed to close file: %w", err)
	}

	if contentLength > 0 && written != contentLength {
		os.Remove(tempFilePath)
		return fmt.Errorf("file size validation failed: expected %d bytes, got %d bytes", contentLength, written)
	}

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

	if err := os.Chtimes(tempFilePath, lastModified, lastModified); err != nil {
		logging.Warning("failed to set file modification time: %v", err)
	}

	if err := os.Rename(tempFilePath, filePath); err != nil {
		os.Remove(tempFilePath)
		return fmt.Errorf("failed to rename temporary file: %w", err)
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if element, exists := c.items[key]; exists {
		item := element.Value.(*cacheItem)
		c.currentSize -= item.size
		item.size = written
		item.lastModified = lastModified
		c.lruList.MoveToFront(element)
	} else {
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

func (c *LRUCache) makeRoom(size int64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	logging.Debug("Cache: Making room for %d bytes", size)
	logging.Debug("Cache: Current size=%d bytes, Max size=%d bytes", c.currentSize, c.maxSizeBytes)

	if c.lruList.Len() == 0 || size <= 0 {
		return
	}

	if c.maxSizeBytes <= 0 {
		return
	}

	if c.currentSize+size <= c.maxSizeBytes {
		logging.Debug("Cache: No need to free space")
		return
	}

	spaceToFree := (c.currentSize + size) - c.maxSizeBytes
	spaceToFree += spaceToFree / 10
	logging.Debug("Cache: Need to free %d bytes", spaceToFree)

	freedSpace := int64(0)

	for c.lruList.Len() > 0 && freedSpace < spaceToFree {
		element := c.lruList.Back()
		if element == nil {
			break
		}

		item := element.Value.(*cacheItem)
		logging.Debug("Cache: Evicting item=%s (size=%d bytes)", item.key, item.size)

		c.lruList.Remove(element)
		delete(c.items, item.key)

		c.currentSize -= item.size
		freedSpace += item.size

		if err := c.fileOps.DeleteCacheFile(item.key); err != nil && !os.IsNotExist(err) {
			logging.Warning("failed to remove file %s: %v", item.key, err)
		}
	}
	logging.Debug("Cache: Total freed space=%d bytes", freedSpace)
}

func (c *LRUCache) GetCacheStats() (int, int64, int64) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.lruList.Len(), c.currentSize, c.maxSizeBytes
}

type FileHeaderCache struct {
	basePath string
	fileOps  *FileOperations
	mutex    sync.RWMutex
}

func NewFileHeaderCache(basePath string) (*FileHeaderCache, error) {
	return &FileHeaderCache{
		basePath: basePath,
		fileOps:  NewFileOperations(basePath),
	}, nil
}

func (c *FileHeaderCache) GetHeaders(key string) (http.Header, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	filePath := c.fileOps.GetFilePath(key + ".headercache")

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("header cache not found: %w", err)
	}

	var headers http.Header
	if err := json.Unmarshal(data, &headers); err != nil {
		return nil, fmt.Errorf("failed to parse header cache: %w", err)
	}
	return headers, nil
}

func (c *FileHeaderCache) PutHeaders(key string, headers http.Header) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	data, err := json.Marshal(headers)
	if err != nil {
		return fmt.Errorf("failed to marshal headers: %w", err)
	}

	filePath := c.fileOps.GetFilePath(key + ".headercache")

	dirPath := filepath.Dir(filePath)
	if err := utils.CreateDirectory(dirPath); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	tempFilePath := filePath + ".tmp"
	if err := os.WriteFile(tempFilePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temporary file: %w", err)
	}

	if err := os.Rename(tempFilePath, filePath); err != nil {
		os.Remove(tempFilePath)
		return fmt.Errorf("failed to rename temporary file: %w", err)
	}

	return nil
}

func CleanCacheDirectory(dirPath string) error {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	for _, entry := range entries {
		entryPath := filepath.Join(dirPath, entry.Name())
		if entry.IsDir() {
			if err := CleanCacheDirectory(entryPath); err != nil {
				logging.Warning("failed to clean subdirectory %s: %v", entryPath, err)
			}
		} else if strings.HasSuffix(entry.Name(), ".filecache") || strings.HasSuffix(entry.Name(), ".headercache") {
			if err := os.Remove(entryPath); err != nil {
				logging.Warning("failed to remove file %s: %v", entryPath, err)
			}
		}
	}
	return nil
}
