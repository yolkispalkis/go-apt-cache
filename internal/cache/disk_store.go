package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

// fileInfo хранит информацию о файле на диске для LRU.
type fileInfo struct {
	key      string
	path     string
	size     int64
	lastUsed time.Time
}

// diskStore управляет хранением файлов на диске с LRU-вытеснением.
type diskStore struct {
	baseDir  string
	maxBytes int64
	log      *logging.Logger

	mu           sync.Mutex
	items        map[string]*fileInfo
	currentBytes atomic.Int64
}

func newDiskStore(baseDir string, maxSizeStr string, logger *logging.Logger) (*diskStore, error) {
	maxBytes, err := util.ParseSize(maxSizeStr)
	if err != nil {
		return nil, fmt.Errorf("invalid cache max size for disk store: %w", err)
	}

	if err := os.MkdirAll(baseDir, 0750); err != nil {
		return nil, fmt.Errorf("failed to create cache base dir %s: %w", baseDir, err)
	}

	ds := &diskStore{
		baseDir:  baseDir,
		maxBytes: maxBytes,
		log:      logger.WithComponent("diskStore"),
		items:    make(map[string]*fileInfo),
	}

	go ds.initialScan()

	return ds, nil
}

// initialScan сканирует директорию при старте для заполнения LRU-информации.
func (ds *diskStore) initialScan() {
	ds.log.Info().Msg("Starting initial disk scan for LRU data...")
	startTime := time.Now()
	var totalSize int64

	filepath.Walk(ds.baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		// Мы не знаем исходный ключ, поэтому используем путь как ключ для внутреннего отслеживания.
		// Этого достаточно для LRU.
		key := filepath.Base(path)
		ds.mu.Lock()
		ds.items[key] = &fileInfo{
			key:      key,
			path:     path,
			size:     info.Size(),
			lastUsed: util.GetAtime(info),
		}
		ds.mu.Unlock()
		totalSize += info.Size()
		return nil
	})

	ds.currentBytes.Store(totalSize)
	ds.log.Info().
		Int("items_found", len(ds.items)).
		Str("total_size", util.FormatSize(totalSize)).
		Dur("duration", time.Since(startTime)).
		Msg("Initial disk scan complete.")
}

// keyToPath преобразует ключ в безопасный путь на диске.
func (ds *diskStore) keyToPath(key string) string {
	hash := sha256.Sum256([]byte(key))
	hashStr := hex.EncodeToString(hash[:])
	return filepath.Join(ds.baseDir, hashStr[:2], hashStr[2:])
}

// Get открывает файл для чтения и обновляет его время доступа.
func (ds *diskStore) Get(key string) (*os.File, error) {
	path := ds.keyToPath(key)
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, os.ErrNotExist
	}
	if err != nil {
		return nil, err
	}

	// Обновляем время доступа для LRU.
	now := time.Now()
	go func() {
		if err := os.Chtimes(path, now, now); err != nil {
			ds.log.Warn().Err(err).Str("path", path).Msg("Failed to update file access time")
		}
		ds.mu.Lock()
		if item, ok := ds.items[key]; ok {
			item.lastUsed = now
		}
		ds.mu.Unlock()
	}()

	return file, nil
}

// Put сохраняет поток данных в файл и запускает вытеснение, если нужно.
func (ds *diskStore) Put(key string, r io.Reader) (int64, error) {
	path := ds.keyToPath(key)
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return 0, fmt.Errorf("failed to create dir for cache item: %w", err)
	}

	tmpFile, err := os.CreateTemp(dir, "temp-")
	if err != nil {
		return 0, err
	}
	defer os.Remove(tmpFile.Name())

	buf := util.GetBuffer()
	written, err := io.CopyBuffer(tmpFile, r, buf)
	util.ReturnBuffer(buf)

	if err != nil {
		tmpFile.Close()
		return 0, err
	}

	if err := tmpFile.Close(); err != nil {
		return 0, err
	}

	// Перед перемещением файла проверим, хватит ли места.
	ds.ensureSpace(written)

	if err := os.Rename(tmpFile.Name(), path); err != nil {
		return 0, err
	}

	// Добавляем информацию о новом файле в нашу карту.
	now := time.Now()
	ds.mu.Lock()
	ds.items[key] = &fileInfo{
		key:      key,
		path:     path,
		size:     written,
		lastUsed: now,
	}
	ds.mu.Unlock()
	ds.currentBytes.Add(written)

	return written, nil
}

// Delete удаляет файл.
func (ds *diskStore) Delete(key string) error {
	path := ds.keyToPath(key)
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	err = os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	ds.mu.Lock()
	delete(ds.items, key)
	ds.mu.Unlock()
	ds.currentBytes.Add(-info.Size())

	return nil
}

// ensureSpace проверяет доступное место и запускает вытеснение при необходимости.
func (ds *diskStore) ensureSpace(requiredSize int64) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	for ds.currentBytes.Load()+requiredSize > ds.maxBytes && len(ds.items) > 0 {
		// Собираем все элементы для сортировки.
		items := make([]*fileInfo, 0, len(ds.items))
		for _, item := range ds.items {
			items = append(items, item)
		}

		// Сортируем по времени последнего использования (старые в начале).
		sort.Slice(items, func(i, j int) bool {
			return items[i].lastUsed.Before(items[j].lastUsed)
		})

		// Удаляем самый старый элемент.
		if len(items) > 0 {
			itemToEvict := items[0]
			ds.log.Info().
				Str("key", itemToEvict.key).
				Str("size", util.FormatSize(itemToEvict.size)).
				Time("last_used", itemToEvict.lastUsed).
				Msg("Evicting file from disk cache")

			if err := os.Remove(itemToEvict.path); err != nil && !os.IsNotExist(err) {
				ds.log.Error().Err(err).Str("path", itemToEvict.path).Msg("Failed to evict file")
			}

			delete(ds.items, itemToEvict.key)
			ds.currentBytes.Add(-itemToEvict.size)
		}
	}
}
