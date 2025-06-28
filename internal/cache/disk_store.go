package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

// diskStore управляет хранением файлов на диске.
type diskStore struct {
	baseDir string
	log     *logging.Logger
}

func newDiskStore(baseDir string, logger *logging.Logger) (*diskStore, error) {
	if err := os.MkdirAll(baseDir, 0750); err != nil {
		return nil, fmt.Errorf("failed to create cache base dir %s: %w", baseDir, err)
	}
	return &diskStore{
		baseDir: baseDir,
		log:     logger.WithComponent("diskStore"),
	}, nil
}

// keyToPath преобразует ключ в безопасный путь на диске (e.g., /base/ab/cdef...).
func (ds *diskStore) keyToPath(key string) string {
	hash := sha256.Sum256([]byte(key))
	hashStr := hex.EncodeToString(hash[:])
	return filepath.Join(ds.baseDir, hashStr[:2], hashStr[2:])
}

// Get открывает файл для чтения.
func (ds *diskStore) Get(key string) (*os.File, error) {
	path := ds.keyToPath(key)
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, os.ErrNotExist
	}
	return file, err
}

// Put сохраняет поток данных в файл.
func (ds *diskStore) Put(key string, r io.Reader) (int64, error) {
	path := ds.keyToPath(key)
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return 0, fmt.Errorf("failed to create dir for cache item: %w", err)
	}

	// Атомарная запись через временный файл.
	tmpFile, err := os.CreateTemp(dir, "temp-")
	if err != nil {
		return 0, err
	}
	defer os.Remove(tmpFile.Name()) // Удаляем временный файл в случае ошибки.

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

	if err := os.Rename(tmpFile.Name(), path); err != nil {
		return 0, err
	}

	return written, nil
}

// Delete удаляет файл.
func (ds *diskStore) Delete(key string) error {
	path := ds.keyToPath(key)
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
