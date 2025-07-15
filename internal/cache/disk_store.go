package cache

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	json "github.com/goccy/go-json"

	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

const (
	contentSuffix = ".content"
	metaSuffix    = ".meta"
)

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

func (ds *diskStore) keyToPath(key, suffix string) string {
	hash := sha256.Sum256([]byte(key))
	hashStr := hex.EncodeToString(hash[:])
	return filepath.Join(ds.baseDir, hashStr[:2], hashStr[2:]+suffix)
}

func (ds *diskStore) GetContent(key string) (*os.File, error) {
	path := ds.keyToPath(key, contentSuffix)
	return os.Open(path)
}

func (ds *diskStore) PutContent(key string, r io.Reader) (int64, error) {
	path := ds.keyToPath(key, contentSuffix)
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return 0, err
	}
	tmpFile, err := os.CreateTemp(dir, "temp-")
	if err != nil {
		return 0, err
	}
	tmpName := tmpFile.Name() // Захватываем имя
	defer func() {
		if err := os.Remove(tmpName); err != nil && !os.IsNotExist(err) { // Игнорим только NotExist
			ds.log.Warn().Err(err).Str("temp_file", tmpName).Msg("Failed to clean up temporary file")
		}
	}()

	buf := util.GetBuffer()
	written, err := io.CopyBuffer(tmpFile, r, buf)
	util.ReturnBuffer(buf)

	if err != nil {
		tmpFile.Close() // Закрываем перед return
		return 0, err
	}
	if err := tmpFile.Close(); err != nil {
		return 0, err
	}
	return written, os.Rename(tmpName, path)
}

func (ds *diskStore) WriteMetadata(meta *ItemMeta) error {
	path := ds.keyToPath(meta.Key, metaSuffix)
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}
	tmpFile, err := os.CreateTemp(dir, "temp-meta-")
	if err != nil {
		return err
	}
	defer os.Remove(tmpFile.Name())

	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		tmpFile.Close()
		return err
	}
	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}
	return os.Rename(tmpFile.Name(), path)
}

func (ds *diskStore) ReadMeta(path string) (*ItemMeta, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var meta ItemMeta
	if err := json.NewDecoder(bufio.NewReader(file)).Decode(&meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

func (ds *diskStore) ScanMetaFiles() ([]string, error) {
	var files []string
	err := filepath.Walk(ds.baseDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.HasSuffix(info.Name(), metaSuffix) {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

func (ds *diskStore) Delete(key string) error {
	contentPath := ds.keyToPath(key, contentSuffix)
	metaPath := ds.keyToPath(key, metaSuffix)

	errContent := os.Remove(contentPath)
	errMeta := os.Remove(metaPath)

	if errContent != nil && !os.IsNotExist(errContent) {
		return fmt.Errorf("failed to remove content file: %w", errContent)
	}
	if errMeta != nil && !os.IsNotExist(errMeta) {
		return fmt.Errorf("failed to remove meta file: %w", errMeta)
	}
	return nil
}
