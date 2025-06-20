package cache

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cespare/xxhash/v2"
	"github.com/rs/zerolog"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

const (
	contentSuffix  = ".content"
	metadataSuffix = ".meta"
	tempPrefix     = "temp_"
)

type diskStore struct {
	baseDir string
	log     zerolog.Logger
}

func newDiskStore(baseDir string, logger zerolog.Logger) (*diskStore, error) {
	if err := os.MkdirAll(baseDir, 0750); err != nil {
		return nil, fmt.Errorf("create cache base dir %s: %w", baseDir, err)
	}
	return &diskStore{
		baseDir: baseDir,
		log:     logger.With().Str("component", "diskStore").Logger(),
	}, nil
}

// hashPath создает xxhash от ключа и возвращает путь вида /aa/bb/hash
func (ds *diskStore) hashPath(key string) string {
	hash := xxhash.Sum64String(key)
	hashStr := hex.EncodeToString([]byte(fmt.Sprintf("%016x", hash)))

	// Создаем структуру /aa/bb/hash
	return filepath.Join(
		hashStr[:2],  // первые 2 символа
		hashStr[2:4], // следующие 2 символа
		hashStr,      // полный хеш как имя файла
	)
}

func (ds *diskStore) contentPath(key string) string {
	return filepath.Join(ds.baseDir, ds.hashPath(key)+contentSuffix)
}

func (ds *diskStore) metadataPath(key string) string {
	return filepath.Join(ds.baseDir, ds.hashPath(key)+metadataSuffix)
}

func (ds *diskStore) write(key string, r io.Reader, meta *ItemMeta) (
	writtenSize int64, cPath, mPath string, err error) {

	finalCPath := ds.contentPath(key)
	finalMPath := ds.metadataPath(key)
	itemDir := filepath.Dir(finalCPath)

	if err = os.MkdirAll(itemDir, 0750); err != nil {
		return 0, "", "", fmt.Errorf("mkdir %s: %w", itemDir, err)
	}

	var tmpCPath string
	var tmpMPath string

	// ИСПРАВЛЕНО: Улучшена логика defer для надежной очистки временных файлов только при сбое.
	isSuccess := false
	defer func() {
		if !isSuccess {
			if tmpCPath != "" {
				if rmErr := os.Remove(tmpCPath); rmErr != nil && !os.IsNotExist(rmErr) {
					ds.log.Warn().Err(rmErr).Str("path", tmpCPath).Msg("Failed to remove temp content file on error")
				}
			}
			if tmpMPath != "" {
				if rmErr := os.Remove(tmpMPath); rmErr != nil && !os.IsNotExist(rmErr) {
					ds.log.Warn().Err(rmErr).Str("path", tmpMPath).Msg("Failed to remove temp metadata file on error")
				}
			}
		}
	}()

	if r != nil {
		var tmpCFileHandle *os.File
		tmpCFileHandle, err = os.CreateTemp(itemDir, tempPrefix+"*"+contentSuffix)
		if err != nil {
			return 0, "", "", fmt.Errorf("create temp content file: %w", err)
		}
		tmpCPath = tmpCFileHandle.Name()

		buf := util.GetBuffer()
		defer util.ReturnBuffer(buf)

		writtenSize, err = io.CopyBuffer(tmpCFileHandle, r, buf)

		// ИСПРАВЛЕНО: Сохраняем частично скачанные файлы, если ошибка вызвана разрывом соединения.
		if err != nil {
			if util.IsClientDisconnectedError(err) {
				ds.log.Debug().Err(err).Str("key", key).Int64("bytes_written", writtenSize).Msg("Client disconnected during download. Committing partial file to cache.")
				// Сбрасываем ошибку, так как для кеша это не сбой, а штатное завершение.
				err = nil
			} else {
				// Это реальная ошибка записи (например, нет места на диске), выходим.
				_ = tmpCFileHandle.Close()
				return 0, "", "", fmt.Errorf("write content to temp: %w", err)
			}
		}
		// Закрываем файл после всех операций, включая обработку ошибок.
		if closeErr := tmpCFileHandle.Close(); closeErr != nil && err == nil {
			return 0, "", "", fmt.Errorf("close temp content file: %w", closeErr)
		}

		originalContentLengthStr := meta.Headers.Get("Content-Length")
		if originalContentLengthStr != "" {
			originalContentLength, parseErr := strconv.ParseInt(originalContentLengthStr, 10, 64)
			if parseErr == nil && originalContentLength >= 0 {
				if writtenSize != originalContentLength && err == nil { // Проверяем только если не было ошибки обрыва
					ds.log.Error().
						Str("key", meta.Key).
						Int64("expected_size_upstream", originalContentLength).
						Int64("actual_written_size", writtenSize).
						Msg("Upstream Content-Length mismatch with bytes read")
					err = fmt.Errorf("upstream Content-Length %d did not match received bytes %d for key %s", originalContentLength, writtenSize, meta.Key)
					return 0, "", "", err
				}
			} else {
				ds.log.Warn().
					Str("key", meta.Key).
					Str("content_length_header", originalContentLengthStr).
					Err(parseErr).
					Msg("Invalid Content-Length header, proceeding with actual written size")
			}
		}

		if syncErr := tmpCFileHandle.Sync(); syncErr != nil {
			ds.log.Warn().Err(syncErr).Str("path", tmpCPath).Msg("Sync temp content file failed")
		}
	} else {
		writtenSize = 0
	}

	meta.Size = writtenSize
	meta.Path = finalCPath
	meta.MetaPath = finalMPath

	var tmpMFileHandle *os.File
	tmpMFileHandle, err = os.CreateTemp(itemDir, tempPrefix+"*"+metadataSuffix)
	if err != nil {
		return 0, "", "", fmt.Errorf("create temp metadata file: %w", err)
	}
	tmpMPath = tmpMFileHandle.Name()

	enc := json.NewEncoder(tmpMFileHandle)
	enc.SetIndent("", "  ")
	if err = enc.Encode(meta); err != nil {
		_ = tmpMFileHandle.Close()
		return 0, "", "", fmt.Errorf("encode metadata: %w", err)
	}
	if syncErr := tmpMFileHandle.Sync(); syncErr != nil {
		ds.log.Warn().Err(syncErr).Str("path", tmpMPath).Msg("Sync temp metadata file failed")
	}
	if err = tmpMFileHandle.Close(); err != nil {
		return 0, "", "", fmt.Errorf("close temp metadata file: %w", err)
	}

	if err = os.Rename(tmpMPath, finalMPath); err != nil {
		return 0, "", "", fmt.Errorf("rename temp metadata to %s: %w", finalMPath, err)
	}
	tmpMPath = ""

	if r != nil && tmpCPath != "" {
		if err = os.Rename(tmpCPath, finalCPath); err != nil {
			_ = os.Remove(finalMPath)
			return 0, "", "", fmt.Errorf("rename temp content to %s: %w", finalCPath, err)
		}
		tmpCPath = ""
	}

	isSuccess = true // Отмечаем успех, чтобы defer не удалил файлы.
	return writtenSize, finalCPath, finalMPath, nil
}

func (ds *diskStore) writeMetadata(mPath string, meta *ItemMeta) error {
	itemDir := filepath.Dir(mPath)

	if err := os.MkdirAll(itemDir, 0750); err != nil {
		return fmt.Errorf("mkdir %s for metadata update: %w", itemDir, err)
	}

	var tmpMPath string
	var err error

	defer func() {
		if err != nil && tmpMPath != "" {
			if rmErr := os.Remove(tmpMPath); rmErr != nil && !os.IsNotExist(rmErr) {
				ds.log.Warn().Err(rmErr).Str("path", tmpMPath).Msg("Failed to remove temp metadata file for update on error")
			}
		}
	}()

	var tmpMFileHandle *os.File
	tmpMFileHandle, err = os.CreateTemp(itemDir, tempPrefix+"*"+metadataSuffix)
	if err != nil {
		return fmt.Errorf("create temp metadata for update: %w", err)
	}
	tmpMPath = tmpMFileHandle.Name()

	enc := json.NewEncoder(tmpMFileHandle)
	enc.SetIndent("", "  ")
	if err = enc.Encode(meta); err != nil {
		_ = tmpMFileHandle.Close()
		return fmt.Errorf("encode metadata for update: %w", err)
	}
	if syncErr := tmpMFileHandle.Sync(); syncErr != nil {
		ds.log.Warn().Err(syncErr).Str("path", tmpMPath).Msg("Sync temp metadata for update failed")
	}
	if err = tmpMFileHandle.Close(); err != nil {
		return fmt.Errorf("close temp metadata for update: %w", err)
	}

	if err = os.Rename(tmpMPath, mPath); err != nil {
		return fmt.Errorf("rename temp metadata to %s for update: %w", mPath, err)
	}
	tmpMPath = ""
	return nil
}

func (ds *diskStore) readMeta(mPath string) (*ItemMeta, error) {
	f, err := os.Open(mPath)
	if err != nil {
		return nil, fmt.Errorf("open metadata file %s: %w", mPath, err)
	}
	defer f.Close()

	var meta ItemMeta

	if err = json.NewDecoder(bufio.NewReader(f)).Decode(&meta); err != nil {
		return nil, fmt.Errorf("decode metadata from %s: %w", mPath, err)
	}

	if meta.Version < MetadataVersion {
		ds.log.Warn().Str("path", mPath).Int("file_version", meta.Version).Int("expected_version", MetadataVersion).Msg("Outdated metadata version")
		return nil, fmt.Errorf("outdated metadata version in %s (version %d, expected >= %d)", mPath, meta.Version, MetadataVersion)
	}
	if meta.Key == "" {
		ds.log.Warn().Str("path", mPath).Msg("Metadata file missing 'key' field")
		return nil, fmt.Errorf("missing 'key' in metadata for %s", mPath)
	}

	expectedMPath := ds.metadataPath(meta.Key)
	if mPath != expectedMPath {
		ds.log.Warn().Str("path", mPath).Str("key_in_file", meta.Key).Str("expected_path", expectedMPath).Msg("Metadata file path does not match its internal key. File may be corrupted or misplaced.")
		return nil, fmt.Errorf("key %q in %s does not match file path", meta.Key, mPath)
	}

	return &meta, nil
}

func (ds *diskStore) openContentFile(cPath string) (*os.File, error) {
	f, err := os.Open(cPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("open content file %s: %w", cPath, err)
	}
	return f, nil
}

func (ds *diskStore) deleteFiles(cPath, mPath string) error {
	var errs []string
	if cPath != "" {
		if err := os.Remove(cPath); err != nil && !os.IsNotExist(err) {
			errs = append(errs, fmt.Sprintf("delete content %s: %v", cPath, err))
		}
	}
	if mPath != "" {
		if err := os.Remove(mPath); err != nil && !os.IsNotExist(err) {
			errs = append(errs, fmt.Sprintf("delete metadata %s: %v", mPath, err))
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func (ds *diskStore) scanMetaFiles() ([]string, error) {
	var metaFiles []string
	err := filepath.WalkDir(ds.baseDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			ds.log.Warn().Err(err).Str("path", path).Msg("Error accessing path during scan, skipping")
			return nil
		}
		if !d.IsDir() && strings.HasSuffix(d.Name(), metadataSuffix) &&
			!strings.HasPrefix(d.Name(), tempPrefix) {
			metaFiles = append(metaFiles, path)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk cache directory %s: %w", ds.baseDir, err)
	}
	return metaFiles, nil
}

func (ds *diskStore) cleanDir() error {
	ds.log.Info().Str("dir", ds.baseDir).Msg("Cleaning all contents of cache directory")
	entries, err := os.ReadDir(ds.baseDir)
	if err != nil {
		return fmt.Errorf("read cache directory %s for cleaning: %w", ds.baseDir, err)
	}
	for _, entry := range entries {
		p := filepath.Join(ds.baseDir, entry.Name())
		if err := os.RemoveAll(p); err != nil {
			ds.log.Error().Err(err).Str("path", p).Msg("Failed to remove item during cache clean")
		}
	}

	if err := os.MkdirAll(ds.baseDir, 0750); err != nil {
		ds.log.Error().Err(err).Str("path", ds.baseDir).Msg("Failed to recreate base directory after cleaning")
		return fmt.Errorf("recreate base directory %s after cleaning: %w", ds.baseDir, err)
	}
	return nil
}
