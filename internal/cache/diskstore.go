package cache

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

type diskStore struct {
	baseDir string
}

func newDiskStore(baseDir string) (*diskStore, error) {
	cleanBaseDir := util.CleanPath(baseDir)
	if err := os.MkdirAll(cleanBaseDir, 0755); err != nil {
		logging.ErrorE("Failed to create cache directory", err, "directory", cleanBaseDir)
		return nil, fmt.Errorf("failed to create cache directory %s: %w", cleanBaseDir, err)
	}
	return &diskStore{baseDir: cleanBaseDir}, nil
}

func (ds *diskStore) getBaseFilePath(key string) string {
	safeRelativePath := util.SanitizePath(key)
	if safeRelativePath == "" || safeRelativePath == "." {
		safeRelativePath = "_invalid_key_path_"
	}
	return filepath.Join(ds.baseDir, safeRelativePath)
}

func (ds *diskStore) getContentFilePath(key string) string {
	return ds.getBaseFilePath(key) + ContentSuffix
}

func (ds *diskStore) getMetaFilePath(key string) string {
	return ds.getBaseFilePath(key) + MetadataSuffix
}

func (ds *diskStore) readMetadata(key string) (*CacheMetadata, error) {
	metaPath := ds.getMetaFilePath(key)
	metaFile, err := os.Open(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("failed to open metadata file %s: %w", metaPath, err)
	}
	defer metaFile.Close()

	var metadata CacheMetadata
	decoder := json.NewDecoder(metaFile)
	if err := decoder.Decode(&metadata); err != nil {
		_ = metaFile.Close()
		return nil, fmt.Errorf("failed to decode metadata file %s: %w", metaPath, err)
	}

	metadata.Key = key
	metadata.FilePath = ds.getContentFilePath(key)
	metadata.MetaPath = metaPath
	return &metadata, nil
}

func (ds *diskStore) openContentFile(key string) (*os.File, error) {
	contentPath := ds.getContentFilePath(key)
	contentFile, err := os.Open(contentPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("failed to open content file %s: %w", contentPath, err)
	}
	return contentFile, nil
}

func (ds *diskStore) preparePutDirectories(key string) (string, error) {
	basePath := ds.getBaseFilePath(key)
	dirPath := filepath.Dir(basePath)

	dirInfo, dirStatErr := os.Stat(dirPath)
	if dirStatErr != nil {
		if errors.Is(dirStatErr, os.ErrNotExist) {
			if mkdirErr := os.MkdirAll(dirPath, 0755); mkdirErr != nil {
				return "", fmt.Errorf("failed to create directory %s for cache item %s: %w", dirPath, key, mkdirErr)
			}
			return dirPath, nil
		}
		return "", fmt.Errorf("failed to stat directory %s for cache item %s: %w", dirPath, key, dirStatErr)
	}

	if !dirInfo.IsDir() {
		return "", fmt.Errorf("path conflict: cannot create directory %s because a file exists", dirPath)
	}

	return dirPath, nil
}

func (ds *diskStore) createTemporaryFiles(key, dirPath string) (tempContentPath, tempMetaPath string, cleanup func(), err error) {
	var tempContentFile, tempMetaFile *os.File
	baseFilename := filepath.Base(ds.getBaseFilePath(key))
	pattern := baseFilename + ".*.tmp"

	tempContentFile, err = os.CreateTemp(dirPath, pattern+ContentSuffix)
	if err != nil {
		err = fmt.Errorf("failed to create temporary content file for key %s: %w", key, err)
		cleanup = func() {}
		return
	}
	tempContentPath = tempContentFile.Name()
	_ = tempContentFile.Close()

	tempMetaFile, err = os.CreateTemp(dirPath, pattern+MetadataSuffix)
	if err != nil {
		_ = os.Remove(tempContentPath)
		err = fmt.Errorf("failed to create temporary metadata file for key %s: %w", key, err)
		cleanup = func() {}
		return
	}
	tempMetaPath = tempMetaFile.Name()
	_ = tempMetaFile.Close()

	cleanup = func() {
		if tempContentPath != "" {
			if remErr := os.Remove(tempContentPath); remErr != nil && !errors.Is(remErr, os.ErrNotExist) {
				logging.ErrorE("Failed to remove temporary content file", remErr, "temp_path", tempContentPath)
			}
		}
		if tempMetaPath != "" {
			if remErr := os.Remove(tempMetaPath); remErr != nil && !errors.Is(remErr, os.ErrNotExist) {
				logging.ErrorE("Failed to remove temporary metadata file", remErr, "temp_path", tempMetaPath)
			}
		}
	}

	return tempContentPath, tempMetaPath, cleanup, nil
}

func (ds *diskStore) writeToTemporaryFiles(reader io.Reader, metadata CacheMetadata, tempContentPath, tempMetaPath string) (finalSize int64, err error) {
	contentFile, err := os.OpenFile(tempContentPath, os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return -1, fmt.Errorf("failed to open temporary content file %s: %w", tempContentPath, err)
	}

	writtenSize, err := io.Copy(contentFile, reader)
	if err != nil {
		_ = contentFile.Close()
		return -1, fmt.Errorf("failed to write content for key %s: %w", metadata.Key, err)
	}
	if syncErr := contentFile.Sync(); syncErr != nil {
		logging.Warn("Failed to sync temporary content file", "error", syncErr, "temp_path", tempContentPath)
	}
	if closeErr := contentFile.Close(); closeErr != nil {
		logging.ErrorE("Failed to close temporary content file after writing", closeErr, "temp_path", tempContentPath, "key", metadata.Key)
	}

	metadata.Version = MetadataVersion
	if metadata.FetchTime.IsZero() {
		metadata.FetchTime = time.Now().UTC()
	}
	if metadata.Size >= 0 {
		if writtenSize != metadata.Size {
			return -1, fmt.Errorf("cache write size mismatch for %s: expected %d, wrote %d", metadata.Key, metadata.Size, writtenSize)
		}
	} else {
		metadata.Size = writtenSize
	}
	finalSize = metadata.Size

	if metadata.Headers == nil {
		metadata.Headers = make(http.Header)
	}
	if metadata.Headers.Get("Content-Type") == "" {
		metadata.Headers.Set("Content-Type", "application/octet-stream")
	}

	metaFile, err := os.OpenFile(tempMetaPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return -1, fmt.Errorf("failed to open temporary metadata file %s: %w", tempMetaPath, err)
	}

	encoder := json.NewEncoder(metaFile)
	encoder.SetIndent("", "  ")
	if encErr := encoder.Encode(&metadata); encErr != nil {
		_ = metaFile.Close()
		return -1, fmt.Errorf("failed to encode metadata for key %s: %w", metadata.Key, encErr)
	}
	if syncErr := metaFile.Sync(); syncErr != nil {
		logging.Warn("Failed to sync temporary metadata file", "error", syncErr, "temp_path", tempMetaPath)
	}
	if closeErr := metaFile.Close(); closeErr != nil {
		logging.ErrorE("Failed to close temporary metadata file after writing", closeErr, "temp_path", tempMetaPath, "key", metadata.Key)
	}

	return finalSize, nil
}

func (ds *diskStore) commitTemporaryFiles(key, tempContentPath, tempMetaPath string) error {
	finalContentPath := ds.getContentFilePath(key)
	finalMetaPath := ds.getMetaFilePath(key)

	if err := os.Rename(tempMetaPath, finalMetaPath); err != nil {
		return fmt.Errorf("failed to commit metadata for key %s: %w", key, err)
	}

	if err := os.Rename(tempContentPath, finalContentPath); err != nil {
		if removeMetaErr := os.Remove(finalMetaPath); removeMetaErr != nil && !os.IsNotExist(removeMetaErr) {
			logging.ErrorE("Failed to remove committed meta file during content rename failure rollback", removeMetaErr, "meta_path", finalMetaPath, "key", key)
		}
		return fmt.Errorf("failed to commit content for key %s: %w", key, err)
	}

	return nil
}

func (ds *diskStore) deleteFiles(key string) error {
	contentPath := ds.getContentFilePath(key)
	metaPath := ds.getMetaFilePath(key)

	contentErr := os.Remove(contentPath)
	metaErr := os.Remove(metaPath)

	var combinedErr error
	if contentErr != nil && !errors.Is(contentErr, os.ErrNotExist) {
		combinedErr = errors.Join(combinedErr, fmt.Errorf("failed deleting content %s: %w", contentPath, contentErr))
	}
	if metaErr != nil && !errors.Is(metaErr, os.ErrNotExist) {
		combinedErr = errors.Join(combinedErr, fmt.Errorf("failed deleting metadata %s: %w", metaPath, metaErr))
	}
	return combinedErr
}

func (ds *diskStore) cleanDirectory() error {
	dirEntries, err := os.ReadDir(ds.baseDir)
	if err != nil {
		return fmt.Errorf("failed to read cache directory for cleaning %s: %w", ds.baseDir, err)
	}

	cleanedCount := 0
	var combinedErr error
	for _, entry := range dirEntries {
		path := filepath.Join(ds.baseDir, entry.Name())
		if err := os.RemoveAll(path); err != nil {
			logging.Warn("Failed to remove item during cache clean", "error", err, "path", path)
			combinedErr = errors.Join(combinedErr, fmt.Errorf("failed removing %s: %w", path, err))
		} else {
			cleanedCount++
		}
	}
	logging.Info("Cache directory cleaned.", "items_removed", cleanedCount)
	return combinedErr
}

func (ds *diskStore) scanDirectory() ([]*cacheEntry, int64, error) {
	discoveredEntries := []*cacheEntry{}
	var scannedFiles int64
	var totalDiscoveredSize int64
	var inconsistencyCorruptMetadata atomic.Uint64
	var inconsistencySizeMismatch atomic.Uint64
	var inconsistencyContentWithoutMeta atomic.Uint64

	walkErr := filepath.Walk(ds.baseDir, func(path string, info os.FileInfo, walkErrIn error) error {
		if walkErrIn != nil {
			logging.Warn("Error accessing path during cache scan", "error", walkErrIn, "path", path)
			if info != nil && info.IsDir() && errors.Is(walkErrIn, filepath.SkipDir) {
				return filepath.SkipDir
			}
			return nil
		}

		if info.IsDir() || path == ds.baseDir || !strings.HasSuffix(path, ContentSuffix) || strings.HasSuffix(path, ".tmp") {
			return nil
		}

		atomic.AddInt64(&scannedFiles, 1)
		contentPath := path
		basePath := strings.TrimSuffix(contentPath, ContentSuffix)
		metaPath := basePath + MetadataSuffix

		relBasePath, relErr := filepath.Rel(ds.baseDir, basePath)
		if relErr != nil {
			logging.Warn("Failed to get relative path, skipping item", "error", relErr, "path", path)
			return nil
		}
		cacheKey := filepath.ToSlash(relBasePath)
		if cacheKey == "" || cacheKey == "." {
			logging.Warn("Invalid cache key generated, skipping item", "path", path, "key", cacheKey)
			return nil
		}

		realFileSize := info.Size()
		metaFile, metaErr := os.Open(metaPath)

		if metaErr != nil {
			if os.IsNotExist(metaErr) {
				inconsistencyContentWithoutMeta.Add(1)
				logging.Warn("Cache inconsistency: Content file found without metadata file, skipping item", "content_path", contentPath, "missing_meta_path", metaPath)
			} else {
				logging.Warn("Error opening metadata file during scan, skipping item.", "error", metaErr, "meta_path", metaPath)
			}
			return nil
		}
		defer metaFile.Close()

		var meta CacheMetadata
		decoder := json.NewDecoder(metaFile)
		decodeErr := decoder.Decode(&meta)
		if decodeErr != nil {
			inconsistencyCorruptMetadata.Add(1)
			logging.Warn("Cache inconsistency: Failed to decode metadata file, skipping item", "error", decodeErr, "meta_path", metaPath)
			_ = metaFile.Close()
			return nil
		}

		entrySize := realFileSize
		if meta.Size >= 0 {
			if meta.Size != realFileSize {
				inconsistencySizeMismatch.Add(1)
				logging.Warn("Cache inconsistency: Metadata size mismatch with actual file size, skipping item", "meta_path", metaPath, "meta_size", meta.Size, "file_size", realFileSize)
				_ = metaFile.Close()
				return nil
			}
			entrySize = meta.Size
		} else {
			logging.Debug("Metadata size was -1, using actual file size from scan", "key", cacheKey, "size", entrySize)
		}

		discoveredEntries = append(discoveredEntries, &cacheEntry{
			key:  cacheKey,
			size: entrySize,
		})
		atomic.AddInt64(&totalDiscoveredSize, entrySize)
		return nil
	})

	if walkErr != nil {
		return nil, 0, fmt.Errorf("cache directory scan failed: %w", walkErr)
	}

	logIfInconsistent(inconsistencyContentWithoutMeta.Load(), "content_without_meta")
	logIfInconsistent(inconsistencyCorruptMetadata.Load(), "corrupt_metadata")
	logIfInconsistent(inconsistencySizeMismatch.Load(), "size_mismatch")

	return discoveredEntries, scannedFiles, nil
}

func logIfInconsistent(count uint64, reason string) {
	if count > 0 {
		logging.Warn("Cache inconsistencies found during scan", "reason", reason, "count", count)
	}
}
