package cache

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

const (
	contentSuffix   = ".content"
	metadataSuffix  = ".meta"
	tempFilePattern = "temp_*"
)

type diskStore struct {
	baseDir string
	logger  zerolog.Logger
}

func newDiskStore(baseDir string, logger zerolog.Logger) (*diskStore, error) {
	if err := os.MkdirAll(baseDir, 0750); err != nil {
		return nil, fmt.Errorf("failed to create cache base directory %s: %w", baseDir, err)
	}
	return &diskStore{
		baseDir: baseDir,
		logger:  logger.With().Str("component", "diskStore").Logger(),
	}, nil
}

func (ds *diskStore) getSafeRelativePath(key string) (string, error) {

	parts := strings.Split(key, "/")
	if len(parts) == 0 {
		return "", errors.New("cache key cannot be empty")
	}

	sanitizedParts := make([]string, len(parts))
	for i, part := range parts {
		sPart, err := util.SanitizePathForFilesystem(part)
		if err != nil {

			sPart = util.SanitizeCacheKeyPathComponent(part)
			if sPart == "_" && part != "_" {
				ds.logger.Warn().Str("original_part", part).Str("sanitized_part", sPart).Msg("Path component sanitized aggressively.")
			}
		}
		sanitizedParts[i] = sPart
	}

	if len(sanitizedParts) == 0 {
		return "", errors.New("cache key resulted in no valid path components")
	}

	return filepath.Join(sanitizedParts...), nil
}

func (ds *diskStore) contentPath(key string) string {

	safeRelPath, err := ds.getSafeRelativePath(key)
	if err != nil {
		ds.logger.Error().Err(err).Str("key", key).Msg("Failed to generate safe relative path for content.")

		safeRelPath = util.SanitizeCacheKeyPathComponent(key)
	}
	return filepath.Join(ds.baseDir, safeRelPath+contentSuffix)
}

func (ds *diskStore) metadataPath(key string) string {
	safeRelPath, err := ds.getSafeRelativePath(key)
	if err != nil {
		ds.logger.Error().Err(err).Str("key", key).Msg("Failed to generate safe relative path for metadata.")
		safeRelPath = util.SanitizeCacheKeyPathComponent(key)
	}
	return filepath.Join(ds.baseDir, safeRelPath+metadataSuffix)
}

func (ds *diskStore) write(key string, reader io.Reader, meta *CacheItemMetadata) (int64, string, string, error) {
	finalContentPath := ds.contentPath(key)
	finalMetaPath := ds.metadataPath(key)

	dir := filepath.Dir(finalContentPath)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return 0, "", "", fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	tempContentFile, err := os.CreateTemp(dir, tempFilePattern+contentSuffix)
	if err != nil {
		return 0, "", "", fmt.Errorf("failed to create temp content file: %w", err)
	}
	defer func() {
		if tempContentFile != nil {
			tempContentFile.Close()
			os.Remove(tempContentFile.Name())
		}
	}()

	var writtenSize int64
	if reader != nil {
		bufWriter := bufio.NewWriter(tempContentFile)
		writtenSize, err = io.Copy(bufWriter, reader)
		if err != nil {
			return 0, "", "", fmt.Errorf("failed to write content to temp file: %w", err)
		}
		if err := bufWriter.Flush(); err != nil {
			return 0, "", "", fmt.Errorf("failed to flush content buffer: %w", err)
		}
		if err := tempContentFile.Sync(); err != nil {

			ds.logger.Warn().Err(err).Str("path", tempContentFile.Name()).Msg("Failed to sync temp content file")
		}
	}
	tempContentPath := tempContentFile.Name()
	if err := tempContentFile.Close(); err != nil {
		return 0, "", "", fmt.Errorf("failed to close temp content file: %w", err)
	}
	tempContentFile = nil

	tempMetaFile, err := os.CreateTemp(dir, tempFilePattern+metadataSuffix)
	if err != nil {
		os.Remove(tempContentPath)
		return 0, "", "", fmt.Errorf("failed to create temp metadata file: %w", err)
	}
	defer func() {
		if tempMetaFile != nil {
			tempMetaFile.Close()
			os.Remove(tempMetaFile.Name())
		}
	}()

	meta.Size = writtenSize
	meta.Path = finalContentPath
	meta.MetaPath = finalMetaPath

	encoder := json.NewEncoder(tempMetaFile)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(meta); err != nil {
		os.Remove(tempContentPath)
		return 0, "", "", fmt.Errorf("failed to encode metadata: %w", err)
	}
	if err := tempMetaFile.Sync(); err != nil {
		ds.logger.Warn().Err(err).Str("path", tempMetaFile.Name()).Msg("Failed to sync temp metadata file")
	}
	tempMetaPath := tempMetaFile.Name()
	if err := tempMetaFile.Close(); err != nil {
		os.Remove(tempContentPath)
		return 0, "", "", fmt.Errorf("failed to close temp metadata file: %w", err)
	}
	tempMetaFile = nil

	if err := os.Rename(tempMetaPath, finalMetaPath); err != nil {
		os.Remove(tempContentPath)
		os.Remove(tempMetaPath)
		return 0, "", "", fmt.Errorf("failed to rename temp metadata file to %s: %w", finalMetaPath, err)
	}
	if err := os.Rename(tempContentPath, finalContentPath); err != nil {
		os.Remove(finalMetaPath)
		os.Remove(tempContentPath)
		return 0, "", "", fmt.Errorf("failed to rename temp content file to %s: %w", finalContentPath, err)
	}

	return writtenSize, finalContentPath, finalMetaPath, nil
}

func (ds *diskStore) writeMetadata(metaPath string, meta *CacheItemMetadata) error {
	dir := filepath.Dir(metaPath)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return fmt.Errorf("failed to create directory %s for metadata: %w", dir, err)
	}

	tempMetaFile, err := os.CreateTemp(dir, tempFilePattern+metadataSuffix)
	if err != nil {
		return fmt.Errorf("failed to create temp metadata file: %w", err)
	}

	encoder := json.NewEncoder(tempMetaFile)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(meta); err != nil {
		tempMetaFile.Close()
		os.Remove(tempMetaFile.Name())
		return fmt.Errorf("failed to encode metadata: %w", err)
	}
	if err := tempMetaFile.Sync(); err != nil {
		ds.logger.Warn().Err(err).Str("path", tempMetaFile.Name()).Msg("Failed to sync temp metadata file")
	}

	tempPath := tempMetaFile.Name()
	if err := tempMetaFile.Close(); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to close temp metadata file: %w", err)
	}

	if err := os.Rename(tempPath, metaPath); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to rename temp metadata file to %s: %w", metaPath, err)
	}
	return nil
}

func (ds *diskStore) readMetadataFromFile(metaPath string) (*CacheItemMetadata, error) {
	file, err := os.Open(metaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open metadata file %s: %w", metaPath, err)
	}
	defer file.Close()

	var meta CacheItemMetadata

	bufReader := bufio.NewReader(file)
	decoder := json.NewDecoder(bufReader)
	if err := decoder.Decode(&meta); err != nil {
		return nil, fmt.Errorf("failed to decode metadata from %s: %w", metaPath, err)
	}
	return &meta, nil
}

func (ds *diskStore) openContentFile(contentPath string) (*os.File, error) {
	file, err := os.Open(contentPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("failed to open content file %s: %w", contentPath, err)
	}
	return file, nil
}

func (ds *diskStore) deleteFiles(contentPath, metaPath string) error {
	var deleteErrors []error
	if contentPath != "" {
		err := os.Remove(contentPath)
		if err != nil && !os.IsNotExist(err) {
			deleteErrors = append(deleteErrors, fmt.Errorf("delete content %s: %w", contentPath, err))
		}
	}
	if metaPath != "" {
		err := os.Remove(metaPath)
		if err != nil && !os.IsNotExist(err) {
			deleteErrors = append(deleteErrors, fmt.Errorf("delete metadata %s: %w", metaPath, err))
		}
	}
	if len(deleteErrors) > 0 {

		var errorStrings []string
		for _, e := range deleteErrors {
			errorStrings = append(errorStrings, e.Error())
		}
		return errors.New(strings.Join(errorStrings, "; "))
	}
	return nil
}

func (ds *diskStore) scanMetadataFiles() ([]string, error) {
	var metaFiles []string
	err := filepath.Walk(ds.baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {

			ds.logger.Warn().Err(err).Str("path", path).Msg("Error accessing path during scan")
			return nil
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), metadataSuffix) && !strings.HasPrefix(info.Name(), tempFilePattern) {
			metaFiles = append(metaFiles, path)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error walking cache directory %s: %w", ds.baseDir, err)
	}
	return metaFiles, nil
}

func (ds *diskStore) cleanDirectory() error {
	ds.logger.Info().Str("directory", ds.baseDir).Msg("Cleaning all contents of cache directory.")

	dirEntries, err := os.ReadDir(ds.baseDir)
	if err != nil {
		return fmt.Errorf("failed to read cache directory %s for cleaning: %w", ds.baseDir, err)
	}
	for _, entry := range dirEntries {
		path := filepath.Join(ds.baseDir, entry.Name())
		if err := os.RemoveAll(path); err != nil {

			ds.logger.Error().Err(err).Str("path", path).Msg("Failed to remove item during cache clean.")
		}
	}

	if err := os.MkdirAll(ds.baseDir, 0750); err != nil {
		ds.logger.Error().Err(err).Str("path", ds.baseDir).Msg("Failed to recreate base directory after cleaning.")
		return fmt.Errorf("failed to recreate base directory %s after cleaning: %w", ds.baseDir, err)
	}
	return nil
}
