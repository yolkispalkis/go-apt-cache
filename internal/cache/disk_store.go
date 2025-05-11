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

func (ds *diskStore) contentPath(key string) string {
	return filepath.Join(ds.baseDir, key+contentSuffix)
}

func (ds *diskStore) metadataPath(key string) string {
	return filepath.Join(ds.baseDir, key+metadataSuffix)
}

func (ds *diskStore) write(key string, r io.Reader, meta *ItemMeta) (
	writtenSize int64, cPath, mPath string, err error) {

	cPath = ds.contentPath(key)
	mPath = ds.metadataPath(key)
	itemDir := filepath.Dir(cPath)

	if err = os.MkdirAll(itemDir, 0750); err != nil {
		return 0, "", "", fmt.Errorf("mkdir %s: %w", itemDir, err)
	}

	var tmpCPath string
	var tmpMPath string

	defer func() {
		if err != nil {
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

		bufW := bufio.NewWriter(tmpCFileHandle)
		writtenSize, err = io.Copy(bufW, r)
		if err != nil {
			_ = tmpCFileHandle.Close()
			return 0, "", "", fmt.Errorf("write content to temp: %w", err)
		}
		if err = bufW.Flush(); err != nil {
			_ = tmpCFileHandle.Close()
			return 0, "", "", fmt.Errorf("flush content buffer: %w", err)
		}
		if syncErr := tmpCFileHandle.Sync(); syncErr != nil {
			ds.log.Warn().Err(syncErr).Str("path", tmpCPath).Msg("Sync temp content file failed")
		}
		if err = tmpCFileHandle.Close(); err != nil {
			return 0, "", "", fmt.Errorf("close temp content file: %w", err)
		}
	} else {
		writtenSize = 0
	}

	meta.Size = writtenSize
	meta.Path = cPath
	meta.MetaPath = mPath

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

	if err = os.Rename(tmpMPath, mPath); err != nil {
		_ = os.Remove(tmpMPath)
		return 0, "", "", fmt.Errorf("rename temp metadata to %s: %w", mPath, err)
	}
	tmpMPath = ""

	if r != nil && tmpCPath != "" {
		if err = os.Rename(tmpCPath, cPath); err != nil {
			_ = os.Remove(mPath)
			_ = os.Remove(tmpCPath)
			return 0, "", "", fmt.Errorf("rename temp content to %s: %w", cPath, err)
		}
		tmpCPath = ""
	}
	return writtenSize, cPath, mPath, nil
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
		_ = os.Remove(tmpMPath)
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
		ds.log.Warn().Str("path", mPath).Int("file_version", meta.Version).Int("expected_version", MetadataVersion).Msg("Outdated metadata version.")
		return nil, fmt.Errorf("outdated metadata version in %s (version %d, expected >= %d)", mPath, meta.Version, MetadataVersion)
	}
	if meta.Key == "" {
		ds.log.Warn().Str("path", mPath).Msg("Metadata file missing 'key' field or failed to parse due to format mismatch.")
		return nil, fmt.Errorf("missing 'key' in metadata or format mismatch for %s", mPath)
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
			ds.log.Warn().Err(err).Str("path", path).Msg("Error accessing path during scan, skipping.")
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
	ds.log.Info().Str("dir", ds.baseDir).Msg("Cleaning all contents of cache directory.")
	entries, err := os.ReadDir(ds.baseDir)
	if err != nil {
		return fmt.Errorf("read cache directory %s for cleaning: %w", ds.baseDir, err)
	}
	for _, entry := range entries {
		p := filepath.Join(ds.baseDir, entry.Name())
		if err := os.RemoveAll(p); err != nil {
			ds.log.Error().Err(err).Str("path", p).Msg("Failed to remove item during cache clean.")
		}
	}

	if err := os.MkdirAll(ds.baseDir, 0750); err != nil {
		ds.log.Error().Err(err).Str("path", ds.baseDir).Msg("Failed to recreate base directory after cleaning.")
		return fmt.Errorf("recreate base directory %s after cleaning: %w", ds.baseDir, err)
	}
	return nil
}
