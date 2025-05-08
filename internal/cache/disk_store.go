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

	var tmpCFile *os.File
	var tmpCPath string

	if r != nil {
		tmpCFile, err = os.CreateTemp(itemDir, tempPrefix+"*"+contentSuffix)
		if err != nil {
			return 0, "", "", fmt.Errorf("create temp content file: %w", err)
		}

		defer func() {
			if tmpCFile != nil {
				tmpCFile.Close()
				os.Remove(tmpCFile.Name())
			}
		}()

		bufW := bufio.NewWriter(tmpCFile)
		if writtenSize, err = io.Copy(bufW, r); err != nil {
			return 0, "", "", fmt.Errorf("write content to temp: %w", err)
		}
		if err = bufW.Flush(); err != nil {
			return 0, "", "", fmt.Errorf("flush content buffer: %w", err)
		}
		if err = tmpCFile.Sync(); err != nil {
			ds.log.Warn().Err(err).Str("path", tmpCFile.Name()).Msg("Sync temp content file failed")
		}

		tmpCPath = tmpCFile.Name()
		if err = tmpCFile.Close(); err != nil {
			return 0, "", "", fmt.Errorf("close temp content file: %w", err)
		}
		tmpCFile = nil
	} else {
		writtenSize = 0
	}

	meta.Size = writtenSize
	meta.Path = cPath
	meta.MetaPath = mPath

	tmpMFile, err := os.CreateTemp(itemDir, tempPrefix+"*"+metadataSuffix)
	if err != nil {
		if tmpCPath != "" {
			os.Remove(tmpCPath)
		}
		return 0, "", "", fmt.Errorf("create temp metadata file: %w", err)
	}
	defer func() {
		if tmpMFile != nil {
			tmpMFile.Close()
			os.Remove(tmpMFile.Name())
		}
	}()

	enc := json.NewEncoder(tmpMFile)
	enc.SetIndent("", "  ")
	if err = enc.Encode(meta); err != nil {
		if tmpCPath != "" {
			os.Remove(tmpCPath)
		}
		return 0, "", "", fmt.Errorf("encode metadata: %w", err)
	}
	if err = tmpMFile.Sync(); err != nil {
		ds.log.Warn().Err(err).Str("path", tmpMFile.Name()).Msg("Sync temp metadata file failed")
	}

	tmpMPath := tmpMFile.Name()
	if err = tmpMFile.Close(); err != nil {
		if tmpCPath != "" {
			os.Remove(tmpCPath)
		}
		return 0, "", "", fmt.Errorf("close temp metadata file: %w", err)
	}
	tmpMFile = nil

	if err = os.Rename(tmpMPath, mPath); err != nil {
		if tmpCPath != "" {
			os.Remove(tmpCPath)
		}
		os.Remove(tmpMPath)
		return 0, "", "", fmt.Errorf("rename temp metadata to %s: %w", mPath, err)
	}
	if r != nil && tmpCPath != "" {
		if err = os.Rename(tmpCPath, cPath); err != nil {
			os.Remove(mPath)
			os.Remove(tmpCPath)
			return 0, "", "", fmt.Errorf("rename temp content to %s: %w", cPath, err)
		}
	}
	return writtenSize, cPath, mPath, nil
}

func (ds *diskStore) writeMetadata(mPath string, meta *ItemMeta) error {
	itemDir := filepath.Dir(mPath)

	if err := os.MkdirAll(itemDir, 0750); err != nil {
		return fmt.Errorf("mkdir %s for metadata update: %w", itemDir, err)
	}

	tmpMFile, err := os.CreateTemp(itemDir, tempPrefix+"*"+metadataSuffix)
	if err != nil {
		return fmt.Errorf("create temp metadata for update: %w", err)
	}
	defer func() {
		if tmpMFile != nil {
			tmpMFile.Close()
			os.Remove(tmpMFile.Name())
		}
	}()

	enc := json.NewEncoder(tmpMFile)
	enc.SetIndent("", "  ")
	if err = enc.Encode(meta); err != nil {
		return fmt.Errorf("encode metadata for update: %w", err)
	}
	if err = tmpMFile.Sync(); err != nil {
		ds.log.Warn().Err(err).Str("path", tmpMFile.Name()).Msg("Sync temp metadata for update failed")
	}

	tmpPath := tmpMFile.Name()
	if err = tmpMFile.Close(); err != nil {
		return fmt.Errorf("close temp metadata for update: %w", err)
	}
	tmpMFile = nil

	if err = os.Rename(tmpPath, mPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename temp metadata to %s for update: %w", mPath, err)
	}
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
