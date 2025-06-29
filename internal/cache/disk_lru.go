package cache

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

const numShards = 64

type ShardedManager struct {
	shards []*lruShard
	log    *logging.Logger
}

func NewDiskLRU(cfg config.CacheConfig, logger *logging.Logger) (Manager, error) {
	if !cfg.Enabled {
		logger.Info().Msg("Cache is disabled in configuration.")
		return &noopManager{}, nil
	}

	log := logger.WithComponent("ShardedDiskLRU")

	maxBytes, err := util.ParseSize(cfg.MaxSize)
	if err != nil {
		return nil, fmt.Errorf("invalid cache MaxSize %q: %w", cfg.MaxSize, err)
	}
	maxBytesPerShard := maxBytes / numShards

	store, err := newDiskStore(cfg.Dir, log)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize disk store at %s: %w", cfg.Dir, err)
	}

	manager := &ShardedManager{
		shards: make([]*lruShard, numShards),
		log:    log,
	}

	var wg sync.WaitGroup
	initErrs := make(chan error, numShards)

	for i := 0; i < numShards; i++ {
		wg.Add(1)
		go func(shardIndex int) {
			defer wg.Done()
			shard, err := newLRUShard(cfg, log, store, maxBytesPerShard)
			if err != nil {
				initErrs <- fmt.Errorf("failed to create shard %d: %w", shardIndex, err)
				return
			}
			manager.shards[shardIndex] = shard
		}(i)
	}

	wg.Wait()
	close(initErrs)

	for err := range initErrs {
		return nil, err
	}

	go manager.initialScan()

	return manager, nil
}

func (sm *ShardedManager) getShard(key string) *lruShard {
	hasher := fnv.New64a()
	hasher.Write([]byte(key))
	return sm.shards[hasher.Sum64()&(numShards-1)]
}

func (sm *ShardedManager) initialScan() {
	sm.log.Info().Msg("Starting initial disk scan for all shards...")
	startTime := time.Now()

	metaFiles, err := sm.shards[0].store.ScanMetaFiles()
	if err != nil {
		sm.log.Error().Err(err).Msg("Failed during initial scan file listing")
		for _, s := range sm.shards {
			s.initErr = err
			s.ready = true
		}
		return
	}

	shardMetaPaths := make([][]string, numShards)
	for i := range shardMetaPaths {
		shardMetaPaths[i] = make([]string, 0)
	}

	for _, mPath := range metaFiles {
		meta, err := sm.shards[0].store.ReadMeta(mPath)
		if err != nil {
			sm.log.Warn().Err(err).Str("path", mPath).Msg("Could not read meta for sharding, deleting artifact")
			key := strings.TrimSuffix(filepath.Base(mPath), ".meta")
			sm.shards[0].store.Delete(key)
			continue
		}
		shardIndex := sm.getShard(meta.Key).shardIndex()
		shardMetaPaths[shardIndex] = append(shardMetaPaths[shardIndex], mPath)
	}

	var wg sync.WaitGroup
	for i, s := range sm.shards {
		wg.Add(1)
		go func(shard *lruShard, paths []string) {
			defer wg.Done()
			shard.populateFromScan(paths)
		}(s, shardMetaPaths[i])
	}
	wg.Wait()

	sm.log.Info().Dur("duration", time.Since(startTime)).Msg("All shards finished initial scan.")
}

func (sm *ShardedManager) Get(ctx context.Context, key string) (*ItemMeta, bool) {
	return sm.getShard(key).Get(ctx, key)
}

func (sm *ShardedManager) Put(ctx context.Context, meta *ItemMeta) error {
	return sm.getShard(meta.Key).Put(ctx, meta)
}

func (sm *ShardedManager) Delete(ctx context.Context, key string) error {
	return sm.getShard(key).Delete(ctx, key)
}

func (sm *ShardedManager) GetContent(ctx context.Context, key string) (io.ReadCloser, error) {
	return sm.getShard(key).GetContent(ctx, key)
}

func (sm *ShardedManager) PutContent(ctx context.Context, key string, r io.Reader) (int64, error) {
	return sm.getShard(key).PutContent(ctx, key, r)
}

func (sm *ShardedManager) Close() {
	var wg sync.WaitGroup
	for _, s := range sm.shards {
		wg.Add(1)
		go func(shard *lruShard) {
			defer wg.Done()
			shard.Close()
		}(s)
	}
	wg.Wait()
	sm.log.Info().Msg("All cache shards closed.")
}

type lruShard struct {
	cfg      config.CacheConfig
	log      *logging.Logger
	store    *diskStore
	maxBytes int64

	mu           sync.RWMutex
	items        map[string]*list.Element
	lruList      *list.List
	currentBytes atomic.Int64

	initErr     error
	ready       bool
	metaBatcher *metadataBatcher
}

func newLRUShard(cfg config.CacheConfig, logger *logging.Logger, store *diskStore, maxBytes int64) (*lruShard, error) {
	batchInterval := 30 * time.Second
	shard := &lruShard{
		cfg:         cfg,
		log:         logger,
		store:       store,
		maxBytes:    maxBytes,
		items:       make(map[string]*list.Element),
		lruList:     list.New(),
		metaBatcher: newMetadataBatcher(store, logger, batchInterval),
	}
	return shard, nil
}

func (s *lruShard) shardIndex() uint64 {
	// Простой способ получить "ID" шарда для распределения файлов.
	// Мы не можем использовать адрес памяти, так как он не постоянен.
	// Вместо этого, мы можем использовать хэш от чего-то уникального,
	// но для простоты, мы будем распределять файлы в `initialScan` менеджера.
	// Эта функция здесь для примера, как можно было бы это сделать.
	// В текущей реализации она не используется для определения индекса.
	return 0
}

func (s *lruShard) populateFromScan(metaPaths []string) {
	s.log.Debug().Int("files", len(metaPaths)).Msg("Shard starting population from scan")
	itemsToSort := make([]*ItemMeta, 0, len(metaPaths))
	var totalSize int64

	for _, mPath := range metaPaths {
		meta, err := s.store.ReadMeta(mPath)
		if err != nil {
			s.log.Warn().Err(err).Str("path", mPath).Msg("Failed to read metadata, removing artifact")
			key := strings.TrimSuffix(filepath.Base(mPath), ".meta")
			s.store.Delete(key)
			continue
		}
		itemsToSort = append(itemsToSort, meta)
		totalSize += meta.Size
	}

	sort.Slice(itemsToSort, func(i, j int) bool {
		return itemsToSort[i].LastUsedAt.Before(itemsToSort[j].LastUsedAt)
	})

	s.mu.Lock()
	for _, meta := range itemsToSort {
		entry := &lruEntry{key: meta.Key, meta: meta}
		elem := s.lruList.PushFront(entry)
		s.items[meta.Key] = elem
	}
	s.mu.Unlock()
	s.currentBytes.Store(totalSize)

	s.log.Debug().
		Int("items", len(itemsToSort)).
		Str("size", util.FormatSize(totalSize)).
		Msg("Shard population complete")

	s.ready = true
}

func (s *lruShard) checkReady() error {
	if !s.ready {
		return errors.New("cache shard is not ready (initial scan in progress)")
	}
	return s.initErr
}

func (s *lruShard) Get(ctx context.Context, key string) (*ItemMeta, bool) {
	if err := s.checkReady(); err != nil {
		s.log.Error().Err(err).Msg("Cache shard is not ready")
		return nil, false
	}

	s.mu.RLock()
	elem, exists := s.items[key]
	s.mu.RUnlock()

	if !exists {
		return nil, false
	}

	go s.MarkUsed(ctx, key)

	return elem.Value.(*lruEntry).meta, true
}

func (s *lruShard) Put(ctx context.Context, meta *ItemMeta) error {
	if err := s.checkReady(); err != nil {
		return err
	}

	if err := s.store.WriteMetadata(meta); err != nil {
		return fmt.Errorf("disk store write for key %s: %w", meta.Key, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if oldElem, exists := s.items[meta.Key]; exists {
		oldMeta := oldElem.Value.(*lruEntry).meta
		s.currentBytes.Add(-oldMeta.Size)
		s.lruList.Remove(oldElem)
	}

	entry := &lruEntry{key: meta.Key, meta: meta}
	elem := s.lruList.PushFront(entry)
	s.items[meta.Key] = elem
	s.currentBytes.Add(meta.Size)

	s.log.Debug().Str("key", meta.Key).Str("size", util.FormatSize(meta.Size)).Msg("Item metadata added/updated in shard")
	return nil
}

func (s *lruShard) MarkUsed(ctx context.Context, key string) {
	s.mu.Lock()
	elem, exists := s.items[key]
	if !exists {
		s.mu.Unlock()
		return
	}

	meta := elem.Value.(*lruEntry).meta
	meta.LastUsedAt = time.Now()
	s.lruList.MoveToFront(elem)

	metaCopy := *meta
	metaCopy.Headers = util.CopyHeader(meta.Headers)
	s.mu.Unlock()

	s.metaBatcher.schedule(key, &metaCopy)
}

func (s *lruShard) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	if elem, exists := s.items[key]; exists {
		meta := elem.Value.(*lruEntry).meta
		s.currentBytes.Add(-meta.Size)
		s.lruList.Remove(elem)
		delete(s.items, key)
	}
	s.mu.Unlock()

	return s.store.Delete(key)
}

func (s *lruShard) GetContent(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.store.GetContent(key)
}

func (s *lruShard) PutContent(ctx context.Context, key string, r io.Reader) (int64, error) {
	s.ensureSpace(0)
	return s.store.PutContent(key, r)
}

func (s *lruShard) Close() {
	if s.metaBatcher != nil {
		s.metaBatcher.close()
	}
}

func (s *lruShard) ensureSpace(requiredSize int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for s.currentBytes.Load()+requiredSize > s.maxBytes && s.lruList.Len() > 0 {
		elem := s.lruList.Back()
		if elem == nil {
			return
		}
		entry := s.lruList.Remove(elem).(*lruEntry)
		delete(s.items, entry.key)
		s.currentBytes.Add(-entry.meta.Size)

		s.log.Info().
			Str("key", entry.key).
			Str("size", util.FormatSize(entry.meta.Size)).
			Time("last_used", entry.meta.LastUsedAt).
			Msg("Evicting item from shard")

		go s.store.Delete(entry.key)
	}
}

type lruEntry struct {
	key  string
	meta *ItemMeta
}

type metadataUpdate struct {
	Key  string
	Meta *ItemMeta
}

type metadataBatcher struct {
	updates chan metadataUpdate
	ticker  *time.Ticker
	store   *diskStore
	log     *logging.Logger
	done    chan struct{}
	wg      sync.WaitGroup
}

func newMetadataBatcher(store *diskStore, logger *logging.Logger, interval time.Duration) *metadataBatcher {
	mb := &metadataBatcher{
		updates: make(chan metadataUpdate, 1000),
		ticker:  time.NewTicker(interval),
		store:   store,
		log:     logger,
		done:    make(chan struct{}),
	}
	mb.wg.Add(1)
	go mb.run()
	return mb
}

func (mb *metadataBatcher) run() {
	defer mb.wg.Done()
	updates := make(map[string]*ItemMeta)

	for {
		select {
		case update := <-mb.updates:
			if oldMeta, exists := updates[update.Key]; exists {
				util.ReturnHeader(oldMeta.Headers)
			}
			updates[update.Key] = update.Meta
		case <-mb.ticker.C:
			if len(updates) > 0 {
				mb.flush(updates)
				updates = make(map[string]*ItemMeta)
			}
		case <-mb.done:
			if len(updates) > 0 {
				mb.flush(updates)
			}
			return
		}
	}
}

func (mb *metadataBatcher) flush(updates map[string]*ItemMeta) {
	for _, meta := range updates {
		if err := mb.store.WriteMetadata(meta); err != nil {
			mb.log.Warn().Err(err).Str("key", meta.Key).Msg("Failed to batch update metadata")
		}
		util.ReturnHeader(meta.Headers)
	}
	mb.log.Debug().Int("count", len(updates)).Msg("Batch updated metadata files")
}

func (mb *metadataBatcher) schedule(key string, meta *ItemMeta) {
	select {
	case mb.updates <- metadataUpdate{Key: key, Meta: meta}:
	default:
		mb.log.Warn().Str("key", key).Msg("Metadata update queue full, dropping update")
		util.ReturnHeader(meta.Headers)
	}
}

func (mb *metadataBatcher) close() {
	close(mb.done)
	mb.ticker.Stop()
	mb.wg.Wait()
}
