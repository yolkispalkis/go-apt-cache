package cache

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

const (
	numShards        = 64
	defaultQueueSize = 1000
	batchInterval    = 30 * time.Second
	dropLogRateLimit = 10 * time.Second
)

type ShardedManager struct {
	shards []*lruShard
	store  *diskStore
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
		store:  store,
		log:    log,
	}

	for i := 0; i < numShards; i++ {
		shard, err := newLRUShard(i, cfg, log, store, maxBytesPerShard)
		if err != nil {
			return nil, fmt.Errorf("failed to create shard %d: %w", i, err)
		}
		manager.shards[i] = shard
	}

	go func() {
		if err := manager.initialScan(); err != nil {
			log.Error().Err(err).Msg("Initial background scan failed")
		}
	}()

	return manager, nil
}

func (sm *ShardedManager) getShardIndex(key string) uint64 {
	hasher := fnv.New64a()
	hasher.Write([]byte(key))
	return hasher.Sum64() & (numShards - 1)
}

func (sm *ShardedManager) getShard(key string) *lruShard {
	return sm.shards[sm.getShardIndex(key)]
}

func (sm *ShardedManager) initialScan() error {
	sm.log.Info().Msg("Starting initial disk scan for all shards...")
	startTime := time.Now()

	var metaFiles []string
	var scanErr error
	for attempt := 1; attempt <= 3; attempt++ {
		metaFiles, scanErr = sm.store.ScanMetaFiles()
		if scanErr == nil {
			break
		}
		sm.log.Warn().Err(scanErr).Int("attempt", attempt).Msg("Failed during initial scan, retrying")
		time.Sleep(time.Second)
	}
	if scanErr != nil {
		return fmt.Errorf("failed initial scan after retries: %w", scanErr)
	}

	shardMetas := make([][]*ItemMeta, numShards)
	for i := range shardMetas {
		shardMetas[i] = make([]*ItemMeta, 0)
	}

	for _, mPath := range metaFiles {
		meta, err := sm.store.ReadMeta(mPath)
		if err != nil {
			sm.log.Warn().Err(err).Str("path", mPath).Msg("Could not read meta for sharding, deleting artifact")
			_ = os.Remove(mPath)
			_ = os.Remove(strings.TrimSuffix(mPath, metaSuffix) + contentSuffix)
			continue
		}
		targetShardIndex := sm.getShardIndex(meta.Key)
		shardMetas[targetShardIndex] = append(shardMetas[targetShardIndex], meta)
	}

	var wg sync.WaitGroup
	errs := make(chan error, numShards)
	for i, s := range sm.shards {
		wg.Add(1)
		go func(shard *lruShard, metas []*ItemMeta, idx int) {
			defer wg.Done()
			if popErr := shard.populateFromScan(metas); popErr != nil {
				errs <- fmt.Errorf("shard %d: %w", idx, popErr)
			}
		}(s, shardMetas[i], i)
	}
	wg.Wait()
	close(errs)

	var allErrs []error
	for err := range errs {
		allErrs = append(allErrs, err)
	}
	if len(allErrs) > 0 {
		return fmt.Errorf("initial scan errors: %v", allErrs)
	}

	sm.log.Info().Dur("duration", time.Since(startTime)).Int("files", len(metaFiles)).Msg("All shards finished initial scan.")
	return nil
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
	index        int
	cfg          config.CacheConfig
	log          *logging.Logger
	store        *diskStore
	maxBytes     int64
	mu           sync.RWMutex
	items        map[string]*list.Element
	lruList      *list.List
	currentBytes atomic.Int64
	initErr      error
	ready        bool
	metaBatcher  *metadataBatcher
}

func newLRUShard(index int, cfg config.CacheConfig, logger *logging.Logger, store *diskStore, maxBytes int64) (*lruShard, error) {
	shardLogger := logger.WithContext("shard", index)
	shard := &lruShard{
		index:       index,
		cfg:         cfg,
		log:         shardLogger,
		store:       store,
		maxBytes:    maxBytes,
		items:       make(map[string]*list.Element),
		lruList:     list.New(),
		metaBatcher: newMetadataBatcher(store, shardLogger, batchInterval, defaultQueueSize),
	}
	return shard, nil
}

func (s *lruShard) populateFromScan(metas []*ItemMeta) error {
	s.log.Debug().Int("items", len(metas)).Msg("Shard starting population from scan")
	itemsToSort := make([]*ItemMeta, len(metas))
	copy(itemsToSort, metas)
	var totalSize int64

	for _, meta := range itemsToSort {
		totalSize += meta.Size
	}

	sort.Slice(itemsToSort, func(i, j int) bool {
		return itemsToSort[i].LastUsedAt.After(itemsToSort[j].LastUsedAt)
	})

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, meta := range itemsToSort {
		entry := &lruEntry{key: meta.Key, meta: meta}
		elem := s.lruList.PushFront(entry)
		s.items[meta.Key] = elem
	}
	s.currentBytes.Store(totalSize)

	s.log.Debug().
		Int("items", len(itemsToSort)).
		Str("size", util.FormatSize(totalSize)).
		Msg("Shard population complete")

	s.ready = true
	return nil
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

	metaCopy := *elem.Value.(*lruEntry).meta
	metaCopy.Headers = util.CopyHeader(metaCopy.Headers)
	return &metaCopy, true
}

func (s *lruShard) Put(ctx context.Context, meta *ItemMeta) error {
	if err := s.checkReady(); err != nil {
		return err
	}

	var sizeDelta int64
	s.mu.RLock()
	if oldElem, exists := s.items[meta.Key]; exists {
		sizeDelta = meta.Size - oldElem.Value.(*lruEntry).meta.Size
	} else {
		sizeDelta = meta.Size
	}
	s.mu.RUnlock()

	if err := s.store.WriteMetadata(meta); err != nil {
		return fmt.Errorf("disk store write for key %s: %w", meta.Key, err)
	}

	s.ensureSpace(sizeDelta)

	s.mu.Lock()
	defer s.mu.Unlock()

	if oldElem, exists := s.items[meta.Key]; exists {
		oldMeta := oldElem.Value.(*lruEntry).meta
		s.currentBytes.Add(-oldMeta.Size)
		s.lruList.Remove(oldElem)
		util.ReturnHeader(oldMeta.Headers)
	}

	entry := &lruEntry{key: meta.Key, meta: meta}
	elem := s.lruList.PushFront(entry)
	s.items[meta.Key] = elem
	s.currentBytes.Add(meta.Size)
	if s.currentBytes.Load() < 0 {
		s.log.Warn().Str("key", meta.Key).Msg("Negative cache size detected, resetting to 0")
		s.currentBytes.Store(0)
	}

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

	if ctx.Err() != nil {
		util.ReturnHeader(metaCopy.Headers)
		return
	}
	s.metaBatcher.schedule(key, &metaCopy)
}

func (s *lruShard) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	if elem, exists := s.items[key]; exists {
		meta := elem.Value.(*lruEntry).meta
		s.currentBytes.Add(-meta.Size)
		s.lruList.Remove(elem)
		delete(s.items, key)
		util.ReturnHeader(meta.Headers)
	}
	s.mu.Unlock()

	return s.store.Delete(key)
}

func (s *lruShard) GetContent(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.store.GetContent(key)
}

func (s *lruShard) PutContent(ctx context.Context, key string, r io.Reader) (int64, error) {
	return s.store.PutContent(key, r)
}

func (s *lruShard) Close() {
	if s.metaBatcher != nil {
		s.metaBatcher.close()
	}
}

func (s *lruShard) ensureSpace(sizeDelta int64) {
	if sizeDelta <= 0 || s.maxBytes <= 0 || s.currentBytes.Load()+sizeDelta <= s.maxBytes {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for s.currentBytes.Load()+sizeDelta > s.maxBytes && s.lruList.Len() > 0 {
		if err := s.evictOne(); err != nil {
			s.log.Warn().Err(err).Msg("Failed to evict item during space ensuring")
		}
	}
}

func (s *lruShard) evictOne() error {
	elem := s.lruList.Back()
	if elem == nil {
		return nil
	}
	entry := s.lruList.Remove(elem).(*lruEntry)
	delete(s.items, entry.key)
	s.currentBytes.Add(-entry.meta.Size)
	util.ReturnHeader(entry.meta.Headers)

	s.log.Info().
		Str("key", entry.key).
		Str("size", util.FormatSize(entry.meta.Size)).
		Time("last_used", entry.meta.LastUsedAt).
		Msg("Evicting item from shard")

	if err := s.store.Delete(entry.key); err != nil {
		s.log.Warn().Err(err).Str("key", entry.key).Msg("Failed to delete evicted item files, retrying once")
		time.Sleep(100 * time.Millisecond)
		if err := s.store.Delete(entry.key); err != nil {
			s.log.Error().Err(err).Str("key", entry.key).Msg("Failed to delete evicted item files after retry; orphan files possible")
			return err
		}
	}
	return nil
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
	updates      chan metadataUpdate
	ticker       *time.Ticker
	store        *diskStore
	log          *logging.Logger
	done         chan struct{}
	wg           sync.WaitGroup
	closeOnce    sync.Once
	droppedCount atomic.Int64
	lastDropLog  time.Time
}

func newMetadataBatcher(store *diskStore, logger *logging.Logger, interval time.Duration, queueSize int) *metadataBatcher {
	mb := &metadataBatcher{
		updates: make(chan metadataUpdate, queueSize),
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
		mb.droppedCount.Add(1)
		now := time.Now()
		if now.Sub(mb.lastDropLog) > dropLogRateLimit {
			mb.log.Warn().Str("key", key).Int64("total_dropped", mb.droppedCount.Load()).Msg("Metadata update queue full, dropping update")
			mb.lastDropLog = now
		}
		util.ReturnHeader(meta.Headers)
	}
}

func (mb *metadataBatcher) close() {
	mb.closeOnce.Do(func() {
		close(mb.done)
	})
	mb.ticker.Stop()
	mb.wg.Wait()
}
