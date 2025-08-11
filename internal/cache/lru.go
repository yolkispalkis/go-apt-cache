package cache

import (
	"bufio"
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/log"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

type ItemMeta struct {
	Key         string      `json:"key"`
	UpstreamURL string      `json:"upstreamURL"`
	StatusCode  int         `json:"statusCode"`
	Headers     http.Header `json:"headers"`
	Size        int64       `json:"size"`
	FetchedAt   time.Time   `json:"fetchedAt"`
	LastUsedAt  time.Time   `json:"lastUsedAt"`
	ExpiresAt   time.Time   `json:"expiresAt"`
}

type Manager interface {
	Get(ctx context.Context, key string) (*ItemMeta, bool)
	Put(ctx context.Context, meta *ItemMeta) error
	Delete(ctx context.Context, key string) error
	GetContent(ctx context.Context, key string) (io.ReadCloser, error)
	PutContent(ctx context.Context, key string, r io.Reader) (int64, error)
	Close()
}

type noop struct{}

func (*noop) Get(context.Context, string) (*ItemMeta, bool)                { return nil, false }
func (*noop) Put(context.Context, *ItemMeta) error                         { return nil }
func (*noop) Delete(context.Context, string) error                         { return nil }
func (*noop) GetContent(context.Context, string) (io.ReadCloser, error)    { return nil, os.ErrNotExist }
func (*noop) PutContent(context.Context, string, io.Reader) (int64, error) { return 0, nil }
func (*noop) Close()                                                       {}

type entry struct {
	key  string
	meta *ItemMeta
}

type LRU struct {
	mu       sync.Mutex
	maxBytes int64
	curBytes int64
	index    map[string]*list.Element
	order    *list.List
	dir      string
	log      *log.Logger
}

func NewLRU(cfg config.CacheConfig, lg *log.Logger) (Manager, error) {
	if !cfg.Enabled {
		lg.Info().Msg("cache disabled")
		return &noop{}, nil
	}
	if cfg.CleanOnStart {
		_ = os.RemoveAll(cfg.Dir)
	}
	if err := os.MkdirAll(cfg.Dir, 0750); err != nil {
		return nil, err
	}
	max, err := util.ParseSize(cfg.MaxSize)
	if err != nil {
		return nil, err
	}
	l := &LRU{
		maxBytes: max,
		index:    map[string]*list.Element{},
		order:    list.New(),
		dir:      cfg.Dir,
		log:      lg.WithComponent("cache"),
	}
	_ = l.scan()
	l.trim()
	return l, nil
}

const (
	sMeta = ".meta.json"
	sData = ".content"
)

func (l *LRU) pathFor(key, suf string) string {
	sum := sha256.Sum256([]byte(key))
	hexs := hex.EncodeToString(sum[:])
	return filepath.Join(l.dir, hexs[:2], hexs[2:4], hexs+suf)
}

func (l *LRU) scan() error {
	var metas []*ItemMeta
	_ = filepath.WalkDir(l.dir, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(d.Name(), sMeta) {
			return nil
		}
		f, err := os.Open(p)
		if err != nil {
			return nil
		}
		defer f.Close()
		var m ItemMeta
		if err := json.NewDecoder(bufio.NewReader(f)).Decode(&m); err == nil {
			metas = append(metas, &m)
		} else {
			_ = os.Remove(p)
			_ = os.Remove(strings.TrimSuffix(p, sMeta) + sData)
		}
		return nil
	})
	sort.Slice(metas, func(i, j int) bool { return metas[i].LastUsedAt.After(metas[j].LastUsedAt) })
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, m := range metas {
		el := l.order.PushFront(&entry{key: m.Key, meta: m})
		l.index[m.Key] = el
		l.curBytes += m.Size
	}
	return nil
}

func (l *LRU) writeMeta(m *ItemMeta) error {
	p := l.pathFor(m.Key, sMeta)
	if err := os.MkdirAll(filepath.Dir(p), 0750); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), "m-")
	if err != nil {
		return err
	}
	enc := json.NewEncoder(tmp)
	enc.SetIndent("", "  ")
	if err := enc.Encode(m); err != nil {
		tmp.Close()
		_ = os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), p)
}

func (l *LRU) Get(ctx context.Context, key string) (*ItemMeta, bool) {
	l.mu.Lock()
	el, ok := l.index[key]
	if !ok {
		l.mu.Unlock()
		return nil, false
	}
	l.order.MoveToFront(el)
	m := el.Value.(*entry).meta
	m.LastUsedAt = time.Now()
	cp := *m
	cp.Headers = util.CopyHeader(m.Headers)
	l.mu.Unlock()
	// best-effort update meta without holding lock
	go func(mm ItemMeta) { _ = l.writeMeta(&mm) }(cp)
	return &cp, true
}

func (l *LRU) Put(ctx context.Context, m *ItemMeta) error {
	mm := *m
	mm.Headers = util.CopyHeader(m.Headers)
	if err := l.writeMeta(&mm); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if el, ok := l.index[mm.Key]; ok {
		l.curBytes -= el.Value.(*entry).meta.Size
		l.order.Remove(el)
	}
	el := l.order.PushFront(&entry{key: mm.Key, meta: &mm})
	l.index[mm.Key] = el
	l.curBytes += mm.Size
	go l.trim()
	return nil
}

func (l *LRU) Delete(ctx context.Context, key string) error {
	l.mu.Lock()
	if el, ok := l.index[key]; ok {
		l.curBytes -= el.Value.(*entry).meta.Size
		l.order.Remove(el)
		delete(l.index, key)
	}
	l.mu.Unlock()
	_ = os.Remove(l.pathFor(key, sData))
	_ = os.Remove(l.pathFor(key, sMeta))
	return nil
}

func (l *LRU) GetContent(ctx context.Context, key string) (io.ReadCloser, error) {
	return os.Open(l.pathFor(key, sData))
}

func (l *LRU) PutContent(ctx context.Context, key string, r io.Reader) (int64, error) {
	p := l.pathFor(key, sData)
	if err := os.MkdirAll(filepath.Dir(p), 0750); err != nil {
		return 0, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), "c-")
	if err != nil {
		return 0, err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()

	buf := util.GetBuffer()
	n, werr := io.CopyBuffer(tmp, r, buf)
	util.PutBuffer(buf)
	if cerr := tmp.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		return 0, werr
	}
	if err := os.Rename(tmp.Name(), p); err != nil {
		return 0, err
	}
	return n, nil
}

func (l *LRU) Close() { l.log.Info().Msg("cache closed") }

func (l *LRU) trim() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for l.maxBytes > 0 && l.curBytes > l.maxBytes {
		back := l.order.Back()
		if back == nil {
			return
		}
		en := l.order.Remove(back).(*entry)
		delete(l.index, en.key)
		l.curBytes -= en.meta.Size
		go func(k string) {
			_ = os.Remove(l.pathFor(k, sData))
			_ = os.Remove(l.pathFor(k, sMeta))
		}(en.key)
	}
}
