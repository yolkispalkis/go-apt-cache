package server

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	pathpkg "path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bmatcuk/doublestar/v4"
	"github.com/go-chi/chi/v5"

	"github.com/yolkispalkis/go-apt-cache/internal/cache"
	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/fetch"
	"github.com/yolkispalkis/go-apt-cache/internal/log"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

type Application struct {
	cfg     *config.Config
	log     *log.Logger
	cache   cache.Manager
	fetcher *fetch.Coordinator
}

func NewApplication(cfg *config.Config, lg *log.Logger, cm cache.Manager, fc *fetch.Coordinator) *Application {
	return &Application{cfg: cfg, log: lg, cache: cm, fetcher: fc}
}

func (a *Application) Routes() http.Handler {
	r := chi.NewRouter()
	r.Use(a.recover(), a.accessLog())

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Add("Via", "1.1 go-apt-cache")
		_, _ = w.Write([]byte("go-apt-cache async RFC-compliant\n"))
	})
	r.Get("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Add("Via", "1.1 go-apt-cache")
		_, _ = w.Write([]byte("OK\n"))
	})

	r.Route("/{repo}", func(r chi.Router) {
		r.Use(a.repoContext())
		r.Get("/*", a.serve)
		r.Head("/*", a.serve)
	})
	return r
}

type ctxKey string

const repoKey ctxKey = "repo"

func (a *Application) repoContext() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			name := chi.URLParam(r, "repo")
			repo, ok := a.cfg.GetRepo(name)
			if !ok {
				http.NotFound(w, r)
				return
			}
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), repoKey, repo)))
		})
	}
}

func (a *Application) accessLog() func(http.Handler) http.Handler {
	lg := a.log.WithComponent("http")
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := util.NewRespLogWriter(w)
			next.ServeHTTP(ww, r)
			lg.Info().Str("method", r.Method).Str("path", r.URL.Path).
				Int("status", ww.Status()).Int64("bytes", ww.Bytes()).Dur("dur", time.Since(start)).
				Str("remote", r.RemoteAddr).Msg("req")
		})
	}
}

func (a *Application) recover() func(http.Handler) http.Handler {
	lg := a.log.WithComponent("panic")
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					w.Header().Set("Connection", "close")
					lg.Error().Interface("panic", rec).Msg("recovered")
					http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

func (a *Application) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	repo := r.Context().Value(repoKey).(config.Repository)
	raw := chi.URLParam(r, "*")
	if u1, err := url.PathUnescape(raw); err == nil {
		raw = u1
		if u2, err2 := url.PathUnescape(raw); err2 == nil {
			raw = u2
		}
	}
	trailing := strings.HasSuffix(raw, "/") && raw != "/"
	rel := pathpkg.Clean(raw)
	if rel == "." {
		rel = ""
	}
	if trailing && rel != "" {
		rel += "/"
	}
	if strings.HasPrefix(rel, "..") || rel == ".." {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}
	// remove leading slash for overrides matching
	rel = strings.TrimPrefix(rel, "/")

	key := repo.Name + "/" + rel
	up := repo.URL + rel

	// HIT?
	if meta, ok := a.cache.Get(r.Context(), key); ok {
		if a.mustRevalidate(meta.Headers) || a.isStale(meta) {
			a.revalidateOrServeStale(w, r, key, up, rel, meta)
			return
		}
		a.serveFromCache(w, r, key, meta)
		return
	}

	// MISS
	a.fetchAndMaybeStore(w, r, key, up, rel, nil)
}

func (a *Application) mustRevalidate(h http.Header) bool {
	cc := util.ParseCacheControl(h.Get("Cache-Control"))
	_, noCache := cc["no-cache"]
	_, must := cc["must-revalidate"]
	_, proxy := cc["proxy-revalidate"]
	return noCache || must || proxy
}

func (a *Application) isStale(m *cache.ItemMeta) bool {
	return m.ExpiresAt.IsZero() || time.Now().After(m.ExpiresAt)
}

func (a *Application) serveFromCache(w http.ResponseWriter, r *http.Request, key string, m *cache.ItemMeta) {
	// prepare headers upfront for HIT/304
	h := util.CopyHeader(m.Headers)
	h.Set("Age", a.mergeAgeFor(m))
	h.Add("Via", "1.1 go-apt-cache")
	util.CopyWhitelisted(w.Header(), h)
	w.Header().Set("X-Cache-Status", "HIT")

	// Conditional HIT -> 304 with validators
	if util.ClientHasValidators(r) && util.IsNotModified(r, m.Headers) {
		w.Header().Del("Content-Length") // avoid length on 304
		w.WriteHeader(http.StatusNotModified)
		return
	}

	// Non-2xx cached responses (e.g., negative cache 404) — no body
	if m.StatusCode < 200 || m.StatusCode >= 300 {
		w.WriteHeader(m.StatusCode)
		return
	}

	if r.Method == http.MethodHead {
		w.WriteHeader(m.StatusCode)
		return
	}

	rc, err := a.cache.GetContent(r.Context(), key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			_ = a.cache.Delete(r.Context(), key)
			a.fetchAndMaybeStore(w, r, key, m.UpstreamURL, relFromKey(key), nil)
			return
		}
		http.Error(w, "cache read failed", http.StatusInternalServerError)
		return
	}
	defer rc.Close()

	// If-Range: only strong ETag or HTTP-date; drop Range otherwise
	if ir := r.Header.Get("If-Range"); ir != "" {
		drop := true
		if len(ir) > 0 && ir[0] == '"' {
			et := m.Headers.Get("ETag")
			if et != "" && !strings.HasPrefix(et, "W/") && et == ir {
				drop = false
			}
		} else if t, err := http.ParseTime(ir); err == nil {
			if lm := m.Headers.Get("Last-Modified"); lm != "" {
				if lmt, err2 := http.ParseTime(lm); err2 == nil && !lmt.After(t) {
					drop = false
				}
			}
		}
		if drop {
			r2 := r.Clone(r.Context())
			r2.Header.Del("Range")
			r = r2
		}
	}

	if f, ok := rc.(*os.File); ok {
		// Let ServeContent handle Content-Length/Range
		w.Header().Del("Content-Length")
		mod, _ := http.ParseTime(m.Headers.Get("Last-Modified"))
		http.ServeContent(w, r, "", mod, f)
		return
	}

	// Stream content with known length
	w.Header().Set("Content-Length", strconv.FormatInt(m.Size, 10))
	w.WriteHeader(m.StatusCode)
	buf := util.GetBuffer()
	defer util.PutBuffer(buf)
	_, _ = io.CopyBuffer(w, rc, buf)
}

func (a *Application) mergeAgeFor(m *cache.ItemMeta) string {
	age := 0
	if v := m.Headers.Get("Age"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			age = n
		}
	}
	add := int(time.Since(m.FetchedAt).Seconds())
	if add > 0 {
		age += add
	}
	return strconv.Itoa(age)
}

func (a *Application) revalidateOrServeStale(w http.ResponseWriter, r *http.Request, key, up, rel string, stale *cache.ItemMeta) {
	cc := util.ParseCacheControl(stale.Headers.Get("Cache-Control"))
	now := time.Now()

	// stale-while-revalidate
	if s, ok := util.ParseUint(cc["stale-while-revalidate"]); ok {
		if now.Before(stale.ExpiresAt.Add(time.Duration(s) * time.Second)) {
			go a.backgroundRevalidate(key, up, rel, stale)
			a.serveFromCache(w, r, key, stale)
			return
		}
	}
	a.fetchAndMaybeStore(w, r, key, up, rel, stale)
}

func (a *Application) backgroundRevalidate(key, up, rel string, stale *cache.ItemMeta) {
	ctx, cancel := context.WithTimeout(context.Background(), a.cfg.Server.ReqTimeout)
	defer cancel()
	opts := fetch.NewOptions(&http.Request{}, stale.Headers.Get("ETag"), stale.Headers.Get("Last-Modified"))
	res, err, _ := a.fetcher.Fetch(ctx, "GET:"+key, up, opts)
	if err != nil || res == nil {
		return
	}
	if res.Body != nil {
		defer res.Body.Close()
	}

	if res.Status == http.StatusNotModified {
		upd := *stale
		upd.Headers = util.CopyHeader(stale.Headers)
		util.CopyWhitelisted(upd.Headers, res.Header)
		upd.LastUsedAt = time.Now()
		upd.ExpiresAt = a.computeExpiry(upd.Headers, time.Now(), rel)
		_ = a.cache.Put(context.Background(), &upd)
		return
	}
	if res.Status >= 200 && res.Status < 300 && res.Body != nil {
		now := time.Now()
		meta := &cache.ItemMeta{
			Key:         key,
			UpstreamURL: up,
			StatusCode:  res.Status,
			Headers:     util.CopyHeader(res.Header),
			FetchedAt:   now,
			LastUsedAt:  now,
			Size:        res.Size,
			ExpiresAt:   a.computeExpiry(res.Header, now, rel),
		}
		if err := a.cache.Put(context.Background(), meta); err != nil {
			return
		}
		pr, pw := io.Pipe()
		go func() { defer pw.Close(); _, _ = io.Copy(pw, res.Body) }()
		n, err := a.cache.PutContent(context.Background(), key, pr)
		if err == nil && n != meta.Size {
			meta.Size = n
			_ = a.cache.Put(context.Background(), meta)
		}
	}
}

func (a *Application) fetchAndMaybeStore(w http.ResponseWriter, r *http.Request, key, up, rel string, stale *cache.ItemMeta) {
	// Build upstream conditional options: client + stale validators
	opts := fetch.NewOptions(r, "", "")
	if r.Method == http.MethodHead {
		if opts == nil {
			opts = &fetch.Options{}
		}
		opts.UseHEAD = true
	}
	if stale != nil {
		opts = fetch.NewOptions(r, stale.Headers.Get("ETag"), stale.Headers.Get("Last-Modified"))
		if r.Method == http.MethodHead {
			opts.UseHEAD = true
		}
	}

	// If client requested Range — forward it and bypass caching
	rangeReq := r.Header.Get("Range") != ""
	if rangeReq {
		if opts == nil {
			opts = &fetch.Options{}
		}
		opts.Range = r.Header.Get("Range")
	}

	// include Range and validators in singleflight key
	sfKey := r.Method + ":" + key +
		"|rng=" + r.Header.Get("Range") +
		"|inm=" + r.Header.Get("If-None-Match") +
		"|ims=" + r.Header.Get("If-Modified-Since")

	res, err, shared := a.fetcher.Fetch(r.Context(), sfKey, up, opts)
	if err != nil && shared {
		if m, ok := a.cache.Get(r.Context(), key); ok {
			a.serveFromCache(w, r, key, m)
			return
		}
	}

	if err != nil {
		// stale-if-error
		if stale != nil {
			cc := util.ParseCacheControl(stale.Headers.Get("Cache-Control"))
			if s, ok := util.ParseUint(cc["stale-if-error"]); ok {
				if time.Now().Before(stale.ExpiresAt.Add(time.Duration(s) * time.Second)) {
					a.serveFromCache(w, r, key, stale)
					return
				}
			}
		}
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}

	if res.Body != nil {
		defer res.Body.Close()
	}

	w.Header().Add("Via", "1.1 go-apt-cache")

	// Always bypass cache for Range requests or upstream 206
	if rangeReq || res.Status == http.StatusPartialContent {
		util.CopyWhitelisted(w.Header(), res.Header)
		w.Header().Set("X-Cache-Status", "BYPASS")
		w.WriteHeader(res.Status)
		if r.Method == http.MethodGet && res.Body != nil {
			buf := util.GetBuffer()
			defer util.PutBuffer(buf)
			_, _ = io.CopyBuffer(w, res.Body, buf)
		}
		return
	}

	switch res.Status {
	case http.StatusNotModified:
		if stale == nil {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		upd := *stale
		upd.Headers = util.CopyHeader(stale.Headers)
		util.CopyWhitelisted(upd.Headers, res.Header)
		upd.LastUsedAt = time.Now()
		upd.ExpiresAt = a.computeExpiry(upd.Headers, time.Now(), rel)
		_ = a.cache.Put(r.Context(), &upd)
		a.serveFromCache(w, r, key, &upd)
		return

	case http.StatusNotFound:
		// Negative cache по конфигу
		if a.cfg.Cache.NegativeTTL > 0 {
			now := time.Now()
			meta := &cache.ItemMeta{
				Key:         key,
				UpstreamURL: up,
				StatusCode:  http.StatusNotFound,
				Headers:     util.CopyHeader(res.Header),
				FetchedAt:   now,
				LastUsedAt:  now,
				ExpiresAt:   now.Add(a.cfg.Cache.NegativeTTL),
			}
			_ = a.cache.Put(r.Context(), meta)
		}
		util.CopyWhitelisted(w.Header(), res.Header)
		w.Header().Set("X-Cache-Status", "MISS")
		w.WriteHeader(http.StatusNotFound)
		return

	default:
		// Forward other upstream 4xx as-is (no cache)
		if res.Status >= 400 && res.Status < 500 {
			util.CopyWhitelisted(w.Header(), res.Header)
			w.Header().Set("X-Cache-Status", "BYPASS")
			w.WriteHeader(res.Status)
			if r.Method == http.MethodGet && res.Body != nil {
				buf := util.GetBuffer()
				defer util.PutBuffer(buf)
				_, _ = io.CopyBuffer(w, res.Body, buf)
			}
			return
		}

		// 2xx/HEAD success
		cc := util.ParseCacheControl(res.Header.Get("Cache-Control"))
		if _, ok := cc["no-store"]; ok || res.Header.Get("Vary") == "*" || a.requestProhibitsStore(r, cc) {
			util.CopyWhitelisted(w.Header(), res.Header)
			w.Header().Set("X-Cache-Status", "MISS")
			w.WriteHeader(res.Status)
			if r.Method == http.MethodGet && res.Body != nil {
				buf := util.GetBuffer()
				defer util.PutBuffer(buf)
				_, _ = io.CopyBuffer(w, res.Body, buf)
			}
			return
		}

		now := time.Now()
		meta := &cache.ItemMeta{
			Key:         key,
			UpstreamURL: up,
			StatusCode:  res.Status,
			Headers:     util.CopyHeader(res.Header),
			FetchedAt:   now,
			LastUsedAt:  now,
			Size:        res.Size,
			ExpiresAt:   a.computeExpiry(res.Header, now, rel), // с учётом overrides
		}

		// если нет явной свежести и эвристика выключена, и override не матчится — не кэшируем
		if meta.ExpiresAt.Equal(now) && !a.hasExplicitFreshness(res.Header) && !a.overrideMatched(rel) && !a.cfg.Cache.HeuristicTTL10 {
			util.CopyWhitelisted(w.Header(), res.Header)
			w.Header().Set("X-Cache-Status", "MISS")
			w.WriteHeader(res.Status)
			if r.Method == http.MethodGet && res.Body != nil {
				buf := util.GetBuffer()
				defer util.PutBuffer(buf)
				_, _ = io.CopyBuffer(w, res.Body, buf)
			}
			return
		}

		util.CopyWhitelisted(w.Header(), meta.Headers)
		w.Header().Set("X-Cache-Status", "MISS")

		// Не кэшируем HEAD — чтобы не создавать мету без тела
		if r.Method == http.MethodHead {
			w.WriteHeader(meta.StatusCode)
			return
		}

		if err := a.cache.Put(r.Context(), meta); err != nil {
			http.Error(w, "meta write failed", http.StatusInternalServerError)
			return
		}

		pr, pw := io.Pipe()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := a.cache.PutContent(r.Context(), key, pr)
			if err != nil {
				if !util.IsClientDisconnect(err) {
					a.log.Error().Err(err).Msg("cache write failed")
				}
				_ = a.cache.Delete(context.Background(), key)
				return
			}
			if meta.Size != n {
				meta.Size = n
				_ = a.cache.Put(context.Background(), meta)
			}
		}()

		w.WriteHeader(meta.StatusCode)
		_, copyErr := io.Copy(w, io.TeeReader(res.Body, pw))
		if copyErr != nil {
			_ = pw.CloseWithError(copyErr)
		} else {
			_ = pw.Close()
		}
		wg.Wait()
	}
}

func (a *Application) hasExplicitFreshness(h http.Header) bool {
	cc := util.ParseCacheControl(h.Get("Cache-Control"))
	if _, ok := cc["s-maxage"]; ok {
		return true
	}
	if _, ok := cc["max-age"]; ok {
		return true
	}
	return h.Get("Expires") != ""
}

func (a *Application) overrideMatched(rel string) bool {
	for _, o := range a.cfg.Cache.Overrides {
		ok, _ := doublestar.Match(o.PathPattern, rel)
		if ok {
			return true
		}
	}
	return false
}

// computeExpiry: учитываем overrides, но не нарушаем no-store/private/no-cache/Vary:*.
func (a *Application) computeExpiry(h http.Header, now time.Time, rel string) time.Time {
	cc := util.ParseCacheControl(h.Get("Cache-Control"))
	// запреты на хранение в shared cache
	if _, ok := cc["no-store"]; ok {
		return time.Time{}
	}
	if h.Get("Vary") == "*" {
		return time.Time{}
	}
	// no-cache/must-revalidate/proxy-revalidate — немедленная протухлость
	if _, ok := cc["no-cache"]; ok {
		return now
	}
	if _, ok := cc["must-revalidate"]; ok {
		return now
	}
	if _, ok := cc["proxy-revalidate"]; ok {
		return now
	}

	// override TTL (жёсткая замена)
	for _, o := range a.cfg.Cache.Overrides {
		ok, _ := doublestar.Match(o.PathPattern, rel)
		if ok && o.TTL > 0 {
			return now.Add(o.TTL)
		}
	}

	// RFC: s-maxage > max-age > Expires
	if v := cc["s-maxage"]; v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			return now.Add(time.Duration(n) * time.Second)
		}
	}
	if v := cc["max-age"]; v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			return now.Add(time.Duration(n) * time.Second)
		}
	}
	if ex := h.Get("Expires"); ex != "" {
		if t, err := http.ParseTime(ex); err == nil {
			if t.After(now) {
				return t
			}
			return now
		}
	}

	// эвристика 10% (если включена)
	if a.cfg.Cache.HeuristicTTL10 {
		if lm := h.Get("Last-Modified"); lm != "" {
			if t, err := http.ParseTime(lm); err == nil {
				ttl := now.Sub(t) / 10
				if ttl < 0 {
					ttl = 0
				}
				return now.Add(ttl)
			}
		}
	}
	// по умолчанию — немедленно протухает
	return now
}

func (a *Application) requestProhibitsStore(req *http.Request, cc map[string]string) bool {
	if _, ok := cc["private"]; ok {
		return true
	}
	if req.Header.Get("Authorization") != "" {
		if _, pub := cc["public"]; !pub {
			if _, smax := cc["s-maxage"]; !smax {
				return true
			}
		}
	}
	return false
}

// derive relative path from cache key "<repo>/<rel>"
func relFromKey(key string) string {
	if i := strings.IndexByte(key, '/'); i >= 0 && i+1 < len(key) {
		return key[i+1:]
	}
	return ""
}
