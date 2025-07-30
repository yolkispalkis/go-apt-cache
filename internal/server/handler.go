package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/yolkispalkis/go-apt-cache/internal/cache"
	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/fetch"
	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

const (
	cacheStatusHit  = "HIT"
	cacheStatusMiss = "MISS"
)

type requestHandler struct {
	w   http.ResponseWriter
	r   *http.Request
	log *logging.Logger

	cache   cache.Manager
	fetcher *fetch.Coordinator
	cfg     *config.Config

	repo         config.Repository
	key          string
	upstreamURL  string
	cleanRelPath string
}

func (app *Application) newRequestHandler(w http.ResponseWriter, r *http.Request) (*requestHandler, error) {
	repo := r.Context().Value(repoContextKey).(config.Repository)
	relPath := chi.URLParam(r, "*")

	// двойная раскодировка — защита от %252e%252e/
	if un, err := url.PathUnescape(relPath); err == nil {
		relPath = un
		if un2, err2 := url.PathUnescape(relPath); err2 == nil {
			relPath = un2
		}
	}

	cleanRelPath := filepath.Clean(relPath)
	if strings.HasPrefix(cleanRelPath, "..") || cleanRelPath == ".." || cleanRelPath == "." {
		app.Logger.Warn().
			Str("repo", repo.Name).
			Str("remote_addr", r.RemoteAddr).
			Str("raw_path", relPath).
			Msg("Path traversal attempt blocked")
		return nil, errors.New("invalid path")
	}

	key := repo.Name + "/" + cleanRelPath
	return &requestHandler{
		w:            w,
		r:            r,
		log:          app.Logger.WithContext("key", key, "repo", repo.Name),
		cache:        app.Cache,
		fetcher:      app.Fetcher,
		cfg:          app.Config,
		repo:         repo,
		key:          key,
		upstreamURL:  repo.URL + cleanRelPath,
		cleanRelPath: cleanRelPath,
	}, nil
}

func (h *requestHandler) sendError(code int, msg string, err error) {
	h.log.Warn().Err(err).Msg(msg)
	http.Error(h.w, msg, code)
}

func (h *requestHandler) process() {
	meta, found := h.cache.Get(h.r.Context(), h.key)
	if !found {
		h.log.Info().Msg("Cache miss, fetching from upstream")
		h.fetchAndServe(nil)
		return
	}

	h.log.Debug().Msg("Cache hit")

	if meta.StatusCode == http.StatusNotFound {
		h.serveNegative(meta)
		return
	}
	if meta.IsStale(time.Now()) {
		h.log.Info().Msg("Revalidating stale item")
		h.fetchAndServe(meta)
		return
	}
	h.serveFromCache(meta)
}

func (h *requestHandler) serveFromCache(meta *cache.ItemMeta) {
	if util.ClientHasFreshVersion(h.r) {
		logConditionalRequest(h.r, meta, h.log)
		if util.CheckConditional(h.w, h.r, meta.Headers) {
			h.log.Debug().Msg("Client has up-to-date copy — 304")
			return
		}
	}
	util.CopyWhitelistedHeaders(h.w.Header(), meta.Headers)
	h.w.Header().Set("X-Cache-Status", cacheStatusHit)

	if h.r.Method == http.MethodHead {
		h.w.WriteHeader(meta.StatusCode)
		return
	}

	rc, err := h.cache.GetContent(h.r.Context(), h.key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			h.log.Warn().Msg("Metadata exists but content missing; refetching")
			h.cache.Delete(h.r.Context(), h.key)
			h.fetchAndServe(nil)
			return
		}
		h.sendError(http.StatusInternalServerError, "Cache content unavailable", err)
		return
	}
	defer rc.Close()

	h.w.WriteHeader(meta.StatusCode)
	buf := util.GetBuffer()
	defer util.ReturnBuffer(buf)

	if _, err := io.CopyBuffer(h.w, rc, buf); err != nil {
		h.log.Warn().Err(err).Msg("Copy cached content -> client failed")
	}
}

func (h *requestHandler) serveNegative(meta *cache.ItemMeta) {
	if meta.IsStale(time.Now()) {
		h.log.Info().Msg("Stale negative cache; refetching")
		h.cache.Delete(h.r.Context(), h.key)
		h.fetchAndServe(nil)
		return
	}
	util.CopyWhitelistedHeaders(h.w.Header(), meta.Headers)
	http.Error(h.w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
}

func (h *requestHandler) fetchAndServe(revalMeta *cache.ItemMeta) {
	opts := fetch.NewFetchOptions(h.r, revalMeta)
	resAny, _, shared := h.fetcher.Fetch(h.r.Context(), h.key, h.upstreamURL, opts)

	sfRes, ok := resAny.(*fetch.SharedFetch)
	if !ok {
		if resAny == nil {
			h.log.Warn().Msg("Fetch initiation failed (client disconnect?)")
			return
		}
		h.sendError(http.StatusInternalServerError, "singleflight type mismatch", fmt.Errorf("%T", resAny))
		return
	}

	if shared {
		h.log.Debug().Msg("Fetch was shared; serving from cache")
		if meta, ok := h.cache.Get(h.r.Context(), h.key); ok {
			h.serveFromCache(meta)
		} else {
			h.sendError(http.StatusInternalServerError, "Shared fetch finished but item absent", nil)
		}
		return
	}

	if sfRes.Err != nil {
		h.handleFetchError(sfRes.Err, sfRes.Result, revalMeta)
		return
	}
	h.handleFetchSuccess(sfRes.Result)
}

func (h *requestHandler) handleFetchError(err error, res *fetch.Result, revalMeta *cache.ItemMeta) {
	switch {
	case revalMeta != nil && (errors.Is(err, fetch.ErrNetwork) || errors.Is(err, fetch.ErrUpstreamServer)):
		h.log.Warn().Err(err).Msg("Upstream error; grace-mode – serve stale")
		h.serveFromCache(revalMeta)

	case errors.Is(err, fetch.ErrUpstreamNotFound):
		h.handleUpstream404(res)

	case errors.Is(err, fetch.ErrUpstreamNotModified):
		h.handleRevalidation(res, revalMeta)

	default:
		h.sendError(http.StatusBadGateway, "Upstream fetch failed", err)
	}
}

func (h *requestHandler) handleUpstream404(res *fetch.Result) {
	h.log.Info().Msg("Upstream 404 → store negative cache")
	if h.cfg.Cache.NegativeTTL > 0 {
		now := time.Now()
		meta := &cache.ItemMeta{
			Key:         h.key,
			UpstreamURL: h.upstreamURL,
			Headers:     util.CopyHeader(res.Header),
			StatusCode:  http.StatusNotFound,
			FetchedAt:   now,
			LastUsedAt:  now,
			ExpiresAt:   now.Add(h.cfg.Cache.NegativeTTL),
		}
		if err := h.cache.Put(h.r.Context(), meta); err != nil {
			h.log.Error().Err(err).Msg("Save negative cache failed")
		}
	}
	util.CopyWhitelistedHeaders(h.w.Header(), res.Header)
	http.Error(h.w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
}

func (h *requestHandler) handleRevalidation(res *fetch.Result, old *cache.ItemMeta) {
	if old == nil { // conditional miss – просто форвардим 304
		util.CopyWhitelistedHeaders(h.w.Header(), res.Header)
		h.w.WriteHeader(http.StatusNotModified)
		return
	}

	h.log.Info().Msg("Revalidation 304 OK – bump metadata")
	newMeta := *old
	newMeta.Headers = util.CopyHeader(old.Headers)

	freshHeaders := res.Header
	if len(freshHeaders) == 0 {
		freshHeaders = old.Headers
	}
	newMeta.ExpiresAt = cache.CalculateFreshness(freshHeaders, time.Now(), h.cleanRelPath, h.cfg.Cache.Overrides)
	newMeta.LastUsedAt = time.Now()
	util.UpdateCacheHeaders(newMeta.Headers, res.Header)

	if err := h.cache.Put(h.r.Context(), &newMeta); err != nil {
		h.log.Error().Err(err).Msg("Metadata update after 304 failed")
	}
	h.serveFromCache(&newMeta)
}

func (h *requestHandler) handleFetchSuccess(res *fetch.Result) {
	meta := h.buildMeta(res)

	util.CopyWhitelistedHeaders(h.w.Header(), meta.Headers)
	h.w.Header().Set("X-Cache-Status", cacheStatusMiss)

	if h.r.Method == http.MethodHead {
		h.w.WriteHeader(meta.StatusCode)
		_ = h.cache.Put(h.r.Context(), meta) // HEAD – только метаданные
		return
	}
	h.streamAndStore(res, meta)
}

func (h *requestHandler) buildMeta(res *fetch.Result) *cache.ItemMeta {
	now := time.Now()
	hdr := util.CopyHeader(res.Header)
	return &cache.ItemMeta{
		Key:         h.key,
		UpstreamURL: h.upstreamURL,
		FetchedAt:   now,
		LastUsedAt:  now,
		StatusCode:  res.Status,
		Headers:     hdr,
		Size:        res.Size,
		ExpiresAt:   cache.CalculateFreshness(hdr, now, h.cleanRelPath, h.cfg.Cache.Overrides),
	}
}

func (h *requestHandler) streamAndStore(res *fetch.Result, meta *cache.ItemMeta) {
	pr, pw := io.Pipe()
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		n, err := h.cache.PutContent(h.r.Context(), h.key, pr)
		if err != nil {
			if !util.IsClientDisconnectedError(err) {
				h.log.Error().Err(err).Msg("Write content to cache failed")
			}
			h.cache.Delete(context.Background(), h.key)
			return
		}
		meta.Size = n
		_ = h.cache.Put(h.r.Context(), meta)
	}()

	h.w.WriteHeader(meta.StatusCode)
	_, copyErr := io.Copy(h.w, io.TeeReader(res.Body, pw))

	if copyErr != nil {
		pw.CloseWithError(copyErr)
	} else {
		pw.Close()
	}
	wg.Wait()
}

func (app *Application) handleServeRepoContent(w http.ResponseWriter, r *http.Request) {
	h, err := app.newRequestHandler(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	h.process()
}

func (app *Application) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write([]byte("OK\nCache status endpoint is active.\n"))
}

func logConditionalRequest(r *http.Request, meta *cache.ItemMeta, log *logging.Logger) {
	ev := log.Debug()
	if v := r.Header.Get("If-None-Match"); v != "" {
		ev.Str("client_if_none_match", v)
	}
	if v := r.Header.Get("If-Modified-Since"); v != "" {
		ev.Str("client_if_modified_since", v)
	}
	if v := meta.Headers.Get("ETag"); v != "" {
		ev.Str("cached_etag", v)
	}
	if v := meta.Headers.Get("Last-Modified"); v != "" {
		ev.Str("cached_last_modified", v)
	}
	ev.Msg("Checking conditional request")
}
