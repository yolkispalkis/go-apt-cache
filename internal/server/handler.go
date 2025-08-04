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
	"strconv"
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

	if u1, err := url.PathUnescape(relPath); err == nil {
		relPath = u1
		if u2, err2 := url.PathUnescape(relPath); err2 == nil {
			relPath = u2
		}
	}
	hasTrailing := strings.HasSuffix(relPath, "/") && relPath != "/"
	cleanRelPath := filepath.Clean(relPath)
	if cleanRelPath == "." {
		cleanRelPath = ""
	}
	if hasTrailing && cleanRelPath != "" {
		cleanRelPath += "/"
	}

	if strings.HasPrefix(cleanRelPath, "..") || cleanRelPath == ".." {
		app.Logger.Warn().Str("repo", repo.Name).Str("remote_addr", r.RemoteAddr).Str("raw_path", relPath).Msg("Path traversal blocked")
		return nil, errors.New("invalid path")
	}

	key := repo.Name + "/"
	if cleanRelPath != "" {
		key += cleanRelPath
	}

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
	meta, ok := h.cache.Get(h.r.Context(), h.key)
	if !ok {
		h.log.Info().Msg("Cache miss, fetching")
		h.fetchAndServe(nil)
		return
	}

	switch {
	case meta.StatusCode == http.StatusNotFound:
		h.serveNegative(meta)
	case meta.IsStale(time.Now()):
		h.log.Info().Msg("Revalidating stale item")
		h.fetchAndServe(meta)
	default:
		h.serveFromCache(meta)
	}
}

func (h *requestHandler) serveFromCache(meta *cache.ItemMeta) {
	if util.ClientHasFreshVersion(h.r) {
		logConditionalRequest(h.r, meta, h.log)
		if util.CheckConditional(h.w, h.r, meta.Headers) {
			return
		}
	}

	util.CopyWhitelistedHeaders(h.w.Header(), meta.Headers)
	h.w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
	h.w.Header().Set("X-Cache-Status", cacheStatusHit)

	if h.r.Method == http.MethodHead {
		h.w.WriteHeader(meta.StatusCode)
		return
	}

	content, err := h.cache.GetContent(h.r.Context(), h.key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			h.cache.Delete(h.r.Context(), h.key)
			h.fetchAndServe(nil)
			return
		}
		h.sendError(http.StatusInternalServerError, "Cache content unavailable", err)
		return
	}
	defer content.Close()

	h.w.WriteHeader(meta.StatusCode)
	buf := util.GetBuffer()
	defer util.ReturnBuffer(buf)
	_, _ = io.CopyBuffer(h.w, content, buf)
}

func (h *requestHandler) serveNegative(meta *cache.ItemMeta) {
	if meta.IsStale(time.Now()) {
		h.cache.Delete(h.r.Context(), h.key)
		h.fetchAndServe(nil)
		return
	}
	util.CopyWhitelistedHeaders(h.w.Header(), meta.Headers)
	http.Error(h.w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
}

func (h *requestHandler) fetchAndServe(revalMeta *cache.ItemMeta) {
	opts := fetch.NewFetchOptions(h.r, revalMeta)
	if revalMeta != nil {
		opts.UseHEAD = true
	}

	raw, _, shared := h.fetcher.Fetch(h.r.Context(), h.key, h.upstreamURL, opts)
	sf, ok := raw.(*fetch.SharedFetch)
	if !ok {
		if raw == nil {
			h.log.Warn().Msg("Fetch aborted (client cancel)")
			return
		}
		h.sendError(http.StatusInternalServerError, "singleflight type mismatch", fmt.Errorf("%T", raw))
		return
	}

	if shared {
		if meta, ok := h.cache.Get(h.r.Context(), h.key); ok {
			h.serveFromCache(meta)
		} else {
			h.sendError(http.StatusInternalServerError, "shared fetch: item lost", nil)
		}
		return
	}

	if opts.UseHEAD {

		if sf.Err != nil && errors.Is(sf.Err, fetch.ErrUpstreamClient) {
			h.log.Debug().Int("status", sf.Result.Status).Msg("HEAD 4xx, retrying GET")
			h.fetchAndServe(nil)
			return
		}

		if sf.Err == nil && sf.Result != nil && sf.Result.Body == nil &&
			sf.Result.Status >= 200 && sf.Result.Status < 300 {

			if resourceChanged(revalMeta, sf.Result.Header) {
				h.log.Debug().Msg("HEAD validators changed, perform full GET")
				h.fetchAndServe(nil)
				return
			}

			updated := *revalMeta
			updated.LastUsedAt = time.Now()
			updated.ExpiresAt = cache.CalculateFreshness(sf.Result.Header, time.Now(), h.cleanRelPath, h.cfg.Cache.Overrides)

			util.UpdateCacheHeaders(updated.Headers, sf.Result.Header)
			_ = h.cache.Put(h.r.Context(), &updated)
			h.serveFromCache(&updated)
			return
		}
	}

	if sf.Err != nil {
		h.handleFetchError(sf.Err, sf.Result, revalMeta)
		return
	}

	h.handleFetchSuccess(sf.Result)
}

func resourceChanged(old *cache.ItemMeta, hdr http.Header) bool {
	if old == nil {
		return true
	}
	switch {
	case hdr.Get("ETag") != "" && hdr.Get("ETag") == old.Headers.Get("ETag"):
		return false
	case hdr.Get("Last-Modified") != "" && hdr.Get("Last-Modified") == old.Headers.Get("Last-Modified"):
		return false
	case hdr.Get("Content-Length") != "" && hdr.Get("Content-Length") == old.Headers.Get("Content-Length"):
		return false
	default:
		return true
	}
}

func (h *requestHandler) handleFetchError(err error, res *fetch.Result, stale *cache.ItemMeta) {
	switch {
	case stale != nil && (errors.Is(err, fetch.ErrNetwork) || errors.Is(err, fetch.ErrUpstreamServer)):
		h.log.Warn().Err(err).Msg("Upstream error; serving stale")
		h.serveFromCache(stale)
	case errors.Is(err, fetch.ErrUpstreamNotFound):
		h.handleUpstream404(res)
	case errors.Is(err, fetch.ErrUpstreamNotModified):
		h.handleRevalidation(res, stale)
	default:
		h.sendError(http.StatusBadGateway, "Upstream fetch failed", err)
	}
}

func (h *requestHandler) handleUpstream404(res *fetch.Result) {
	if res == nil {
		h.sendError(http.StatusNotFound, "Not found", nil)
		return
	}
	if h.cfg.Cache.NegativeTTL > 0 {
		now := time.Now()
		meta := &cache.ItemMeta{
			Key:         h.key,
			UpstreamURL: h.upstreamURL,
			StatusCode:  http.StatusNotFound,
			Headers:     util.CopyHeader(res.Header),
			FetchedAt:   now,
			LastUsedAt:  now,
			ExpiresAt:   now.Add(h.cfg.Cache.NegativeTTL),
		}
		_ = h.cache.Put(h.r.Context(), meta)
	}
	util.CopyWhitelistedHeaders(h.w.Header(), res.Header)
	http.Error(h.w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
}

func (h *requestHandler) handleRevalidation(res *fetch.Result, old *cache.ItemMeta) {
	if old == nil {
		util.CopyWhitelistedHeaders(h.w.Header(), res.Header)
		h.w.WriteHeader(http.StatusNotModified)
		return
	}
	updated := *old
	updated.Headers = util.CopyHeader(old.Headers)
	fresh := res.Header
	if len(fresh) == 0 {
		fresh = old.Headers
	}
	updated.ExpiresAt = cache.CalculateFreshness(fresh, time.Now(), h.cleanRelPath, h.cfg.Cache.Overrides)
	updated.LastUsedAt = time.Now()
	util.UpdateCacheHeaders(updated.Headers, res.Header)
	_ = h.cache.Put(h.r.Context(), &updated)
	h.serveFromCache(&updated)
}

func (h *requestHandler) handleFetchSuccess(res *fetch.Result) {
	meta := h.buildMeta(res)

	util.CopyWhitelistedHeaders(h.w.Header(), meta.Headers)
	h.w.Header().Set("X-Cache-Status", cacheStatusMiss)

	if h.r.Method == http.MethodHead {
		h.w.WriteHeader(meta.StatusCode)
		_ = h.cache.Put(h.r.Context(), meta)
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
		StatusCode:  res.Status,
		Headers:     hdr,
		FetchedAt:   now,
		LastUsedAt:  now,
		Size:        res.Size,
		ExpiresAt:   cache.CalculateFreshness(hdr, now, h.cleanRelPath, h.cfg.Cache.Overrides),
	}
}

func (h *requestHandler) streamAndStore(res *fetch.Result, meta *cache.ItemMeta) {
	if err := h.cache.Put(h.r.Context(), meta); err != nil {
		h.log.Error().Err(err).Msg("Failed to write preliminary metadata")
		h.sendError(http.StatusInternalServerError, "failed to write cache metadata", err)
		return
	}

	pr, pw := io.Pipe()
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		n, err := h.cache.PutContent(h.r.Context(), h.key, pr)
		if err != nil {
			if !util.IsClientDisconnectedError(err) {
				h.log.Error().Err(err).Msg("Write to cache failed")
			}
			_ = h.cache.Delete(context.Background(), h.key)
			return
		}

		if meta.Size != n {
			h.log.Debug().
				Int64("header_size", meta.Size).
				Int64("actual_size", n).
				Msg("Content-Length mismatch, updating metadata with actual size.")
			meta.Size = n
			_ = h.cache.Put(h.r.Context(), meta)
		}
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
