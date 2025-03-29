package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/yolkispalkis/go-apt-cache/internal/cache"
	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/fetch"
	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

type Server struct {
	*http.Server
	cfg          *config.Config
	cacheManager cache.CacheManager
	fetcher      *fetch.Coordinator
}

func New(
	cfg *config.Config,
	cacheManager cache.CacheManager,
	fetcher *fetch.Coordinator,
) (*Server, error) {
	mux := http.NewServeMux()

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, "OK")
		cacheStats := cacheManager.Stats()
		fmt.Fprintf(w, "Cache Items: %d\n", cacheStats.ItemCount)
		fmt.Fprintf(w, "Cache Size: %s / %s\n", util.FormatSize(cacheStats.CurrentSize), util.FormatSize(cacheStats.MaxSize))

	})

	for _, repo := range cfg.Repositories {
		if !repo.Enabled {
			logging.Info("Skipping disabled repository: %s", repo.Name)
			continue
		}

		pathPrefix := "/" + repo.Name + "/"

		repoHandler := NewRepositoryHandler(repo, cfg.Server, cacheManager, fetcher)

		mux.Handle(pathPrefix, http.StripPrefix(pathPrefix, repoHandler))
		logging.Info("Registered handler for repository %q at path %s (Upstream: %s)", repo.Name, pathPrefix, repo.URL)
	}

	var handler http.Handler = mux
	handler = LoggingMiddleware(handler)
	handler = RecoveryMiddleware(handler)

	httpServer := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout.Duration(),
		IdleTimeout:       cfg.Server.IdleTimeout.Duration(),
	}

	return &Server{
		Server:       httpServer,
		cfg:          cfg,
		cacheManager: cacheManager,
		fetcher:      fetcher,
	}, nil
}

type RepositoryHandler struct {
	repoConfig   config.Repository
	serverConfig config.ServerConfig
	cacheManager cache.CacheManager
	fetcher      *fetch.Coordinator
}

func NewRepositoryHandler(
	repo config.Repository,
	serverCfg config.ServerConfig,
	cache cache.CacheManager,
	fetcher *fetch.Coordinator,
) *RepositoryHandler {
	return &RepositoryHandler{
		repoConfig:   repo,
		serverConfig: serverCfg,
		cacheManager: cache,
		fetcher:      fetcher,
	}
}

func (h *RepositoryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	filePath := strings.TrimPrefix(r.URL.Path, "/")
	if filePath == "" {
		if strings.HasSuffix(r.URL.Path, "/") {
			http.Error(w, "Directory listing not supported", http.StatusForbidden)
			return
		}
	}

	cacheKey := h.repoConfig.Name + "/" + filePath
	upstreamURL := h.repoConfig.URL + "/" + filePath

	useValidationCache := true
	if useValidationCache {
		validationTime, ok := h.cacheManager.GetValidation(cacheKey)
		if ok {
			if h.checkClientCacheHeaders(w, r, validationTime) {
				return
			}
		}
	}

	cacheReader, cacheSize, cacheModTime, err := h.cacheManager.Get(r.Context(), cacheKey)
	if err == nil {
		defer cacheReader.Close()
		logging.Debug("Cache hit for key: %s", cacheKey)

		if h.checkClientCacheHeaders(w, r, cacheModTime) {
			return
		}

		w.Header().Set("Last-Modified", cacheModTime.UTC().Format(http.TimeFormat))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", cacheSize))
		w.Header().Set("Content-Type", util.GetContentType(filePath))

		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}

		readSeeker, ok := cacheReader.(io.ReadSeeker)
		if !ok {
			logging.Error("Cache reader for %s does not implement io.ReadSeeker", cacheKey)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}

		http.ServeContent(w, r, filePath, cacheModTime, readSeeker)
		return
	}

	if !errors.Is(err, os.ErrNotExist) {
		logging.Error("Error reading from cache for key %s: %v", cacheKey, err)
		http.Error(w, "Internal Cache Error", http.StatusInternalServerError)
		return
	}

	logging.Debug("Cache miss for key: %s, fetching from upstream: %s", cacheKey, upstreamURL)

	fetchResult, err := h.fetcher.Fetch(r.Context(), cacheKey, upstreamURL, r.Header)
	if err != nil {
		logging.Error("Failed to fetch %s (key %s) from upstream: %v", upstreamURL, cacheKey, err)
		if errors.Is(err, fetch.ErrNotFound) {
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		} else if errors.Is(err, fetch.ErrUpstreamNotModified) {
			w.WriteHeader(http.StatusNotModified)
		} else {
			http.Error(w, "Bad Gateway", http.StatusBadGateway)
		}
		return
	}
	defer fetchResult.Body.Close()

	h.cacheManager.PutValidation(cacheKey, time.Now())

	util.CopyRelevantHeaders(w.Header(), fetchResult.Header)
	w.Header().Set("Content-Type", util.GetContentType(filePath))
	if cl := fetchResult.Header.Get("Content-Length"); cl != "" {
		w.Header().Set("Content-Length", cl)
	}

	pr, pw := io.Pipe()
	cacheErrChan := make(chan error, 1)

	go func() {
		defer close(cacheErrChan)
		modTimeToUse := fetchResult.ModTime
		if modTimeToUse.IsZero() {
			modTimeToUse = time.Now()
		}
		err := h.cacheManager.Put(context.Background(), cacheKey, pr, fetchResult.Size, modTimeToUse)
		if err != nil {
			logging.Error("Failed to write key %s to cache: %v", cacheKey, err)
		}
		cacheErrChan <- err
	}()

	teeReader := io.TeeReader(fetchResult.Body, pw)

	w.WriteHeader(fetchResult.StatusCode)

	if r.Method == http.MethodHead {
		pw.Close()
		return
	}

	_, copyErr := io.Copy(w, teeReader)

	_ = pw.Close()

	if copyErr != nil {
		logging.Warn("Error copying response to client for %s: %v", cacheKey, copyErr)
	}

	select {
	case cacheErr := <-cacheErrChan:
		if cacheErr != nil {
			logging.Error("Error writing to cache for %s: %v", cacheKey, cacheErr)
		}
	case <-time.After(500 * time.Millisecond):
		logging.Warn("Cache write for %s is taking longer than expected", cacheKey)
	}
}

func (h *RepositoryHandler) checkClientCacheHeaders(w http.ResponseWriter, r *http.Request, modTime time.Time) bool {
	if modTime.IsZero() {
		return false
	}

	ifims := r.Header.Get("If-Modified-Since")
	if ifims != "" {
		if t, err := http.ParseTime(ifims); err == nil {
			modTimeTruncated := modTime.Truncate(time.Second)
			if !modTimeTruncated.After(t.Truncate(time.Second)) {
				w.WriteHeader(http.StatusNotModified)
				return true
			}
		}
	}

	return false
}
