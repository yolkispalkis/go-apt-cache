package handlers

import (
	"net/http"
	"strings"
	"time"

	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/storage"
)

type RepositoryHandler struct {
	config ServerConfig
}

func NewRepositoryHandler(
	upstreamURL string,
	cache storage.Cache,
	headerCache storage.HeaderCache,
	validationCache storage.ValidationCache,
	client *http.Client,
	localPath string,
	globalConfig *config.Config,
) http.Handler {
	config := NewRepositoryServerConfig(
		upstreamURL,
		cache,
		headerCache,
		validationCache,
		client,
		globalConfig,
	)

	config.LocalPath = localPath
	config.ValidationCache.SetTTL(time.Duration(globalConfig.Cache.ValidationCacheTTL) * time.Second)

	return &RepositoryHandler{
		config: config,
	}
}

func (rh *RepositoryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestPath := r.URL.Path
	if requestPath == "" {
		requestPath = "/"
	}

	logging.Info("Repository handler processing: %s", requestPath)

	// Use the same path handling logic as in handlers.go
	cacheKey := getCacheKey(rh.config, requestPath)
	repoName := strings.Trim(rh.config.LocalPath, "/")
	if repoName == "" {
		repoName = "root"
	}

	logging.Info("Repository: %s, Path: %s, Cache key: %s", repoName, requestPath, cacheKey)

	handler := HandleRequest(rh.config, true)
	handler(w, r)
}
