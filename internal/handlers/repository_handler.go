package handlers

import (
	"net/http"
	"strings"

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
) http.Handler {
	config := NewRepositoryServerConfig(
		upstreamURL,
		cache,
		headerCache,
		validationCache,
		client,
	)

	config.LocalPath = localPath

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

	// Extract repository name from localPath
	repoName := strings.Trim(rh.config.LocalPath, "/")
	if repoName == "" {
		repoName = "root"
	}

	// Prepend repository name to the request path for logging
	logging.Info("Repository: %s, Path: %s", repoName, requestPath)

	handler := HandleRequest(rh.config, true)
	handler(w, r)
}
