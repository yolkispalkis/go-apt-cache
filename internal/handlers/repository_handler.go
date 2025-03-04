package handlers

import (
	"net/http"

	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/storage"
)

// ServerConfig definition has been moved to server_config.go

// RepositoryHandler handles requests for a specific repository
type RepositoryHandler struct {
	// Embed ServerConfig to avoid duplication
	config ServerConfig
}

// NewRepositoryHandler creates a new repository handler
func NewRepositoryHandler(
	upstreamURL string,
	cache storage.Cache,
	headerCache storage.HeaderCache,
	validationCache storage.ValidationCache,
	client *http.Client,
	localPath string,
) http.Handler {
	// Use the new factory function from server_config.go
	config := NewRepositoryServerConfig(
		upstreamURL,
		cache,
		headerCache,
		validationCache,
		client,
	)

	// Set the local path for URL mapping
	config.LocalPath = localPath

	return &RepositoryHandler{
		config: config,
	}
}

// ServeHTTP implements the http.Handler interface
func (rh *RepositoryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Get the path from the request
	requestPath := r.URL.Path
	if requestPath == "" {
		requestPath = "/"
	}

	logging.Info("Repository handler processing: %s", requestPath)

	// Use the common handler function with the embedded config
	handler := HandleRequest(rh.config, true)
	handler(w, r)
}
