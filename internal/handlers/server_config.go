package handlers

import (
	"net/http"

	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/storage"
)

// ServerConfig holds the configuration for the APT mirror server
// This is a consolidated structure that combines all server configuration needs
type ServerConfig struct {
	// Repository configuration
	UpstreamURL string
	LocalPath   string // Local path prefix for URL mapping

	// Cache configuration
	Cache           storage.Cache
	HeaderCache     storage.HeaderCache
	ValidationCache storage.ValidationCache

	// HTTP configuration
	Client *http.Client // HTTP client for making requests to upstream servers

	// Logging configuration
	LogRequests bool

	// Reference to the main application config
	Config *config.Config
}

// NewServerConfig creates a new ServerConfig with default values
func NewServerConfig() ServerConfig {
	return ServerConfig{
		LogRequests: true,
	}
}

// NewServerConfigFromGlobalConfig creates a ServerConfig from the global config
func NewServerConfigFromGlobalConfig(cfg *config.Config, client *http.Client) ServerConfig {
	return ServerConfig{
		LogRequests: cfg.Server.LogRequests,
		Client:      client,
		Config:      cfg,
	}
}

// NewRepositoryServerConfig creates a ServerConfig for a specific repository
func NewRepositoryServerConfig(
	upstreamURL string,
	cache storage.Cache,
	headerCache storage.HeaderCache,
	validationCache storage.ValidationCache,
	client *http.Client,
) ServerConfig {
	return ServerConfig{
		UpstreamURL:     upstreamURL,
		Cache:           cache,
		HeaderCache:     headerCache,
		ValidationCache: validationCache,
		Client:          client,
		LogRequests:     true,
	}
}
