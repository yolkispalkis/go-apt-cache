package handlers

import (
	"net/http"

	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/storage"
)

type ServerConfig struct {
	UpstreamURL     string
	LocalPath       string
	Cache           storage.Cache
	HeaderCache     storage.HeaderCache
	ValidationCache storage.ValidationCache
	Client          *http.Client
	LogRequests     bool
	Config          *config.Config // Keep the global config for access to other settings
}

func NewServerConfig() ServerConfig {
	return ServerConfig{
		LogRequests: true,
	}
}

// NewServerConfigFromGlobalConfig is a helper to create a ServerConfig from the global config.
func NewServerConfigFromGlobalConfig(cfg *config.Config, client *http.Client) ServerConfig {
	return ServerConfig{
		LogRequests: cfg.Server.LogRequests,
		Client:      client,
		Config:      cfg, // Store the global config here.
	}
}

func NewRepositoryServerConfig(
	upstreamURL string,
	cache storage.Cache,
	headerCache storage.HeaderCache,
	validationCache storage.ValidationCache,
	client *http.Client,
	globalConfig *config.Config,
) ServerConfig {
	return ServerConfig{
		UpstreamURL:     upstreamURL,
		Cache:           cache,
		HeaderCache:     headerCache,
		ValidationCache: validationCache,
		Client:          client,
		LogRequests:     true,
		Config:          globalConfig,
	}
}
