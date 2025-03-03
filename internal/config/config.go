package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/yolkispalkis/go-apt-cache/internal/utils"
)

// Repository represents a single APT repository configuration
type Repository struct {
	URL     string `json:"url"`     // Full repository URL
	Path    string `json:"path"`    // Path prefix for the repository
	Enabled bool   `json:"enabled"` // Whether this repository is enabled
}

// CacheConfig represents cache configuration
type CacheConfig struct {
	Directory          string `json:"directory"`          // Cache directory
	MaxSize            int64  `json:"maxSize"`            // Maximum cache size
	SizeUnit           string `json:"sizeUnit"`           // Size unit: "bytes", "MB", or "GB"
	Enabled            bool   `json:"enabled"`            // Whether cache is enabled
	LRU                bool   `json:"lru"`                // Whether to use LRU eviction policy
	CleanOnStart       bool   `json:"cleanOnStart"`       // Whether to clean the cache on startup
	ValidationCacheTTL int    `json:"validationCacheTTL"` // Time in seconds to cache validation results
}

// ServerConfig represents the server configuration
type ServerConfig struct {
	ListenAddress string `json:"listenAddress"` // Address to listen on (e.g. ":8080")
	LogRequests   bool   `json:"logRequests"`   // Whether to log all requests
	Timeout       int    `json:"timeout"`       // Timeout in seconds for HTTP requests
}

// Config represents the complete application configuration
type Config struct {
	Server       ServerConfig `json:"server"`
	Cache        CacheConfig  `json:"cache"`
	Repositories []Repository `json:"repositories"`
	Version      string       `json:"version"` // Configuration version
}

// DefaultConfig returns a default configuration
func DefaultConfig() Config {
	return Config{
		Server: ServerConfig{
			ListenAddress: ":8080",
			LogRequests:   true,
			Timeout:       30,
		},
		Cache: CacheConfig{
			Directory:          "./cache",
			MaxSize:            1024,
			SizeUnit:           "MB",
			Enabled:            true,
			LRU:                true,
			CleanOnStart:       false,
			ValidationCacheTTL: 300, // 5 minutes default
		},
		Repositories: []Repository{
			{
				URL:     "http://archive.ubuntu.com/ubuntu",
				Path:    "/",
				Enabled: true,
			},
		},
		Version: "1.0.0",
	}
}

// LoadConfig loads configuration from a file
func LoadConfig(path string) (Config, error) {
	// Check if file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return DefaultConfig(), fmt.Errorf("config file %s does not exist", path)
	}

	// Read file
	data, err := os.ReadFile(path)
	if err != nil {
		return DefaultConfig(), fmt.Errorf("error reading config file: %w", err)
	}

	// Parse JSON
	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return DefaultConfig(), fmt.Errorf("error parsing config file: %w", err)
	}

	// Set defaults for any missing fields
	if config.Server.Timeout <= 0 {
		config.Server.Timeout = 30
	}

	// Filter out disabled repositories
	var enabledRepos []Repository
	for _, repo := range config.Repositories {
		if repo.Enabled {
			enabledRepos = append(enabledRepos, repo)
		}
	}
	config.Repositories = enabledRepos

	// Set version if not present
	if config.Version == "" {
		config.Version = "1.0.0"
	}

	return config, nil
}

// SaveConfig saves configuration to a file
func SaveConfig(config Config, path string) error {
	// Create directory if it doesn't exist
	dir := filepath.Dir(path)
	if err := utils.CreateDirectory(dir); err != nil {
		return fmt.Errorf("error creating directory: %w", err)
	}

	// Marshal JSON
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshaling config: %w", err)
	}

	// Write file
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("error writing config file: %w", err)
	}

	return nil
}

// CreateDefaultConfigFile creates a default configuration file
func CreateDefaultConfigFile(path string) error {
	// Check if file already exists
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("config file %s already exists", path)
	}

	// Create default config
	config := DefaultConfig()

	// Save config
	return SaveConfig(config, path)
}

// ValidateConfig validates the configuration
func ValidateConfig(config Config) error {
	// Check if there are any repositories
	if len(config.Repositories) == 0 {
		return fmt.Errorf("no repositories configured")
	}

	// Check if cache directory is valid
	if config.Cache.Enabled {
		if config.Cache.Directory == "" {
			return fmt.Errorf("cache directory not specified")
		}
	}

	// Check if listen address is valid
	if config.Server.ListenAddress == "" {
		return fmt.Errorf("listen address not specified")
	}

	return nil
}
