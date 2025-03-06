package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/utils"
)

type Repository struct {
	URL     string `json:"url"`
	Path    string `json:"path"`
	Enabled bool   `json:"enabled"`
}

type CacheConfig struct {
	Directory          string `json:"directory"`
	MaxSize            string `json:"maxSize"`
	Enabled            bool   `json:"enabled"`
	LRU                bool   `json:"lru"`
	CleanOnStart       bool   `json:"cleanOnStart"`
	ValidationCacheTTL int    `json:"validationCacheTTL"`
}

type LoggingConfig struct {
	FilePath        string `json:"filePath"`
	DisableTerminal bool   `json:"disableTerminal"`
	MaxSize         string `json:"maxSize"`
	Level           string `json:"level"`
}

type ServerConfig struct {
	ListenAddress  string `json:"listenAddress"`
	UnixSocketPath string `json:"unixSocketPath"`
	LogRequests    bool   `json:"logRequests"`
	Timeout        int    `json:"timeout"`
}

type Config struct {
	Server       ServerConfig  `json:"server"`
	Cache        CacheConfig   `json:"cache"`
	Logging      LoggingConfig `json:"logging"`
	Repositories []Repository  `json:"repositories"`
	Version      string        `json:"version"`
}

func DefaultConfig() Config {
	return Config{
		Server: ServerConfig{
			ListenAddress:  ":8080",
			UnixSocketPath: "",
			LogRequests:    true,
			Timeout:        30,
		},
		Cache: CacheConfig{
			Directory:          "./cache",
			MaxSize:            "1GB",
			Enabled:            true,
			LRU:                true,
			CleanOnStart:       false,
			ValidationCacheTTL: 300,
		},
		Logging: LoggingConfig{
			FilePath:        "./logs/go-apt-cache.log",
			DisableTerminal: false,
			MaxSize:         "10MB",
			Level:           "info",
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

func LoadConfig(path string) (Config, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		logging.Warning("Config file %s does not exist", path)
		return DefaultConfig(), fmt.Errorf("config file %s does not exist", path)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		logging.Warning("Error reading config file: %v", err)
		return DefaultConfig(), fmt.Errorf("error reading config file: %w", err)
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		logging.Warning("Error parsing config file: %v", err)
		return DefaultConfig(), fmt.Errorf("error parsing config file: %w", err)
	}

	if config.Server.Timeout <= 0 {
		config.Server.Timeout = 30
	}

	if config.Logging.MaxSize == "" {
		config.Logging.MaxSize = "10MB"
	}
	if config.Logging.Level == "" {
		config.Logging.Level = "info"
	}

	var enabledRepos []Repository
	for _, repo := range config.Repositories {
		if repo.Enabled {
			enabledRepos = append(enabledRepos, repo)
		}
	}
	config.Repositories = enabledRepos

	if config.Version == "" {
		config.Version = "1.0.0"
	}

	return config, nil
}

func SaveConfig(config Config, path string) error {
	dir := filepath.Dir(path)
	if err := utils.CreateDirectory(dir); err != nil {
		return fmt.Errorf("error creating directory: %w", err)
	}

	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshaling config: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("error writing config file: %w", err)
	}

	return nil
}

func CreateDefaultConfigFile(path string) error {
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("config file %s already exists", path)
	}

	config := DefaultConfig()

	return SaveConfig(config, path)
}

func ValidateConfig(config Config) error {
	if len(config.Repositories) == 0 {
		return fmt.Errorf("no repositories configured")
	}

	if config.Cache.Enabled {
		if config.Cache.Directory == "" {
			return fmt.Errorf("cache directory not specified")
		}
	}

	if config.Server.ListenAddress == "" {
		return fmt.Errorf("listen address not specified")
	}

	return nil
}
