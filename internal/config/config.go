package config

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"

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
	ListenAddress         string      `json:"listenAddress"`
	UnixSocketPath        string      `json:"unixSocketPath"`
	UnixSocketPermissions os.FileMode `json:"unixSocketPermissions"`
	LogRequests           bool        `json:"logRequests"`
	Timeout               int         `json:"timeout"` // General timeout, kept for backward compatibility
	ReadTimeout           int         `json:"readTimeout"`
	WriteTimeout          int         `json:"writeTimeout"`
	IdleTimeout           int         `json:"idleTimeout"`
}

type Config struct {
	Server       ServerConfig  `json:"server"`
	Cache        CacheConfig   `json:"cache"`
	Logging      LoggingConfig `json:"logging"`
	Repositories []Repository  `json:"repositories"`
	Version      string        `json:"version"`
}

const (
	DefaultListenAddress = ":8080"
	DefaultCacheMaxSize  = 1024 * 1024 * 1024 // 1GB
	DefaultReadTimeout   = 30
	DefaultWriteTimeout  = 60
	DefaultIdleTimeout   = 120
	DefaultLogLevel      = "info"
	DefaultLogMaxSize    = "10MB"
	DefaultTimeout       = 60
)

func DefaultConfig() Config {
	return Config{
		Server: ServerConfig{
			ListenAddress:         DefaultListenAddress,
			UnixSocketPath:        "",
			UnixSocketPermissions: 0666,
			LogRequests:           true,
			Timeout:               DefaultTimeout,
			ReadTimeout:           DefaultReadTimeout,
			WriteTimeout:          DefaultWriteTimeout,
			IdleTimeout:           DefaultIdleTimeout,
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
			MaxSize:         DefaultLogMaxSize,
			Level:           DefaultLogLevel,
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
		return DefaultConfig(), fmt.Errorf("config file %s does not exist", path)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return DefaultConfig(), fmt.Errorf("error reading config file: %w", err)
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return DefaultConfig(), fmt.Errorf("error parsing config file: %w", err)
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

		if _, err := utils.ParseSize(config.Cache.MaxSize); err != nil {
			return fmt.Errorf("invalid cache max size: %s", config.Cache.MaxSize)
		}
	}

	if config.Server.ListenAddress == "" && config.Server.UnixSocketPath == "" {
		return fmt.Errorf("neither listen address nor unix socket path specified")
	}

	if _, _, err := net.SplitHostPort(config.Server.ListenAddress); config.Server.ListenAddress != "" && err != nil {
		return fmt.Errorf("invalid listen address: %s", config.Server.ListenAddress)
	}

	return nil
}
