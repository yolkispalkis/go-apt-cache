package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/knadh/koanf/parsers/json" as kjson
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	"github.com/yolkispalkis/go-apt-cache/internal/appinfo"
	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

type Repository struct {
	Name    string `koanf:"name"`
	URL     string `koanf:"url"`
	Enabled bool   `koanf:"enabled"`
}

type ServerConfig struct {
	ListenAddr        string        `koanf:"listenAddress"`
	UnixPath          string        `koanf:"unixSocketPath"`
	UnixPerms         os.FileMode   `koanf:"unixSocketPermissions"`
	ReqTimeout        time.Duration `koanf:"requestTimeout"`
	ShutdownTimeout   time.Duration `koanf:"shutdownTimeout"`
	IdleTimeout       time.Duration `koanf:"idleTimeout"`
	ReadHeaderTimeout time.Duration `koanf:"readHeaderTimeout"`
	MaxConcurrent     int           `koanf:"maxConcurrentFetches"`
	UserAgent         string        `koanf:"userAgent"`
}

type CacheOverride struct {
	PathPattern string        `koanf:"pathPattern"`
	TTL         time.Duration `koanf:"ttl"`
}

type CacheConfig struct {
	Dir          string          `koanf:"directory"`
	MaxSize      string          `koanf:"maxSize"`
	Enabled      bool            `koanf:"enabled"`
	CleanOnStart bool            `koanf:"cleanOnStart"`
	BufferSize   string          `koanf:"bufferSize"`
	Overrides    []CacheOverride `koanf:"overrides"`
}

type Config struct {
	Server       ServerConfig    `koanf:"server"`
	Cache        CacheConfig     `koanf:"cache"`
	Logging      logging.Config  `koanf:"logging"`
	Repositories []Repository    `koanf:"repositories"`
}

// Default возвращает конфигурацию по умолчанию.
func Default() *Config {
	return &Config{
		Server: ServerConfig{
			ListenAddr:        ":8080",
			UnixPath:          "/run/go-apt-cache/go-apt-cache.sock",
			UnixPerms:         0660,
			ReqTimeout:        60 * time.Second,
			ShutdownTimeout:   15 * time.Second,
			IdleTimeout:       120 * time.Second,
			ReadHeaderTimeout: 10 * time.Second,
			MaxConcurrent:     20,
			UserAgent:         appinfo.UserAgent(),
		},
		Cache: CacheConfig{
			Dir:          "/var/cache/go-apt-cache",
			MaxSize:      "10GB",
			Enabled:      true,
			CleanOnStart: false,
			BufferSize:   "64KB",
			Overrides: []CacheOverride{
				{PathPattern: "dists/**/InRelease", TTL: 5 * time.Minute},
				{PathPattern: "dists/**/Release", TTL: 5 * time.Minute},
				{PathPattern: "**/*.deb", TTL: 30 * 24 * time.Hour},
			},
		},
		Logging: logging.Config{
			Level:      "info",
			File:       "/var/log/go-apt-cache/go-apt-cache.log",
			MaxSizeMB:  100,
			MaxBackups: 3,
			MaxAgeDays: 7,
		},
		Repositories: []Repository{
			{Name: "ubuntu", URL: "http://archive.ubuntu.com/ubuntu/", Enabled: true},
			{Name: "debian", URL: "http://deb.debian.org/debian/", Enabled: false},
		},
	}
}

// Load загружает конфигурацию из файла.
func Load(path string) (*Config, error) {
	k := koanf.New(".")
	cfg := Default()

	if err := k.Load(file.Provider(path), kjson.Parser()); err != nil {
		if os.IsNotExist(err) {
			return cfg, fmt.Errorf("config file not found at %s, please create one or use -create-config flag", path)
		}
		return nil, fmt.Errorf("error loading config file: %w", err)
	}

	if err := k.UnmarshalWithConf("", &cfg, koanf.UnmarshalConf{Tag: "koanf"}); err != nil {
		return nil, fmt.Errorf("error unmarshalling config: %w", err)
	}

	return cfg, validate(cfg)
}

// validate проверяет корректность значений в конфигурации.
func validate(c *Config) error {
	if c.Server.ListenAddr == "" && c.Server.UnixPath == "" {
		return errors.New("server: must set listenAddress or unixSocketPath")
	}
	if c.Server.MaxConcurrent <= 0 {
		return errors.New("server.maxConcurrentFetches must be > 0")
	}

	if c.Cache.Enabled {
		if c.Cache.Dir == "" {
			return errors.New("cache.directory must be set if cache is enabled")
		}
		if _, err := util.ParseSize(c.Cache.MaxSize); err != nil {
			return fmt.Errorf("invalid cache.maxSize %q: %w", c.Cache.MaxSize, err)
		}
	}

	repoNames := make(map[string]struct{})
	for i := range c.Repositories {
		repo := &c.Repositories[i]
		if !util.IsRepoNameSafe(repo.Name) {
			return fmt.Errorf("repo %q: name is invalid", repo.Name)
		}
		if _, exists := repoNames[repo.Name]; exists {
			return fmt.Errorf("duplicate repo name found: %q", repo.Name)
		}
		repoNames[repo.Name] = struct{}{}

		u, err := url.Parse(repo.URL)
		if err != nil {
			return fmt.Errorf("repo %q: invalid URL %q: %w", repo.Name, repo.URL, err)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return fmt.Errorf("repo %q: URL scheme must be http or https", repo.Name)
		}
		if !strings.HasSuffix(repo.URL, "/") {
			repo.URL += "/"
		}
	}
	return nil
}

// EnsureDefault создает файл конфигурации по умолчанию.
func EnsureDefault(path string) error {
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("config file already exists at %s", path)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("failed to create directory for config: %w", err)
	}
	data, err := json.MarshalIndent(Default(), "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal default config: %w", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write default config: %w", err)
	}
	fmt.Printf("Default config created at %s. Please review it.\n", path)
	return nil
}

// GetRepo находит репозиторий по имени.
func (c *Config) GetRepo(name string) (Repository, bool) {
	for _, repo := range c.Repositories {
		if repo.Name == name && repo.Enabled {
			return repo, true
		}
	}
	return Repository{}, false
}