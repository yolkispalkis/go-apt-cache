package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	json "github.com/goccy/go-json"
	kjson "github.com/knadh/koanf/parsers/json"
	kyaml "github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	"github.com/yolkispalkis/go-apt-cache/internal/appinfo"
	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
	"gopkg.in/yaml.v3"
)

type Repository struct {
	Name    string `koanf:"name" yaml:"name"`
	URL     string `koanf:"url" yaml:"url"`
	Enabled bool   `koanf:"enabled" yaml:"enabled"`
}

type ServerConfig struct {
	ListenAddr        string        `koanf:"listenAddress" yaml:"listenAddress"`
	UnixPath          string        `koanf:"unixSocketPath" yaml:"unixSocketPath"`
	UnixPerms         os.FileMode   `koanf:"unixSocketPermissions" yaml:"unixSocketPermissions"`
	ReqTimeout        time.Duration `koanf:"requestTimeout" yaml:"requestTimeout"`
	ShutdownTimeout   time.Duration `koanf:"shutdownTimeout" yaml:"shutdownTimeout"`
	IdleTimeout       time.Duration `koanf:"idleTimeout" yaml:"idleTimeout"`
	ReadHeaderTimeout time.Duration `koanf:"readHeaderTimeout" yaml:"readHeaderTimeout"`
	MaxConcurrent     int           `koanf:"maxConcurrentFetches" yaml:"maxConcurrentFetches"`
	UserAgent         string        `koanf:"userAgent" yaml:"userAgent"`
}

type CacheOverride struct {
	PathPattern string        `koanf:"pathPattern" yaml:"pathPattern"`
	TTL         time.Duration `koanf:"ttl" yaml:"ttl"`
}

type CacheConfig struct {
	Dir          string          `koanf:"directory" yaml:"directory"`
	MaxSize      string          `koanf:"maxSize" yaml:"maxSize"`
	Enabled      bool            `koanf:"enabled" yaml:"enabled"`
	CleanOnStart bool            `koanf:"cleanOnStart" yaml:"cleanOnStart"`
	BufferSize   string          `koanf:"bufferSize" yaml:"bufferSize"`
	Overrides    []CacheOverride `koanf:"overrides" yaml:"overrides"`
	NegativeTTL  time.Duration   `koanf:"negativeCacheTTL" yaml:"negativeCacheTTL"`
}

type Config struct {
	Server       ServerConfig   `koanf:"server" yaml:"server"`
	Cache        CacheConfig    `koanf:"cache" yaml:"cache"`
	Logging      logging.Config `koanf:"logging" yaml:"logging"`
	Repositories []Repository   `koanf:"repositories" yaml:"repositories"`
}

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
			BufferSize:   "256KB",
			NegativeTTL:  5 * time.Minute,
			Overrides: []CacheOverride{
				{PathPattern: "dists/**", TTL: 5 * time.Minute},
				{PathPattern: "**/*.deb", TTL: 30 * 24 * time.Hour},
			},
		},
		Logging: logging.Config{
			Level:      "info",
			File:       "/var/log/go-apt-cache/go-apt-cache.log",
			MaxSizeMB:  100,
			MaxBackups: 3,
			MaxAgeDays: 7,
			NoConsole:  true,
		},
		Repositories: []Repository{
			{Name: "ubuntu", URL: "http://archive.ubuntu.com/ubuntu/", Enabled: true},
			{Name: "debian", URL: "http://deb.debian.org/debian/", Enabled: false},
		},
	}
}

func Load(path string) (*Config, error) {
	k := koanf.New(".")
	cfg := Default()

	var parser koanf.Parser
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".yaml", ".yml":
		parser = kyaml.Parser()
	case ".json":
		parser = kjson.Parser()
	default:
		return nil, fmt.Errorf("unsupported config file format: '%s'. Use .json, .yaml, or .yml", ext)
	}

	if err := k.Load(file.Provider(path), parser); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("config file not found at %s, please create one or use -create-config flag", path)
		}
		return nil, fmt.Errorf("error loading config file: %w", err)
	}

	if err := k.UnmarshalWithConf("", &cfg, koanf.UnmarshalConf{Tag: "koanf"}); err != nil {
		return nil, fmt.Errorf("error unmarshalling config: %w", err)
	}

	if err := validate(cfg); err != nil {
		return nil, err
	}

	normalize(cfg)
	return cfg, nil
}

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
		maxBytes, err := util.ParseSize(c.Cache.MaxSize)
		if err != nil {
			return fmt.Errorf("invalid cache.maxSize %q: %w", c.Cache.MaxSize, err)
		}
		if maxBytes <= 0 { // Добавлено
			return errors.New("cache.maxSize must be greater than 0 if cache is enabled")
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
	}
	return nil
}

func normalize(c *Config) {
	for i := range c.Repositories {
		repo := &c.Repositories[i]
		u, _ := url.Parse(repo.URL) // Assume valid from validate
		if u.Path != "" && !strings.HasSuffix(u.Path, "/") {
			u.Path += "/"
		}
		repo.URL = u.String() // Safe rebuild
	}
}

func EnsureDefault(path string) error {
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("config file already exists at %s", path)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("failed to create directory for config: %w", err)
	}

	defaultCfg := Default()
	var data []byte
	var err error

	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".yaml", ".yml", "":
		data, err = yaml.Marshal(defaultCfg)
	case ".json":
		data, err = json.MarshalIndent(defaultCfg, "", "  ")
	default:
		return fmt.Errorf("unsupported config file format for creation: '%s'. Use .json, .yaml, or .yml", ext)
	}

	if err != nil {
		return fmt.Errorf("failed to marshal default config: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write default config: %w", err)
	}
	fmt.Printf("Default config created at %s. Please review it.\n", path)
	return nil
}

func (c *Config) GetRepo(name string) (Repository, bool) {
	for _, repo := range c.Repositories {
		if repo.Name == name && repo.Enabled {
			return repo, true
		}
	}
	return Repository{}, false
}
