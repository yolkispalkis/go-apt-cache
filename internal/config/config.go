package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/yolkispalkis/go-apt-cache/internal/log"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

type Repository struct {
	Name string `yaml:"name"`
	URL  string `yaml:"url"`
}

type ServerConfig struct {
	ListenAddr        string        `yaml:"listenAddress"`
	UnixPath          string        `yaml:"unixSocketPath"`
	UnixPerms         os.FileMode   `yaml:"unixSocketPermissions"`
	ReqTimeout        time.Duration `yaml:"requestTimeout"`
	ShutdownTimeout   time.Duration `yaml:"shutdownTimeout"`
	IdleTimeout       time.Duration `yaml:"idleTimeout"`
	ReadHeaderTimeout time.Duration `yaml:"readHeaderTimeout"`
	MaxConcurrent     int           `yaml:"maxConcurrentFetches"`
	UserAgent         string        `yaml:"userAgent"`
}

type CacheOverride struct {
	PathPattern string        `yaml:"pathPattern"`
	TTL         time.Duration `yaml:"ttl"`
}

type CacheConfig struct {
	Dir            string          `yaml:"directory"`
	MaxSize        string          `yaml:"maxSize"`
	Enabled        bool            `yaml:"enabled"`
	CleanOnStart   bool            `yaml:"cleanOnStart"`
	BufferSize     string          `yaml:"bufferSize"`
	HeuristicTTL10 bool            `yaml:"heuristicTTL10Percent"`
	Overrides      []CacheOverride `yaml:"overrides"`
	NegativeTTL    time.Duration   `yaml:"negativeCacheTTL"`
}

type Config struct {
	Server       ServerConfig `yaml:"server"`
	Cache        CacheConfig  `yaml:"cache"`
	Logging      log.Config   `yaml:"logging"`
	Repositories []Repository `yaml:"repositories"`
}

func Default(app, ver string) *Config {
	return &Config{
		Server: ServerConfig{
			ListenAddr:        ":8080",
			UnixPath:          "/run/go-apt-cache/go-apt-cache.sock",
			UnixPerms:         0660,
			ReqTimeout:        time.Minute,
			ShutdownTimeout:   15 * time.Second,
			IdleTimeout:       2 * time.Minute,
			ReadHeaderTimeout: 10 * time.Second,
			MaxConcurrent:     150,
			UserAgent:         fmt.Sprintf("%s/%s", app, ver),
		},
		Cache: CacheConfig{
			Dir:            "/var/cache/go-apt-cache",
			MaxSize:        "10GB",
			Enabled:        true,
			CleanOnStart:   false,
			BufferSize:     "256KB",
			HeuristicTTL10: true,
			Overrides: []CacheOverride{
				{PathPattern: "dists/**", TTL: 5 * time.Minute},
				{PathPattern: "**/*.deb", TTL: 30 * 24 * time.Hour},
			},
			NegativeTTL: 5 * time.Minute,
		},
		Logging: log.Config{
			Level:     "info",
			File:      "/var/log/go-apt-cache/go-apt-cache.log",
			NoConsole: true,
		},
		Repositories: []Repository{
			{Name: "ubuntu", URL: "http://archive.ubuntu.com/ubuntu/"},
		},
	}
}

func Load(path string, def *Config) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg := *def
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}
	if err := validate(&cfg); err != nil {
		return nil, err
	}
	normalize(&cfg)
	return &cfg, nil
}

func EnsureDefault(path string, def *Config) error {
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("file exists: %s", path)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	b, err := yaml.Marshal(def)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0644)
}

func (c *Config) GetRepo(name string) (Repository, bool) {
	for _, r := range c.Repositories {
		if r.Name == name {
			return r, true
		}
	}
	return Repository{}, false
}

func validate(c *Config) error {
	if c.Server.ListenAddr == "" && c.Server.UnixPath == "" {
		return errors.New("server: set listenAddress or unixSocketPath")
	}
	if c.Server.MaxConcurrent <= 0 {
		return errors.New("server.maxConcurrentFetches must be > 0")
	}
	if c.Cache.Enabled {
		if c.Cache.Dir == "" {
			return errors.New("cache.directory is required")
		}
		if sz, err := util.ParseSize(c.Cache.MaxSize); err != nil || sz <= 0 {
			return fmt.Errorf("cache.maxSize invalid: %v", err)
		}
		for i, o := range c.Cache.Overrides {
			if o.TTL <= 0 {
				return fmt.Errorf("cache.overrides[%d]: ttl must be > 0", i)
			}
		}
		if c.Cache.NegativeTTL < 0 {
			return errors.New("cache.negativeCacheTTL must be >= 0")
		}
	}
	for _, r := range c.Repositories {
		if !util.IsRepoNameSafe(r.Name) {
			return fmt.Errorf("invalid repo name: %q", r.Name)
		}
		u, err := url.Parse(r.URL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
			return fmt.Errorf("bad repo url: %q", r.URL)
		}
	}
	return nil
}

func normalize(c *Config) {
	for i := range c.Repositories {
		u, _ := url.Parse(c.Repositories[i].URL)
		if u.Path != "" && !strings.HasSuffix(u.Path, "/") {
			u.Path += "/"
		}
		c.Repositories[i].URL = u.String()
	}
}
