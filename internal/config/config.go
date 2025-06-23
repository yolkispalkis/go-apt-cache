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

	"github.com/yolkispalkis/go-apt-cache/internal/appinfo"
	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch val := v.(type) {
	case float64:
		*d = Duration(time.Duration(val))
	case string:
		td, err := time.ParseDuration(val)
		if err != nil {
			s, errInt := json.Number(val).Int64()
			if errInt == nil && s >= 0 {
				td = time.Duration(s) * time.Second
			} else {
				return fmt.Errorf("invalid duration string '%s': %w", val, err)
			}
		}
		*d = Duration(td)
	default:
		return fmt.Errorf("invalid duration type: %T", v)
	}
	return nil
}

func (d Duration) StdDuration() time.Duration {
	return time.Duration(d)
}

type FileMode os.FileMode

func (fm FileMode) MarshalJSON() ([]byte, error) {
	return json.Marshal(fmt.Sprintf("0%o", os.FileMode(fm)))
}

func (fm *FileMode) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	var mode uint32

	if _, err := fmt.Sscanf(s, "0%o", &mode); err != nil {
		if _, err2 := fmt.Sscanf(s, "%o", &mode); err2 != nil {
			mDecimal, errDecimal := json.Number(s).Int64()
			if errDecimal == nil {
				mode = uint32(mDecimal)
			} else {
				return fmt.Errorf("invalid file mode format '%s'", s)
			}
		}
	}
	*fm = FileMode(os.FileMode(mode))
	return nil
}

func (fm FileMode) StdFileMode() os.FileMode {
	if fm == 0 {
		return 0660
	}
	return os.FileMode(fm)
}

type Repository struct {
	Name    string `json:"name"`
	URL     string `json:"url"`
	Enabled bool   `json:"enabled"`
}

type ServerConfig struct {
	ListenAddr        string   `json:"listenAddress"`
	UnixPath          string   `json:"unixSocketPath"`
	UnixPerms         FileMode `json:"unixSocketPermissions"`
	ReqTimeout        Duration `json:"requestTimeout"`
	ShutdownTimeout   Duration `json:"shutdownTimeout"`
	IdleTimeout       Duration `json:"idleTimeout"`
	ReadHeaderTimeout Duration `json:"readHeaderTimeout"`
	MaxConcurrent     int      `json:"maxConcurrentFetches"`
	UserAgent         string   `json:"userAgent,omitempty"`
}

// CacheOverride определяет правило для переопределения TTL кеша для определённых путей.
type CacheOverride struct {
	PathPattern string   `json:"pathPattern"` // Шаблон пути в формате glob, например "dists/*/InRelease"
	TTL         Duration `json:"ttl"`         // Время жизни кеша для этого пути
}

type CacheConfig struct {
	Dir                   string          `json:"directory"`
	MaxSize               string          `json:"maxSize"`
	Enabled               bool            `json:"enabled"`
	CleanOnStart          bool            `json:"cleanOnStart"`
	NegativeTTL           Duration        `json:"negativeCacheTTL"`
	MetadataBatchInterval Duration        `json:"metadataBatchInterval"`
	BufferSize            string          `json:"bufferSize"`
	Overrides             []CacheOverride `json:"overrides,omitempty"` // Правила для переопределения TTL
}

type Config struct {
	Server       ServerConfig   `json:"server"`
	Cache        CacheConfig    `json:"cache"`
	Logging      logging.Config `json:"logging"`
	Repositories []Repository   `json:"repositories"`
}

func Default() *Config {
	return &Config{
		Server: ServerConfig{
			ListenAddr:        ":8080",
			UnixPath:          "/run/go-apt-cache/go-apt-cache.sock",
			UnixPerms:         0660,
			ReqTimeout:        Duration(60 * time.Second),
			ShutdownTimeout:   Duration(15 * time.Second),
			IdleTimeout:       Duration(120 * time.Second),
			ReadHeaderTimeout: Duration(10 * time.Second),
			MaxConcurrent:     20,
			UserAgent:         appinfo.UserAgent(),
		},
		Cache: CacheConfig{
			Dir:                   "/var/cache/go-apt-cache",
			MaxSize:               "10GB",
			Enabled:               true,
			CleanOnStart:          false,
			NegativeTTL:           Duration(5 * time.Minute),
			MetadataBatchInterval: Duration(30 * time.Second),
			BufferSize:            "64KB",
			Overrides: []CacheOverride{
				{PathPattern: "dists/*/InRelease", TTL: Duration(5 * time.Minute)},
				{PathPattern: "**/*.deb", TTL: Duration(30 * 24 * time.Hour)},
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

func Load(path string) (*Config, error) {
	absPath, err := filepath.Abs(util.CleanPath(path))
	if err != nil {
		return nil, fmt.Errorf("abs path for %s: %w", path, err)
	}
	data, err := os.ReadFile(absPath)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", absPath, err)
	}
	cfg := Default()
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", absPath, err)
	}

	if cfg.Cache.Enabled && cfg.Cache.Dir != "" {
		cfg.Cache.Dir = util.CleanPath(cfg.Cache.Dir)
	}
	if cfg.Logging.File != "" {
		cfg.Logging.File = util.CleanPath(cfg.Logging.File)
	}
	if cfg.Server.UnixPath != "" {
		cfg.Server.UnixPath = util.CleanPath(cfg.Server.UnixPath)
	}
	return cfg, nil
}

func Save(cfg *Config, path string) error {
	absPath, err := filepath.Abs(util.CleanPath(path))
	if err != nil {
		return fmt.Errorf("abs path for %s: %w", path, err)
	}
	if err := os.MkdirAll(filepath.Dir(absPath), 0755); err != nil {
		return fmt.Errorf("mkdir for config %s: %w", filepath.Dir(absPath), err)
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	return os.WriteFile(absPath, data, 0644)
}

func EnsureDefault(path string) error {
	absPath, err := filepath.Abs(util.CleanPath(path))
	if err != nil {
		return fmt.Errorf("abs path for %s: %w", path, err)
	}
	if _, err := os.Stat(absPath); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat config %s: %w", absPath, err)
	}

	fmt.Printf("Config file not found at %s, creating default.\n", absPath)
	return Save(Default(), absPath)
}

func Validate(cfg *Config) error {
	s := &cfg.Server
	if s.ListenAddr == "" && s.UnixPath == "" {
		return errors.New("server: must set listenAddress or unixSocketPath")
	}
	if s.ReqTimeout.StdDuration() <= 0 {
		return errors.New("server.requestTimeout must be > 0")
	}
	if s.ShutdownTimeout.StdDuration() <= 0 {
		return errors.New("server.shutdownTimeout must be > 0")
	}
	if s.ReadHeaderTimeout.StdDuration() <= 0 {
		return errors.New("server.readHeaderTimeout must be > 0")
	}
	if s.MaxConcurrent <= 0 {
		return errors.New("server.maxConcurrentFetches must be > 0")
	}
	if s.UserAgent == "" {
		s.UserAgent = appinfo.UserAgent()
	}

	c := &cfg.Cache
	if c.Enabled {
		if c.Dir == "" {
			return errors.New("cache.directory must be set if cache enabled")
		}
		if _, err := util.ParseSize(c.MaxSize); err != nil {
			return fmt.Errorf("invalid cache.maxSize %q: %w", c.MaxSize, err)
		}
		if c.MetadataBatchInterval.StdDuration() <= 0 {
			return errors.New("cache.metadataBatchInterval must be > 0")
		}
		if _, err := util.ParseSize(c.BufferSize); err != nil {
			return fmt.Errorf("invalid cache.bufferSize %q: %w", c.BufferSize, err)
		}
		for i, o := range c.Overrides {
			if o.PathPattern == "" {
				return fmt.Errorf("cache.overrides[%d]: pathPattern cannot be empty", i)
			}
			if _, err := filepath.Match(o.PathPattern, "a"); err != nil {
				return fmt.Errorf("cache.overrides[%d]: invalid pathPattern glob %q: %w", i, o.PathPattern, err)
			}
			if o.TTL.StdDuration() <= 0 {
				return fmt.Errorf("cache.overrides[%d]: ttl for pattern %q must be > 0", i, o.PathPattern)
			}
		}
	}

	repoNames := make(map[string]struct{})
	for i := range cfg.Repositories {
		repo := &cfg.Repositories[i]
		if strings.TrimSpace(repo.Name) == "" {
			return fmt.Errorf("repo %d: name empty", i)
		}
		if !util.IsRepoNameSafe(repo.Name) {
			return fmt.Errorf("repo %q: name invalid, must match %s", repo.Name, util.RepoNameRegexString())
		}
		if strings.TrimSpace(repo.URL) == "" {
			return fmt.Errorf("repo %q: URL empty", repo.Name)
		}
		u, err := url.Parse(repo.URL)
		if err != nil {
			return fmt.Errorf("repo %q: invalid URL %q: %w", repo.Name, repo.URL, err)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return fmt.Errorf("repo %q: URL scheme must be http or https", repo.Name)
		}
		if u.Host == "" {
			return fmt.Errorf("repo %q: URL host missing", repo.Name)
		}

		if !strings.HasSuffix(repo.URL, "/") {
			repo.URL += "/"
		}

		if _, exists := repoNames[repo.Name]; exists {
			return fmt.Errorf("duplicate repo name %q", repo.Name)
		}
		repoNames[repo.Name] = struct{}{}
	}

	return nil
}
