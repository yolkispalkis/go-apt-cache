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

	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		*d = Duration(time.Duration(value))
		return nil
	case string:
		tmp, err := time.ParseDuration(value)
		if err != nil {
			seconds, errInt := json.Number(value).Int64()
			if errInt == nil && seconds >= 0 {
				tmp = time.Duration(seconds) * time.Second
			} else {
				return fmt.Errorf("invalid duration format '%s': %w", value, err)
			}
		}
		*d = Duration(tmp)
		return nil
	default:
		return fmt.Errorf("invalid duration type: %T", v)
	}
}

func (d Duration) Duration() time.Duration {
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
	_, err := fmt.Sscanf(s, "0%o", &mode)
	if err != nil {
		_, err2 := fmt.Sscanf(s, "%o", &mode)
		if err2 != nil {
			modeDecimal, errDecimal := json.Number(s).Int64()
			if errDecimal == nil {
				mode = uint32(modeDecimal)
			} else {
				return fmt.Errorf("invalid file mode format '%s': %v / %v / %v", s, err, err2, errDecimal)
			}
		}
	}
	*fm = FileMode(os.FileMode(mode))
	return nil
}

func (fm FileMode) FileMode() os.FileMode {
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
	ListenAddress         string   `json:"listenAddress"`
	UnixSocketPath        string   `json:"unixSocketPath"`
	UnixSocketPermissions FileMode `json:"unixSocketPermissions"`
	RequestTimeout        Duration `json:"requestTimeout"`
	ShutdownTimeout       Duration `json:"shutdownTimeout"`
	IdleTimeout           Duration `json:"idleTimeout"`
	ReadHeaderTimeout     Duration `json:"readHeaderTimeout"`
	MaxConcurrentFetches  int      `json:"maxConcurrentFetches"`
	UserAgent             string   `json:"userAgent,omitempty"`
}

type CacheConfig struct {
	Directory                string   `json:"directory"`
	MaxSize                  string   `json:"maxSize"`
	Enabled                  bool     `json:"enabled"`
	CleanOnStart             bool     `json:"cleanOnStart"`
	ValidationTTL            Duration `json:"validationTTL"`
	SkipValidationExtensions []string `json:"skipValidationExtensions,omitempty"`
}

type Config struct {
	Server       ServerConfig          `json:"server"`
	Cache        CacheConfig           `json:"cache"`
	Logging      logging.LoggingConfig `json:"logging"`
	Repositories []Repository          `json:"repositories"`
}

func Default() *Config {
	return &Config{
		Server: ServerConfig{
			ListenAddress:         ":8080",
			UnixSocketPath:        "",
			UnixSocketPermissions: 0660,
			RequestTimeout:        Duration(60 * time.Second),
			ShutdownTimeout:       Duration(15 * time.Second),
			IdleTimeout:           Duration(120 * time.Second),
			ReadHeaderTimeout:     Duration(10 * time.Second),
			MaxConcurrentFetches:  10,
			UserAgent:             "go-apt-proxy/1.0 (+https://github.com/yolkispalkis/go-apt-cache)",
		},
		Cache: CacheConfig{
			Directory:                "./cache_data",
			MaxSize:                  "10GB",
			Enabled:                  true,
			CleanOnStart:             false,
			ValidationTTL:            Duration(5 * time.Minute),
			SkipValidationExtensions: []string{".deb", ".udeb", ".ddeb"},
		},
		Logging: logging.LoggingConfig{
			Level:           "info",
			FilePath:        "./logs/apt_cache.log",
			DisableTerminal: false,
			MaxSizeMB:       100,
			MaxBackups:      3,
			MaxAgeDays:      7,
			Compress:        true,
		},
		Repositories: []Repository{
			{
				Name:    "ubuntu",
				URL:     "http://archive.ubuntu.com/ubuntu",
				Enabled: true,
			},
			{
				Name:    "debian",
				URL:     "http://deb.debian.org/debian",
				Enabled: false,
			},
		},
	}
}

func Load(filePath string) (*Config, error) {
	filePath = util.CleanPath(filePath)
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", filePath, err)
	}

	cfg := Default()
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", filePath, err)
	}

	cfg.Cache.Directory = util.CleanPath(cfg.Cache.Directory)
	cfg.Logging.FilePath = util.CleanPath(cfg.Logging.FilePath)
	cfg.Server.UnixSocketPath = util.CleanPath(cfg.Server.UnixSocketPath)

	return cfg, nil
}

func Save(cfg *Config, filePath string) error {
	filePath = util.CleanPath(filePath)
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s for config: %w", dir, err)
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file %s: %w", filePath, err)
	}
	return nil
}

func EnsureDefaultConfig(filePath string) error {
	filePath = util.CleanPath(filePath)
	if _, err := os.Stat(filePath); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to check config file status %s: %w", filePath, err)
	}

	fmt.Printf("Config file not found at %s, creating default.\n", filePath)
	return Save(Default(), filePath)
}

func Validate(cfg *Config) error {
	if cfg.Server.ListenAddress == "" && cfg.Server.UnixSocketPath == "" {
		return errors.New("server must configure at least one of listenAddress or unixSocketPath")
	}
	if cfg.Server.RequestTimeout <= 0 {
		return errors.New("server.requestTimeout must be positive")
	}
	if cfg.Server.ShutdownTimeout <= 0 {
		return errors.New("server.shutdownTimeout must be positive")
	}
	if cfg.Server.IdleTimeout <= 0 {
		return errors.New("server.idleTimeout must be positive")
	}
	if cfg.Server.ReadHeaderTimeout <= 0 {
		return errors.New("server.readHeaderTimeout must be positive")
	}
	if cfg.Server.MaxConcurrentFetches <= 0 {
		return errors.New("server.maxConcurrentFetches must be positive")
	}
	if cfg.Server.UnixSocketPath != "" {
		if cfg.Server.UnixSocketPermissions.FileMode()&0777 == 0 {
			logging.Warn("server.unixSocketPermissions is 0, using default 0660", "path", cfg.Server.UnixSocketPath)
		}
	}
	if cfg.Server.UserAgent == "" {
		logging.Warn("server.userAgent is empty, using default", "default", Default().Server.UserAgent)
		cfg.Server.UserAgent = Default().Server.UserAgent
	}

	if cfg.Cache.Enabled {
		if cfg.Cache.Directory == "" {
			return errors.New("cache.directory must be set when cache is enabled")
		}
		if !filepath.IsAbs(cfg.Cache.Directory) && !strings.HasPrefix(cfg.Cache.Directory, ".") {
			logging.Warn("Cache directory is relative and doesn't start with '.', ensure it's intended relative to the working directory.", "directory", cfg.Cache.Directory)
		}
		parsedSize, err := util.ParseSize(cfg.Cache.MaxSize)
		if err != nil {
			return fmt.Errorf("invalid cache.maxSize %q: %w", cfg.Cache.MaxSize, err)
		}
		if parsedSize <= 0 {
			return fmt.Errorf("cache.maxSize must be positive when cache is enabled (got %s -> %d bytes)", cfg.Cache.MaxSize, parsedSize)
		}
		if cfg.Cache.ValidationTTL < 0 {
			return errors.New("cache.validationTTL cannot be negative")
		}
		for i, ext := range cfg.Cache.SkipValidationExtensions {
			if !strings.HasPrefix(ext, ".") || len(ext) < 2 {
				return fmt.Errorf("invalid extension in cache.skipValidationExtensions[%d]: %q (must start with '.' and have content)", i, ext)
			}
		}
	}

	if _, err := logging.ParseLevel(cfg.Logging.Level); err != nil {
		return fmt.Errorf("invalid logging.level: %w", err)
	}

	if len(cfg.Repositories) == 0 {
		logging.Warn("No repositories configured in the 'repositories' list. Proxy will only serve status.")
	}

	repoNames := make(map[string]struct{})
	hasEnabledRepo := false
	for i, repo := range cfg.Repositories {
		if repo.Name == "" {
			return fmt.Errorf("repository %d must have a 'name'", i)
		}
		if !util.IsRepoNameSafe(repo.Name) {
			return fmt.Errorf("repository name %q contains invalid characters or is empty/dot/dotdot (allowed: a-z, A-Z, 0-9, -, _)", repo.Name)
		}
		if repo.URL == "" {
			return fmt.Errorf("repository %q must have a 'url'", repo.Name)
		}
		parsedURL, err := url.Parse(repo.URL)
		if err != nil {
			return fmt.Errorf("repository %q has an invalid URL %q: %w", repo.Name, repo.URL, err)
		}
		if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
			return fmt.Errorf("repository %q URL scheme must be http or https, got %q", repo.Name, parsedURL.Scheme)
		}
		if parsedURL.Host == "" {
			return fmt.Errorf("repository %q URL is missing a host", repo.Name)
		}
		if _, exists := repoNames[repo.Name]; exists {
			return fmt.Errorf("duplicate repository name %q found", repo.Name)
		}
		repoNames[repo.Name] = struct{}{}
		if repo.Enabled {
			hasEnabledRepo = true
		}
	}

	if !hasEnabledRepo && len(cfg.Repositories) > 0 {
		logging.Warn("No repositories are enabled in the configuration. The proxy will not serve any repository data.")
	}

	return nil
}
