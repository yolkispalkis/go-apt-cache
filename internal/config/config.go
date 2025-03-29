package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
			return err
		}
		*d = Duration(tmp)
		return nil
	default:
		return errors.New("invalid duration type")
	}
}

func (d Duration) Duration() time.Duration {
	return time.Duration(d)
}

type FileMode os.FileMode

func (fm FileMode) MarshalJSON() ([]byte, error) {
	return json.Marshal(fmt.Sprintf("%o", os.FileMode(fm)))
}

func (fm *FileMode) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	var mode uint32
	if _, err := fmt.Sscan(s, &mode); err != nil {
		return fmt.Errorf("invalid file mode format: %w", err)
	}
	*fm = FileMode(os.FileMode(mode))
	return nil
}

func (fm FileMode) FileMode() os.FileMode {
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
}

type CacheConfig struct {
	Directory     string   `json:"directory"`
	MaxSize       string   `json:"maxSize"`
	Enabled       bool     `json:"enabled"`
	CleanOnStart  bool     `json:"cleanOnStart"`
	ValidationTTL Duration `json:"validationTTL"`
}

type Config struct {
	Server       ServerConfig          `json:"server"`
	Cache        CacheConfig           `json:"cache"`
	Logging      logging.LoggingConfig `json:"logging"`
	Repositories []Repository          `json:"repositories"`
}

// Default creates a default configuration instance.
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
		},
		Cache: CacheConfig{
			Directory:     "./cache_data",
			MaxSize:       "10GB",
			Enabled:       true,
			CleanOnStart:  false,
			ValidationTTL: Duration(5 * time.Minute),
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

// Load reads the configuration from a JSON file.
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

	return cfg, nil
}

// Save writes the configuration to a JSON file.
func Save(cfg *Config, filePath string) error {
	filePath = util.CleanPath(filePath)
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
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

// EnsureDefaultConfig creates a default config file if it doesn't exist.
func EnsureDefaultConfig(filePath string) error {
	filePath = util.CleanPath(filePath)
	if _, err := os.Stat(filePath); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to check config file %s: %w", filePath, err)
	}

	fmt.Printf("Config file not found at %s, creating default.\n", filePath)
	return Save(Default(), filePath)
}

// Validate checks the configuration for potential issues.
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

	if cfg.Cache.Enabled {
		if cfg.Cache.Directory == "" {
			return errors.New("cache.directory must be set when cache is enabled")
		}
		if _, err := util.ParseSize(cfg.Cache.MaxSize); err != nil {
			return fmt.Errorf("invalid cache.maxSize %q: %w", cfg.Cache.MaxSize, err)
		}
		if cfg.Cache.ValidationTTL < 0 {
			return errors.New("cache.validationTTL cannot be negative")
		}
	}

	if len(cfg.Repositories) == 0 {
		return errors.New("at least one repository must be configured")
	}

	repoNames := make(map[string]struct{})
	hasEnabledRepo := false
	for i, repo := range cfg.Repositories {
		if repo.Name == "" {
			return fmt.Errorf("repository %d must have a name", i)
		}
		if !util.IsPathSafe(repo.Name) {
			return fmt.Errorf("repository name %q contains unsafe characters", repo.Name)
		}
		if repo.URL == "" {
			return fmt.Errorf("repository %q must have a URL", repo.Name)
		}
		if _, exists := repoNames[repo.Name]; exists {
			return fmt.Errorf("duplicate repository name %q", repo.Name)
		}
		repoNames[repo.Name] = struct{}{}

		if repo.Enabled {
			hasEnabledRepo = true
		}
	}

	if !hasEnabledRepo {
		return errors.New("at least one repository must be enabled")
	}

	return nil
}
