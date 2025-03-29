// internal/config/config.go
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

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
	Name    string `json:"name"` // Unique name for the repository
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
	MaxConcurrentFetches  int      `json:"maxConcurrentFetches"` // Max concurrent fetches from upstream per repo
}

type CacheConfig struct {
	Directory    string `json:"directory"`
	MaxSize      string `json:"maxSize"`
	Enabled      bool   `json:"enabled"`
	CleanOnStart bool   `json:"cleanOnStart"`
	// TTL for caching upstream HEAD validation responses (e.g., 304 Not Modified)
	ValidationTTL Duration `json:"validationTTL"`
}

type LoggingConfig struct {
	Level           string `json:"level"` // debug, info, warn, error
	FilePath        string `json:"filePath"`
	DisableTerminal bool   `json:"disableTerminal"`
	MaxSizeMB       int    `json:"maxSizeMB"`
	MaxBackups      int    `json:"maxBackups"`
	MaxAgeDays      int    `json:"maxAgeDays"`
	Compress        bool   `json:"compress"`
}

type Config struct {
	Server       ServerConfig  `json:"server"`
	Cache        CacheConfig   `json:"cache"`
	Logging      LoggingConfig `json:"logging"`
	Repositories []Repository  `json:"repositories"`
}

// Default creates a default configuration instance.
func Default() *Config {
	return &Config{
		Server: ServerConfig{
			ListenAddress:         ":8080",
			UnixSocketPath:        "", // Disabled by default
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
		Logging: LoggingConfig{
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
				Name:    "ubuntu", // Must be unique and path-safe
				URL:     "http://archive.ubuntu.com/ubuntu",
				Enabled: true,
			},
			{
				Name:    "debian",
				URL:     "http://deb.debian.org/debian",
				Enabled: false, // Example of a disabled repo
			},
		},
	}
}

// Load reads the configuration from a JSON file.
func Load(filePath string) (*Config, error) {
	filePath = util.CleanPath(filePath)
	data, err := os.ReadFile(filePath)
	if err != nil {
		// If file not found, maybe return default config with a warning?
		// Or let the caller handle os.ErrNotExist specifically if needed.
		return nil, fmt.Errorf("failed to read config file %s: %w", filePath, err)
	}

	// Start with defaults and overwrite with file values
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
		// File exists
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		// Other error checking stat
		return fmt.Errorf("failed to check config file %s: %w", filePath, err)
	}

	// File does not exist, create default
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
			return fmt.Errorf("repository name %q is not safe for use in paths", repo.Name)
		}
		if repo.URL == "" {
			return fmt.Errorf("repository %q must have a URL", repo.Name)
		}
		if _, exists := repoNames[repo.Name]; exists {
			return fmt.Errorf("duplicate repository name found: %q", repo.Name)
		}
		repoNames[repo.Name] = struct{}{}
		if repo.Enabled {
			hasEnabledRepo = true
		}
	}

	if !hasEnabledRepo {
		return errors.New("at least one repository must be enabled")
	}

	// Validate logging level? (Let logger handle unknown levels)

	return nil
}
