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
		var err error
		td, err := time.ParseDuration(value)
		if err != nil {
			
			seconds, errInt := json.Number(value).Int64()
			if errInt == nil && seconds >= 0 {
				td = time.Duration(seconds) * time.Second
			} else {
				return fmt.Errorf("invalid duration string '%s': %w", value, err)
			}
		}
		*d = Duration(td)
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
	
	if _, err := fmt.Sscanf(s, "0%o", &mode); err != nil {
		if _, err2 := fmt.Sscanf(s, "%o", &mode); err2 != nil {
			
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
	Directory    string `json:"directory"`
	MaxSize      string `json:"maxSize"` 
	Enabled      bool   `json:"enabled"`
	CleanOnStart bool   `json:"cleanOnStart"` 
	
	DefaultTTL Duration `json:"defaultTTL"`
	
	
	RevalidateOnHitTTL Duration `json:"revalidateOnHitTTL"`
	
	NegativeCacheTTL Duration `json:"negativeCacheTTL"`
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
			RequestTimeout:        Duration(30 * time.Second),
			ShutdownTimeout:       Duration(15 * time.Second),
			IdleTimeout:           Duration(120 * time.Second),
			ReadHeaderTimeout:     Duration(10 * time.Second),
			MaxConcurrentFetches:  20,
			UserAgent:             "go-apt-proxy/2.0 (+https:
		},
		Cache: CacheConfig{
			Directory:          "./apt_cache_data",
			MaxSize:            "10GB",
			Enabled:            true,
			CleanOnStart:       false,
			DefaultTTL:         Duration(1 * time.Hour),
			RevalidateOnHitTTL: Duration(0), 
			NegativeCacheTTL:   Duration(5 * time.Minute),
		},
		Logging: logging.DefaultConfig(), 
		Repositories: []Repository{
			{Name: "ubuntu", URL: "http:
			{Name: "debian", URL: "http:
		},
	}
}

func Load(filePath string) (*Config, error) {
	absFilePath, err := filepath.Abs(util.CleanPath(filePath))
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path for config file %s: %w", filePath, err)
	}

	data, err := os.ReadFile(absFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", absFilePath, err)
	}

	cfg := Default() 
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", absFilePath, err)
	}

	
	if cfg.Cache.Enabled && cfg.Cache.Directory != "" {
		cfg.Cache.Directory = util.CleanPath(cfg.Cache.Directory)
	}
	if cfg.Logging.FilePath != "" {
		cfg.Logging.FilePath = util.CleanPath(cfg.Logging.FilePath)
	}
	if cfg.Server.UnixSocketPath != "" {
		cfg.Server.UnixSocketPath = util.CleanPath(cfg.Server.UnixSocketPath)
	}

	return cfg, nil
}

func Save(cfg *Config, filePath string) error {
	absFilePath, err := filepath.Abs(util.CleanPath(filePath))
	if err != nil {
		return fmt.Errorf("failed to get absolute path for config file %s: %w", filePath, err)
	}

	dir := filepath.Dir(absFilePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s for config: %w", dir, err)
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(absFilePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file %s: %w", absFilePath, err)
	}
	return nil
}

func EnsureDefaultConfig(filePath string) error {
	absFilePath, err := filepath.Abs(util.CleanPath(filePath))
	if err != nil {
		return fmt.Errorf("failed to get absolute path for config file %s: %w", filePath, err)
	}
	if _, err := os.Stat(absFilePath); err == nil {
		return nil 
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to check config file status %s: %w", absFilePath, err)
	}

	
	fmt.Printf("Config file not found at %s, creating default.\n", absFilePath)
	return Save(Default(), absFilePath)
}

func Validate(cfg *Config) error {
	if cfg.Server.ListenAddress == "" && cfg.Server.UnixSocketPath == "" {
		return errors.New("server: must configure at least one of listenAddress or unixSocketPath")
	}
	if cfg.Server.RequestTimeout.Duration() <= 0 {
		return errors.New("server.requestTimeout must be a positive duration")
	}
	if cfg.Server.ShutdownTimeout.Duration() <= 0 {
		return errors.New("server.shutdownTimeout must be a positive duration")
	}
	if cfg.Server.IdleTimeout.Duration() < 0 { 
		return errors.New("server.idleTimeout cannot be negative")
	}
	if cfg.Server.ReadHeaderTimeout.Duration() <= 0 {
		return errors.New("server.readHeaderTimeout must be a positive duration")
	}
	if cfg.Server.MaxConcurrentFetches <= 0 {
		return errors.New("server.maxConcurrentFetches must be positive")
	}
	if cfg.Server.UserAgent == "" {
		cfg.Server.UserAgent = Default().Server.UserAgent 
	}

	if cfg.Cache.Enabled {
		if cfg.Cache.Directory == "" {
			return errors.New("cache.directory must be set when cache is enabled")
		}
		
		if !filepath.IsAbs(cfg.Cache.Directory) && !strings.HasPrefix(cfg.Cache.Directory, "."+string(filepath.Separator)) && cfg.Cache.Directory != "." {
			
			
			
		}
		if _, err := util.ParseSize(cfg.Cache.MaxSize); err != nil {
			return fmt.Errorf("invalid cache.maxSize %q: %w", cfg.Cache.MaxSize, err)
		}
		if cfg.Cache.DefaultTTL.Duration() < 0 {
			return errors.New("cache.defaultTTL cannot be negative")
		}
		if cfg.Cache.RevalidateOnHitTTL.Duration() < 0 {
			return errors.New("cache.revalidateOnHitTTL cannot be negative")
		}
		if cfg.Cache.NegativeCacheTTL.Duration() < 0 {
			return errors.New("cache.negativeCacheTTL cannot be negative")
		}
	}

	if _, err := logging.ParseLevel(cfg.Logging.Level); err != nil { 
		return fmt.Errorf("invalid logging.level: %w", err)
	}

	repoNames := make(map[string]struct{})
	hasEnabledRepo := false
	for i, repo := range cfg.Repositories {
		if strings.TrimSpace(repo.Name) == "" {
			return fmt.Errorf("repository %d: name cannot be empty", i)
		}
		if !util.IsRepoNameSafe(repo.Name) { 
			return fmt.Errorf("repository %q: name contains invalid characters or is a reserved name", repo.Name)
		}
		if strings.TrimSpace(repo.URL) == "" {
			return fmt.Errorf("repository %q: URL cannot be empty", repo.Name)
		}
		parsedURL, err := url.Parse(repo.URL)
		if err != nil {
			return fmt.Errorf("repository %q: invalid URL %q: %w", repo.Name, repo.URL, err)
		}
		if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
			return fmt.Errorf("repository %q: URL scheme must be http or https, got %q", repo.Name, parsedURL.Scheme)
		}
		if parsedURL.Host == "" {
			return fmt.Errorf("repository %q: URL is missing a host", repo.Name)
		}
		
		if !strings.HasSuffix(cfg.Repositories[i].URL, "/") {
			cfg.Repositories[i].URL += "/"
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
		
		
	}

	return nil
}
