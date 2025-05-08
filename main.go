package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/yolkispalkis/go-apt-cache/internal/cache"
	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/fetch"
	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/server"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		_ = logging.Sync()
		os.Exit(1)
	}
	fmt.Println("Shutdown complete.")
	_ = logging.Sync()
}

func run(ctx context.Context) error {
	cfgPath, createCfg, logLevel, listenAddr := parseFlags()

	if createCfg {
		if err := config.EnsureDefault(cfgPath); err != nil {
			return fmt.Errorf("ensure default config at %s: %w", cfgPath, err)
		}
		fmt.Printf("Default config ensured/created at %s. Review and adjust.\n", cfgPath)
		return nil
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		_ = logging.Setup(config.DefaultLogging())
		localLog := logging.L()
		localLog.Fatal().Err(err).Str("file", cfgPath).Msg("Failed to load config")
		return fmt.Errorf("load config %s: %w", cfgPath, err)
	}

	applyOverrides(cfg, logLevel, listenAddr)

	if err := config.Validate(cfg); err != nil {
		_ = logging.Setup(cfg.Logging)
		localLog := logging.L()
		localLog.Fatal().Err(err).Msg("Invalid config")
		return fmt.Errorf("invalid config: %w", err)
	}

	if err := logging.Setup(cfg.Logging); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to setup logging: %v\n", err)
		return fmt.Errorf("setup logging: %w", err)
	}
	log := logging.L()
	log.Info().Str("file", cfgPath).Msg("Config loaded and validated")

	cm, err := cache.NewDiskLRU(cfg.Cache, log)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to init cache manager")
		return fmt.Errorf("init cache: %w", err)
	}
	if err := cm.Init(ctx); err != nil {
		log.Error().Err(err).Msg("Cache manager init warning (continuing)")
	} else {
		log.Info().Str("dir", cfg.Cache.Dir).Str("size", cfg.Cache.MaxSize).
			Int64("items", cm.ItemCount()).Str("used", util.FormatSize(cm.CurrentSize())).
			Msg("Cache manager initialized")
	}
	defer cm.Close()

	fc := fetch.NewCoordinator(cfg.Server, log)
	log.Info().Str("ua", cfg.Server.UserAgent).Int("max_fetches", cfg.Server.MaxConcurrent).Msg("Fetch coordinator initialized")

	srv, err := server.New(cfg, cm, fc, log)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create HTTP server")
		return fmt.Errorf("create server: %w", err)
	}

	errChan := make(chan error, 1)
	go func() {
		errChan <- srv.Start(ctx)
	}()

	select {
	case err = <-errChan:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, http.ErrServerClosed) {
			log.Error().Err(err).Msg("Server error")
			_ = srv.Shutdown()
			return err
		}
	case <-ctx.Done():
		log.Info().Msg("Shutdown signal received...")
		if err = srv.Shutdown(); err != nil {
			log.Error().Err(err).Msg("Server shutdown error")
			return err
		}
	}

	if cfg.Cache.Enabled {
		log.Info().Int64("items", cm.ItemCount()).Str("used", util.FormatSize(cm.CurrentSize())).Msg("Final cache status")
	}
	return nil
}

func parseFlags() (cfgPath string, createCfg bool, logLevel, listenAddr string) {
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fs.StringVar(&cfgPath, "config", "config.json", "Path to config file")
	fs.BoolVar(&createCfg, "create-config", false, "Create default config file and exit")
	fs.StringVar(&logLevel, "log-level", "", "Override log level (debug, info, warn, error)")
	fs.StringVar(&listenAddr, "listen", "", "Override server listen address (e.g., :8080)")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage of %s:\n", os.Args[0])
		fs.PrintDefaults()
	}
	_ = fs.Parse(os.Args[1:])
	return
}

func applyOverrides(cfg *config.Config, logLevel, listenAddr string) {
	if logLevel != "" {
		cfg.Logging.Level = logLevel
	}
	if listenAddr != "" {
		cfg.Server.ListenAddr = listenAddr
	}
}
