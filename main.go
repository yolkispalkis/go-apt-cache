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
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cfgPath, createCfg, logLevelOverride, listenAddrOverride := parseFlags()

	if createCfg {
		if err := config.EnsureDefault(cfgPath); err != nil {
			return fmt.Errorf("ensure default config at %s: %w", cfgPath, err)
		}
		fmt.Printf("Default config created at %s\n", cfgPath)
		return nil
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		return fmt.Errorf("load config %s: %w", cfgPath, err)
	}

	applyOverrides(cfg, logLevelOverride, listenAddrOverride)

	if err := logging.Setup(cfg.Logging); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to setup logging: %v\n", err)
		// Continue with default console logging
	}

	log := logging.L()

	if err := config.Validate(cfg); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	log.Info().Str("file", cfgPath).Msg("Config loaded and validated")

	cm, err := cache.NewDiskLRU(cfg.Cache, log)
	if err != nil {
		return fmt.Errorf("init cache: %w", err)
	}

	if err := cm.Init(ctx); err != nil {
		log.Error().Err(err).Msg("Cache init failed, continuing with empty cache")
	} else {
		log.Info().
			Str("dir", cfg.Cache.Dir).
			Str("size", cfg.Cache.MaxSize).
			Int64("items", cm.ItemCount()).
			Str("used", util.FormatSize(cm.CurrentSize())).
			Msg("Cache initialized")
	}
	defer cm.Close()

	fc := fetch.NewCoordinator(cfg.Server, log)
	log.Info().
		Str("ua", cfg.Server.UserAgent).
		Int("max_fetches", cfg.Server.MaxConcurrent).
		Msg("Fetch coordinator initialized")

	srv, err := server.New(cfg, cm, fc, log)
	if err != nil {
		return fmt.Errorf("create server: %w", err)
	}

	errChan := make(chan error, 1)
	go func() {
		errChan <- srv.Start(ctx)
	}()

	select {
	case err := <-errChan:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, http.ErrServerClosed) {
			log.Error().Err(err).Msg("Server error")
			srv.Shutdown()
			return err
		}
		log.Info().Msg("Server stopped gracefully")
	case <-ctx.Done():
		log.Info().Msg("Shutdown signal received")
		if err := srv.Shutdown(); err != nil {
			log.Error().Err(err).Msg("Server shutdown error")
			return err
		}
		log.Info().Msg("Server shut down gracefully")
	}

	if cfg.Cache.Enabled {
		log.Info().
			Int64("items", cm.ItemCount()).
			Str("used", util.FormatSize(cm.CurrentSize())).
			Msg("Final cache status")
	}
	return nil
}

func parseFlags() (cfgPath string, createCfg bool, logLevel, listenAddr string) {
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fs.PrintDefaults()
	}

	fs.StringVar(&cfgPath, "config", "config.json", "Path to config file")
	fs.BoolVar(&createCfg, "create-config", false, "Create default config file and exit")
	fs.StringVar(&logLevel, "log-level", "", "Override log level (debug, info, warn, error)")
	fs.StringVar(&listenAddr, "listen", "", "Override server listen address (e.g., :8080)")

	if err := fs.Parse(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing flags: %v\n", err)
		os.Exit(2)
	}
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
