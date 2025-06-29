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
	cfgPath := flag.String("config", "/etc/go-apt-cache/config.json", "Path to config file")
	createCfg := flag.Bool("create-config", false, "Create default config file and exit")
	flag.Parse()

	if *createCfg {
		return config.EnsureDefault(*cfgPath)
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	logger, err := logging.New(cfg.Logging)
	if err != nil {
		return fmt.Errorf("failed to setup logging: %w", err)
	}
	logger.Info().Str("path", *cfgPath).Msg("Configuration loaded")

	util.InitBufferPool(cfg.Cache.BufferSize, logger)

	cacheManager, err := cache.NewDiskLRU(cfg.Cache, logger)
	if err != nil {
		return fmt.Errorf("failed to initialize cache manager: %w", err)
	}
	defer cacheManager.Close()

	fetchCoordinator := fetch.NewCoordinator(cfg.Server, logger)

	app := server.NewApplication(cfg, logger, cacheManager, fetchCoordinator)

	srv := server.New(cfg.Server, logger, app.Routes())

	errChan := make(chan error, 1)
	go func() {
		logger.Info().
			Str("tcp_addr", cfg.Server.ListenAddr).
			Str("unix_socket", cfg.Server.UnixPath).
			Msg("Starting server...")
		errChan <- srv.Start()
	}()

	select {
	case err := <-errChan:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error().Err(err).Msg("Server failed")
			return err
		}
	case <-ctx.Done():
		logger.Info().Msg("Shutdown signal received. Shutting down...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error().Err(err).Msg("Graceful server shutdown failed")
			return err
		}
		logger.Info().Msg("Server shut down gracefully")
	}

	return nil
}
