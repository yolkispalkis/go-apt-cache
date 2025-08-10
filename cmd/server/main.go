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
	"github.com/yolkispalkis/go-apt-cache/internal/log"
	"github.com/yolkispalkis/go-apt-cache/internal/server"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

var (
	AppName    = "go-apt-cache"
	AppVersion = "dev" // переопределяется -ldflags
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cfgPath := flag.String("config", "/etc/go-apt-cache/config.yaml", "Path to config file (.yaml, .yml)")
	createCfg := flag.Bool("create-config", false, "Create default config and exit")
	flag.Parse()

	def := config.Default(AppName, AppVersion)
	if *createCfg {
		return config.EnsureDefault(*cfgPath, def)
	}

	cfg, err := config.Load(*cfgPath, def)
	if err != nil {
		return err
	}
	logger, err := log.New(cfg.Logging)
	if err != nil {
		return err
	}
	logger.Info().Str("config", *cfgPath).Msg("configuration loaded")

	util.InitBufferPool(cfg.Cache.BufferSize, logger)

	cm, err := cache.NewLRU(cfg.Cache, logger)
	if err != nil {
		return err
	}
	defer cm.Close()

	fc := fetch.New(cfg.Server, logger)
	app := server.NewApplication(cfg, logger, cm, fc)
	srv := server.New(cfg.Server, logger, app.Routes())

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Start() }()

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		shctx, cancel := context.WithTimeout(context.Background(), srv.ShutdownTimeout())
		defer cancel()
		if err := srv.Shutdown(shctx); err != nil {
			return err
		}
	}
	return nil
}
