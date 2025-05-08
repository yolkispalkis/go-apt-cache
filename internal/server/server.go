package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog"
	"github.com/yolkispalkis/go-apt-cache/internal/cache"
	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/fetch"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

type Server struct {
	httpSrv   *http.Server
	cfg       *config.Config
	log       zerolog.Logger
	listeners []net.Listener
}

func New(cfg *config.Config, cm cache.Manager, fc *fetch.Coordinator, logger zerolog.Logger) (*Server, error) {
	mux := http.NewServeMux()
	log := logger.With().Str("component", "server").Logger()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = fmt.Fprintln(w, "Go APT Proxy operational. Endpoints are per configured repository name (e.g. /ubuntu/...).")
	})

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		statusStr := "OK\n"
		if cfg.Cache.Enabled && cm != nil {
			statusStr += fmt.Sprintf("Cache Items: %d\n", cm.ItemCount())
			statusStr += fmt.Sprintf("Cache Size: %s / %s\n",
				util.FormatSize(cm.CurrentSize()), cfg.Cache.MaxSize)
		} else {
			statusStr += "Cache: Disabled\n"
		}
		_, _ = w.Write([]byte(statusStr))
	})

	registeredCount := 0
	for _, repoCfg := range cfg.Repositories {
		if !repoCfg.Enabled {
			log.Info().Str("repo", repoCfg.Name).Msg("Skipping disabled repository.")
			continue
		}

		rh := newRepoHandler(repoCfg, cfg.Server, cfg.Cache, cm, fc, log)

		pathPrefix := "/" + strings.Trim(repoCfg.Name, "/") + "/"

		mux.Handle(pathPrefix, http.StripPrefix(pathPrefix, rh))

		log.Info().
			Str("repo", repoCfg.Name).
			Str("prefix", pathPrefix).
			Str("upstream", repoCfg.URL).
			Msg("Registered repository handler")
		registeredCount++
	}
	if registeredCount == 0 {
		log.Warn().Msg("No repositories enabled or configured. Proxy will only serve root and /status.")
	}

	var finalHandler http.Handler = mux
	finalHandler = LoggingMiddleware(log)(finalHandler)
	finalHandler = RecoveryMiddleware(log)(finalHandler)

	httpSrv := &http.Server{
		Handler:           finalHandler,
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout.StdDuration(),
		IdleTimeout:       cfg.Server.IdleTimeout.StdDuration(),
	}

	return &Server{httpSrv: httpSrv, cfg: cfg, log: log}, nil
}

func (s *Server) Start(ctx context.Context) error {
	if err := s.setupListeners(); err != nil {
		return fmt.Errorf("setup listeners: %w", err)
	}
	if len(s.listeners) == 0 {
		return errors.New("no listeners configured or started (check ListenAddress and UnixSocketPath in config)")
	}

	errChan := make(chan error, len(s.listeners))

	for _, l := range s.listeners {
		lis := l
		s.log.Info().
			Str("net", lis.Addr().Network()).
			Str("addr", lis.Addr().String()).
			Msg("Starting server on listener")
		go func() {
			if err := s.httpSrv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
				s.log.Error().Err(err).
					Str("net", lis.Addr().Network()).
					Str("addr", lis.Addr().String()).
					Msg("HTTP server error on listener")
				errChan <- err
			} else {
				s.log.Info().
					Str("net", lis.Addr().Network()).
					Str("addr", lis.Addr().String()).
					Msg("HTTP server on listener shut down")
			}
		}()
	}

	s.log.Info().Msg("Go APT Proxy server started and listening.")

	select {
	case err := <-errChan:
		s.log.Error().Err(err).Msg("Listener failed, initiating shutdown.")

		return fmt.Errorf("listener error: %w", err)
	case <-ctx.Done():
		s.log.Info().Msg("Shutdown signal received, stopping server...")
		return s.Shutdown()
	}
}

func (s *Server) setupListeners() error {
	var errs []string

	if addr := s.cfg.Server.ListenAddr; addr != "" {
		tcpLn, err := net.Listen("tcp", addr)
		if err != nil {
			errs = append(errs, fmt.Sprintf("tcp listen on %s: %v", addr, err))
		} else {
			s.listeners = append(s.listeners, tcpLn)
			s.log.Info().Str("addr", tcpLn.Addr().String()).Msg("TCP listener created")
		}
	}

	if sockPath := s.cfg.Server.UnixPath; sockPath != "" {
		cleanSockPath := util.CleanPath(sockPath)
		sockDir := filepath.Dir(cleanSockPath)

		if err := os.MkdirAll(sockDir, 0755); err != nil {
			errs = append(errs, fmt.Sprintf("mkdir for unix socket dir %s: %v", sockDir, err))
		} else {

			if _, err := os.Stat(cleanSockPath); err == nil {
				if rmErr := os.Remove(cleanSockPath); rmErr != nil {
					errs = append(errs, fmt.Sprintf("remove existing unix socket %s: %v", cleanSockPath, rmErr))
				}
			} else if !os.IsNotExist(err) {
				errs = append(errs, fmt.Sprintf("stat unix socket %s: %v", cleanSockPath, err))
			}

			if len(errs) == 0 || (len(errs) > 0 && !strings.Contains(errs[len(errs)-1], cleanSockPath)) {
				unixLn, err := net.Listen("unix", cleanSockPath)
				if err != nil {
					errs = append(errs, fmt.Sprintf("unix listen on %s: %v", cleanSockPath, err))
				} else {
					perms := s.cfg.Server.UnixPerms.StdFileMode()
					if err := os.Chmod(cleanSockPath, perms); err != nil {
						_ = unixLn.Close()
						_ = os.Remove(cleanSockPath)
						errs = append(errs, fmt.Sprintf("chmod unix socket %s to %0o: %v", cleanSockPath, perms, err))
					} else {
						s.listeners = append(s.listeners, unixLn)
						s.log.Info().Str("path", cleanSockPath).Str("perms", fmt.Sprintf("0%o", perms)).Msg("Unix socket listener created")
					}
				}
			}
		}
	}

	if len(errs) > 0 {

		for _, l := range s.listeners {
			_ = l.Close()
		}
		s.listeners = nil
		return errors.New("failed to setup one or more listeners: " + strings.Join(errs, "; "))
	}
	return nil
}

func (s *Server) Shutdown() error {
	s.log.Info().Msg("Attempting graceful server shutdown...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.Server.ShutdownTimeout.StdDuration())
	defer cancel()

	var mainErr error
	if err := s.httpSrv.Shutdown(shutdownCtx); err != nil {
		s.log.Error().Err(err).Msg("HTTP server shutdown error.")
		mainErr = err
	} else {
		s.log.Info().Msg("HTTP server shutdown complete.")
	}

	if s.cfg.Server.UnixPath != "" {
		cleanSockPath := util.CleanPath(s.cfg.Server.UnixPath)

		wasListening := false
		for _, l := range s.listeners {
			if l.Addr().Network() == "unix" && l.Addr().String() == cleanSockPath {
				wasListening = true
				break
			}
		}
		if wasListening {
			s.log.Debug().Str("path", cleanSockPath).Msg("Removing Unix socket file.")

			if rmErr := os.Remove(cleanSockPath); rmErr != nil && !os.IsNotExist(rmErr) {
				s.log.Warn().Err(rmErr).Str("path", cleanSockPath).Msg("Failed to remove unix socket file during shutdown.")
				if mainErr == nil {
					mainErr = rmErr
				}
			}
		}
	}
	return mainErr
}
