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
	httpServer   *http.Server
	cfg          *config.Config
	cacheManager cache.CacheManager
	fetcher      *fetch.Coordinator
	logger       zerolog.Logger
	listeners    []net.Listener
}

func New(cfg *config.Config, cm cache.CacheManager, fc *fetch.Coordinator, logger zerolog.Logger) (*Server, error) {
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintln(w, "Go APT Proxy operational. Configured repositories are active.")
	})

	registeredCount := 0
	for _, repoCfg := range cfg.Repositories {
		if !repoCfg.Enabled {
			logger.Info().Str("repository", repoCfg.Name).Msg("Skipping disabled repository.")
			continue
		}

		if !strings.HasSuffix(repoCfg.URL, "/") {
			repoCfg.URL += "/"
		}

		repoHandler := NewRepositoryHandler(repoCfg, cfg.Server, cfg.Cache, cm, fc, logger)

		pathPrefix := "/" + strings.Trim(repoCfg.Name, "/") + "/"

		stripPath := "/" + strings.Trim(repoCfg.Name, "/") + "/"

		mux.Handle(pathPrefix, http.StripPrefix(stripPath, repoHandler))

		logger.Info().
			Str("repository", repoCfg.Name).
			Str("path_prefix", pathPrefix).
			Str("upstream_url", repoCfg.URL).
			Msg("Registered repository handler")
		registeredCount++
	}

	if registeredCount == 0 {
		logger.Warn().Msg("No repositories enabled or configured. Proxy will only serve root path.")
	}

	var finalHandler http.Handler = mux
	finalHandler = RecoveryMiddleware(logger)(finalHandler)
	finalHandler = LoggingMiddleware(logger)(finalHandler)

	httpSrv := &http.Server{
		Handler:           finalHandler,
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout.Duration(),
		IdleTimeout:       cfg.Server.IdleTimeout.Duration(),
	}

	return &Server{
		httpServer:   httpSrv,
		cfg:          cfg,
		cacheManager: cm,
		fetcher:      fc,
		logger:       logger,
	}, nil
}

func (s *Server) Start(ctx context.Context) error {

	if err := s.setupListeners(); err != nil {
		return fmt.Errorf("failed to setup listeners: %w", err)
	}
	if len(s.listeners) == 0 {
		return errors.New("no listeners configured or started")
	}

	errChan := make(chan error, len(s.listeners))

	for _, l := range s.listeners {
		listener := l
		s.logger.Info().
			Str("network", listener.Addr().Network()).
			Str("address", listener.Addr().String()).
			Msg("Starting server on listener")

		go func() {
			if err := s.httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				s.logger.Error().Err(err).
					Str("network", listener.Addr().Network()).
					Str("address", listener.Addr().String()).
					Msg("HTTP server error on listener")
				errChan <- err
			} else {
				s.logger.Info().
					Str("network", listener.Addr().Network()).
					Str("address", listener.Addr().String()).
					Msg("HTTP server on listener shut down gracefully")
			}
		}()
	}

	s.logger.Info().Msg("Go APT Proxy server started.")

	select {
	case err := <-errChan:
		s.logger.Error().Err(err).Msg("Listener failed, initiating shutdown.")
		return fmt.Errorf("listener error: %w", err)
	case <-ctx.Done():
		s.logger.Info().Msg("Shutdown signal received, stopping server.")
		return s.Shutdown()
	}
}

func (s *Server) setupListeners() error {
	var setupErrors []error

	if s.cfg.Server.ListenAddress != "" {
		tcpListener, err := net.Listen("tcp", s.cfg.Server.ListenAddress)
		if err != nil {
			setupErrors = append(setupErrors, fmt.Errorf("tcp listen on %s: %w", s.cfg.Server.ListenAddress, err))
		} else {
			s.listeners = append(s.listeners, tcpListener)
			s.logger.Info().Str("address", tcpListener.Addr().String()).Msg("TCP listener created")
		}
	}

	if s.cfg.Server.UnixSocketPath != "" {
		socketPath := util.CleanPath(s.cfg.Server.UnixSocketPath)
		socketDir := filepath.Dir(socketPath)

		if err := os.MkdirAll(socketDir, 0755); err != nil {
			setupErrors = append(setupErrors, fmt.Errorf("mkdir for unix socket dir %s: %w", socketDir, err))
		} else {

			if _, err := os.Stat(socketPath); err == nil {
				if err := os.Remove(socketPath); err != nil {
					setupErrors = append(setupErrors, fmt.Errorf("remove existing unix socket %s: %w", socketPath, err))
				}
			} else if !os.IsNotExist(err) {
				setupErrors = append(setupErrors, fmt.Errorf("stat unix socket %s: %w", socketPath, err))
			}

			if len(setupErrors) == 0 {
				unixListener, err := net.Listen("unix", socketPath)
				if err != nil {
					setupErrors = append(setupErrors, fmt.Errorf("unix listen on %s: %w", socketPath, err))
				} else {
					perms := s.cfg.Server.UnixSocketPermissions.FileMode()
					if err := os.Chmod(socketPath, perms); err != nil {
						_ = unixListener.Close()
						_ = os.Remove(socketPath)
						setupErrors = append(setupErrors, fmt.Errorf("chmod for unix socket %s to %0o: %w", socketPath, perms, err))
					} else {
						s.listeners = append(s.listeners, unixListener)
						s.logger.Info().Str("path", socketPath).Str("permissions", fmt.Sprintf("0%o", perms)).Msg("Unix socket listener created")
					}
				}
			}
		}
	}

	if len(setupErrors) > 0 {

		for _, l := range s.listeners {
			l.Close()
		}
		s.listeners = nil

		var errorStrings []string
		for _, e := range setupErrors {
			errorStrings = append(errorStrings, e.Error())
		}
		return errors.New("failed to setup one or more listeners: " + strings.Join(errorStrings, "; "))
	}
	return nil
}

func (s *Server) Shutdown() error {
	s.logger.Info().Msg("Attempting graceful server shutdown...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.Server.ShutdownTimeout.Duration())
	defer cancel()

	err := s.httpServer.Shutdown(shutdownCtx)
	if err != nil {
		s.logger.Error().Err(err).Msg("HTTP server shutdown failed.")
	} else {
		s.logger.Info().Msg("HTTP server shutdown complete.")
	}

	if s.cfg.Server.UnixSocketPath != "" {
		socketPath := util.CleanPath(s.cfg.Server.UnixSocketPath)

		found := false
		for _, l := range s.listeners {
			if l.Addr().Network() == "unix" && l.Addr().String() == socketPath {
				found = true
				break
			}
		}
		if found {
			s.logger.Debug().Str("path", socketPath).Msg("Removing Unix socket file.")
			if removeErr := os.Remove(socketPath); removeErr != nil && !os.IsNotExist(removeErr) {
				s.logger.Warn().Err(removeErr).Str("path", socketPath).Msg("Failed to remove unix socket file during shutdown.")
				if err == nil {
					err = removeErr
				}
			}
		}
	}
	return err
}
