package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/yolkispalkis/go-apt-cache/internal/cache"
	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/fetch"
	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

type Server struct {
	*http.Server
	cfg          *config.Config
	cacheManager cache.CacheManager
	fetcher      *fetch.Coordinator
}

func New(
	cfg *config.Config,
	cacheManager cache.CacheManager,
	fetcher *fetch.Coordinator,
) (*Server, error) {
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			host := r.Host
			fmt.Fprintf(w, `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>Go APT Proxy</title>
  <style>
    body { font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }
    h1 { color: #333; }
    .repo { margin-bottom: 20px; border: 1px solid #ddd; padding: 15px; border-radius: 5px; }
    .repo h2 { margin-top: 0; color: #0066cc; }
    .status { margin-top: 30px; padding: 10px; background-color: #f8f8f8; border-radius: 5px; }
  </style>
</head>
<body>
  <h1>Go APT Proxy</h1>
  <p>Активные репозитории:</p>
  <div class="repos">`)

			for _, repo := range cfg.Repositories {
				if !repo.Enabled {
					continue
				}
				fmt.Fprintf(w, `
    <div class="repo">
      <h2>%s</h2>
      <p>Upstream URL: <a href="%s">%s</a></p>
      <p>Локальный URL: <a href="/%s/">/%s/</a></p>
      <p>Строка для sources.list: <code id="aptUrl-%s">deb http://%s/%s/ release main</code></p>
      <script>
        document.addEventListener('DOMContentLoaded', function() {
          var baseUrl = window.location.protocol + '//' + window.location.host;
          var repoName = '%s';
          var aptUrlId = 'aptUrl-%s';
          var aptUrl = document.getElementById(aptUrlId);
          if (aptUrl) {
            aptUrl.textContent = 'deb ' + baseUrl + '/' + repoName + '/ release main';
          }
        });
      </script>
    </div>`, repo.Name, repo.URL, repo.URL, repo.Name, repo.Name, repo.Name, host, repo.Name, repo.Name, repo.Name)
			}

			cacheStats := cacheManager.Stats()
			fmt.Fprintf(w, `
  </div>
  <div class="status">
    <h3>Статус кеша:</h3>
    <p>Количество элементов: %d</p>
    <p>Размер кеша: %s / %s</p>
    <p><a href="/status">Подробная информация о статусе</a></p>
  </div>
</body>
</html>`, cacheStats.ItemCount, util.FormatSize(cacheStats.CurrentSize), util.FormatSize(cacheStats.MaxSize))
			return
		}
		if r.URL.Path != "/status" && !strings.HasPrefix(r.URL.Path, "/") {
			http.NotFound(w, r)
			return
		}
	})

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, "OK")
		cacheStats := cacheManager.Stats()
		fmt.Fprintf(w, "Cache Items: %d\n", cacheStats.ItemCount)
		fmt.Fprintf(w, "Cache Size: %s / %s\n", util.FormatSize(cacheStats.CurrentSize), util.FormatSize(cacheStats.MaxSize))
	})

	for _, repo := range cfg.Repositories {
		if !repo.Enabled {
			logging.Info("Skipping disabled repository: %s", repo.Name)
			continue
		}

		pathPrefix := "/" + repo.Name + "/"

		repoHandler := NewRepositoryHandler(repo, cfg.Server, cacheManager, fetcher)

		mux.Handle(pathPrefix, http.StripPrefix(pathPrefix, repoHandler))
		logging.Info("Registered handler for repository %q at path %s (Upstream: %s)", repo.Name, pathPrefix, repo.URL)
	}

	var handler http.Handler = mux
	handler = LoggingMiddleware(handler)
	handler = RecoveryMiddleware(handler)

	httpServer := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout.Duration(),
		IdleTimeout:       cfg.Server.IdleTimeout.Duration(),
	}

	return &Server{
		Server:       httpServer,
		cfg:          cfg,
		cacheManager: cacheManager,
		fetcher:      fetcher,
	}, nil
}

type RepositoryHandler struct {
	repoConfig   config.Repository
	serverConfig config.ServerConfig
	cacheManager cache.CacheManager
	fetcher      *fetch.Coordinator
}

func NewRepositoryHandler(
	repo config.Repository,
	serverCfg config.ServerConfig,
	cache cache.CacheManager,
	fetcher *fetch.Coordinator,
) *RepositoryHandler {
	return &RepositoryHandler{
		repoConfig:   repo,
		serverConfig: serverCfg,
		cacheManager: cache,
		fetcher:      fetcher,
	}
}

func (h *RepositoryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	filePath := strings.TrimPrefix(r.URL.Path, "/")
	if filePath == "" {
		if strings.HasSuffix(r.URL.Path, "/") {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			host := r.Host
			fmt.Fprintf(w, `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>%s Repository - Go APT Proxy</title>
  <style>
    body { font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }
    h1 { color: #333; }
    .repo-info { margin-bottom: 20px; border: 1px solid #ddd; padding: 15px; border-radius: 5px; }
    .repo-info h2 { margin-top: 0; color: #0066cc; }
    .note { margin-top: 30px; padding: 10px; background-color: #f8f8f8; border-radius: 5px; }
  </style>
</head>
<body>
  <h1>%s Repository</h1>
  <div class="repo-info">
    <h2>Repository Information</h2>
    <p>Upstream URL: <a href="%s">%s</a></p>
    <p>Local URL: <a href="/%s/">/%s/</a></p>
  </div>
  <div class="note">
    <p>Это прокси-кеш для репозитория APT. Используйте этот URL в вашем sources.list:</p>
    <pre id="aptUrl">deb http://%s/%s/ release main</pre>
    <script>
      document.addEventListener('DOMContentLoaded', function() {
        var baseUrl = window.location.protocol + '//' + window.location.host;
        var repoName = '%s';
        var aptUrl = document.getElementById('aptUrl');
        aptUrl.textContent = 'deb ' + baseUrl + '/' + repoName + '/ release main';
      });
    </script>
    <p>Замените "release" и "main" соответствующими значениями для вашего дистрибутива.</p>
  </div>
  <p><a href="/">← Вернуться на главную страницу</a></p>
</body>
</html>`, h.repoConfig.Name, h.repoConfig.Name, h.repoConfig.URL, h.repoConfig.URL, h.repoConfig.Name, h.repoConfig.Name, host, h.repoConfig.Name, h.repoConfig.Name)
			return
		}
	}

	cacheKey := h.repoConfig.Name + "/" + filePath
	upstreamURL := h.repoConfig.URL + "/" + filePath

	useValidationCache := true
	if useValidationCache {
		validationTime, ok := h.cacheManager.GetValidation(cacheKey)
		if ok {
			if h.checkClientCacheHeaders(w, r, validationTime) {
				return
			}
		}
	}

	cacheReader, cacheSize, cacheModTime, err := h.cacheManager.Get(r.Context(), cacheKey)
	if err == nil {
		defer cacheReader.Close()
		logging.Debug("Cache hit for key: %s", cacheKey)

		if h.checkClientCacheHeaders(w, r, cacheModTime) {
			return
		}

		w.Header().Set("Last-Modified", cacheModTime.UTC().Format(http.TimeFormat))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", cacheSize))

		// Проверим содержимое файла, чтобы определить, является ли он HTML
		if !strings.HasSuffix(filePath, ".html") && !strings.HasSuffix(filePath, ".htm") {
			// Создаем буфер для проверки начала файла
			peekBuf := make([]byte, 512)
			n, err := io.ReadAtLeast(cacheReader, peekBuf, 1)
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				logging.Error("Failed to read content for MIME detection from cache: %v", err)
			} else {
				// Проверяем на HTML-содержимое
				contentType := http.DetectContentType(peekBuf[:n])
				if strings.HasPrefix(contentType, "text/html") {
					w.Header().Set("Content-Type", contentType)
				} else {
					w.Header().Set("Content-Type", util.GetContentType(filePath))
				}

				// Перестраиваем reader, чтобы включить уже прочитанные байты
				cacheReader = io.NopCloser(io.MultiReader(bytes.NewReader(peekBuf[:n]), cacheReader))
			}
		} else {
			w.Header().Set("Content-Type", util.GetContentType(filePath))
		}

		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}

		readSeeker, ok := cacheReader.(io.ReadSeeker)
		if !ok {
			// Если после наших манипуляций cacheReader больше не ReadSeeker,
			// нам нужно считать весь контент и отправить его напрямую
			body, err := io.ReadAll(cacheReader)
			if err != nil {
				logging.Error("Failed to read cached content: %v", err)
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				return
			}
			_, err = w.Write(body)
			if err != nil {
				logging.Error("Failed to write response: %v", err)
			}
			return
		}

		http.ServeContent(w, r, filePath, cacheModTime, readSeeker)
		return
	}

	if !errors.Is(err, os.ErrNotExist) {
		logging.Error("Error reading from cache for key %s: %v", cacheKey, err)
		http.Error(w, "Internal Cache Error", http.StatusInternalServerError)
		return
	}

	// Если файла нет в кеше и это потенциально index.html
	if strings.HasSuffix(r.URL.Path, "/") && (filePath == "" || strings.HasSuffix(filePath, "/")) {
		// Попробуем загрузить index.html
		indexFilePath := filePath
		if !strings.HasSuffix(indexFilePath, "/") {
			indexFilePath += "/"
		}
		indexFilePath += "index.html"

		indexCacheKey := h.repoConfig.Name + "/" + indexFilePath
		indexUpstreamURL := h.repoConfig.URL + "/" + indexFilePath

		fetchResult, err := h.fetcher.Fetch(r.Context(), indexCacheKey, indexUpstreamURL, r.Header)
		if err == nil {
			// Нашли index.html, устанавливаем правильный тип контента
			defer fetchResult.Body.Close()
			w.Header().Set("Content-Type", "text/html; charset=utf-8")

			// Сохраняем в кэш
			// Сразу читаем содержимое для манипуляций
			body, err := io.ReadAll(fetchResult.Body)
			if err != nil {
				logging.Error("Failed to read HTML content: %v", err)
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				return
			}

			// Сохраняем в кэш
			go func() {
				bodyReader := strings.NewReader(string(body))
				modTimeToUse := fetchResult.ModTime
				if modTimeToUse.IsZero() {
					modTimeToUse = time.Now()
				}
				err := h.cacheManager.Put(context.Background(), indexCacheKey, io.NopCloser(bodyReader), int64(len(body)), modTimeToUse)
				if err != nil {
					logging.Error("Failed to write index.html %s to cache: %v", indexCacheKey, err)
				}
			}()

			// Добавляем JavaScript в HTML-контент для корректного отображения URL за реверс-прокси
			contentType := fetchResult.Header.Get("Content-Type")
			if strings.HasPrefix(contentType, "text/html") {
				htmlContent := string(body)

				// Проверяем, есть ли </body> тег для вставки перед ним
				bodyCloseIndex := strings.LastIndex(htmlContent, "</body>")
				if bodyCloseIndex != -1 {
					jsCode := `<script>
document.addEventListener('DOMContentLoaded', function() {
  // Найти все ссылки на apt репозитории и заменить их на динамические URL
  var links = document.querySelectorAll("a[href*='http://'], a[href*='https://']");
  var baseUrl = window.location.protocol + '//' + window.location.host;
  
  links.forEach(function(link) {
    // Обновляем только релевантные apt-ссылки
    if (link.textContent.includes('deb ') && link.textContent.includes('/ubuntu/')) {
      var repoPath = window.location.pathname.replace(/\/$/, '');
      link.textContent = link.textContent.replace(/deb (https?:\/\/)[^\/]+(\/[^\/]+\/)/, 'deb ' + baseUrl + repoPath + '/');
    }
  });
});
</script>`

					htmlContent = htmlContent[:bodyCloseIndex] + jsCode + htmlContent[bodyCloseIndex:]
				}

				w.Write([]byte(htmlContent))
			} else {
				w.Write(body)
			}
			return
		}
	}

	logging.Debug("Cache miss for key: %s, fetching from upstream: %s", cacheKey, upstreamURL)

	fetchResult, err := h.fetcher.Fetch(r.Context(), cacheKey, upstreamURL, r.Header)
	if err != nil {
		logging.Error("Failed to fetch %s (key %s) from upstream: %v", upstreamURL, cacheKey, err)
		if errors.Is(err, fetch.ErrNotFound) {
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		} else if errors.Is(err, fetch.ErrUpstreamNotModified) {
			w.WriteHeader(http.StatusNotModified)
		} else {
			http.Error(w, "Bad Gateway", http.StatusBadGateway)
		}
		return
	}
	defer fetchResult.Body.Close()

	h.cacheManager.PutValidation(cacheKey, time.Now())

	util.CopyRelevantHeaders(w.Header(), fetchResult.Header)

	// Проверяем Content-Type из заголовков ответа upstream
	upstreamContentType := fetchResult.Header.Get("Content-Type")
	if strings.HasPrefix(upstreamContentType, "text/html") {
		// Если upstream уже отдает HTML, используем его Content-Type
		w.Header().Set("Content-Type", upstreamContentType)
	} else {
		// Пытаемся определить Content-Type по содержимому, если похоже на HTML
		// Создаем буфер для чтения первых байтов контента
		peekBuf := make([]byte, 512)
		n, err := io.ReadAtLeast(fetchResult.Body, peekBuf, 1)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			logging.Error("Failed to read content for MIME detection: %v", err)
		} else {
			// Проверяем, похоже ли на HTML
			contentType := http.DetectContentType(peekBuf[:n])
			if strings.HasPrefix(contentType, "text/html") {
				w.Header().Set("Content-Type", contentType)
			} else {
				// Иначе используем определение типа по расширению файла
				w.Header().Set("Content-Type", util.GetContentType(filePath))
			}

			// Создаем новый reader, который сначала вернет уже прочитанные байты, а затем остальное содержимое
			fetchResult.Body = io.NopCloser(io.MultiReader(bytes.NewReader(peekBuf[:n]), fetchResult.Body))
		}
	}

	if cl := fetchResult.Header.Get("Content-Length"); cl != "" {
		w.Header().Set("Content-Length", cl)
	}

	pr, pw := io.Pipe()
	cacheErrChan := make(chan error, 1)

	go func() {
		defer close(cacheErrChan)
		modTimeToUse := fetchResult.ModTime
		if modTimeToUse.IsZero() {
			modTimeToUse = time.Now()
		}
		err := h.cacheManager.Put(context.Background(), cacheKey, pr, fetchResult.Size, modTimeToUse)
		if err != nil {
			logging.Error("Failed to write key %s to cache: %v", cacheKey, err)
		}
		cacheErrChan <- err
	}()

	teeReader := io.TeeReader(fetchResult.Body, pw)

	w.WriteHeader(fetchResult.StatusCode)

	if r.Method == http.MethodHead {
		pw.Close()
		return
	}

	_, copyErr := io.Copy(w, teeReader)

	_ = pw.Close()

	if copyErr != nil {
		// Проверяем, является ли ошибка типа "broken pipe" или связана с обрывом соединения
		if strings.Contains(copyErr.Error(), "broken pipe") ||
			strings.Contains(copyErr.Error(), "connection reset") ||
			errors.Is(copyErr, syscall.EPIPE) ||
			errors.Is(copyErr, syscall.ECONNRESET) {
			logging.ErrorE("Failed to write response", copyErr)
		} else {
			logging.Warn("Error copying response to client for %s: %v", cacheKey, copyErr)
		}
	}

	select {
	case cacheErr := <-cacheErrChan:
		if cacheErr != nil {
			logging.Error("Error writing to cache for %s: %v", cacheKey, cacheErr)
		}
	case <-time.After(500 * time.Millisecond):
		logging.Warn("Cache write for %s is taking longer than expected", cacheKey)
	}
}

func (h *RepositoryHandler) checkClientCacheHeaders(w http.ResponseWriter, r *http.Request, modTime time.Time) bool {
	if modTime.IsZero() {
		return false
	}

	ifims := r.Header.Get("If-Modified-Since")
	if ifims != "" {
		if t, err := http.ParseTime(ifims); err == nil {
			modTimeTruncated := modTime.Truncate(time.Second)
			if !modTimeTruncated.After(t.Truncate(time.Second)) {
				w.WriteHeader(http.StatusNotModified)
				return true
			}
		}
	}

	return false
}
