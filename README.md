# Go APT Cache

A lightweight APT mirror server written in Go that uses a local cache for efficient package serving. Designed to work behind a reverse proxy.

## Features

- Serves APT repository content from a local cache or upstream server
- Caching hierarchy: Local Cache -> Upstream Server
- Support for multiple repositories (Ubuntu, Debian, etc.)
- LRU (Least Recently Used) cache eviction policy
- Configurable cache size and location
- Cache cleaning on startup option
- JSON configuration file for easy setup
- Simple HTTP server with minimal dependencies
- Designed to work behind a reverse proxy

## Installation

### From Source

1. Clone this repository:
   ```
   git clone https://github.com/yolkispalkis/go-apt-cache.git
   cd go-apt-cache
   ```

2. Build the binary:
   ```
   go build -o apt-cache ./cmd/go-apt-cache
   ```

### Using Docker

```
docker build -t apt-cache .
docker run -p 8080:8080 -v ./config.json:/app/config.json -v ./cache:/app/cache apt-cache
```

With proxy support:

```
docker run -p 8080:8080 \
  -v ./config.json:/app/config.json \
  -v ./cache:/app/cache \
  -e HTTP_PROXY=http://proxy.example.com:8080 \
  -e HTTPS_PROXY=http://proxy.example.com:8080 \
  -e NO_PROXY=localhost,127.0.0.1 \
  apt-cache
```

## Configuration

### Configuration File

The server can be configured using a JSON configuration file. To create a default configuration file:

```
./apt-cache --create-config
```

This will create a `config.json` file with default settings. You can edit this file to customize the server.

Example configuration file:

```json
{
  "server": {
    "listenAddress": ":8080",
    "unixSocketPath": "/var/run/apt-cache.sock",
    "logRequests": true,
    "timeout": 60
  },
  "cache": {
    "directory": "./cache",
    "maxSize": "10GB",
    "enabled": true,
    "lru": true,
    "cleanOnStart": false,
    "validationCacheTTL": 300
  },
  "logging": {
    "filePath": "./logs/go-apt-cache.log",
    "disableTerminal": false,
    "maxSize": "10MB",
    "level": "info"
  },
  "repositories": [
    {
      "url": "http://archive.ubuntu.com/ubuntu",
      "path": "/ubuntu",
      "enabled": true
    },
    {
      "url": "http://deb.debian.org/debian",
      "path": "/debian",
      "enabled": true
    },
    {
      "url": "http://security.ubuntu.com/ubuntu-security",
      "path": "/ubuntu-security",
      "enabled": true
    }
  ],
  "version": "1.0.0"
}
```

### Configuration Sections

#### Server Configuration

- `listenAddress`: The address and port to listen on (e.g. `:8080`). Set to empty string to disable TCP listening.
- `unixSocketPath`: Path to Unix socket (e.g. `/var/run/apt-cache.sock`). Set to empty string to disable Unix socket listening.
- `logRequests`: Whether to log all HTTP requests
- `timeout`: Timeout in seconds for HTTP requests

#### Cache Configuration

- `directory`: The directory where cached files will be stored
- `maxSize`: Maximum cache size with unit (e.g. "1GB", "500MB", "10KB")
- `enabled`: Whether to enable caching
- `lru`: Whether to use LRU (Least Recently Used) cache eviction policy
- `cleanOnStart`: Whether to clean the cache on startup
- `validationCacheTTL`: Time in seconds to cache validation results

#### Logging Configuration

- `filePath`: Path to log file (empty for no file logging)
- `disableTerminal`: Whether to disable terminal output
- `maxSize`: Maximum log file size with unit (e.g. "10MB", "1GB")
- `level`: Log level: "debug", "info", "warning", "error", "fatal"

### Command Line Options

You can also configure the server using command line options, which will override the settings in the configuration file:

```
Usage of ./apt-cache:
  --bind string
        Address to bind to (overrides config file)
  --unix-socket string
        Path to Unix socket (overrides config file)
  --cache-dir string
        Local cache directory (overrides config file)
  --cache-size string
        Maximum cache size with unit, e.g. "1GB", "500MB" (overrides config file)
  --clean-cache
        Clean cache on startup (overrides config file)
  --config string
        Path to configuration file (default "config.json")
  --create-config
        Create default configuration file if it doesn't exist
  --cache-enabled
        Enable local caching (overrides config file) (default true)
  --log-requests
        Log all requests (overrides config file) (default true)
  --lru
        Use LRU cache eviction policy (overrides config file) (default true)
  --url string
        Upstream server URL to mirror (overrides config file)
  --port int
        Port to listen on (overrides config file)
  --timeout int
        Timeout in seconds for HTTP requests to upstream servers (default 60)
  --log-file string
        Path to log file (overrides config file)
  --disable-terminal-log
        Disable terminal logging (overrides config file)
  --log-max-size string
        Maximum log file size with unit, e.g. "10MB", "1GB" (overrides config file)
  --log-level string
        Log level: debug, info, warning, error, fatal (overrides config file)
```

## Usage

### Basic Usage

Run the server with the default configuration:

```
./apt-cache
```

Or specify a configuration file:

```
./apt-cache --config my-config.json
```

### Proxy Support

The application supports HTTP/HTTPS proxies through standard environment variables:

- `HTTP_PROXY`: Proxy server for HTTP requests (e.g., `http://proxy.example.com:8080`)
- `HTTPS_PROXY`: Proxy server for HTTPS requests (e.g., `http://proxy.example.com:8080`)
- `NO_PROXY`: Comma-separated list of hosts to exclude from proxying (e.g., `localhost,127.0.0.1`)

Example usage with proxy:

```bash
# Set proxy environment variables
export HTTP_PROXY=http://proxy.example.com:8080
export HTTPS_PROXY=http://proxy.example.com:8080
export NO_PROXY=localhost,127.0.0.1

# Run the application
./apt-cache
```

When proxy environment variables are set, the application will log the proxy configuration at startup.

### Quick Start with Command Line Options

Run the server with a specific upstream server:

```
./apt-cache --url http://archive.ubuntu.com/ubuntu
```

This will start the server on port 8080 and use the local filesystem for caching.

## Multiple Repositories

You can configure multiple repositories in the configuration file:

```json
"repositories": [
  {
    "url": "http://archive.ubuntu.com/ubuntu",
    "path": "/ubuntu",
    "enabled": true
  },
  {
    "url": "http://deb.debian.org/debian",
    "path": "/debian",
    "enabled": true
  }
]
```

This will make the server handle requests for both Ubuntu and Debian repositories:
- Ubuntu: `http://your-server:8080/ubuntu/...`
- Debian: `http://your-server:8080/debian/...`

## Using the Mirror

1. Edit your APT sources list:

   For Ubuntu 22.04, edit `/etc/apt/sources.list` and replace `archive.ubuntu.com` with your mirror's domain or IP:

   ```
   deb http://your-mirror-domain:8080/ubuntu jammy main restricted universe multiverse
   deb http://your-mirror-domain:8080/ubuntu jammy-updates main restricted universe multiverse
   deb http://your-mirror-domain:8080/ubuntu jammy-backports main restricted universe multiverse
   ```

   For Debian 12, replace `deb.debian.org` with your mirror:

   ```
   deb http://your-mirror-domain:8080/debian bookworm main contrib non-free
   deb http://your-mirror-domain:8080/debian bookworm-updates main contrib non-free
   ```

2. Update APT:

   ```
   sudo apt update
   ```

## Setting Up with a Reverse Proxy

### Nginx Configuration Example

```nginx
server {
    listen 80;
    server_name apt.yourdomain.com;

    location / {
        proxy_pass http://localhost:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # Increase timeouts for large package downloads
        proxy_connect_timeout 300s;
        proxy_send_timeout 300s;
        proxy_read_timeout 300s;
    }
}
```

### Apache Configuration Example

```apache
<VirtualHost *:80>
    ServerName apt.yourdomain.com
    
    ProxyPreserveHost On
    ProxyPass / http://localhost:8080/
    ProxyPassReverse / http://localhost:8080/
    
    # Increase timeouts for large package downloads
    ProxyTimeout 300
    
    # Log settings
    ErrorLog ${APACHE_LOG_DIR}/apt-cache-error.log
    CustomLog ${APACHE_LOG_DIR}/apt-cache-access.log combined
</VirtualHost>
```

## Docker Support

You can run the server using Docker:

```
docker build -t apt-cache .
docker run -p 8080:8080 -v ./config.json:/app/config.json -v ./cache:/app/cache apt-cache
```

Or using Docker Compose:

```yaml
version: '3'

services:
  apt-cache:
    build:
      context: .
      dockerfile: Dockerfile
    ports:
      - "8080:8080"
    volumes:
      - ./config.json:/app/config.json
      - ./cache:/app/cache
    restart: unless-stopped
```

## Cache Management

The server includes several cache management features:

- **Size Specification**: Cache and log file sizes can be specified with units (e.g., "1GB", "500MB", "10KB").
- **LRU Eviction**: When the cache reaches its maximum size, the least recently used items are removed.
- **Cache Cleaning**: You can enable cache cleaning on startup with the `--clean-cache` flag or by setting `cleanOnStart: true` in the configuration file.
- **Cache Statistics**: The server provides cache statistics via the `/status` endpoint.

## Performance Tuning

For better performance:

1. Increase the cache size to accommodate your needs
2. Place the cache on an SSD for faster access
3. Adjust the HTTP client timeout based on your network conditions
4. Run behind a reverse proxy for TLS termination and additional caching

## License

MIT 