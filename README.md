# Go APT Mirror

A lightweight APT mirror server written in Go that uses a local cache for efficient package serving. Designed to work behind a reverse proxy.

## Features

- Serves APT repository content from a local cache or origin server
- Caching hierarchy: Local Cache -> Origin Server
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
   git clone https://github.com/yolkispalkis/go-apt-mirror.git
   cd go-apt-mirror
   ```

2. Build the binary:
   ```
   go build -o apt-mirror ./cmd/server
   ```

### Using Docker

```
docker build -t apt-mirror .
docker run -p 8080:8080 -v ./config.json:/app/config.json -v ./cache:/app/cache apt-mirror
```

## Configuration

### Configuration File

The server can be configured using a JSON configuration file. To create a default configuration file:

```
./apt-mirror --create-config
```

This will create a `config.json` file with default settings. You can edit this file to customize the server.

Example configuration file:

```json
{
  "server": {
    "listenAddress": ":8080",
    "logRequests": true,
    "timeout": 30
  },
  "cache": {
    "directory": "./cache",
    "maxSize": 1024,
    "sizeUnit": "MB",
    "enabled": true,
    "lru": true,
    "cleanOnStart": false
  },
  "repositories": [
    {
      "origin": "archive.ubuntu.com",
      "path": "/ubuntu",
      "enabled": true
    },
    {
      "origin": "deb.debian.org",
      "path": "/debian",
      "enabled": true
    }
  ],
  "version": "1.0.0"
}
```

### Command Line Options

You can also configure the server using command line options, which will override the settings in the configuration file:

```
Usage of ./apt-mirror:
  --bind string
        Address to bind to (overrides config file)
  --cache-dir string
        Local cache directory (overrides config file)
  --cache-size int
        Maximum cache size (overrides config file)
  --cache-size-unit string
        Cache size unit: bytes, MB, or GB (overrides config file)
  --clean-cache
        Clean cache on startup (overrides config file)
  --config string
        Path to configuration file (default "config.json")
  --create-config
        Create default configuration file if it doesn't exist
  --disable-cache
        Disable local caching (overrides config file)
  --log-requests
        Log all requests (overrides config file) (default true)
  --lru
        Use LRU cache eviction policy (overrides config file) (default true)
  --origin string
        Origin APT server (overrides config file)
  --port int
        Port to listen on (overrides config file)
  --timeout int
        Timeout in seconds for HTTP requests to origin servers (default 30)
```

## Usage

### Basic Usage

Run the server with the default configuration:

```
./apt-mirror
```

Or specify a configuration file:

```
./apt-mirror --config my-config.json
```

### Quick Start with Command Line Options

Run the server with a specific origin server:

```
./apt-mirror --origin archive.ubuntu.com
```

This will start the server on port 8080 and use the local filesystem for caching.

## Multiple Repositories

You can configure multiple repositories in the configuration file:

```json
"repositories": [
  {
    "origin": "archive.ubuntu.com",
    "path": "/ubuntu",
    "enabled": true
  },
  {
    "origin": "deb.debian.org",
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
    ErrorLog ${APACHE_LOG_DIR}/apt-mirror-error.log
    CustomLog ${APACHE_LOG_DIR}/apt-mirror-access.log combined
</VirtualHost>
```

## Docker Support

You can run the server using Docker:

```
docker build -t apt-mirror .
docker run -p 8080:8080 -v ./config.json:/app/config.json -v ./cache:/app/cache apt-mirror
```

Or using Docker Compose:

```yaml
version: '3'

services:
  apt-mirror:
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