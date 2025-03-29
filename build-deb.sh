#!/bin/bash
set -e

export PATH=$PATH:/usr/local/go/bin

# Параметры пакета
PKG_NAME="go-apt-proxy"
PKG_VERSION="1.0.0"
PKG_ARCH="amd64"
PKG_MAINTAINER="yolkispalkis <admin@example.com>"
PKG_DESCRIPTION="Высокопроизводительный прокси-сервер для APT, написанный на Go"

# Временные директории для сборки
BUILD_DIR="$(pwd)/build"
STAGE_DIR="${BUILD_DIR}/staging"
DEBIAN_DIR="${STAGE_DIR}/DEBIAN"
BIN_DIR="${STAGE_DIR}/usr/local/bin"
CONFIG_DIR="${STAGE_DIR}/etc/go-apt-proxy"
SYSTEMD_DIR="${STAGE_DIR}/etc/systemd/system"
CACHE_DIR="${STAGE_DIR}/var/cache/go-apt-proxy"
LOG_DIR="${STAGE_DIR}/var/log/go-apt-proxy"

# Очистка предыдущей сборки
rm -rf "${BUILD_DIR}"

# Создание структуры директорий
mkdir -p "${DEBIAN_DIR}" "${BIN_DIR}" "${CONFIG_DIR}" "${SYSTEMD_DIR}" "${CACHE_DIR}" "${LOG_DIR}"

# Установка прав на директорию с логами сразу при создании
chmod 750 "${LOG_DIR}"

# Сборка приложения
echo "Сборка go-apt-proxy..."
go build -o "${BIN_DIR}/${PKG_NAME}" main.go

# Создание модифицированного конфигурационного файла с правильными путями
cat > "${CONFIG_DIR}/config.json" << EOF
{
  "server": {
    "listenAddress": ":8080",
    "unixSocketPath": "/var/run/go-apt-proxy/apt-proxy.sock",
    "unixSocketPermissions": "660",
    "requestTimeout": "60s",
    "shutdownTimeout": "15s",
    "idleTimeout": "120s",
    "readHeaderTimeout": "10s",
    "maxConcurrentFetches": 10
  },
  "cache": {
    "directory": "/var/cache/go-apt-proxy",
    "maxSize": "10GB",
    "enabled": true,
    "cleanOnStart": false,
    "validationTTL": "5m"
  },
  "logging": {
    "level": "info",
    "filePath": "/var/log/go-apt-proxy/apt_cache.log",
    "disableTerminal": false,
    "maxSizeMB": 100,
    "maxBackups": 3,
    "maxAgeDays": 7,
    "compress": true
  },
  "repositories": [
    {
      "name": "ubuntu",
      "url": "http://archive.ubuntu.com/ubuntu",
      "enabled": true
    },
    {
      "name": "debian",
      "url": "http://deb.debian.org/debian",
      "enabled": false
    },
    {
      "name": "security",
      "url": "http://security.ubuntu.com/ubuntu",
      "enabled": false
    },
    {
      "name": "ppa-example",
      "url": "http://ppa.launchpadcontent.net/example/ppa/ubuntu",
      "enabled": false
    }
  ]
}
EOF

# Настройка прав доступа
chmod 755 "${BIN_DIR}/${PKG_NAME}"
chmod 644 "${CONFIG_DIR}/config.json"

# Создание файла systemd service
cat > "${SYSTEMD_DIR}/go-apt-proxy.service" << EOF
[Unit]
Description=Go APT Proxy Service
After=network.target

[Service]
ExecStart=/usr/local/bin/go-apt-proxy -config /etc/go-apt-proxy/config.json
Restart=on-failure
User=apt-proxy
Group=apt-proxy
SupplementaryGroups=adm
WorkingDirectory=/var/cache/go-apt-proxy

# Настройки безопасности
ProtectSystem=full
ProtectHome=true
PrivateTmp=true
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
EOF

chmod 644 "${SYSTEMD_DIR}/go-apt-proxy.service"

# Создание файла прокси для APT
mkdir -p "${STAGE_DIR}/etc/apt/apt.conf.d"
cat > "${STAGE_DIR}/etc/apt/apt.conf.d/01proxy" << EOF
Acquire::http::Proxy "http://localhost:8080";
EOF

chmod 644 "${STAGE_DIR}/etc/apt/apt.conf.d/01proxy"

# Создание postinst скрипта
cat > "${DEBIAN_DIR}/postinst" << EOF
#!/bin/bash
set -e

# Создание пользователя для сервиса, если он не существует
if ! getent passwd apt-proxy > /dev/null; then
    adduser --system --group --no-create-home --home /var/cache/go-apt-proxy apt-proxy
fi

# Создание дополнительных директорий
mkdir -p /var/run/go-apt-proxy

# Установка прав доступа
chown -R apt-proxy:apt-proxy /var/cache/go-apt-proxy /var/run/go-apt-proxy
chown -R apt-proxy:adm /var/log/go-apt-proxy
chmod 750 /var/cache/go-apt-proxy
chmod 755 /var/run/go-apt-proxy
chmod 750 /var/log/go-apt-proxy

# Перезагрузка systemd и включение сервиса
systemctl daemon-reload
systemctl enable go-apt-proxy.service

# Запуск сервиса, если система не в режиме chroot
if [ ! -e /run/systemd/system ]; then
    echo "Systemd не обнаружен, сервис не будет запущен автоматически"
    exit 0
fi

echo "Запуск go-apt-proxy сервиса..."
systemctl start go-apt-proxy.service

exit 0
EOF

chmod 755 "${DEBIAN_DIR}/postinst"

# Создание prerm скрипта
cat > "${DEBIAN_DIR}/prerm" << EOF
#!/bin/bash
set -e

# Остановка и отключение сервиса при удалении пакета
if [ -e /run/systemd/system ] && systemctl is-active --quiet go-apt-proxy.service; then
    systemctl stop go-apt-proxy.service
fi

if [ -e /run/systemd/system ] && systemctl is-enabled --quiet go-apt-proxy.service; then
    systemctl disable go-apt-proxy.service
fi

exit 0
EOF

chmod 755 "${DEBIAN_DIR}/prerm"

# Создание postrm скрипта
cat > "${DEBIAN_DIR}/postrm" << EOF
#!/bin/bash
set -e

if [ "\$1" = "purge" ]; then
    # Удаление конфигурационных файлов
    rm -rf /etc/go-apt-proxy

    # Удаление кеша и логов
    rm -rf /var/cache/go-apt-proxy
    rm -rf /var/log/go-apt-proxy
    rm -rf /var/run/go-apt-proxy

    # Удаление пользователя
    if getent passwd apt-proxy > /dev/null; then
        deluser --system apt-proxy
    fi
    
    if getent group apt-proxy > /dev/null; then
        delgroup --system apt-proxy
    fi
    
    # Перезагрузка systemd
    systemctl daemon-reload
fi

exit 0
EOF

chmod 755 "${DEBIAN_DIR}/postrm"

# Создание control-файла
cat > "${DEBIAN_DIR}/control" << EOF
Package: ${PKG_NAME}
Version: ${PKG_VERSION}
Architecture: ${PKG_ARCH}
Maintainer: ${PKG_MAINTAINER}
Description: ${PKG_DESCRIPTION}
 go-apt-proxy — это высокопроизводительный прокси-сервер для APT,
 написанный на языке Go. Позволяет кешировать пакеты и метаданные
 из репозиториев APT, значительно ускоряя установку и обновление
 пакетов в системах на базе Debian/Ubuntu.
 .
 Основные возможности:
  * Кеширование пакетов и метаданных для ускорения работы APT
  * Поддержка нескольких репозиториев одновременно
  * Настраиваемые параметры кеширования и TTL валидации
  * Эффективное управление кешем по алгоритму LRU
  * Поддержка HTTP и Unix socket подключений
Section: net
Priority: optional
Depends: systemd
Homepage: https://github.com/yolkispalkis/go-apt-cache
EOF

# Создание conffiles для отслеживания файлов конфигурации
cat > "${DEBIAN_DIR}/conffiles" << EOF
/etc/go-apt-proxy/config.json
/etc/apt/apt.conf.d/01proxy
EOF

# Создание copyright файла
mkdir -p "${STAGE_DIR}/usr/share/doc/${PKG_NAME}"
cat > "${STAGE_DIR}/usr/share/doc/${PKG_NAME}/copyright" << EOF
Format: https://www.debian.org/doc/packaging-manuals/copyright-format/1.0/
Upstream-Name: ${PKG_NAME}
Source: https://github.com/yolkispalkis/go-apt-cache

Files: *
Copyright: $(date +%Y) yolkispalkis
License: MIT
 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:
 .
 The above copyright notice and this permission notice shall be included in all
 copies or substantial portions of the Software.
 .
 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 SOFTWARE.
EOF

# Создание changelog
cat > "${STAGE_DIR}/usr/share/doc/${PKG_NAME}/changelog.Debian" << EOF
${PKG_NAME} (${PKG_VERSION}) unstable; urgency=medium

  * Initial release

 -- ${PKG_MAINTAINER}  $(date -R)
EOF

gzip -9 -n "${STAGE_DIR}/usr/share/doc/${PKG_NAME}/changelog.Debian"

# Сборка deb-пакета
echo "Сборка .deb пакета..."
cd "${BUILD_DIR}"
fakeroot dpkg-deb --build staging
mv staging.deb "${PKG_NAME}_${PKG_VERSION}_${PKG_ARCH}.deb"

echo "Готово! Пакет создан: ${BUILD_DIR}/${PKG_NAME}_${PKG_VERSION}_${PKG_ARCH}.deb"

# Вывод инструкций по установке
echo -e "\nДля установки пакета выполните:"
echo "sudo dpkg -i ${PKG_NAME}_${PKG_VERSION}_${PKG_ARCH}.deb"
echo "sudo apt-get install -f  # Установка зависимостей, если требуется" 