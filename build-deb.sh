#!/bin/bash
set -e

export PATH=$PATH:/usr/local/go/bin

PKG_NAME="go-apt-proxy"
PKG_VERSION="2.2.0"
PKG_ARCH="amd64"
PKG_MAINTAINER="yolkispalkis <me@w3h.su>"
PKG_DESCRIPTION="Высокопроизводительный прокси-сервер для APT, написанный на Go"
APP_MAIN_PACKAGE="github.com/yolkispalkis/go-apt-cache"

BUILD_DIR="$(pwd)/build"
STAGE_DIR="${BUILD_DIR}/staging"
DEBIAN_DIR="${STAGE_DIR}/DEBIAN"
BIN_DIR="${STAGE_DIR}/usr/local/bin"
CONFIG_DIR="${STAGE_DIR}/etc/go-apt-proxy"
SYSTEMD_DIR="${STAGE_DIR}/etc/systemd/system"
CACHE_DIR="${STAGE_DIR}/var/cache/go-apt-proxy"
LOG_DIR="${STAGE_DIR}/var/log/go-apt-proxy"
DOC_DIR="${STAGE_DIR}/usr/share/doc/${PKG_NAME}"
RUN_DIR_NAME="go-apt-proxy"

echo "Очистка предыдущей сборки..."
rm -rf "${BUILD_DIR}"

echo "Создание структуры директорий..."
mkdir -p "${DEBIAN_DIR}" "${BIN_DIR}" "${CONFIG_DIR}" "${SYSTEMD_DIR}" "${CACHE_DIR}" "${LOG_DIR}" "${DOC_DIR}"

echo "Сборка go-apt-proxy..."
LD_FLAGS="-s -w -X '${APP_MAIN_PACKAGE}/internal/appinfo.AppVersion=${PKG_VERSION}'"
go build -ldflags="${LD_FLAGS}" -o "${BIN_DIR}/${PKG_NAME}" main.go

echo "Создание конфигурационного файла..."
cat >"${CONFIG_DIR}/config.json" <<EOF
{
  "server": {
    "listenAddress": ":8080",
    "unixSocketPath": "/run/${RUN_DIR_NAME}/apt-proxy.sock",
    "unixSocketPermissions": "0660",
    "requestTimeout": "30s",
    "shutdownTimeout": "15s",
    "idleTimeout": "120s",
    "readHeaderTimeout": "10s",
    "maxConcurrentFetches": 20
  },
  "cache": {
    "directory": "/var/cache/go-apt-proxy",
    "maxSize": "20GB",
    "enabled": true,
    "cleanOnStart": false,
    "defaultTTL": "1h",
    "revalidateOnHitTTL": "0s",
    "negativeCacheTTL": "5m"
  },
  "logging": {
    "level": "info",
    "filePath": "/var/log/go-apt-proxy/apt_cache.log",
    "disableTerminal": false,
    "maxSizeMB": 100,
    "maxBackups": 5,
    "maxAgeDays": 30,
    "compress": true
  },
  "repositories": [
    {
      "name": "ubuntu",
      "url": "http://archive.ubuntu.com/ubuntu/",
      "enabled": true
    },
    {
      "name": "debian",
      "url": "http://deb.debian.org/debian/",
      "enabled": true
    },
    {
      "name": "raspbian",
      "url": "http://archive.raspberrypi.org/raspbian/",
      "enabled": false
    }
  ]
}
EOF

chmod 755 "${BIN_DIR}/${PKG_NAME}"
chmod 644 "${CONFIG_DIR}/config.json"

echo "Создание systemd service файла..."
cat >"${SYSTEMD_DIR}/go-apt-proxy.service" <<EOF
[Unit]
Description=Go APT Proxy Service
Documentation=${APP_REPO_URL}
After=network.target

[Service]
ExecStart=/usr/local/bin/go-apt-proxy -config /etc/go-apt-proxy/config.json
Restart=on-failure
User=apt-proxy
Group=apt-proxy
SupplementaryGroups=adm
WorkingDirectory=/var/cache/go-apt-proxy
EnvironmentFile=/etc/environment

RuntimeDirectory=${RUN_DIR_NAME}
RuntimeDirectoryMode=0750

ProtectSystem=full
ProtectHome=true
PrivateTmp=true
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
EOF

chmod 644 "${SYSTEMD_DIR}/go-apt-proxy.service"

echo "Создание postinst скрипта..."
cat >"${DEBIAN_DIR}/postinst" <<EOF
#!/bin/bash
set -e

PROXY_USER=apt-proxy
PROXY_GROUP=apt-proxy
LOG_DIR_PATH="/var/log/go-apt-proxy"
CACHE_DIR_PATH="/var/cache/go-apt-proxy"

if ! getent group "\${PROXY_GROUP}" > /dev/null; then
    addgroup --system "\${PROXY_GROUP}"
fi
if ! getent passwd "\${PROXY_USER}" > /dev/null; then
    adduser --system --ingroup "\${PROXY_GROUP}" --no-create-home \
            --home "\${CACHE_DIR_PATH}" --shell /bin/false "\${PROXY_USER}"
fi

mkdir -p "\${CACHE_DIR_PATH}"
chown -R "\${PROXY_USER}":"\${PROXY_GROUP}" "\${CACHE_DIR_PATH}"
chmod 750 "\${CACHE_DIR_PATH}"

mkdir -p "\${LOG_DIR_PATH}"
chown "\${PROXY_USER}":adm "\${LOG_DIR_PATH}"
chmod 2750 "\${LOG_DIR_PATH}"

systemctl daemon-reload

if [ "\$1" = "configure" ]; then
    systemctl enable go-apt-proxy.service
    if [ "\$(stat -c %d:%i /)" != "\$(stat -c %d:%i /proc/1/root/.)" ]; then
        echo "Обнаружен режим chroot, сервис не будет запущен автоматически."
    elif systemctl is-system-running --quiet --wait; then
        echo "Запуск go-apt-proxy сервиса..."
        systemctl start go-apt-proxy.service || \
          echo "Предупреждение: не удалось запустить сервис go-apt-proxy. Проверьте журнал: journalctl -u go-apt-proxy.service"
    else
        echo "Systemd не активен или система не полностью загружена, сервис не будет запущен автоматически."
    fi
fi

exit 0
EOF
chmod 755 "${DEBIAN_DIR}/postinst"

echo "Создание prerm скрипта..."
cat >"${DEBIAN_DIR}/prerm" <<EOF
#!/bin/bash
set -e

if [ "\$1" = "remove" ]; then
    if systemctl list-units --full --all | grep -q '^go-apt-proxy.service'; then
        if systemctl is-active --quiet go-apt-proxy.service; then
            systemctl stop go-apt-proxy.service
        fi
        if systemctl is-enabled --quiet go-apt-proxy.service; then
            systemctl disable go-apt-proxy.service
        fi
    fi
fi

exit 0
EOF
chmod 755 "${DEBIAN_DIR}/prerm"

echo "Создание postrm скрипта..."
cat >"${DEBIAN_DIR}/postrm" <<EOF
#!/bin/bash
set -e

PROXY_USER=apt-proxy
PROXY_GROUP=apt-proxy

if [ "\$1" = "purge" ]; then
    echo "Очистка после полного удаления пакета ${PKG_NAME}..."
    rm -rf /etc/go-apt-proxy
    rm -rf /var/cache/go-apt-proxy
    rm -rf /var/log/go-apt-proxy

    if getent passwd "\${PROXY_USER}" > /dev/null; then
        deluser --system "\${PROXY_USER}" || echo "Предупреждение: не удалось удалить пользователя \${PROXY_USER}"
    fi
    if getent group "\${PROXY_GROUP}" > /dev/null; then
        if [ -z "\$(getent group "\${PROXY_GROUP}" | cut -d: -f4)" ]; then
           delgroup --system "\${PROXY_GROUP}" || echo "Предупреждение: не удалось удалить группу \${PROXY_GROUP}"
        else
           echo "Группа \${PROXY_GROUP} не пуста, пропускаем удаление."
        fi
    fi
fi

systemctl daemon-reload || true

exit 0
EOF
chmod 755 "${DEBIAN_DIR}/postrm"

echo "Создание control файла..."
cat >"${DEBIAN_DIR}/control" <<EOF
Package: ${PKG_NAME}
Version: ${PKG_VERSION}
Architecture: ${PKG_ARCH}
Maintainer: ${PKG_MAINTAINER}
Depends: systemd, adduser, libc6 (>= 2.17)
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
  * Поддержка HTTP и Unix socket подключений (/run/${RUN_DIR_NAME}/apt-proxy.sock)
Section: net
Priority: optional
Homepage: ${APP_REPO_URL}
EOF

echo "Создание conffiles..."
echo "/etc/go-apt-proxy/config.json" >"${DEBIAN_DIR}/conffiles"

echo "Создание copyright файла..."
cat >"${DOC_DIR}/copyright" <<EOF
Format: https://www.debian.org/doc/packaging-manuals/copyright-format/1.0/
Upstream-Name: ${PKG_NAME}
Source: ${APP_REPO_URL}

Files: *
Copyright: $(date +%Y) ${PKG_MAINTAINER}
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

Files: debian/*
Copyright: $(date +%Y) ${PKG_MAINTAINER}
License: MIT
 (Same as above)
EOF

echo "Создание changelog файла..."
cat >"${DOC_DIR}/changelog.Debian" <<EOF
${PKG_NAME} (${PKG_VERSION}-1) unstable; urgency=medium

  * Initial release for version ${PKG_VERSION}.
  * Use systemd RuntimeDirectory for socket directory management.
  * Centralized application versioning and User-Agent string.

 -- ${PKG_MAINTAINER}  $(date -R)
EOF
gzip -9 -n "${DOC_DIR}/changelog.Debian"

echo "Установка финальных прав доступа..."
find "${STAGE_DIR}" -type d -exec chmod 755 {} \;

echo "Сборка .deb пакета..."
cd "${BUILD_DIR}"
fakeroot dpkg-deb --build staging "${PKG_NAME}_${PKG_VERSION}_${PKG_ARCH}.deb"

echo "Готово! Пакет создан: ${BUILD_DIR}/${PKG_NAME}_${PKG_VERSION}_${PKG_ARCH}.deb"

echo -e "\nДля установки пакета выполните:"
echo "sudo dpkg -i ${BUILD_DIR}/${PKG_NAME}_${PKG_VERSION}_${PKG_ARCH}.deb"
echo "sudo apt-get install -f  # Установка зависимостей, если требуется"
