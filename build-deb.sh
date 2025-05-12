#!/bin/bash
set -e

export PATH=$PATH:/usr/local/go/bin

PKG_NAME="go-apt-cache"
PKG_VERSION="2.2.8"
PKG_ARCH="amd64"
PKG_MAINTAINER="yolkispalkis <me@w3h.su>"
PKG_DESCRIPTION="Высокопроизводительный прокси-сервер для APT, написанный на Go"
APP_MAIN_PACKAGE="github.com/yolkispalkis/go-apt-cache"

BUILD_DIR="$(pwd)/build"
STAGE_DIR="${BUILD_DIR}/staging"
DEBIAN_DIR="${STAGE_DIR}/DEBIAN"
BIN_DIR="${STAGE_DIR}/usr/local/bin"
CONFIG_DIR="${STAGE_DIR}/etc/${PKG_NAME}"
SYSTEMD_DIR="${STAGE_DIR}/etc/systemd/system"
CACHE_DIR="${STAGE_DIR}/var/cache/${PKG_NAME}"
LOG_DIR="${STAGE_DIR}/var/log/${PKG_NAME}"
DOC_DIR="${STAGE_DIR}/usr/share/doc/${PKG_NAME}"
RUN_DIR_NAME="${PKG_NAME}"

echo "Очистка предыдущей сборки..."
rm -rf "${BUILD_DIR}"

echo "Создание структуры директорий..."
mkdir -p "${DEBIAN_DIR}" "${BIN_DIR}" "${CONFIG_DIR}" "${SYSTEMD_DIR}" "${CACHE_DIR}" "${LOG_DIR}" "${DOC_DIR}"

echo "Сборка ${PKG_NAME}..."
LD_FLAGS="-s -w -X '${APP_MAIN_PACKAGE}/internal/appinfo.AppVersion=${PKG_VERSION}'"
go build -ldflags="${LD_FLAGS}" -o "${BIN_DIR}/${PKG_NAME}" main.go

echo "Создание конфигурационного файла..."
cat >"${CONFIG_DIR}/config.json" <<EOF
{
  "server": {
    "listenAddress": ":8080",
    "unixSocketPath": "/run/${RUN_DIR_NAME}/${PKG_NAME}.sock",
    "unixSocketPermissions": "0660",
    "requestTimeout": "30s",
    "shutdownTimeout": "15s",
    "idleTimeout": "120s",
    "readHeaderTimeout": "10s",
    "maxConcurrentFetches": 20
  },
  "cache": {
    "directory": "/var/cache/${PKG_NAME}",
    "maxSize": "20GB",
    "enabled": true,
    "cleanOnStart": false,
    "negativeCacheTTL": "5m"
  },
  "logging": {
    "level": "info",
    "filePath": "/var/log/${PKG_NAME}/${PKG_NAME}.log",
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
SERVICE_USER="${PKG_NAME}-user"
SERVICE_GROUP="${PKG_NAME}-group"

cat >"${SYSTEMD_DIR}/${PKG_NAME}.service" <<EOF
[Unit]
Description=Go APT Cache Service
Documentation=${APP_MAIN_PACKAGE}
After=network.target

[Service]
ExecStart=/usr/local/bin/${PKG_NAME} -config /etc/${PKG_NAME}/config.json
Restart=on-failure
User=${SERVICE_USER}
Group=${SERVICE_GROUP}
SupplementaryGroups=adm
WorkingDirectory=/var/cache/${PKG_NAME}
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

chmod 644 "${SYSTEMD_DIR}/${PKG_NAME}.service"

echo "Создание postinst скрипта..."
cat >"${DEBIAN_DIR}/postinst" <<EOF
#!/bin/bash
set -e

PROXY_USER=${SERVICE_USER}
PROXY_GROUP=${SERVICE_GROUP}
LOG_DIR_PATH="/var/log/${PKG_NAME}"
CACHE_DIR_PATH="/var/cache/${PKG_NAME}"
SERVICE_NAME="${PKG_NAME}.service"

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

case "\$1" in
    configure)
        if ! systemctl is-enabled --quiet "\${SERVICE_NAME}"; then
            systemctl enable "\${SERVICE_NAME}"
        fi

        if [ "\$(stat -c %d:%i /)" != "\$(stat -c %d:%i /proc/1/root/.)" ]; then
            echo "Обнаружен режим chroot, сервис не будет запущен/перезапущен автоматически."
        elif systemctl is-system-running --quiet --wait; then
            if systemctl is-active --quiet "\${SERVICE_NAME}"; then
                echo "Сервис \${SERVICE_NAME} активен, перезапуск..."
                systemctl try-restart "\${SERVICE_NAME}" || \
                  echo "Предупреждение: не удалось перезапустить сервис \${SERVICE_NAME}. Проверьте журнал: journalctl -u \${SERVICE_NAME}"
            else
                echo "Запуск сервиса \${SERVICE_NAME}..."
                systemctl start "\${SERVICE_NAME}" || \
                  echo "Предупреждение: не удалось запустить сервис \${SERVICE_NAME}. Проверьте журнал: journalctl -u \${SERVICE_NAME}"
            fi
        else
            echo "Systemd не активен или система не полностью загружена, сервис не будет запущен/перезапущен автоматически."
        fi
    ;;
    abort-upgrade|abort-remove|abort-deconfigure)
    ;;
    *)
        echo "postinst called with unknown argument '\$1'" >&2
        exit 1
    ;;
esac

exit 0
EOF
chmod 755 "${DEBIAN_DIR}/postinst"

echo "Создание prerm скрипта..."
cat >"${DEBIAN_DIR}/prerm" <<EOF
#!/bin/bash
set -e

SERVICE_NAME="${PKG_NAME}.service"

case "\$1" in
    remove|upgrade|deconfigure)
        if systemctl list-units --full --all | grep -q "^\${SERVICE_NAME}"; then
            if systemctl is-active --quiet "\${SERVICE_NAME}"; then
                systemctl stop "\${SERVICE_NAME}"
            fi
            if systemctl is-enabled --quiet "\${SERVICE_NAME}"; then
                systemctl disable "\${SERVICE_NAME}"
            fi
        fi
    ;;
    failed-upgrade)
    ;;
    *)
        echo "prerm called with unknown argument '\$1'" >&2
        exit 1
    ;;
esac

exit 0
EOF
chmod 755 "${DEBIAN_DIR}/prerm"

echo "Создание postrm скрипта..."
cat >"${DEBIAN_DIR}/postrm" <<EOF
#!/bin/bash
set -e

PROXY_USER=${SERVICE_USER}
PROXY_GROUP=${SERVICE_GROUP}
SERVICE_NAME="${PKG_NAME}.service"

case "\$1" in
    purge)
        echo "Очистка после полного удаления пакета ${PKG_NAME}..."
        rm -rf "/etc/${PKG_NAME}"
        rm -rf "/var/cache/${PKG_NAME}"
        rm -rf "/var/log/${PKG_NAME}"

        if getent passwd "\${PROXY_USER}" > /dev/null; then
            deluser --quiet --system "\${PROXY_USER}" || echo "Предупреждение: не удалось удалить пользователя \${PROXY_USER}"
        fi
        if getent group "\${PROXY_GROUP}" > /dev/null; then
            if [ -z "\$(getent group "\${PROXY_GROUP}" | cut -d: -f4)" ]; then
               delgroup --quiet --system "\${PROXY_GROUP}" || echo "Предупреждение: не удалось удалить группу \${PROXY_GROUP}"
            else
               echo "Группа \${PROXY_GROUP} не пуста, пропускаем удаление."
            fi
        fi
        systemctl daemon-reload || true
    ;;
    remove|upgrade|abort-install|abort-upgrade|disappear)
        systemctl daemon-reload || true
    ;;
    *)
        echo "postrm called with unknown argument '\$1'" >&2
        exit 1
    ;;
esac

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
 ${PKG_NAME} — это высокопроизводительный прокси-сервер для APT,
 написанный на языке Go. Позволяет кешировать пакеты и метаданные
 из репозиториев APT, значительно ускоряя установку и обновление
 пакетов в системах на базе Debian/Ubuntu.
 .
 Основные возможности:
  * Кеширование пакетов и метаданных для ускорения работы APT
  * Поддержка нескольких репозиториев одновременно
  * Настраиваемые параметры кеширования и TTL валидации
  * Эффективное управление кешем по алгоритму LRU
  * Поддержка HTTP и Unix socket подключений (/run/${RUN_DIR_NAME}/${PKG_NAME}.sock)
Section: net
Priority: optional
Homepage: ${APP_MAIN_PACKAGE}
EOF

echo "Создание conffiles..."
echo "/etc/${PKG_NAME}/config.json" >"${DEBIAN_DIR}/conffiles"

echo "Создание copyright файла..."
cat >"${DOC_DIR}/copyright" <<EOF
Format: https://www.debian.org/doc/packaging-manuals/copyright-format/1.0/
Upstream-Name: ${PKG_NAME}
Source: ${APP_MAIN_PACKAGE}

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
  * Synchronized project name to ${PKG_NAME}.

 -- ${PKG_MAINTAINER}  $(date -R)
EOF
gzip -9 -n "${DOC_DIR}/changelog.Debian"

echo "Установка финальных прав доступа..."
find "${STAGE_DIR}" -type d -exec chmod 755 {} \;
find "${DEBIAN_DIR}" -type f \( -name "postinst" -o -name "prerm" -o -name "postrm" -o -name "preinst" \) -exec chmod 755 {} \;

echo "Сборка .deb пакета..."
cd "${BUILD_DIR}"
fakeroot dpkg-deb --build staging "${PKG_NAME}_${PKG_VERSION}_${PKG_ARCH}.deb"

echo "Готово! Пакет создан: ${BUILD_DIR}/${PKG_NAME}_${PKG_VERSION}_${PKG_ARCH}.deb"

echo -e "\nДля установки пакета выполните:"
echo "sudo dpkg -i ${BUILD_DIR}/${PKG_NAME}_${PKG_VERSION}_${PKG_ARCH}.deb"
echo "sudo apt-get install -f  # Установка зависимостей, если требуется"
