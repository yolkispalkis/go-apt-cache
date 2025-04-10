#!/bin/bash
set -e

export PATH=$PATH:/usr/local/go/bin

# Параметры пакета
PKG_NAME="go-apt-proxy"
PKG_VERSION="1.0.0"
PKG_ARCH="amd64"
PKG_MAINTAINER="yolkispalkis <me@w3h.su>"
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
DOC_DIR="${STAGE_DIR}/usr/share/doc/${PKG_NAME}"
RUN_DIR_NAME="go-apt-proxy" # Имя директории в /run

# Очистка предыдущей сборки
echo "Очистка предыдущей сборки..."
rm -rf "${BUILD_DIR}"

# Создание структуры директорий
echo "Создание структуры директорий..."
mkdir -p "${DEBIAN_DIR}" "${BIN_DIR}" "${CONFIG_DIR}" "${SYSTEMD_DIR}" "${CACHE_DIR}" "${LOG_DIR}" "${DOC_DIR}"

# Установка прав на директорию с логами сразу при создании (будет изменено в postinst)
chmod 750 "${LOG_DIR}"

# Сборка приложения
echo "Сборка go-apt-proxy..."
go build -o "${BIN_DIR}/${PKG_NAME}" main.go

# Создание модифицированного конфигурационного файла с правильными путями
echo "Создание конфигурационного файла..."
cat > "${CONFIG_DIR}/config.json" << EOF
{
  "server": {
    "listenAddress": ":8080",
    "unixSocketPath": "/run/${RUN_DIR_NAME}/apt-proxy.sock",
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
    "validationTTL": "5m",
    "skipValidationExtensions": [".deb", ".udeb", ".ddeb"]
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

# Настройка прав доступа для файлов в пакете
chmod 755 "${BIN_DIR}/${PKG_NAME}"
chmod 644 "${CONFIG_DIR}/config.json"

# Создание файла systemd service с использованием RuntimeDirectory
echo "Создание systemd service файла..."
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
EnvironmentFile=/etc/environment

# Systemd создаст /run/go-apt-proxy с правами 755 и владельцем apt-proxy:apt-proxy
RuntimeDirectory=${RUN_DIR_NAME}
RuntimeDirectoryMode=0755

# Настройки безопасности
ProtectSystem=full
ProtectHome=true
PrivateTmp=true
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
EOF

chmod 644 "${SYSTEMD_DIR}/go-apt-proxy.service"

# Создание postinst скрипта
echo "Создание postinst скрипта..."
cat > "${DEBIAN_DIR}/postinst" << EOF
#!/bin/bash
set -e

# Определяем пользователя и группу
PROXY_USER=apt-proxy
PROXY_GROUP=apt-proxy
LOG_GROUP=adm

# Создание пользователя и группы для сервиса, если они не существуют
if ! getent group \${PROXY_GROUP} > /dev/null; then
    addgroup --system \${PROXY_GROUP}
fi
if ! getent passwd \${PROXY_USER} > /dev/null; then
    adduser --system --ingroup \${PROXY_GROUP} --no-create-home --home /var/cache/go-apt-proxy \${PROXY_USER}
fi

# Установка прав доступа для директорий кеша и логов
# Директория /run/${RUN_DIR_NAME} будет создана и настроена systemd через RuntimeDirectory
chown -R \${PROXY_USER}:\${PROXY_GROUP} /var/cache/go-apt-proxy
chown -R \${PROXY_USER}:\${LOG_GROUP} /var/log/go-apt-proxy
chmod 750 /var/cache/go-apt-proxy
chmod 2750 /var/log/go-apt-proxy # Устанавливаем SGID для логов

# Перезагрузка конфигурации systemd
systemctl daemon-reload

# Включение сервиса для автозапуска при загрузке
systemctl enable go-apt-proxy.service

# Запуск сервиса, если система не в режиме chroot и systemd активен
# Проверка на chroot (приблизительная)
if [ "\$(stat -c %d:%i /)" != "\$(stat -c %d:%i /proc/1/root/.)" ]; then
    echo "Обнаружен режим chroot, сервис не будет запущен автоматически."
elif systemctl is-system-running --quiet --wait; then
    echo "Запуск go-apt-proxy сервиса..."
    systemctl start go-apt-proxy.service || echo "Не удалось запустить сервис go-apt-proxy. Проверьте журнал: journalctl -u go-apt-proxy.service"
else
    echo "Systemd не активен, сервис не будет запущен автоматически."
fi


exit 0
EOF

chmod 755 "${DEBIAN_DIR}/postinst"

# Создание prerm скрипта
echo "Создание prerm скрипта..."
cat > "${DEBIAN_DIR}/prerm" << EOF
#!/bin/bash
set -e

# Остановка и отключение сервиса при удалении пакета
if [ "\$1" = "remove" ] || [ "\$1" = "upgrade" ]; then
    # Проверяем, существует ли юнит и активен ли он
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

# Создание postrm скрипта
echo "Создание postrm скрипта..."
cat > "${DEBIAN_DIR}/postrm" << EOF
#!/bin/bash
set -e

# Перезагружаем systemd после удаления скриптов, если пакет удаляется
if [ "\$1" = "remove" ] || [ "\$1" = "purge" ]; then
    systemctl daemon-reload
fi

if [ "\$1" = "purge" ]; then
    echo "Очистка после удаления пакета ${PKG_NAME}..."
    # Удаление конфигурационных файлов
    echo "Удаление /etc/go-apt-proxy..."
    rm -rf /etc/go-apt-proxy

    # Удаление кеша и логов
    echo "Удаление /var/cache/go-apt-proxy..."
    rm -rf /var/cache/go-apt-proxy
    echo "Удаление /var/log/go-apt-proxy..."
    rm -rf /var/log/go-apt-proxy
    # /run/go-apt-proxy удалять не нужно, это временная директория

    # Удаление пользователя и группы
    if getent passwd apt-proxy > /dev/null; then
        echo "Удаление пользователя apt-proxy..."
        deluser --system apt-proxy || echo "Предупреждение: не удалось удалить пользователя apt-proxy"
    fi

    if getent group apt-proxy > /dev/null; then
        # Проверяем, остались ли другие пользователи в группе перед удалением
        if [ -z "\$(getent group apt-proxy | cut -d: -f4)" ]; then
           echo "Удаление группы apt-proxy..."
           delgroup --system apt-proxy || echo "Предупреждение: не удалось удалить группу apt-proxy"
        else
           echo "Группа apt-proxy не пуста, пропускаем удаление."
        fi
    fi
fi

exit 0
EOF

chmod 755 "${DEBIAN_DIR}/postrm"

# Создание control-файла
echo "Создание control файла..."
cat > "${DEBIAN_DIR}/control" << EOF
Package: ${PKG_NAME}
Version: ${PKG_VERSION}
Architecture: ${PKG_ARCH}
Maintainer: ${PKG_MAINTAINER}
Depends: systemd, adduser
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
Homepage: https://github.com/yolkispalkis/go-apt-cache
EOF

# Создание conffiles для отслеживания файлов конфигурации
echo "Создание conffiles..."
cat > "${DEBIAN_DIR}/conffiles" << EOF
/etc/go-apt-proxy/config.json
EOF

# Создание copyright файла
echo "Создание copyright файла..."
cat > "${DOC_DIR}/copyright" << EOF
Format: https://www.debian.org/doc/packaging-manuals/copyright-format/1.0/
Upstream-Name: ${PKG_NAME}
Source: https://github.com/yolkispalkis/go-apt-cache

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

# Создание changelog
echo "Создание changelog файла..."
cat > "${DOC_DIR}/changelog.Debian" << EOF
${PKG_NAME} (${PKG_VERSION}-1) unstable; urgency=medium

  * Initial release.
  * Use systemd RuntimeDirectory for socket directory management.

 -- ${PKG_MAINTAINER}  $(date -R)
EOF

gzip -9 -n "${DOC_DIR}/changelog.Debian"

# Установка корректных прав на все файлы перед сборкой
echo "Установка финальных прав доступа..."
find "${STAGE_DIR}" -type d -exec chmod 755 {} \;
find "${STAGE_DIR}" -type f -exec chmod 644 {} \;
chmod 755 "${BIN_DIR}/${PKG_NAME}"
chmod 755 "${DEBIAN_DIR}/postinst" "${DEBIAN_DIR}/prerm" "${DEBIAN_DIR}/postrm"
# Права на DEBIAN/* устанавливаются dpkg-deb

# Сборка deb-пакета
echo "Сборка .deb пакета..."
cd "${BUILD_DIR}"
fakeroot dpkg-deb --build staging "${PKG_NAME}_${PKG_VERSION}_${PKG_ARCH}.deb"
# mv staging.deb "${PKG_NAME}_${PKG_VERSION}_${PKG_ARCH}.deb" # dpkg-deb >= 1.17.11 позволяет указать имя файла

echo "Готово! Пакет создан: ${BUILD_DIR}/${PKG_NAME}_${PKG_VERSION}_${PKG_ARCH}.deb"

# Вывод инструкций по установке
echo -e "\nДля установки пакета выполните:"
echo "sudo dpkg -i ${BUILD_DIR}/${PKG_NAME}_${PKG_VERSION}_${PKG_ARCH}.deb"
echo "sudo apt-get install -f  # Установка зависимостей (systemd, adduser), если требуется"
