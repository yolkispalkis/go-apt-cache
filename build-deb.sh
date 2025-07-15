#!/bin/bash
set -e

PKG_NAME="go-apt-cache"
PKG_VERSION="3.1.1"
PKG_ARCH=$(dpkg --print-architecture)
PKG_MAINTAINER="yolkispalkis <me@w3h.su>"
PKG_DESCRIPTION="A high-performance caching proxy for APT repositories, written in Go."
APP_MAIN_PACKAGE="github.com/yolkispalkis/go-apt-cache"
APP_CMD_PATH="./cmd/server"

BUILD_DIR="$(pwd)/build"
STAGE_DIR="${BUILD_DIR}/staging"
DEBIAN_DIR="${STAGE_DIR}/DEBIAN"
BIN_DIR="${STAGE_DIR}/usr/local/bin"
CONFIG_DIR="${STAGE_DIR}/etc/${PKG_NAME}"
SYSTEMD_DIR="${STAGE_DIR}/etc/systemd/system"
DOC_DIR="${STAGE_DIR}/usr/share/doc/${PKG_NAME}"
RUN_DIR_NAME="${PKG_NAME}"

SERVICE_USER="${PKG_NAME}"
SERVICE_GROUP="${PKG_NAME}"

cleanup() {
    echo "--- Cleaning up previous build ---"
    rm -rf "${BUILD_DIR}"
}

create_dirs() {
    echo "--- Creating directory structure ---"
    mkdir -p "${DEBIAN_DIR}" "${BIN_DIR}" "${CONFIG_DIR}" "${SYSTEMD_DIR}" "${DOC_DIR}"
}

build_binary() {
    echo "--- Building ${PKG_NAME} binary ---"
    LD_FLAGS="-s -w -X '${APP_MAIN_PACKAGE}/internal/appinfo.AppVersion=${PKG_VERSION}'"
    GOARCH=${PKG_ARCH} go build -ldflags="${LD_FLAGS}" -o "${BIN_DIR}/${PKG_NAME}" "${APP_CMD_PATH}"
    chmod 755 "${BIN_DIR}/${PKG_NAME}"
}

create_config() {
    echo "--- Creating default config file ---"
    cp config.yaml.example "${CONFIG_DIR}/config.yaml"
    chmod 644 "${CONFIG_DIR}/config.yaml"
}

create_systemd_service() {
    echo "--- Creating systemd service file ---"
    local exec_path="/usr/local/bin/${PKG_NAME}"
    local config_path="/etc/${PKG_NAME}/config.yaml"

    cat >"${SYSTEMD_DIR}/${PKG_NAME}.service" <<EOF
[Unit]
Description=Go APT Cache Service
Documentation=https://${APP_MAIN_PACKAGE}
After=network.target

[Service]
ExecStart=${exec_path} -config ${config_path}
User=${SERVICE_USER}
Group=${SERVICE_GROUP}
EnvironmentFile=-/etc/environment
Restart=on-failure
RestartSec=5s
WorkingDirectory=/var/cache/${PKG_NAME}
RuntimeDirectory=${RUN_DIR_NAME}
RuntimeDirectoryMode=0750

ProtectSystem=full
ProtectHome=true
PrivateTmp=true
NoNewPrivileges=true
PrivateDevices=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6
RestrictRealtime=true

[Install]
WantedBy=multi-user.target
EOF
    chmod 644 "${SYSTEMD_DIR}/${PKG_NAME}.service"
}

create_deb_scripts() {
    echo "--- Creating debian maintainer scripts ---"

    cat >"${DEBIAN_DIR}/postinst" <<EOF
#!/bin/bash
set -e
LOG_DIR="/var/log/${PKG_NAME}"
CACHE_DIR="/var/cache/${PKG_NAME}"

if ! getent group "${SERVICE_GROUP}" >/dev/null; then
    addgroup --system "${SERVICE_GROUP}"
fi
if ! getent passwd "${SERVICE_USER}" >/dev/null; then
    adduser --system --ingroup "${SERVICE_GROUP}" --no-create-home \\
            --home "\${CACHE_DIR}" --shell /bin/false "${SERVICE_USER}"
fi

mkdir -p "\${CACHE_DIR}"
chown -R "${SERVICE_USER}:${SERVICE_GROUP}" "\${CACHE_DIR}"
chmod 750 "\${CACHE_DIR}"

mkdir -p "\${LOG_DIR}"
chown "${SERVICE_USER}:${SERVICE_GROUP}" "\${LOG_DIR}"
chmod 750 "\${LOG_DIR}"

systemctl daemon-reload
if [ "\$1" = "configure" ]; then
    systemctl enable ${PKG_NAME}.service
    if [ -z "\${DPKG_ROOT}" ]; then
        systemctl start ${PKG_NAME}.service || true
    fi
fi
EOF

    cat >"${DEBIAN_DIR}/prerm" <<EOF
#!/bin/bash
set -e
if [ "\$1" = "remove" ]; then
    systemctl stop ${PKG_NAME}.service || true
    systemctl disable ${PKG_NAME}.service || true
fi
EOF

    cat >"${DEBIAN_DIR}/postrm" <<EOF
#!/bin/bash
set -e
if [ "\$1" = "purge" ]; then
    echo "Purging configuration for ${PKG_NAME}..."
    rm -rf "/var/cache/${PKG_NAME}"
    rm -rf "/var/log/${PKG_NAME}"
    if getent passwd "${SERVICE_USER}" >/dev/null; then
        deluser --system "${SERVICE_USER}" || true
    fi
    if getent group "${SERVICE_GROUP}" >/dev/null; then
        delgroup --system "${SERVICE_GROUP}" || true
    fi
fi
systemctl daemon-reload
EOF

    chmod 755 "${DEBIAN_DIR}/postinst" "${DEBIAN_DIR}/prerm" "${DEBIAN_DIR}/postrm"
}

create_control_file() {
    echo "--- Creating control file ---"
    INSTALLED_SIZE=$(du -sk "${STAGE_DIR}" | cut -f1)
    cat >"${DEBIAN_DIR}/control" <<EOF
Package: ${PKG_NAME}
Version: ${PKG_VERSION}
Architecture: ${PKG_ARCH}
Maintainer: ${PKG_MAINTAINER}
Installed-Size: ${INSTALLED_SIZE}
Depends: adduser, systemd
Section: net
Priority: optional
Homepage: https://${APP_MAIN_PACKAGE}
Description: ${PKG_DESCRIPTION}
 A modern, high-performance caching proxy for APT repositories, designed
 for speed and efficiency. It helps to significantly speed up package
 installation and updates on Debian/Ubuntu-based systems by caching
 packages and metadata locally.
EOF
}

create_conffiles() {
    echo "--- Creating conffiles ---"
    echo "/etc/${PKG_NAME}/config.yaml" >"${DEBIAN_DIR}/conffiles"
}

build_package() {
    echo "--- Building .deb package ---"
    local output_file="${BUILD_DIR}/${PKG_NAME}_${PKG_VERSION}_${PKG_ARCH}.deb"
    fakeroot dpkg-deb --build "${STAGE_DIR}" "${output_file}"
}

main() {
    cleanup
    create_dirs
    build_binary
    create_config
    create_systemd_service
    create_deb_scripts
    create_conffiles
    create_control_file
    build_package
    echo "--- Build complete! ---"
    echo "Package created at: ${BUILD_DIR}/${PKG_NAME}_${PKG_VERSION}_${PKG_ARCH}.deb"
}

main
