#!/usr/bin/env bash
# setup.sh — install and configure konto on a fresh Ubuntu 24 Linode
#
# Usage:
#   ./setup.sh                                  # localhost only, no SSL
#   ./setup.sh --domain example.com             # SSL via certbot (email prompted)
#   ./setup.sh --domain example.com \
#              --email  admin@example.com        # fully non-interactive
#   ./setup.sh --domain example.com \
#              --email  admin@example.com \
#              --port   9090                     # custom konto port
#
# Requirements:
#   - Ubuntu 22.04 or 24.04
#   - Run as root (or with sudo)
#   - konto.go present in the same directory as this script
#   - Domain (if provided) must already point at this server's IP
#
# What this does:
#   1. Installs Go, nginx, certbot
#   2. Builds konto and installs it to /usr/local/bin
#   3. Creates a dedicated 'konto' system user
#   4. Writes config/data dirs under /etc/konto and /var/log/konto
#   5. Installs a systemd service (starts on boot, restarts on crash)
#   6. Configures nginx as a reverse proxy
#   7. Attempts SSL via certbot — falls back to HTTP silently if it fails
#   8. Configures logrotate for access.log
#   9. Starts konto

set -euo pipefail

# ── colours ───────────────────────────────────────────────────────────────────

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[0;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

info()    { echo -e "${CYAN}▸${RESET} $*"; }
success() { echo -e "${GREEN}✓${RESET} $*"; }
warn()    { echo -e "${YELLOW}⚠${RESET}  $*"; }
fatal()   { echo -e "${RED}✗${RESET} $*" >&2; exit 1; }
header()  { echo -e "\n${BOLD}$*${RESET}"; }

# ── defaults ──────────────────────────────────────────────────────────────────

DOMAIN=""
EMAIL=""
KONTO_PORT=7878
KONTO_USER=konto
KONTO_BIN=/usr/local/bin/konto
KONTO_DIR=/etc/konto
KONTO_LOG_DIR=/var/log/konto
KONTO_DATA="${KONTO_DIR}/append.log"
KONTO_ACCESS_LOG="${KONTO_LOG_DIR}/access.log"
KONTO_SERVICE=/etc/systemd/system/konto.service
NGINX_CONF=/etc/nginx/sites-available/konto
SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SSL_OK=false

# ── argument parsing ──────────────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
    case $1 in
        --domain) DOMAIN="$2"; shift 2 ;;
        --email)  EMAIL="$2";  shift 2 ;;
        --port)   KONTO_PORT="$2"; shift 2 ;;
        *) fatal "Unknown argument: $1" ;;
    esac
done

# ── preflight ─────────────────────────────────────────────────────────────────

header "konto setup"
info "Source directory: ${SOURCE_DIR}"

[[ $EUID -eq 0 ]] || fatal "Run as root: sudo ./setup.sh"
[[ -f "${SOURCE_DIR}/konto.go" ]] || fatal "konto.go not found in ${SOURCE_DIR}"

if [[ -n "$DOMAIN" ]]; then
    info "Domain: ${DOMAIN}"
    [[ -n "$EMAIL" ]] || { read -rp "Email for SSL certificate: " EMAIL; }
else
    warn "No domain provided — will configure for localhost only (HTTP)"
fi

# ── 1. system packages ────────────────────────────────────────────────────────

header "1/8  Installing system packages"
apt-get update -qq
apt-get install -y -qq \
    golang-go \
    nginx \
    certbot \
    python3-certbot-nginx \
    logrotate \
    curl
success "Packages installed"

# ── 2. build konto ────────────────────────────────────────────────────────────

header "2/8  Building konto"
BUILD_DIR=$(mktemp -d)
cp "${SOURCE_DIR}/konto.go" "${BUILD_DIR}/konto.go"
cd "${BUILD_DIR}"
go mod init konto 2>/dev/null
go build -o "${KONTO_BIN}" konto.go
chmod 755 "${KONTO_BIN}"
cd - > /dev/null
rm -rf "${BUILD_DIR}"
success "konto installed to ${KONTO_BIN}"
"${KONTO_BIN}" --help 2>&1 | head -1 || true

# ── 3. system user ────────────────────────────────────────────────────────────

header "3/8  Creating system user"
if id "${KONTO_USER}" &>/dev/null; then
    info "User '${KONTO_USER}' already exists — skipping"
else
    useradd \
        --system \
        --no-create-home \
        --shell /usr/sbin/nologin \
        --comment "konto ledger server" \
        "${KONTO_USER}"
    success "User '${KONTO_USER}' created"
fi

# ── 4. directories ────────────────────────────────────────────────────────────

header "4/8  Creating directories"
mkdir -p "${KONTO_DIR}" "${KONTO_LOG_DIR}"
chown "${KONTO_USER}:${KONTO_USER}" "${KONTO_DIR}" "${KONTO_LOG_DIR}"
chmod 750 "${KONTO_DIR}" "${KONTO_LOG_DIR}"

# Touch the log files so logrotate doesn't complain on first run.
touch "${KONTO_DATA}" "${KONTO_ACCESS_LOG}"
chown "${KONTO_USER}:${KONTO_USER}" "${KONTO_DATA}" "${KONTO_ACCESS_LOG}"

success "Directories ready"
info "  Data:       ${KONTO_DATA}"
info "  Access log: ${KONTO_ACCESS_LOG}"

# ── 5. systemd service ────────────────────────────────────────────────────────

header "5/8  Installing systemd service"
cat > "${KONTO_SERVICE}" << EOF
[Unit]
Description=konto tamper-evident ledger
Documentation=https://github.com/yourorg/konto
After=network.target
Wants=network.target

[Service]
Type=simple
User=${KONTO_USER}
Group=${KONTO_USER}

ExecStart=${KONTO_BIN} \\
    -addr    127.0.0.1:${KONTO_PORT} \\
    -log     ${KONTO_DATA} \\
    -access-log ${KONTO_ACCESS_LOG}

# Restart policy
Restart=on-failure
RestartSec=5s
StartLimitIntervalSec=60s
StartLimitBurst=3

# Hardening
NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=strict
ReadWritePaths=${KONTO_DIR} ${KONTO_LOG_DIR}
ProtectHome=yes
CapabilityBoundingSet=

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=konto

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable konto
success "systemd service installed (konto.service)"

# ── 6. nginx ──────────────────────────────────────────────────────────────────

header "6/8  Configuring nginx"

# Write a base HTTP config; certbot will modify it if SSL succeeds.
if [[ -n "$DOMAIN" ]]; then
    SERVER_NAME="${DOMAIN} www.${DOMAIN}"
else
    SERVER_NAME="localhost"
fi

cat > "${NGINX_CONF}" << EOF
# konto reverse proxy
# Generated by setup.sh on $(date -u +"%Y-%m-%dT%H:%M:%SZ")

upstream konto {
    server 127.0.0.1:${KONTO_PORT};
    keepalive 32;
}

server {
    listen 80;
    listen [::]:80;
    server_name ${SERVER_NAME};

    # Logging
    access_log  /var/log/nginx/konto-access.log combined;
    error_log   /var/log/nginx/konto-error.log warn;

    # Security headers
    add_header X-Content-Type-Options  nosniff;
    add_header X-Frame-Options         DENY;
    add_header Referrer-Policy         strict-origin-when-cross-origin;

    # Proxy to konto
    location / {
        proxy_pass         http://konto;
        proxy_http_version 1.1;

        # Real IP forwarding (so konto access log shows client IPs).
        proxy_set_header   X-Forwarded-For   \$proxy_add_x_forwarded_for;
        proxy_set_header   X-Real-IP         \$remote_addr;
        proxy_set_header   Host              \$host;

        # Connection keepalive.
        proxy_set_header   Connection        "";

        # Timeouts — generous for bulk import.
        proxy_connect_timeout  10s;
        proxy_send_timeout    300s;
        proxy_read_timeout    300s;

        # Buffer tuning.
        proxy_buffering           on;
        proxy_buffer_size         16k;
        proxy_buffers             8 16k;
        proxy_busy_buffers_size   32k;
    }

    # Health check endpoint (no access log noise).
    location = /__konto {
        proxy_pass http://konto;
        access_log off;
    }
}
EOF

# Enable the site.
ln -sf "${NGINX_CONF}" /etc/nginx/sites-enabled/konto
rm -f /etc/nginx/sites-enabled/default  # remove the nginx default page

nginx -t
systemctl reload nginx || systemctl start nginx
success "nginx configured for ${SERVER_NAME}"

# ── 7. SSL via certbot ────────────────────────────────────────────────────────

header "7/8  SSL certificate"

if [[ -z "$DOMAIN" ]]; then
    warn "No domain — skipping SSL. Run certbot manually when DNS is ready:"
    warn "  certbot --nginx -d yourdomain.com --email you@example.com"
else
    info "Requesting certificate for ${DOMAIN}..."
    if certbot --nginx \
               --non-interactive \
               --agree-tos \
               --redirect \
               --domain "${DOMAIN}" \
               --domain "www.${DOMAIN}" \
               --email  "${EMAIL}" \
               2>&1 | tee /tmp/certbot.log; then
        SSL_OK=true
        success "SSL certificate issued for ${DOMAIN}"
    else
        warn "certbot failed — running on HTTP only."
        warn "Common causes:"
        warn "  • DNS for ${DOMAIN} doesn't point at this server yet"
        warn "  • Port 80 is blocked by a firewall"
        warn "  • Let's Encrypt rate limit hit"
        warn "Full certbot output: /tmp/certbot.log"
        warn "Re-run when DNS is ready: certbot --nginx -d ${DOMAIN} --email ${EMAIL}"
        # nginx is still running on HTTP — this is intentional.
    fi
fi

# ── 8. logrotate ──────────────────────────────────────────────────────────────

header "8/8  Configuring logrotate"
cat > /etc/logrotate.d/konto << EOF
# konto access log rotation
# copytruncate: no signal needed — konto holds the fd open.

${KONTO_ACCESS_LOG} {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
    copytruncate
    create 0640 ${KONTO_USER} ${KONTO_USER}
}
EOF
success "logrotate configured (daily, 30-day retention)"

# ── start konto ───────────────────────────────────────────────────────────────

header "Starting konto"
systemctl start konto

# Give it a moment then health-check.
sleep 2
if curl -sf "http://127.0.0.1:${KONTO_PORT}/__konto" > /dev/null; then
    success "konto is running on port ${KONTO_PORT}"
else
    warn "konto may not have started — check: journalctl -u konto -n 50"
fi

# ── summary ───────────────────────────────────────────────────────────────────

echo ""
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo -e "${GREEN}${BOLD}  konto is up${RESET}"
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo ""

if [[ -n "$DOMAIN" && "$SSL_OK" == "true" ]]; then
    echo -e "  API:      ${CYAN}https://${DOMAIN}${RESET}"
    echo -e "  Health:   ${CYAN}https://${DOMAIN}/__konto${RESET}"
elif [[ -n "$DOMAIN" ]]; then
    echo -e "  API:      ${CYAN}http://${DOMAIN}${RESET}  ${YELLOW}(HTTP only — SSL pending)${RESET}"
    echo -e "  Health:   ${CYAN}http://${DOMAIN}/__konto${RESET}"
else
    echo -e "  API:      ${CYAN}http://localhost${RESET}"
    echo -e "  Health:   ${CYAN}http://localhost/__konto${RESET}"
fi

echo ""
echo -e "  Data:     ${KONTO_DATA}"
echo -e "  Log:      ${KONTO_ACCESS_LOG}"
echo ""
echo -e "  ${BOLD}Useful commands:${RESET}"
echo -e "  systemctl status konto          — service status"
echo -e "  journalctl -u konto -f          — live server log"
echo -e "  tail -f ${KONTO_ACCESS_LOG}     — access log"
echo -e "  systemctl restart konto         — restart after upgrade"
echo ""
