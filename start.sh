#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")"

if ! command -v node >/dev/null 2>&1; then
  echo "Node.js not found."
  echo "Termux: pkg update && pkg install -y nodejs git"
  echo "Ubuntu/Debian: sudo apt update && sudo apt install -y nodejs git"
  exit 1
fi

NODE_MAJOR="$(node -p "Number(process.versions.node.split('.')[0])")"
if [ "${NODE_MAJOR}" -lt 22 ]; then
  echo "Node.js 22 or newer is required (Node.js 24 LTS recommended)."
  echo "Current version: $(node --version)"
  exit 1
fi

if [ ! -f config.json ]; then
  cp config.example.json config.json
  echo "Created config.json from config.example.json."
  echo "Edit API keys and channels in the WebUI after startup."
fi

PORT="$(node -e "const c=require('./config.json'); console.log(c.port || 8300)")"
HOST_IP="$(hostname -I 2>/dev/null | awk '{print $1}' || true)"
LOCAL_URL="http://127.0.0.1:${PORT}"

echo "========================================"
echo "API Gateway is starting"
echo "WebUI: ${LOCAL_URL}"
if [ -n "${HOST_IP}" ]; then
  echo "LAN WebUI: http://${HOST_IP}:${PORT}"
fi
echo "API: ${LOCAL_URL}/v1"
echo "========================================"

if command -v termux-open-url >/dev/null 2>&1; then
  termux-open-url "${LOCAL_URL}" >/dev/null 2>&1 || true
elif command -v xdg-open >/dev/null 2>&1; then
  xdg-open "${LOCAL_URL}" >/dev/null 2>&1 || true
fi

exec node index.mjs config.json
