# API Gateway

OpenAI-compatible aggregate API gateway with a WebUI for channels, models, logs, stats, key rotation, and Pioneer billing status.

Requires Node.js 22 or newer. Node.js 24 LTS is recommended.

## Three Steps

```bash
git clone https://github.com/SycamoreW/api-gateway.git
cd api-gateway
sh start.sh
```

For Node.js panels such as 1Panel, use `npm start` as the startup command. This project has no third-party npm dependencies.

After startup, the terminal prints the WebUI address:

```text
WebUI: http://127.0.0.1:8300
API:   http://127.0.0.1:8300/v1
```

Open the WebUI and enter the management key configured in `config.json`:

```text
replace-with-management-key
```

Then edit channels, upstream API keys, and change both keys in the WebUI before exposing it publicly.

## Termux

Install Node.js and Git once:

```bash
pkg update && pkg install -y nodejs git
```

Then run the same three steps:

```bash
git clone https://github.com/SycamoreW/api-gateway.git
cd api-gateway
sh start.sh
```

On Termux, `start.sh` will try to open the WebUI automatically. If it does not open, copy the printed `WebUI` address into your browser.

## Features

- OpenAI-compatible `/v1/models` and `/v1/chat/completions`
- Route models to multiple upstream channels
- WebUI channel and model management
- Disable channels without deleting their saved configuration, keys, or model list
- Multiple upstream keys per channel with round-robin rotation
- Generated client API keys for `/v1` access
- Request logs and usage stats, including per-IP usage
- Pioneer billing status in the WebUI header
- Prompt Cache controls for Anthropic Messages API and compatible Claude proxy channels
- Anthropic thinking controls for compatible Claude models and providers

## Key Management

Each channel can use either a single upstream key or a key pool. Keep `key` for backward compatibility, and add more keys in `keys`; the gateway deduplicates them and rotates one key per request for that channel:

```json
{
  "key": "provider-key-1",
  "keys": ["provider-key-1", "provider-key-2", "provider-key-3"]
}
```

The WebUI channel editor exposes this as the upstream key pool. The selected upstream key is logged only as a short fingerprint.

The management key `admin_key` can access only the WebUI management API under `/api/*`. The primary client key `api_key` and generated keys in `api_keys` can call `/v1/*`, but cannot access management routes. `admin_key` and `api_key` must be different.

The WebUI never stores `admin_key` in browser storage. A successful login creates a 12-hour `HttpOnly`, `SameSite=Strict` session cookie; restarting the gateway, changing the management key, or clicking **退出管理** invalidates the session.

## Pioneer Billing Status

If a channel's `base_url` points to `api.pioneer.ai`, the WebUI header shows a Pioneer quota card. Click it to query `GET /billing/billing-status` for every configured Pioneer upstream key, then display the aggregated remaining free tier and total usage. Hover the card to see per-key results by fingerprint.

## Anthropic Prompt Cache

Enable Prompt Cache per channel in the WebUI. For channels that use the Anthropic Messages API, set the channel format to `anthropic`; the gateway converts OpenAI-compatible chat requests to `/v1/messages` and adds top-level Anthropic cache control.

For OpenAI-compatible Claude proxy channels, leave the format as OpenAI compatible and enable Prompt Cache only if the upstream accepts Anthropic-style content-block `cache_control`. The gateway adds `cache_control` to the last reusable message content block instead of sending it as a top-level field:

```json
{
  "messages": [
    {
      "role": "system",
      "content": [
        {
          "type": "text",
          "text": "long reusable context...",
          "cache_control": {
            "type": "ephemeral",
            "ttl": "1h"
          }
        }
      ]
    },
    {
      "role": "user",
      "content": "question"
    }
  ]
}
```

The WebUI defaults this option to `1h` for longer conversations. Use `5m` for short-lived cache if the provider charges less for 5-minute writes. The 1-hour cache can reduce repeated context processing after longer pauses, but cache writes cost more than the default 5-minute cache on Anthropic.

## Anthropic Thinking

Anthropic-format channels can optionally inject thinking controls in the request body. The WebUI supports two modes:

- `enabled + budget_tokens`: sends `thinking: {"type":"enabled","budget_tokens":32000}`. If `max_tokens` is lower than the thinking budget, the gateway raises `max_tokens` to `budget_tokens + 1024`.
- `adaptive + effort`: sends `thinking: {"type":"adaptive"}` and, when selected, `output_config: {"effort":"max"}` or another effort level.

If a client request already includes `thinking` or `output_config`, the gateway passes those through and does not replace them.

## Files

- `index.mjs`: server, auth, routing, proxying, logs, stats, config API
- `ui.html`: WebUI
- `start.sh`: three-step startup script
- `config.example.json`: sanitized example config
- `config.json`: local runtime config, ignored by Git
- `stats.json`: runtime stats, ignored by Git

## Config

On first startup, `start.sh` creates `config.json` from `config.example.json`.

Important fields:

```json
{
  "port": 8300,
  "host": "127.0.0.1",
  "admin_key": "replace-with-management-key",
  "api_key": "replace-with-gateway-api-key",
  "api_keys": ["client-key-1"],
  "channels": {
    "anthropic": {
      "name": "anthropic",
      "base_url": "https://api.anthropic.com/v1",
      "key": "sk-ant-...",
      "enabled": true,
      "format": "anthropic",
      "anthropic_version": "2023-06-01",
      "prompt_cache_enabled": true,
      "prompt_cache_ttl": "1h",
      "anthropic_thinking_type": "enabled",
      "anthropic_thinking_budget_tokens": 32000,
      "models": ["anthropic/claude-3-7-sonnet-latest"]
    },
    "pio": {
      "name": "pio",
      "base_url": "https://api.pioneer.ai/v1",
      "key": "pio_sk_...",
      "enabled": true,
      "keys": ["pio_sk_...", "pio_sk_second..."],
      "prompt_cache_enabled": true,
      "prompt_cache_ttl": "1h",
      "models": ["pio/claude-opus-4-7"]
    }
  }
}
```

Use `admin_key` to open and manage the WebUI. Use `api_key` as the Bearer token for model clients:

```text
Authorization: Bearer replace-with-gateway-api-key
```

Change both keys from the WebUI after first login. For public use, put the gateway behind an HTTPS reverse proxy; the Node.js service listens on `127.0.0.1` by default.

When running inside Docker with a published port, set `host` to `0.0.0.0`. Keep the published host port bound to `127.0.0.1` when an OpenResty or Nginx reverse proxy is used.
