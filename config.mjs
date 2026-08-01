import fs from 'node:fs';
import { config, modelMap, channelKeyCursors, CONFIG_FILE } from './state.mjs';

function normalizeKeyList(keys = []) {
  const seen = new Set();
  const result = [];
  for (const key of keys) {
    const normalized = String(key || '').trim();
    if (!normalized || seen.has(normalized)) continue;
    seen.add(normalized);
    result.push(normalized);
  }
  return result;
}

function getChannelKeys(channel = {}) {
  return normalizeKeyList([
    channel.key,
    ...(Array.isArray(channel.keys) ? channel.keys : []),
  ]);
}

function isKeyDisabled(channel = {}, key = '') {
  const disabled = Array.isArray(channel.disabled_keys) ? channel.disabled_keys : [];
  return disabled.includes(String(key).trim());
}

function getForceEnabledChannelKeys(channel = {}) {
  const allKeys = new Set(getChannelKeys(channel));
  return normalizeKeyList(channel.force_enabled_keys || []).filter(key => allKeys.has(key));
}

function isKeyForceEnabled(channel = {}, key = '') {
  return getForceEnabledChannelKeys(channel).includes(String(key || '').trim());
}

function getActiveChannelKeys(channel = {}) {
  const allKeys = getChannelKeys(channel);
  const disabled = new Set(Array.isArray(channel.disabled_keys) ? channel.disabled_keys.map(k => String(k || '').trim()) : []);
  const forced = new Set(getForceEnabledChannelKeys(channel));
  return allKeys.filter(k => forced.has(k) || !disabled.has(k));
}

function normalizeChannelForSave(channel = {}) {
  const keys = getChannelKeys(channel);
  const next = { ...channel };
  next.enabled = channel.enabled !== false;
  if (Object.prototype.hasOwnProperty.call(channel, 'model_prefix')) {
    next.model_prefix = String(channel.model_prefix || '').trim();
  }
  if (Object.prototype.hasOwnProperty.call(channel, 'website')) {
    const site = String(channel.website || '').trim();
    if (site) next.website = site;
    else delete next.website;
  }
  if (next.prompt_cache_enabled) {
    next.prompt_cache_ttl = normalizePromptCacheTtl(next.prompt_cache_ttl);
  } else {
    delete next.prompt_cache_ttl;
  }
  if (next.auto_truncate) next.auto_truncate = true;
  else delete next.auto_truncate;
  if (keys.length > 0) {
    next.key = keys[0];
    if (keys.length > 1) next.keys = keys;
    else delete next.keys;
  } else {
    next.key = '';
    delete next.keys;
  }
  const keySet = new Set(keys);
  next.force_enabled_keys = normalizeKeyList(next.force_enabled_keys || []).filter(key => keySet.has(key));
  if (next.force_enabled_keys.length === 0) delete next.force_enabled_keys;
  const forceEnabledSet = new Set(next.force_enabled_keys || []);
  // Normalize disabled_keys: dedupe, trim, remove missing and force-enabled entries.
  if (Array.isArray(next.disabled_keys) && next.disabled_keys.length > 0) {
    next.disabled_keys = normalizeKeyList(next.disabled_keys.filter(k => keySet.has(String(k || '').trim()) && !forceEnabledSet.has(String(k || '').trim())));
    if (next.disabled_keys.length === 0) delete next.disabled_keys;
  } else {
    delete next.disabled_keys;
  }
  // Keep failure metadata only for currently configured and disabled keys.
  const rawDisabledMeta = next.disabled_key_meta && typeof next.disabled_key_meta === 'object' && !Array.isArray(next.disabled_key_meta)
    ? next.disabled_key_meta
    : {};
  const normalizedDisabledMeta = {};
  for (const key of next.disabled_keys || []) {
    const meta = rawDisabledMeta[key];
    if (!meta || typeof meta !== 'object' || Array.isArray(meta)) continue;
    normalizedDisabledMeta[key] = {
      reason: String(meta.reason || '').slice(0, 500),
      status: Number(meta.status) || null,
      disabled_at: String(meta.disabled_at || ''),
      last_error: String(meta.last_error || meta.reason || '').slice(0, 1000),
      last_error_at: String(meta.last_error_at || meta.disabled_at || ''),
    };
  }
  if (Object.keys(normalizedDisabledMeta).length) next.disabled_key_meta = normalizedDisabledMeta;
  else delete next.disabled_key_meta;
  return next;
}

const HARD_KEY_FAILURE_PATTERNS = [
  { re: /invalid[_\s-]*api[_\s-]*key|incorrect api key|api key.*(?:invalid|not valid)/i, reason: 'invalid_api_key' },
  { re: /unauthorized|authentication (?:failed|error)/i, reason: 'unauthorized' },
  { re: /permission|forbidden|not allowed/i, reason: 'forbidden' },
  { re: /insufficient|no (?:remaining )?(?:quota|balance|credit)/i, reason: 'insufficient_quota' },
  { re: /quota (?:exceeded|exhausted)|out of (?:quota|credit)/i, reason: 'quota_exhausted' },
  { re: /expired|过期|失效/i, reason: 'expired' },
  { re: /余额不足|额度(?:不足|用尽|耗尽)|欠费/i, reason: 'insufficient_quota_zh' },
  { re: /无效(?:的)?(?:key|密钥|令牌|token)|密钥无效|令牌无效/i, reason: 'invalid_api_key_zh' },
  { re: /封禁|禁用|已停用|冻结|suspend|banned|deactivat/i, reason: 'banned' },
  { re: /account.*(?:disabled|suspended|blocked)/i, reason: 'account_disabled' },
];

function classifyUpstreamKeyFailure(status = 0, message = '') {
  const text = String(message || '');
  for (const item of HARD_KEY_FAILURE_PATTERNS) {
    if (item.re.test(text)) return { disable: true, reason: item.reason, status: Number(status) || null };
  }
  const code = Number(status) || 0;
  if ([401, 402, 403].includes(code)) return { disable: true, reason: `http_${code}`, status: code };
  if (code === 429) return { disable: false, retry: true, reason: 'rate_limited', status: code };
  if (code >= 500) return { disable: false, retry: true, reason: `http_${code}`, status: code };
  return { disable: false, retry: false, reason: code >= 400 ? `http_${code}` : '', status: code || null };
}

function disableUpstreamChannelKey(channelKey, key, details = {}) {
  const channel = config.channels?.[channelKey];
  const trimmedKey = String(key || '').trim();
  if (!channel || !trimmedKey || !getChannelKeys(channel).includes(trimmedKey)) return false;
  if (isKeyForceEnabled(channel, trimmedKey)) return false;
  const disabledKeys = normalizeKeyList([...(channel.disabled_keys || []), trimmedKey]);
  const now = new Date().toISOString();
  const meta = {
    ...(channel.disabled_key_meta && typeof channel.disabled_key_meta === 'object' ? channel.disabled_key_meta : {}),
    [trimmedKey]: {
      reason: String(details.reason || 'manual'),
      status: Number(details.status) || null,
      disabled_at: String(details.disabled_at || now),
      last_error: String(details.error || details.reason || 'manual'),
      last_error_at: String(details.last_error_at || now),
    },
  };
  config.channels[channelKey] = normalizeChannelForSave({
    ...channel,
    disabled_keys: disabledKeys,
    disabled_key_meta: meta,
  });
  channelKeyCursors.delete(channelKey);
  saveConfig();
  return true;
}

function enableUpstreamChannelKey(channelKey, key) {
  const channel = config.channels?.[channelKey];
  const trimmedKey = String(key || '').trim();
  if (!channel || !trimmedKey || !getChannelKeys(channel).includes(trimmedKey)) return false;
  const disabledKeys = (channel.disabled_keys || []).filter(item => item !== trimmedKey);
  const meta = { ...(channel.disabled_key_meta || {}) };
  delete meta[trimmedKey];
  config.channels[channelKey] = normalizeChannelForSave({
    ...channel,
    disabled_keys: disabledKeys,
    disabled_key_meta: meta,
  });
  channelKeyCursors.delete(channelKey);
  saveConfig();
  return true;
}

function isChannelEnabled(channel = {}) {
  return channel?.enabled !== false;
}

function normalizePromptCacheTtl(ttl = '') {
  return String(ttl || '').trim() === '1h' ? '1h' : '5m';
}

function rebuildModelMap() {
  modelMap.clear();
  for (const [ckey, ch] of Object.entries(config.channels)) {
    if (!isChannelEnabled(ch)) continue;
    const overrides = (ch && typeof ch.model_overrides === 'object' && ch.model_overrides) || {};
    for (const m of ch.models) {
      // Allow per-channel model_overrides to rewrite the upstream model id.
      // Example: { "provider/auto": "provider/model-id" } makes inbound
      // `provider/auto` resolve to the same channel but forward `model-id`
      // (after stripModelPrefix) to the upstream provider.
      const override = Object.prototype.hasOwnProperty.call(overrides, m) ? overrides[m] : null;
      const upstreamModel = (typeof override === 'string' && override.trim()) ? override.trim() : m;
      modelMap.set(m, { channelKey: ckey, upstreamModel });
    }
  }

}

function normalizeStringArray(value) {
  const items = Array.isArray(value) ? value : [];
  return [...new Set(items.map(item => String(item || '').trim()).filter(Boolean))];
}

function normalizeExpiresAt(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const time = Date.parse(raw);
  if (!Number.isFinite(time)) return '';
  return new Date(time).toISOString();
}

function normalizeClientKeyEntry(entry) {
  if (typeof entry === 'string') {
    const key = entry.trim();
    return key ? {
      key,
      name: '',
      allowed_channels: [],
      allowed_models: [],
      quota_limit: 0,
      quota_used: 0,
      expires_at: '',
      enabled: true,
    } : null;
  }
  if (!entry || typeof entry !== 'object' || Array.isArray(entry)) return null;
  const key = String(entry.key || '').trim();
  if (!key) return null;
  const quotaLimit = Math.max(0, Math.floor(Number(entry.quota_limit) || 0));
  const quotaUsed = Math.max(0, Math.floor(Number(entry.quota_used) || 0));
  return {
    key,
    name: String(entry.name || '').trim(),
    allowed_channels: normalizeStringArray(entry.allowed_channels),
    allowed_models: normalizeStringArray(entry.allowed_models),
    quota_limit: quotaLimit,
    quota_used: quotaUsed,
    expires_at: normalizeExpiresAt(entry.expires_at),
    enabled: entry.enabled !== false,
  };
}

function saveConfig() {
  fs.writeFileSync(CONFIG_FILE, JSON.stringify(config, null, 2));
}

function normalizeImportedConfig(input) {
  const nextConfig = input && typeof input === 'object' && input.config && typeof input.config === 'object'
    ? input.config
    : input;
  if (!nextConfig || typeof nextConfig !== 'object' || Array.isArray(nextConfig)) {
    throw new Error('备份文件格式不正确');
  }
  if (!nextConfig.channels || typeof nextConfig.channels !== 'object' || Array.isArray(nextConfig.channels)) {
    throw new Error('备份文件缺少 channels');
  }
  if (nextConfig.models && !Array.isArray(nextConfig.models)) {
    throw new Error('models 必须是数组');
  }
  const normalized = {
    ...nextConfig,
    port: Number(nextConfig.port) || config.port || 8300,
    admin_key: String(nextConfig.admin_key || config.admin_key || '').trim(),
    api_key: String(nextConfig.api_key || config.api_key || '').trim(),
    api_keys: Array.isArray(nextConfig.api_keys)
      ? nextConfig.api_keys.map(normalizeClientKeyEntry).filter(Boolean)
      : [],
    channels: nextConfig.channels,
    models: Array.isArray(nextConfig.models) ? nextConfig.models : [],
  };
  for (const [key, channel] of Object.entries(normalized.channels || {})) {
    normalized.channels[key] = normalizeChannelForSave(channel);
  }
  if (normalized.api_key.length < 4) {
    throw new Error('主调用 Key 至少 4 位');
  }
  if (!/^[\x21-\x7E]+$/.test(normalized.api_key)) {
    throw new Error('主调用 Key 只能包含英文、数字和常见英文符号');
  }
  if (normalized.admin_key.length < 8) {
    throw new Error('管理 Key 至少 8 位');
  }
  if (!/^[\x21-\x7E]+$/.test(normalized.admin_key)) {
    throw new Error('管理 Key 只能包含英文、数字和常见英文符号');
  }
  if (normalized.admin_key === normalized.api_key) {
    throw new Error('管理 Key 不能与主调用 Key 相同');
  }
  const clientKeySet = new Set(normalized.api_keys.map(entry => entry.key));
  if (clientKeySet.has(normalized.admin_key)) {
    throw new Error('管理 Key 不能与额外调用 Key 相同');
  }
  if (clientKeySet.has(normalized.api_key)) {
    throw new Error('主调用 Key 不能与额外调用 Key 相同');
  }
  return normalized;
}


export {
  normalizeKeyList,
  getChannelKeys,
  getForceEnabledChannelKeys,
  isKeyForceEnabled,
  getActiveChannelKeys,
  isKeyDisabled,
  normalizeChannelForSave,
  isChannelEnabled,
  normalizePromptCacheTtl,
  rebuildModelMap,
  normalizeStringArray,
  normalizeExpiresAt,
  normalizeClientKeyEntry,
  saveConfig,
  normalizeImportedConfig,
  classifyUpstreamKeyFailure,
  disableUpstreamChannelKey,
  enableUpstreamChannelKey,
};
