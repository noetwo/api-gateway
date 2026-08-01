import crypto from 'node:crypto';
import { config, modelMap, stats } from './state.mjs';
import { fingerprintKey, writeGatewayLog, getClientIp } from './logger.mjs';
import {
  isChannelEnabled,
  normalizeClientKeyEntry,
  saveConfig,
} from './config.mjs';

const ADMIN_SESSION_COOKIE = 'api_gateway_admin_session';
const ADMIN_SESSION_TTL_MS = 12 * 60 * 60 * 1000;
const ADMIN_LOGIN_WINDOW_MS = 15 * 60 * 1000;
const ADMIN_LOGIN_MAX_FAILURES = 10;
const adminSessions = new Map();
const adminLoginFailures = new Map();

function getClientKeyEntries() {
  return (Array.isArray(config.api_keys) ? config.api_keys : [])
    .map(normalizeClientKeyEntry)
    .filter(Boolean);
}

function withClientKeyUsage(entry, type = 'generated') {
  const usage = stats.clientKeyUsage?.[fingerprintKey(entry.key)] || {};
  const byChannel = usage.byChannel && typeof usage.byChannel === 'object' ? usage.byChannel : {};
  const usage_by_channel = Object.entries(byChannel).map(([channel, ch]) => ({
    channel,
    usage_count: Number(ch.totalRequests) || 0,
    usage_errors: Number(ch.totalErrors) || 0,
    usage_input_tokens: Number(ch.totalInputTokens) || 0,
    usage_output_tokens: Number(ch.totalOutputTokens) || 0,
    last_used_at: ch.lastUsedAt || '',
    last_status: ch.lastStatus || null,
  })).sort((a, b) => (b.usage_input_tokens + b.usage_output_tokens) - (a.usage_input_tokens + a.usage_output_tokens));
  return {
    ...entry,
    key_type: type,
    usage_count: Number(usage.totalRequests) || 0,
    usage_errors: Number(usage.totalErrors) || 0,
    usage_input_tokens: Number(usage.totalInputTokens) || 0,
    usage_output_tokens: Number(usage.totalOutputTokens) || 0,
    last_used_at: usage.lastUsedAt || '',
    last_status: usage.lastStatus || null,
    usage_by_channel,
  };
}

function getClientKeyDashboardEntries() {
  const admin = withClientKeyUsage({
    key: config.api_key,
    name: '主调用 Key',
    allowed_channels: [],
    allowed_models: [],
    quota_limit: 0,
    quota_used: 0,
    expires_at: '',
    enabled: true,
  }, 'admin');
  return [admin, ...getClientKeyEntries().map(entry => withClientKeyUsage(entry))];
}

function saveClientKeyEntries(entries) {
  config.api_keys = entries.map(entry => ({
    key: entry.key,
    ...(entry.name ? { name: entry.name } : {}),
    ...(entry.allowed_channels?.length ? { allowed_channels: entry.allowed_channels } : {}),
    ...(entry.allowed_models?.length ? { allowed_models: entry.allowed_models } : {}),
    ...(entry.quota_limit > 0 ? { quota_limit: entry.quota_limit } : {}),
    ...(entry.quota_used > 0 ? { quota_used: entry.quota_used } : {}),
    ...(entry.expires_at ? { expires_at: entry.expires_at } : {}),
    ...(entry.enabled === false ? { enabled: false } : {}),
  }));
}

function findClientKeyEntry(token) {
  return getClientKeyEntries().find(entry => entry.key === token) || null;
}

function isClientKeyExpired(entry = {}) {
  if (!entry.expires_at) return false;
  const time = Date.parse(entry.expires_at);
  return Number.isFinite(time) && time <= Date.now();
}

function clientCanUseChannel(req, channelKey = '') {
  if (!isChannelEnabled(config.channels?.[channelKey])) return false;
  if (req.clientApiKeyType === 'admin') return true;
  const allowedChannels = req.clientAllowedChannels || [];
  if (allowedChannels.length === 0) return true;
  return allowedChannels.includes(channelKey);
}

function clientCanUseModel(req, modelName = '', channelKey = '') {
  if (!clientCanUseChannel(req, channelKey)) return false;
  if (req.clientApiKeyType === 'admin') return true;
  const allowedModels = req.clientAllowedModels || [];
  if (allowedModels.length === 0) return true;
  return allowedModels.includes(modelName);
}

function getAccessibleModelEntries(req) {
  return [...modelMap.entries()].filter(([model, entry]) => clientCanUseModel(req, model, entry.channelKey));
}

function getModelQuotaCost(modelName = '') {
  return 1;
}

function consumeClientQuota(req, modelName = '') {
  if (req.clientApiKeyType === 'admin') return { ok: true, cost: 0, remaining: Infinity };
  const entries = getClientKeyEntries();
  
  // Find entry by key
  const index = entries.findIndex(entry => entry.key === req.clientApiKey);
  if (index < 0) return { ok: false, statusCode: 401, message: 'Invalid API key' };

  const entry = entries[index];
  const cost = getModelQuotaCost(modelName);
  const limit = Math.max(0, Math.floor(Number(entry.quota_limit) || 0));
  const used = Math.max(0, Math.floor(Number(entry.quota_used) || 0));
  if (limit > 0 && used + cost > limit) {
    return {
      ok: false,
      statusCode: 429,
      message: `Quota exceeded: need ${cost}, remaining ${Math.max(0, limit - used)}`,
      cost,
      limit,
      used,
      remaining: Math.max(0, limit - used),
    };
  }

  if (limit > 0) {
    entries[index] = { ...entry, quota_used: used + cost };
    saveClientKeyEntries(entries);
    saveConfig();
  }
  return {
    ok: true,
    cost,
    limit,
    used: limit > 0 ? used + cost : used,
    remaining: limit > 0 ? Math.max(0, limit - used - cost) : Infinity,
  };
}

function getBearerToken(req) {
  const authHeader = String(req.headers['authorization'] || '');
  if (authHeader.startsWith('Bearer ')) return authHeader.slice(7);
  // Also accept x-api-key header (Anthropic native format)
  const xApiKey = String(req.headers['x-api-key'] || '');
  if (xApiKey) return xApiKey;
  return '';
}

function secureKeyEqual(candidate = '', expected = '') {
  const candidateBuffer = Buffer.from(String(candidate));
  const expectedBuffer = Buffer.from(String(expected));
  return candidateBuffer.length === expectedBuffer.length
    && candidateBuffer.length > 0
    && crypto.timingSafeEqual(candidateBuffer, expectedBuffer);
}

function adminKeyDigest(key = '') {
  return crypto.createHash('sha256').update(String(key)).digest('hex');
}

function getCookie(req, name) {
  const raw = String(req.headers.cookie || '');
  for (const part of raw.split(';')) {
    const index = part.indexOf('=');
    if (index < 0) continue;
    if (part.slice(0, index).trim() === name) return part.slice(index + 1).trim();
  }
  return '';
}

function isSecureRequest(req) {
  const forwardedProto = String(req.headers['x-forwarded-proto'] || '').split(',')[0].trim().toLowerCase();
  return forwardedProto === 'https' || Boolean(req.socket.encrypted);
}

function buildAdminSessionCookie(req, token = '', maxAgeSeconds = 0) {
  const parts = [
    `${ADMIN_SESSION_COOKIE}=${token}`,
    'Path=/',
    'HttpOnly',
    'SameSite=Strict',
    `Max-Age=${Math.max(0, Math.floor(maxAgeSeconds))}`,
  ];
  if (isSecureRequest(req)) parts.push('Secure');
  return parts.join('; ');
}

function pruneAdminAuthState(now = Date.now()) {
  for (const [token, session] of adminSessions) {
    if (!session || session.expiresAt <= now) adminSessions.delete(token);
  }
  for (const [clientIp, entry] of adminLoginFailures) {
    if (!entry || entry.windowStartedAt + ADMIN_LOGIN_WINDOW_MS <= now) adminLoginFailures.delete(clientIp);
  }
}

function getValidAdminSession(req) {
  const token = getCookie(req, ADMIN_SESSION_COOKIE);
  if (!token) return null;
  const session = adminSessions.get(token);
  if (!session || session.expiresAt <= Date.now()) {
    adminSessions.delete(token);
    return null;
  }
  const currentDigest = adminKeyDigest(String(config.admin_key || '').trim());
  if (!secureKeyEqual(session.keyDigest, currentDigest)) {
    adminSessions.delete(token);
    return null;
  }
  return { token, session };
}

function isAdminRequestAuthenticated(req) {
  const adminKey = String(config.admin_key || '').trim();
  return secureKeyEqual(getBearerToken(req), adminKey) || Boolean(getValidAdminSession(req));
}

function handleAdminLogin(req, res, body = '') {
  const now = Date.now();
  pruneAdminAuthState(now);
  const clientIp = getClientIp(req) || 'unknown';
  const failure = adminLoginFailures.get(clientIp);
  if (failure && failure.windowStartedAt + ADMIN_LOGIN_WINDOW_MS > now && failure.count >= ADMIN_LOGIN_MAX_FAILURES) {
    const retryAfter = Math.max(1, Math.ceil((failure.windowStartedAt + ADMIN_LOGIN_WINDOW_MS - now) / 1000));
    res.writeHead(429, {
      'Content-Type': 'application/json',
      'Cache-Control': 'no-store',
      'Retry-After': String(retryAfter),
    });
    res.end(JSON.stringify({ error: { message: 'Too many login attempts. Try again later.', type: 'rate_limit_error' } }));
    return;
  }

  let parsed;
  try {
    parsed = JSON.parse(body || '{}');
  } catch {
    res.writeHead(400, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    res.end(JSON.stringify({ error: { message: 'Invalid JSON body', type: 'invalid_request_error' } }));
    return;
  }

  const suppliedKey = String(parsed.admin_key || '').trim();
  const adminKey = String(config.admin_key || '').trim();
  if (!secureKeyEqual(suppliedKey, adminKey)) {
    const current = failure && failure.windowStartedAt + ADMIN_LOGIN_WINDOW_MS > now
      ? failure
      : { count: 0, windowStartedAt: now };
    adminLoginFailures.set(clientIp, { ...current, count: current.count + 1 });
    rejectAuth(req, res, 'Invalid management key');
    return;
  }

  adminLoginFailures.delete(clientIp);
  const token = crypto.randomBytes(32).toString('base64url');
  adminSessions.set(token, {
    expiresAt: now + ADMIN_SESSION_TTL_MS,
    keyDigest: adminKeyDigest(adminKey),
  });
  res.writeHead(200, {
    'Content-Type': 'application/json',
    'Cache-Control': 'no-store',
    'Set-Cookie': buildAdminSessionCookie(req, token, ADMIN_SESSION_TTL_MS / 1000),
  });
  res.end(JSON.stringify({ ok: true }));
}

function handleAdminLogout(req, res) {
  const token = getCookie(req, ADMIN_SESSION_COOKIE);
  if (token) adminSessions.delete(token);
  res.writeHead(200, {
    'Content-Type': 'application/json',
    'Cache-Control': 'no-store',
    'Set-Cookie': buildAdminSessionCookie(req, '', 0),
  });
  res.end(JSON.stringify({ ok: true }));
}

function rejectAuth(req, res, message = 'Invalid API key', type = 'auth_error') {
    res.writeHead(401, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    res.end(JSON.stringify({ error: { message, type } }));
    writeGatewayLog('request_complete', {
      requestId: res.getHeader('X-Request-Id') || '',
      method: req.method,
      url: req.url,
      clientIp: getClientIp(req),
      statusCode: 401,
      inputTokens: 0,
      outputTokens: 0,
      totalTokens: 0,
      errorMessage: message,
    });
    return false;
}

function adminAuth(req, res) {
  const adminKey = String(config.admin_key || '').trim();
  if (!isAdminRequestAuthenticated(req)) {
    return rejectAuth(req, res, 'Invalid management key');
  }
  req.clientApiKey = adminKey;
  req.clientApiKeyFingerprint = fingerprintKey(adminKey);
  req.clientApiKeyType = 'admin';
  req.clientAllowedChannels = [];
  req.clientAllowedModels = [];
  return true;
}

function clientAuth(req, res) {
  const token = getBearerToken(req);
  if (secureKeyEqual(token, String(config.admin_key || '').trim())) {
    return rejectAuth(req, res, 'Management key cannot access model APIs');
  }
  if (token === config.api_key) {
    req.clientApiKey = token;
    req.clientApiKeyFingerprint = fingerprintKey(token);
    req.clientApiKeyType = 'admin';
    req.clientAllowedChannels = [];
    req.clientAllowedModels = [];
    return true;
  }
  const clientKeyEntry = findClientKeyEntry(token);
  if (!clientKeyEntry) {
    return rejectAuth(req, res);
  }
  if (clientKeyEntry.enabled === false) {
    return rejectAuth(req, res, 'API key disabled', 'auth_error');
  }
  if (isClientKeyExpired(clientKeyEntry)) {
    return rejectAuth(req, res, 'API key expired', 'auth_error');
  }
  req.clientApiKey = token;
  req.clientApiKeyFingerprint = fingerprintKey(token);
  req.clientApiKeyType = 'generated';
  req.clientKeyName = clientKeyEntry.name;
  req.clientAllowedChannels = clientKeyEntry.allowed_channels || [];
  req.clientAllowedModels = clientKeyEntry.allowed_models || [];
  return true;
}


export {
  getClientKeyEntries,
  getClientKeyDashboardEntries,
  saveClientKeyEntries,
  findClientKeyEntry,
  isClientKeyExpired,
  clientCanUseChannel,
  clientCanUseModel,
  getAccessibleModelEntries,
  getModelQuotaCost,
  consumeClientQuota,
  getBearerToken,
  rejectAuth,
  isAdminRequestAuthenticated,
  handleAdminLogin,
  handleAdminLogout,
  adminAuth,
  clientAuth,
};
