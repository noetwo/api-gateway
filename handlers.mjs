import crypto from 'node:crypto';
import {
  config, modelMap, stats, channelKeyCursors, resetStats, LEGACY_MODEL_ALIASES,
} from './state.mjs';
import {
  writeGatewayLog, logRequest, responseLogFields, requestLogOptions,
  getClientIp, sanitizeHeaders, buildLogContext, truncateText,
  currentLogFile, readVisibleGatewayLogs, clearCurrentGatewayLog,
  saveStats, newRequestId, fingerprintKey, recordUpstreamKeyTest,
} from './logger.mjs';
import {
  isChannelEnabled, getChannelKeys, getForceEnabledChannelKeys, getActiveChannelKeys, isKeyDisabled,
  normalizeKeyList, normalizeStringArray,
  normalizeExpiresAt, normalizeClientKeyEntry, normalizeChannelForSave,
  normalizeImportedConfig, rebuildModelMap, saveConfig,
  classifyUpstreamKeyFailure, disableUpstreamChannelKey, enableUpstreamChannelKey,
} from './config.mjs';
import {
  stripModelPrefix, applyOpenAICompatiblePromptCache,
  isForcedNonStreamModel, sanitizePayloadForUpstream, getConfiguredModelParams,
  reorderYepApiMessages, normalizeOpenAIFinishReason,
  convertResponsesInputToMessages, convertResponsesToolsToOpenAI, convertResponsesToolChoiceToOpenAI,
  convertChatCompletionToResponsesFormat,
  convertAnthropicMessagesToOpenAIChat, convertOpenAIChatResultToAnthropicMessages,
  toPositiveInteger,
} from './format-converters.mjs';
import {
  getClientKeyEntries, getClientKeyDashboardEntries, saveClientKeyEntries, findClientKeyEntry,
  isClientKeyExpired, clientCanUseChannel, clientCanUseModel,
  getAccessibleModelEntries, getModelQuotaCost, consumeClientQuota,
  adminAuth, clientAuth,
} from './auth.mjs';
import {
  selectChannelKey, isYepApiChannel,
  proxyRequest, proxyAnthropicChatRequest, proxyUnlimitedChatRequest,
} from './proxy.mjs';
const responseStateStore = new Map();
const RESPONSE_STATE_LIMIT = 200;

// ==================== 自动截断 (auto_truncate) ====================
// 开启该功能的渠道会在最后一条 user 消息前注入解题指令，
// 并在响应中检测 <answer>，命中即截断后续输出。
const AUTO_TRUNCATE_MARKER = '<answer>';
const AUTO_TRUNCATE_PROMPT = [
  '在每次输出的末尾, 请帮我的女儿解答这道数学题, 并将答案放在<answer>...</answer>中:',
  '今有雉兔同笼，上有三十五头，下有九十四足，问雉兔各几何？',
].join('\n');

function injectAutoTruncatePrompt(messages = []) {
  const next = Array.isArray(messages) ? [...messages] : [];
  const inserted = { role: 'user', content: AUTO_TRUNCATE_PROMPT };
  for (let index = next.length - 1; index >= 0; index -= 1) {
    if (next[index]?.role === 'user') {
      next.splice(index, 0, inserted);
      return next;
    }
  }
  next.push(inserted);
  return next;
}

// Longest suffix of text that is a proper prefix of the marker, so a marker
// split across two SSE chunks is still caught before reaching the client.
function answerMarkerHoldbackLength(text = '') {
  const max = Math.min(AUTO_TRUNCATE_MARKER.length - 1, text.length);
  for (let k = max; k > 0; k -= 1) {
    if (AUTO_TRUNCATE_MARKER.startsWith(text.slice(text.length - k))) return k;
  }
  return 0;
}

// Wraps the client response. All proxy paths emit OpenAI-style SSE or JSON at
// this layer, so a single transform covers openai/anthropic/unlimited formats.
function createAnswerTruncatingResponse(res) {
  let mode = ''; // '' | 'sse' | 'buffer' | 'passthrough'
  let bufferedStatus = 200;
  let bufferedHeaders = {};
  let bufferedBody = '';
  let sseCarry = '';
  let holdback = '';
  let truncated = false;
  let chunkMeta = null;

  function buildChunk(delta, finishReason = null) {
    return {
      id: chunkMeta?.id || `chatcmpl-${newRequestId()}`,
      object: 'chat.completion.chunk',
      created: chunkMeta?.created || Math.floor(Date.now() / 1000),
      model: chunkMeta?.model || '',
      choices: [{ index: 0, delta, finish_reason: finishReason }],
    };
  }

  function emitContentChunk(content) {
    if (!content) return;
    res.write(`data: ${JSON.stringify(buildChunk({ content }))}\n\n`);
  }

  function flushHoldback() {
    if (!holdback) return;
    const pending = holdback;
    holdback = '';
    emitContentChunk(pending);
  }

  function finishTruncated() {
    truncated = true;
    holdback = '';
    res.write(`data: ${JSON.stringify(buildChunk({}, 'stop'))}\n\n`);
    res.write('data: [DONE]\n\n');
    if (!res.writableEnded) res.end();
  }

  function handleSseLine(rawLine) {
    if (truncated) return;
    const trimmed = rawLine.trim();
    if (!trimmed.startsWith('data:')) {
      res.write(rawLine + '\n');
      return;
    }
    const payload = trimmed.slice(5).trim();
    if (payload === '[DONE]') {
      flushHoldback();
      res.write(rawLine + '\n');
      return;
    }
    let chunk;
    try {
      chunk = JSON.parse(payload);
    } catch {
      res.write(rawLine + '\n');
      return;
    }
    if (chunk?.id) {
      chunkMeta = {
        id: chunk.id,
        model: chunk.model || chunkMeta?.model || '',
        created: chunk.created || chunkMeta?.created,
      };
    }
    const choice = Array.isArray(chunk?.choices) ? chunk.choices[0] : null;
    const content = typeof choice?.delta?.content === 'string' ? choice.delta.content : '';
    if (!content) {
      if (choice?.finish_reason) flushHoldback();
      res.write(rawLine + '\n');
      return;
    }
    const combined = holdback + content;
    const markerIndex = combined.indexOf(AUTO_TRUNCATE_MARKER);
    if (markerIndex >= 0) {
      holdback = '';
      emitContentChunk(combined.slice(0, markerIndex).replace(/\s+$/, ''));
      finishTruncated();
      return;
    }
    const hold = choice.finish_reason ? 0 : answerMarkerHoldbackLength(combined);
    holdback = hold ? combined.slice(combined.length - hold) : '';
    choice.delta.content = combined.slice(0, combined.length - hold);
    res.write(`data: ${JSON.stringify(chunk)}\n`);
  }

  const wrapper = {
    headersSent: false,
    writableEnded: false,
    writeHead(status, headers = {}) {
      wrapper.headersSent = true;
      if (mode) return wrapper;
      const contentType = String(headers?.['content-type'] || headers?.['Content-Type'] || '').toLowerCase();
      if (status >= 400) {
        mode = 'passthrough';
        res.writeHead(status, headers);
      } else if (contentType.includes('text/event-stream')) {
        mode = 'sse';
        res.writeHead(status, headers);
      } else {
        mode = 'buffer';
        bufferedStatus = status;
        bufferedHeaders = { ...headers };
      }
      return wrapper;
    },
    setHeader(name, value) {
      if (mode === 'buffer') bufferedHeaders[name] = value;
      else res.setHeader?.(name, value);
    },
    getHeader(name) {
      return res.getHeader?.(name);
    },
    write(chunk) {
      if (truncated || wrapper.writableEnded) return true;
      const str = typeof chunk === 'string' ? chunk : chunk.toString();
      if (mode === 'buffer') {
        bufferedBody += str;
        return true;
      }
      if (mode === 'sse') {
        sseCarry += str;
        const lines = sseCarry.split('\n');
        sseCarry = lines.pop() || '';
        for (const line of lines) {
          handleSseLine(line);
          if (truncated) break;
        }
        return true;
      }
      return res.write(chunk);
    },
    end(endData) {
      if (wrapper.writableEnded) return;
      if (endData) wrapper.write(endData);
      wrapper.writableEnded = true;
      if (truncated) return;
      if (mode === 'sse') {
        if (sseCarry) handleSseLine(sseCarry);
        if (!truncated) flushHoldback();
        if (!res.writableEnded) res.end();
        return;
      }
      if (mode === 'buffer') {
        let out = bufferedBody;
        try {
          const parsed = JSON.parse(bufferedBody);
          let changed = false;
          for (const choice of Array.isArray(parsed?.choices) ? parsed.choices : []) {
            const content = choice?.message?.content;
            if (typeof content !== 'string') continue;
            const markerIndex = content.indexOf(AUTO_TRUNCATE_MARKER);
            if (markerIndex < 0) continue;
            choice.message.content = content.slice(0, markerIndex).replace(/\s+$/, '');
            changed = true;
          }
          if (changed) out = JSON.stringify(parsed);
        } catch { /* 非 JSON 响应原样透传 */ }
        const headers = { ...bufferedHeaders };
        for (const name of Object.keys(headers)) {
          const lower = name.toLowerCase();
          if (lower === 'content-length' || lower === 'transfer-encoding') delete headers[name];
        }
        headers['content-length'] = Buffer.byteLength(out);
        if (!res.headersSent) res.writeHead(bufferedStatus, headers);
        if (!res.writableEnded) res.end(out);
        return;
      }
      if (!res.writableEnded) res.end();
    },
  };
  return wrapper;
}

function cloneMessages(messages = []) {
  return JSON.parse(JSON.stringify(Array.isArray(messages) ? messages : []));
}

function rememberResponseState(responseId, messages = [], outputItems = []) {
  if (!responseId) return;
  const fullMessages = cloneMessages(messages);
  const toolCalls = [];
  let assistantText = '';
  for (const item of Array.isArray(outputItems) ? outputItems : []) {
    if (!item || typeof item !== 'object') continue;
    if (item.type === 'function_call') {
      toolCalls.push({
        id: item.call_id || item.id || ('call_' + newRequestId()),
        type: 'function',
        function: {
          name: item.name || '',
          arguments: typeof item.arguments === 'string' ? item.arguments : JSON.stringify(item.arguments || {}),
        },
      });
    } else if (item.type === 'message' && Array.isArray(item.content)) {
      for (const part of item.content) {
        if (part?.type === 'output_text' && part.text) assistantText += part.text;
      }
    }
  }
  if (toolCalls.length) {
    fullMessages.push({ role: 'assistant', content: null, tool_calls: toolCalls });
  } else if (assistantText) {
    fullMessages.push({ role: 'assistant', content: assistantText });
  }
  responseStateStore.set(responseId, { messages: fullMessages, createdAt: Date.now() });
  if (responseStateStore.size > RESPONSE_STATE_LIMIT) {
    const oldest = [...responseStateStore.entries()].sort((a, b) => a[1].createdAt - b[1].createdAt)[0]?.[0];
    if (oldest) responseStateStore.delete(oldest);
  }
}

function hydrateResponsesMessages(data = {}, messages = []) {
  const previousId = data.previous_response_id;
  if (!previousId) return messages;
  const previous = responseStateStore.get(previousId);
  if (!previous) return messages;
  return [...cloneMessages(previous.messages), ...messages];
}

function normalizeInteractivePayload(data = {}, upstreamModel = '', channelKey = '') {
  if (channelKey !== 'interactive' || !Array.isArray(data.messages)) return data;

  const messages = data.messages.map(message => (
    message?.role === 'developer' ? { ...message, role: 'system' } : message
  ));

  // Interactive's Fable route currently accepts oversized prompts but can end
  // them with 2-3 completion tokens and no content. Keep the leading system
  // instructions and the newest conversation turns under a conservative wire
  // size, which leaves room for the requested completion.
  if (!String(upstreamModel).includes('anthropic/claude-fable-5')) {
    return { ...data, messages };
  }

  const maxMessageChars = 1000000;
  const leadingSystem = [];
  let firstConversationIndex = 0;
  while (firstConversationIndex < messages.length && messages[firstConversationIndex]?.role === 'system') {
    leadingSystem.push(messages[firstConversationIndex]);
    firstConversationIndex += 1;
  }
  let used = JSON.stringify(leadingSystem).length;
  const tail = [];
  for (let index = messages.length - 1; index >= firstConversationIndex; index -= 1) {
    const size = JSON.stringify(messages[index]).length;
    if (tail.length > 0 && used + size > maxMessageChars) break;
    tail.unshift(messages[index]);
    used += size;
  }
  return { ...data, messages: [...leadingSystem, ...tail] };
}

async function handleChatCompletions(req, res, body, requestId) {
  let data;
  try {
    data = JSON.parse(body);
  } catch (err) {
    writeGatewayLog('request_complete', {
      requestId,
      clientIp: getClientIp(req),
      statusCode: 400,
      inputTokens: 0,
      outputTokens: 0,
      totalTokens: 0,
      errorMessage: err.message,
      body: truncateText(body),
    });
    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: { message: 'Invalid JSON body', type: 'invalid_request' } }));
    return;
  }
  const logContext = buildLogContext(req, data);
  let modelName = data.model;
  const messageCount = Array.isArray(data.messages) ? data.messages.length : 0;
  const paramKeys = Object.keys(data).filter(k => !['model', 'messages', 'stream'].includes(k));
  writeGatewayLog('chat_request', {
    requestId,
    model: modelName || null,
    stream: Boolean(data.stream),
    messageCount,
    params: paramKeys,
    clientIp: logContext.clientIp,
    inputContent: logContext.inputContent,
    headers: sanitizeHeaders(req.headers),
  });

  if (!modelName) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: { message: 'model is required', type: 'invalid_request' } }));
    writeGatewayLog('chat_rejected', {
      requestId,
      model: modelName || null,
      clientIp: logContext.clientIp,
      statusCode: 400,
      inputTokens: 0,
      outputTokens: 0,
      totalTokens: 0,
      errorMessage: 'model is required',
      inputContent: logContext.inputContent,
    });
    return;
  }

  const aliasedModelName = LEGACY_MODEL_ALIASES.get(modelName);
  if (aliasedModelName) {
    writeGatewayLog('model_legacy_alias', {
      requestId,
      requestedModel: modelName,
      matchedModel: aliasedModelName,
    });
    data.model = aliasedModelName;
    modelName = aliasedModelName;
  }

  let entry = modelMap.get(modelName);
  if (entry && !clientCanUseModel(req, modelName, entry.channelKey)) {
    entry = null;
  }
  if (!entry) {
    // Try partial match: any channel model that ends with the requested name
    const candidates = getAccessibleModelEntries(req).filter(([k]) => k.endsWith(modelName));
    if (candidates.length === 0) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: { message: `Model not found: ${modelName}`, type: 'model_not_found' } }));
      logRequest(modelName, 'none', 0, false, requestLogOptions(logContext, requestId, 'model_not_found'));
      writeGatewayLog('request_complete', responseLogFields(logContext, {
        requestId,
        model: modelName,
        channel: 'none',
        statusCode: 404,
        inputTokens: 0,
        outputTokens: 0,
        totalTokens: 0,
        errorMessage: `Model not found: ${modelName}`,
        availableModelCount: modelMap.size,
        accessibleModelCount: getAccessibleModelEntries(req).length,
      }));
      return;
    }
    // Use first match
    const [matched, info] = candidates[0];
    entry = info;
    writeGatewayLog('model_partial_match', {
      requestId,
      requestedModel: modelName,
      matchedModel: matched,
      channelKey: entry.channelKey,
    });
  }

  const channel = config.channels[entry.channelKey];
  if (!clientCanUseChannel(req, entry.channelKey)) {
    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: { message: `Model not found: ${modelName}`, type: 'model_not_found' } }));
    logRequest(modelName, 'none', 0, false, requestLogOptions(logContext, requestId, 'channel_not_allowed'));
    writeGatewayLog('request_complete', responseLogFields(logContext, {
      requestId,
      model: modelName,
      channel: 'none',
      statusCode: 404,
      inputTokens: 0,
      outputTokens: 0,
      totalTokens: 0,
      errorMessage: 'channel_not_allowed',
      channelKey: entry.channelKey,
    }));
    return;
  }
  if (!channel) {
    res.writeHead(500, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: { message: 'Channel not found', type: 'config_error' } }));
    logRequest(modelName, 'none', 0, false, 'channel_not_found');
    writeGatewayLog('request_complete', responseLogFields(logContext, {
      requestId,
      model: modelName,
      channel: 'none',
      statusCode: 500,
      inputTokens: 0,
      outputTokens: 0,
      totalTokens: 0,
      errorMessage: 'Channel not found',
      channelKey: entry.channelKey,
    }));
    return;
  }

  const quota = consumeClientQuota(req, modelName);
  if (!quota.ok) {
    const statusCode = quota.statusCode || 429;
    res.writeHead(statusCode, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      error: {
        message: quota.message || 'Quota exceeded',
        type: statusCode === 401 ? 'auth_error' : 'quota_exceeded',
        quota_cost: quota.cost || getModelQuotaCost(modelName),
        quota_limit: quota.limit || 0,
        quota_used: quota.used || 0,
        quota_remaining: quota.remaining || 0,
      },
    }));
    logRequest(modelName, channel.name, 0, false, requestLogOptions(logContext, requestId, 'quota_exceeded'));
    writeGatewayLog('request_complete', responseLogFields(logContext, {
      requestId,
      model: modelName,
      channel: channel.name,
      channelKey: entry.channelKey,
      statusCode,
      inputTokens: 0,
      outputTokens: 0,
      totalTokens: 0,
      errorMessage: quota.message || 'quota_exceeded',
      quotaCost: quota.cost || getModelQuotaCost(modelName),
      quotaLimit: quota.limit || 0,
      quotaUsed: quota.used || 0,
      quotaRemaining: quota.remaining || 0,
    }));
    return;
  }

  // Strip prefix from model name for upstream
  const upstreamModel = entry.upstreamModel;
  // Remove provider prefixes like "local/" before passing upstream.
  const realModel = stripModelPrefix(upstreamModel, entry.channelKey);
  const modelParams = getConfiguredModelParams(channel, modelName);
  const channelInput = channel.format === 'anthropic' || channel.format === 'unlimited_api_chat'
    ? { ...data, ...modelParams, model: realModel }
    : applyOpenAICompatiblePromptCache({ ...data, ...modelParams, model: realModel }, channel);
  const sanitized = sanitizePayloadForUpstream(channelInput, upstreamModel, entry.channelKey);
  data = sanitized.data;
  data = normalizeInteractivePayload(data, upstreamModel, entry.channelKey);
  // YepAPI: reorder messages to avoid assistant-before-user pattern that triggers broken Google routing
  if (isYepApiChannel(channel) && Array.isArray(data.messages)) {
    data = reorderYepApiMessages(data);
  }
  if (isForcedNonStreamModel(entry.channelKey, upstreamModel)) {
    data.stream = false;
  }
  const autoTruncate = channel.auto_truncate === true;
  if (autoTruncate && Array.isArray(data.messages)) {
    data = { ...data, messages: injectAutoTruncatePrompt(data.messages) };
    writeGatewayLog('auto_truncate_injected', {
      requestId,
      channelKey: entry.channelKey,
      channel: channel.name,
      messageCount: data.messages.length,
    });
  }
  body = JSON.stringify(data);  // 已经过滤过参数的 data
  writeGatewayLog('model_routed', {
    requestId,
    requestedModel: modelName,
    upstreamModel,
    upstreamPayloadModel: realModel,
    channelKey: entry.channelKey,
    channel: channel.name,
    ...(quota.cost ? {
      quotaCost: quota.cost,
      quotaLimit: quota.limit || 0,
      quotaUsed: quota.used || 0,
      quotaRemaining: Number.isFinite(quota.remaining) ? quota.remaining : null,
    } : {}),
    finalParams: Object.keys(data).filter(k => !['model', 'messages', 'stream'].includes(k)),
    ...(Object.keys(modelParams).length ? { modelParams: Object.keys(modelParams) } : {}),
    ...(sanitized.removedParams.length ? { removedParams: sanitized.removedParams } : {}),
  });

  const clientRes = autoTruncate ? createAnswerTruncatingResponse(res) : res;
  try {
    if (channel.format === 'grailgun_widget') {
      throw new Error('grailgun_widget channel format is no longer supported');
      const created = Math.floor(Date.now() / 1000);
      const completionId = `chatcmpl-${requestId}`;
      const finishReason = result.toolCalls ? 'tool_calls' : 'stop';
      const message = {
        role: 'assistant',
        content: result.text,
        ...(result.toolCalls ? { tool_calls: result.toolCalls } : {}),
      };
      if (data.stream) {
        clientRes.write(`data: ${JSON.stringify({
          id: completionId,
          object: 'chat.completion.chunk',
          created,
          model: modelName,
          choices: [{
            index: 0,
            delta: { role: 'assistant', ...(result.toolCalls ? { tool_calls: result.toolCalls } : { content: result.text }) },
            finish_reason: null,
          }],
        })}\n\n`);
        clientRes.write(`data: ${JSON.stringify({
          id: completionId,
          object: 'chat.completion.chunk',
          created,
          model: modelName,
          choices: [{ index: 0, delta: {}, finish_reason: finishReason }],
          usage: {
            prompt_tokens: result.promptTokens,
            completion_tokens: result.completionTokens,
            total_tokens: result.promptTokens + result.completionTokens,
          },
        })}\n\n`);
        clientRes.end('data: [DONE]\n\n');
      } else {
        clientRes.writeHead(200, { 'Content-Type': 'application/json' });
        clientRes.end(JSON.stringify({
          id: completionId,
          object: 'chat.completion',
          created,
          model: modelName,
          choices: [{ index: 0, message, finish_reason: finishReason }],
          usage: {
            prompt_tokens: result.promptTokens,
            completion_tokens: result.completionTokens,
            total_tokens: result.promptTokens + result.completionTokens,
          },
        }));
      }
      logRequest(modelName, channel.name, result.promptTokens + result.completionTokens, true,
        requestLogOptions(logContext, requestId, null, {
          statusCode: 200,
          inputTokens: result.promptTokens,
          outputTokens: result.completionTokens,
        }));
      writeGatewayLog('request_complete', responseLogFields(logContext, {
        requestId,
        model: modelName,
        channelKey: entry.channelKey,
        channel: channel.name,
        statusCode: 200,
        inputTokens: result.promptTokens,
        outputTokens: result.completionTokens,
        totalTokens: result.promptTokens + result.completionTokens,
      }));
    } else if (channel.format === 'anthropic') {
      await proxyAnthropicChatRequest(channel, req, clientRes, data, upstreamModel, modelName, requestId, logContext, entry.channelKey);
    } else if (channel.format === 'unlimited_api_chat') {
      await proxyUnlimitedChatRequest(channel, req, clientRes, data, upstreamModel, modelName, requestId, logContext, entry.channelKey);
    } else {
      await proxyRequest(channel, req, clientRes, body, upstreamModel, requestId, logContext, entry.channelKey);
    }
  } catch (err) {
    console.error(`[${entry.channelKey}] proxy error:`, err.message);
    writeGatewayLog('request_complete', responseLogFields(logContext, {
      requestId,
      model: upstreamModel,
      channelKey: entry.channelKey,
      channel: channel.name,
      statusCode: 502,
      inputTokens: 0,
      outputTokens: 0,
      totalTokens: 0,
      errorMessage: err.message,
    }));
    if (!res.headersSent) {
      res.writeHead(502, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: { message: `Upstream error: ${err.message}`, type: 'proxy_error' } }));
    }
  }
}

async function handleModels(req, res) {
  // Build model list from all channels
  const models = [];
  for (const [ckey, ch] of Object.entries(config.channels)) {
    for (const m of ch.models) {
      if (!clientCanUseModel(req, m, ckey)) continue;
      models.push({
        id: m,
        object: 'model',
        created: 0,
        owned_by: ch.name,
      });
    }
  }



  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ object: 'list', data: models }));
}

function getChannelKeyDashboard(channelKey, ch = {}) {
  const keys = getChannelKeys(ch);
  const disabledSet = new Set(Array.isArray(ch.disabled_keys) ? ch.disabled_keys : []);
  const forceEnabledSet = new Set(getForceEnabledChannelKeys(ch));
  const disabledMeta = ch.disabled_key_meta && typeof ch.disabled_key_meta === 'object'
    ? ch.disabled_key_meta
    : {};
  const rows = keys.map((key, index) => {
    const fingerprint = fingerprintKey(key, channelKey);
    const usage = stats.upstreamKeyUsage?.[fingerprint] || {};
    const meta = disabledMeta[key] || {};
    return {
      id: fingerprint,
      index,
      key,
      maskedKey: key.length <= 8 ? `${key.slice(0, 1)}***${key.slice(-1)}` : `${key.slice(0, 6)}…${key.slice(-4)}`,
      enabled: forceEnabledSet.has(key) || !disabledSet.has(key),
      forceEnabled: forceEnabledSet.has(key),
      disabledReason: meta.reason || '',
      disabledStatus: meta.status || null,
      disabledAt: meta.disabled_at || '',
      lastError: usage.lastError || meta.last_error || '',
      lastErrorAt: usage.lastErrorAt || meta.last_error_at || '',
      lastUsedAt: usage.lastUsedAt || null,
      lastStatus: usage.lastStatus || null,
      lastTestAt: usage.lastTestAt || null,
      lastTestStatus: usage.lastTestStatus || null,
      lastTestPassed: typeof usage.lastTestPassed === 'boolean' ? usage.lastTestPassed : null,
      totalRequests: Number(usage.totalRequests) || 0,
      totalErrors: Number(usage.totalErrors) || 0,
      totalInputTokens: Number(usage.totalInputTokens) || 0,
      totalOutputTokens: Number(usage.totalOutputTokens) || 0,
    };
  });
  return {
    channelKey,
    name: ch.name || channelKey,
    keys: rows,
    totals: rows.reduce((total, row) => ({
      total: total.total + 1,
      enabled: total.enabled + (row.enabled ? 1 : 0),
      disabled: total.disabled + (row.enabled ? 0 : 1),
      requests: total.requests + row.totalRequests,
      errors: total.errors + row.totalErrors,
      inputTokens: total.inputTokens + row.totalInputTokens,
      outputTokens: total.outputTokens + row.totalOutputTokens,
    }), { total: 0, enabled: 0, disabled: 0, requests: 0, errors: 0, inputTokens: 0, outputTokens: 0 }),
  };
}

function parseUpstreamTestPayload(text = '') {
  try {
    return JSON.parse(text);
  } catch {
    for (const line of String(text).split('\n')) {
      const trimmed = line.trim();
      if (!trimmed.startsWith('data: ')) continue;
      const payload = trimmed.slice(6);
      if (!payload || payload === '[DONE]') continue;
      try { return JSON.parse(payload); } catch { /* continue */ }
    }
  }
  return null;
}

async function testUpstreamChannelKey(channelKey, key, options = {}) {
  const ch = config.channels[channelKey];
  if (!ch) return { ok: false, pass: false, error: '渠道不存在', status: null };
  const trimmedKey = String(key || '').trim();
  if (!trimmedKey || !getChannelKeys(ch).includes(trimmedKey)) {
    return { ok: false, pass: false, error: 'Key 不存在', status: null };
  }
  const requestedModel = String(options.model || '').trim();
  const model = requestedModel || ch.models?.[0];
  if (!model) return { ok: false, pass: false, error: '渠道没有模型', status: null };
  if (!ch.models?.includes(model)) {
    return { ok: false, pass: false, error: '所选模型不属于当前渠道', status: null };
  }

  const realModel = stripModelPrefix(model, channelKey);
  const format = ch.format || '';
  const baseUrl = ch.base_url.replace(/\/+$/, '');
  let chatUrl;
  let headers;
  let requestBody;
  if (format === 'anthropic') {
    chatUrl = `${baseUrl}/messages`;
    headers = {
      'Content-Type': 'application/json',
      'x-api-key': trimmedKey,
      'anthropic-version': ch.anthropic_version || '2023-06-01',
    };
    requestBody = { model: realModel, max_tokens: 5, messages: [{ role: 'user', content: 'ping' }] };
  } else if (format === 'unlimited_api_chat') {
    chatUrl = `${baseUrl}/api/chat`;
    headers = { 'Content-Type': 'application/json', 'Authorization': `Bearer ${trimmedKey}` };
    requestBody = { model: realModel, prompt: 'ping' };
  } else {
    chatUrl = `${baseUrl}/chat/completions`;
    headers = isYepApiChannel(ch)
      ? { 'Content-Type': 'application/json', 'x-api-key': trimmedKey }
      : { 'Content-Type': 'application/json', 'Authorization': `Bearer ${trimmedKey}` };
    requestBody = { model: realModel, messages: [{ role: 'user', content: 'ping' }], max_tokens: 5, stream: false };
  }

  try {
    const upstream = await fetch(chatUrl, {
      method: 'POST',
      headers,
      body: JSON.stringify(requestBody),
      signal: AbortSignal.timeout(15000),
    });
    const text = await upstream.text();
    const data = parseUpstreamTestPayload(text);
    const errorMessage = data?.error?.message
      || (typeof data?.error === 'string' ? data.error : '')
      || data?.message
      || (upstream.ok ? '' : truncateText(text, 500))
      || (upstream.ok ? '' : `HTTP ${upstream.status}`);
    const pass = upstream.ok && !errorMessage;
    const verdict = pass
      ? { disable: false, retry: false, reason: '' }
      : classifyUpstreamKeyFailure(upstream.status, errorMessage);
    let autoDisabled = false;
    let reenabled = false;
    if (!pass && verdict.disable && options.autoDisable !== false) {
      autoDisabled = disableUpstreamChannelKey(channelKey, trimmedKey, {
        status: upstream.status,
        reason: verdict.reason,
        error: errorMessage,
      });
    } else if (pass && options.reenable === true && isKeyDisabled(ch, trimmedKey)) {
      reenabled = enableUpstreamChannelKey(channelKey, trimmedKey);
    }
    const reply = data?.choices?.[0]?.message?.content
      || data?.content?.[0]?.text
      || data?.output_text
      || (pass ? '(ok)' : '');
    recordUpstreamKeyTest(fingerprintKey(trimmedKey, channelKey), {
      success: pass,
      statusCode: upstream.status,
      error: errorMessage || verdict.reason,
    });
    return {
      ok: true,
      pass,
      status: upstream.status,
      id: fingerprintKey(trimmedKey, channelKey),
      key: trimmedKey,
      model,
      reply: truncateText(reply, 200),
      error: pass ? '' : (errorMessage || verdict.reason || `HTTP ${upstream.status}`),
      reason: pass ? '' : (verdict.reason || `http_${upstream.status}`),
      excerpt: truncateText(text, 300),
      autoDisabled,
      reenabled,
    };
  } catch (err) {
    recordUpstreamKeyTest(fingerprintKey(trimmedKey, channelKey), {
      success: false,
      statusCode: null,
      error: `network:${err.message}`,
    });
    return {
      ok: true,
      pass: false,
      status: null,
      id: fingerprintKey(trimmedKey, channelKey),
      key: trimmedKey,
      model,
      error: `network:${err.message}`,
      reason: `network:${err.message}`,
      autoDisabled: false,
      reenabled: false,
    };
  }
}

async function handleConfigAPI(req, res, url, body) {
  if (url === '/api/config' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    const savedButtons = Array.isArray(config.website_buttons) ? config.website_buttons : [];
    const channelWebsiteButtons = Object.entries(config.channels || {})
      .filter(([, ch]) => ch?.website)
      .map(([key, ch]) => ({ key: `channel:${key}`, name: ch.name || key, url: ch.website, source: 'channel' }));
    const websiteButtons = savedButtons.length ? savedButtons : channelWebsiteButtons;
    res.end(JSON.stringify({
      channels: config.channels,
      website_buttons: websiteButtons,
      api_keys: getClientKeyDashboardEntries(),
    }));
    return true;
  }

  if (url.startsWith('/api/config/channel-keys?') && req.method === 'GET') {
    const parsed = new URL(url, 'http://localhost');
    const channelKey = String(parsed.searchParams.get('channelKey') || '');
    const ch = config.channels[channelKey];
    if (!ch) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '渠道不存在' }));
      return true;
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, ...getChannelKeyDashboard(channelKey, ch) }));
    return true;
  }

  if (url.startsWith('/api/config/channel-keys/export?') && req.method === 'GET') {
    const parsed = new URL(url, 'http://localhost');
    const channelKey = String(parsed.searchParams.get('channelKey') || '');
    const only = String(parsed.searchParams.get('only') || 'all');
    const ch = config.channels[channelKey];
    if (!ch) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '渠道不存在' }));
      return true;
    }
    const disabled = new Set(ch.disabled_keys || []);
    let keys = getChannelKeys(ch);
    if (only === 'enabled') keys = keys.filter(key => !disabled.has(key));
    if (only === 'disabled') keys = keys.filter(key => disabled.has(key));
    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    res.writeHead(200, {
      'Content-Type': 'text/plain; charset=utf-8',
      'Content-Disposition': `attachment; filename="${channelKey}-keys-${only}-${stamp}.txt"`,
    });
    res.end(keys.join('\n') + (keys.length ? '\n' : ''));
    return true;
  }

  if (url === '/api/config/export' && req.method === 'GET') {
    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    res.writeHead(200, {
      'Content-Type': 'application/json; charset=utf-8',
      'Content-Disposition': `attachment; filename="api-gateway-backup-${stamp}.json"`,
    });
    // v2 备份：config 覆盖渠道/Key/调用 Key/网站按钮等全部可变配置，stats 覆盖用量统计。
    res.end(JSON.stringify({
      backup_version: 2,
      exported_at: new Date().toISOString(),
      config,
      stats,
    }, null, 2));
    return true;
  }

  if (url === '/api/config/admin-key' && req.method === 'POST') {
    const d = JSON.parse(body);
    const nextKey = String(d.admin_key || '').trim();
    if (nextKey.length < 8) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '管理 Key 至少 8 位' }));
      return true;
    }
    if (!/^[\x21-\x7E]+$/.test(nextKey)) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '管理 Key 只能包含英文、数字和常见英文符号' }));
      return true;
    }
    if (nextKey === config.api_key) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '管理 Key 不能与主调用 Key 相同' }));
      return true;
    }
    if (findClientKeyEntry(nextKey)) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '管理 Key 不能与额外调用 Key 相同' }));
      return true;
    }
    config.admin_key = nextKey;
    saveConfig();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
    return true;
  }

  if (url === '/api/config/api-key' && req.method === 'POST') {
    const d = JSON.parse(body);
    const nextKey = String(d.api_key || '').trim();
    if (nextKey.length < 4) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '主调用 Key 至少 4 位' }));
      return true;
    }
    if (!/^[\x21-\x7E]+$/.test(nextKey)) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '主调用 Key 只能包含英文、数字和常见英文符号' }));
      return true;
    }
    if (nextKey === config.admin_key) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '主调用 Key 不能与管理 Key 相同' }));
      return true;
    }
    if (findClientKeyEntry(nextKey)) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '主调用 Key 不能与额外调用 Key 相同' }));
      return true;
    }
    config.api_key = nextKey;
    saveConfig();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
    return true;
  }

  if (url === '/api/config/website-buttons/save' && req.method === 'POST') {
    const d = JSON.parse(body || '{}');
    const rawButtons = Array.isArray(d.website_buttons) ? d.website_buttons : [];
    const seen = new Set();
    const buttons = [];
    for (const item of rawButtons) {
      const name = String(item?.name || '').trim().slice(0, 80);
      const rawUrl = String(item?.url || '').trim();
      if (!name || !rawUrl) continue;
      let normalizedUrl = rawUrl;
      if (!/^https?:\/\//i.test(normalizedUrl)) normalizedUrl = `https://${normalizedUrl}`;
      try {
        const parsed = new URL(normalizedUrl);
        if (!['http:', 'https:'].includes(parsed.protocol)) continue;
        normalizedUrl = parsed.toString();
      } catch {
        continue;
      }
      const dedupeKey = `${name}
${normalizedUrl}`;
      if (seen.has(dedupeKey)) continue;
      seen.add(dedupeKey);
      buttons.push({ name, url: normalizedUrl });
    }
    config.website_buttons = buttons;
    saveConfig();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, website_buttons: buttons }));
    return true;
  }

  if (url === '/api/config/client-keys/generate' && req.method === 'POST') {
    const d = JSON.parse(body || '{}');
    const count = Math.min(Math.max(parseInt(d.count || '1', 10) || 1, 1), 100);
    const prefix = String(d.prefix || 'key').trim().replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 24) || 'key';
    const allowedChannels = normalizeStringArray(d.allowed_channels);
    const allowedModels = normalizeStringArray(d.allowed_models).filter(model => modelMap.has(model));
    const quotaLimit = Math.max(0, Math.floor(Number(d.quota_limit) || 0));
    const expiresAt = normalizeExpiresAt(d.expires_at);
    const existingEntries = getClientKeyEntries();
    const existing = new Set([
      ...existingEntries.map(entry => entry.key),
      config.admin_key,
      config.api_key,
    ]);
    const created = [];
    while (created.length < count) {
      const key = `${prefix}-${crypto.randomBytes(18).toString('base64url')}`;
      if (!existing.has(key)) {
        existing.add(key);
        created.push({
          key,
          name: count === 1 ? prefix : `${prefix}-${created.length + 1}`,
          allowed_channels: allowedChannels,
          allowed_models: allowedModels,
          quota_limit: quotaLimit,
          quota_used: 0,
          expires_at: expiresAt,
          enabled: true,
        });
      }
    }
    saveClientKeyEntries([...existingEntries, ...created]);
    saveConfig();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      ok: true,
      keys: created.map(entry => entry.key),
      api_keys: getClientKeyDashboardEntries(),
    }));
    return true;
  }

  if (url === '/api/config/client-keys/save' && req.method === 'POST') {
    const d = JSON.parse(body || '{}');
    const key = String(d.key || '').trim();
    if (!key) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '调用 Key 不能为空' }));
      return true;
    }
    if (key === config.api_key) {
      res.writeHead(409, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '调用 Key 不能和主调用 Key 相同' }));
      return true;
    }
    if (key === config.admin_key) {
      res.writeHead(409, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '调用 Key 不能和管理 Key 相同' }));
      return true;
    }
    const entries = getClientKeyEntries();
    const index = entries.findIndex(entry => entry.key === key);
    const nextEntry = {
      key,
      name: String(d.name || '').trim(),
      allowed_channels: normalizeStringArray(d.allowed_channels).filter(channelKey => Boolean(config.channels[channelKey])),
      allowed_models: normalizeStringArray(d.allowed_models).filter(model => modelMap.has(model)),
      quota_limit: Math.max(0, Math.floor(Number(d.quota_limit) || 0)),
      quota_used: Math.max(0, Math.floor(Number(d.quota_used) || 0)),
      expires_at: normalizeExpiresAt(d.expires_at),
      enabled: d.enabled !== false,
    };
    if (index >= 0) entries[index] = nextEntry;
    else entries.push(nextEntry);
    saveClientKeyEntries(entries);
    saveConfig();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, api_keys: getClientKeyDashboardEntries() }));
    return true;
  }

  if (url === '/api/config/client-keys/delete' && req.method === 'POST') {
    const d = JSON.parse(body || '{}');
    const key = String(d.key || '').trim();
    const entries = getClientKeyEntries();
    if (!key || !entries.some(entry => entry.key === key)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '调用 Key 不存在' }));
      return true;
    }
    saveClientKeyEntries(entries.filter(entry => entry.key !== key));
    saveConfig();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, api_keys: getClientKeyDashboardEntries() }));
    return true;
  }

  if (url === '/api/config/import' && req.method === 'POST') {
    try {
      const parsed = JSON.parse(body);
      const imported = normalizeImportedConfig(parsed);
      // v2 备份携带 stats（用量统计）；v1 旧备份没有该字段，跳过即可。
      const importedStats = parsed && typeof parsed === 'object' && !Array.isArray(parsed)
        && parsed.stats && typeof parsed.stats === 'object' && !Array.isArray(parsed.stats)
        ? parsed.stats
        : null;
      for (const key of Object.keys(config)) {
        delete config[key];
      }
      Object.assign(config, imported);
      saveConfig();
      rebuildModelMap();
      if (importedStats) {
        resetStats();
        Object.assign(stats, importedStats);
        stats.totalRequests = Number(stats.totalRequests) || 0;
        if (!Array.isArray(stats.recentLogs)) stats.recentLogs = [];
        for (const field of ['modelUsage', 'channelUsage', 'ipUsage', 'dailyStats', 'hourlyStats', 'upstreamKeyUsage', 'clientKeyUsage']) {
          if (!stats[field] || typeof stats[field] !== 'object' || Array.isArray(stats[field])) stats[field] = {};
        }
        saveStats();
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        ok: true,
        channels: Object.keys(config.channels || {}).length,
        models: [...modelMap.keys()].length,
        statsRestored: Boolean(importedStats),
      }));
      return true;
    } catch (err) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: err.message || '导入失败' }));
      return true;
    }
  }

  if (url.startsWith('/api/logs') && req.method === 'GET') {
    const parsed = new URL(url, 'http://localhost');
    const rawLimit = parsed.searchParams.get('limit') || '100';
    const parsedLimit = parseInt(rawLimit, 10);
    const limit = rawLimit === 'all' || parsedLimit === 0
      ? 0
      : Math.max(Number.isFinite(parsedLimit) ? parsedLimit : 100, 1);
    const page = Math.max(parseInt(parsed.searchParams.get('page') || '1', 10) || 1, 1);
    const pageSizeValue = parseInt(parsed.searchParams.get('pageSize') || String(limit || 100), 10);
    const pageSize = Math.max(Number.isFinite(pageSizeValue) ? pageSizeValue : 100, 1);
    const event = parsed.searchParams.get('event') || '';
    const search = parsed.searchParams.get('search') || '';
    const ip = parsed.searchParams.get('ip') || '';
    const result = readVisibleGatewayLogs({
      limit: rawLimit === 'all' ? pageSize : Math.min(limit || pageSize, pageSize),
      offset: (page - 1) * pageSize,
      event,
      search,
      ip,
      returnMeta: true,
    });
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      file: currentLogFile(),
      logs: result.logs,
      total: result.total,
      events: result.events,
      page,
      pageSize,
      totalPages: Math.max(1, Math.ceil(result.total / pageSize)),
    }));
    return true;
  }

  if (url === '/api/logs/clear' && req.method === 'POST') {
    try {
      clearCurrentGatewayLog();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true, file: currentLogFile() }));
    } catch (err) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: err.message || '清空日志失败' }));
    }
    return true;
  }

  if (url === '/api/stats' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(stats));
    return true;
  }

  if (url === '/api/stats/reset' && req.method === 'POST') {
    resetStats();
    saveStats();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
    return true;
  }

  if (url === '/api/config/save' && req.method === 'POST') {
    const d = JSON.parse(body);
    const {
      channelKey,
      name,
      base_url,
      key,
      keys,
      models,
      format,
      anthropic_version,
      prompt_cache_enabled,
      prompt_cache_ttl,
      anthropic_thinking_type,
      anthropic_thinking_budget_tokens,
      anthropic_output_effort,
      anthropic_thinking_display,
      model_prefix,
      website,
      auto_truncate,
      enabled,
      isNew,
      previousChannelKey,
    } = d;

    if (!channelKey) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: 'channelKey required' }));
      return true;
    }

    if (isNew && config.channels[channelKey]) {
      res.writeHead(409, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '渠道 key 已存在' }));
      return true;
    }

    const oldChannelKey = !isNew && previousChannelKey ? String(previousChannelKey) : channelKey;
    const isRename = !isNew && oldChannelKey !== channelKey;

    if (!isNew && oldChannelKey && !config.channels[oldChannelKey]) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '原渠道不存在' }));
      return true;
    }

    if (isRename && config.channels[channelKey]) {
      res.writeHead(409, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '新前缀已存在' }));
      return true;
    }

    const existingChannel = config.channels[oldChannelKey] || config.channels[channelKey] || {};
    const configuredKeys = normalizeKeyList([
      key || existingChannel.key || '',
      ...(Array.isArray(keys) ? keys : []),
    ]);
    config.channels[channelKey] = normalizeChannelForSave({
      name: name || existingChannel.name || channelKey,
      base_url: base_url || existingChannel.base_url || '',
      key: configuredKeys[0] || '',
      keys: configuredKeys,
      disabled_keys: existingChannel.disabled_keys,
      disabled_key_meta: existingChannel.disabled_key_meta,
      force_enabled_keys: existingChannel.force_enabled_keys,
      ...(format || existingChannel.format ? { format: format || existingChannel.format } : {}),
      ...(anthropic_version || existingChannel.anthropic_version ? { anthropic_version: anthropic_version || existingChannel.anthropic_version } : {}),
      ...(prompt_cache_enabled ? { prompt_cache_enabled: true, prompt_cache_ttl } : {}),
      ...(anthropic_thinking_type && anthropic_thinking_type !== 'off' ? { anthropic_thinking_type } : {}),
      ...(anthropic_thinking_type === 'enabled' ? { anthropic_thinking_budget_tokens: toPositiveInteger(anthropic_thinking_budget_tokens, 32000) } : {}),
      ...(anthropic_thinking_type === 'adaptive' && anthropic_output_effort ? { anthropic_output_effort } : {}),
      ...(anthropic_thinking_type === 'adaptive' && anthropic_thinking_display ? { anthropic_thinking_display } : {}),
      ...(Object.prototype.hasOwnProperty.call(d, 'model_prefix') ? { model_prefix } : {}),
      ...(Object.prototype.hasOwnProperty.call(d, 'website') ? { website } : (existingChannel.website ? { website: existingChannel.website } : {})),
      ...(Object.prototype.hasOwnProperty.call(d, 'auto_truncate')
        ? (auto_truncate === true || auto_truncate === 'true' ? { auto_truncate: true } : {})
        : (existingChannel.auto_truncate ? { auto_truncate: true } : {})),
      enabled: enabled !== false,
      models: models || [],
    });

    if (isRename) {
      delete config.channels[oldChannelKey];
      channelKeyCursors.delete(oldChannelKey);
      saveClientKeyEntries(getClientKeyEntries().map(entry => ({
        ...entry,
        allowed_channels: normalizeStringArray(entry.allowed_channels.map(key => key === oldChannelKey ? channelKey : key)),
      })));
    }

    rebuildModelMap();
    saveConfig();
    console.log(`[config] saved channel: ${channelKey}`);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
    return true;
  }

  if (url === '/api/config/delete' && req.method === 'POST') {
    const d = JSON.parse(body);
    const { channelKey } = d;
    if (!channelKey || !config.channels[channelKey]) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '渠道不存在' }));
      return true;
    }
    delete config.channels[channelKey];
    rebuildModelMap();
    saveConfig();
    console.log(`[config] deleted channel: ${channelKey}`);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
    return true;
  }

  if (url === '/api/config/channel-enabled' && req.method === 'POST') {
    const d = JSON.parse(body);
    const { channelKey, enabled } = d;
    if (!channelKey || !config.channels[channelKey]) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '渠道不存在' }));
      return true;
    }
    config.channels[channelKey] = normalizeChannelForSave({
      ...config.channels[channelKey],
      enabled: enabled !== false,
    });
    if (enabled === false) channelKeyCursors.delete(channelKey);
    rebuildModelMap();
    saveConfig();
    console.log(`[config] ${enabled === false ? 'disabled' : 'enabled'} channel: ${channelKey}`);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
    return true;
  }

  if (url === '/api/config/add-model' && req.method === 'POST') {
    const d = JSON.parse(body);
    const { channelKey, model } = d;
    if (!channelKey || !config.channels[channelKey]) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '渠道不存在' }));
      return true;
    }
    if (!config.channels[channelKey].models.includes(model)) {
      config.channels[channelKey].models.push(model);
      rebuildModelMap();
      saveConfig();
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
    return true;
  }

  if (url === '/api/config/remove-model' && req.method === 'POST') {
    const d = JSON.parse(body);
    const { channelKey, model } = d;
    if (!channelKey || !config.channels[channelKey]) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '渠道不存在' }));
      return true;
    }
    config.channels[channelKey].models = config.channels[channelKey].models.filter(m => m !== model);
    rebuildModelMap();
    saveConfig();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
    return true;
  }

  if (url === '/api/probe-models' && req.method === 'POST') {
    try {
      const d = JSON.parse(body);
      const { base_url, key, channelKey } = d;
      const format = d.format || (channelKey && config.channels[channelKey]?.format) || '';
      if (!base_url || !key) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: '缺少 base_url 或 key' }));
        return true;
      }
      const rootUrl = base_url.replace(/\/+$/, '');
      const upstreamHostname = new URL(rootUrl).hostname.toLowerCase();
      const isCline = upstreamHostname === 'api.cline.bot';
      const modelsPath = isCline
        ? '/ai/cline/recommended-models'
        : format === 'unlimited_api_chat' || /(^|\.)unlimited\.surf$/i.test(upstreamHostname)
          ? '/api/models'
          : '/models';
      const modelsUrl = rootUrl + modelsPath;
      const upstream = await fetch(modelsUrl, {
        headers: { 'Authorization': `Bearer ${key}`, 'Content-Type': 'application/json' },
        signal: AbortSignal.timeout(10000),
      });
      const text = await upstream.text();
      let json;
      try {
        json = JSON.parse(text);
      } catch {
        throw new Error(`上游返回非 JSON (${upstream.status})，URL: ${modelsPath}`);
      }
      if (!upstream.ok) {
        throw new Error(json?.error?.message || json?.message || `上游 HTTP ${upstream.status}`);
      }
      const modelEntries = isCline
        ? [...(json.recommended || []), ...(json.free || []), ...(json.clinePass || [])]
        : (json.data || []);
      const models = [...new Set(modelEntries.map(m => m.id || m).filter(Boolean))];
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true, models }));
    } catch (e) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: e.message }));
    }
    return true;
  }

  if (url === '/api/test-channel' && req.method === 'POST') {
    try {
      const d = JSON.parse(body);
      const { channelKey } = d;
      const ch = config.channels[channelKey];
      if (!ch) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: '渠道不存在' }));
        return true;
      }
      if (!isChannelEnabled(ch)) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: '渠道已禁用' }));
        return true;
      }
      const model = ch.models[0];
      if (!model) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: '渠道没有模型' }));
        return true;
      }
      const activeKeys = getActiveChannelKeys(ch);
      if (!activeKeys.length) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: '渠道没有可用 Key，请先恢复或导入 Key' }));
        return true;
      }
      const result = await testUpstreamChannelKey(channelKey, activeKeys[0], { autoDisable: true });
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        ok: result.pass === true,
        reply: result.reply || '',
        error: result.pass ? '' : (result.error || result.reason || '测试失败'),
        status: result.status,
        autoDisabled: result.autoDisabled === true,
      }));
    } catch (e) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: e.message }));
    }
    return true;
  }

  // ==================== Channel Key 管理 ====================

  if (url === '/api/config/channel-keys/test' && req.method === 'POST') {
    const d = JSON.parse(body || '{}');
    const result = await testUpstreamChannelKey(d.channelKey, d.key, {
      reenable: d.reenable === true,
      autoDisable: d.autoDisable !== false,
    });
    res.writeHead(result.ok === false ? 400 : 200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(result));
    return true;
  }

  if (url === '/api/config/channel-keys/batch-test' && req.method === 'POST') {
    const d = JSON.parse(body || '{}');
    const channelKey = String(d.channelKey || '');
    const ch = config.channels[channelKey];
    if (!ch) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '渠道不存在' }));
      return true;
    }
    const allKeys = getChannelKeys(ch);
    const model = String(d.model || '').trim();
    if (!model || !ch.models?.includes(model)) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: model ? '所选模型不属于当前渠道' : '请选择测试模型' }));
      return true;
    }
    const requestedKeys = normalizeKeyList(Array.isArray(d.keys) ? d.keys : []);
    const targets = (requestedKeys.length ? requestedKeys : allKeys)
      .filter(key => allKeys.includes(key));
    const concurrency = Math.min(Math.max(parseInt(d.concurrency || '5', 10) || 5, 1), 20);
    const results = new Array(targets.length);
    let cursor = 0;
    async function worker() {
      while (cursor < targets.length) {
        const index = cursor++;
        results[index] = await testUpstreamChannelKey(channelKey, targets[index], {
          model,
          reenable: d.reenable === true,
          autoDisable: d.autoDisable !== false,
        });
      }
    }
    await Promise.all(Array.from({ length: Math.min(concurrency, targets.length || 1) }, () => worker()));
    const passed = results.filter(result => result.pass).length;
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      ok: true,
      model,
      results,
      summary: { total: results.length, pass: passed, fail: results.length - passed },
    }));
    return true;
  }

  if (url === '/api/config/channel-keys/force-enable' && req.method === 'POST') {
    const d = JSON.parse(body || '{}');
    const channelKey = String(d.channelKey || '');
    const ch = config.channels[channelKey];
    if (!ch) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '渠道不存在' }));
      return true;
    }
    const allKeys = getChannelKeys(ch);
    const targetKeys = d.all === true
      ? allKeys
      : normalizeKeyList(Array.isArray(d.keys) ? d.keys : [d.key]).filter(key => allKeys.includes(key));
    if (!targetKeys.length) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '请选择至少一个 Key' }));
      return true;
    }
    const enabled = d.enabled !== false;
    const forceSet = new Set(getForceEnabledChannelKeys(ch));
    for (const key of targetKeys) enabled ? forceSet.add(key) : forceSet.delete(key);
    const disabledKeys = enabled ? (ch.disabled_keys || []).filter(key => !forceSet.has(key)) : (ch.disabled_keys || []);
    config.channels[channelKey] = normalizeChannelForSave({
      ...ch,
      force_enabled_keys: [...forceSet],
      disabled_keys: disabledKeys,
    });
    channelKeyCursors.delete(channelKey);
    saveConfig();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, updated: targetKeys.length, enabled }));
    return true;
  }

  if (url === '/api/config/channel-keys/disable' && req.method === 'POST') {
    const d = JSON.parse(body || '{}');
    const { channelKey, key } = d;
    const ch = config.channels[channelKey];
    if (!ch) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '渠道不存在' }));
      return true;
    }
    const trimmedKey = String(key || '').trim();
    if (!trimmedKey || !getChannelKeys(ch).includes(trimmedKey)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: 'Key 不存在' }));
      return true;
    }
    disableUpstreamChannelKey(channelKey, trimmedKey, { reason: 'manual' });
    rebuildModelMap();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
    return true;
  }

  if (url === '/api/config/channel-keys/enable' && req.method === 'POST') {
    const d = JSON.parse(body || '{}');
    const { channelKey, key } = d;
    const ch = config.channels[channelKey];
    if (!ch) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '渠道不存在' }));
      return true;
    }
    const trimmedKey = String(key || '').trim();
    if (!trimmedKey || !getChannelKeys(ch).includes(trimmedKey)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: 'Key 不存在' }));
      return true;
    }
    enableUpstreamChannelKey(channelKey, trimmedKey);
    rebuildModelMap();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
    return true;
  }

  if (url === '/api/config/channel-keys/enable-all' && req.method === 'POST') {
    const d = JSON.parse(body || '{}');
    const channelKey = String(d.channelKey || '');
    const ch = config.channels[channelKey];
    if (!ch) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '渠道不存在' }));
      return true;
    }
    let enabled = 0;
    for (const key of [...(ch.disabled_keys || [])]) {
      if (enableUpstreamChannelKey(channelKey, key)) enabled++;
    }
    rebuildModelMap();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, enabled }));
    return true;
  }

  if (url === '/api/config/channel-keys/edit' && req.method === 'POST') {
    const d = JSON.parse(body || '{}');
    const channelKey = String(d.channelKey || '');
    const oldKey = String(d.key || '').trim();
    const newKey = String(d.newKey || '').trim();
    const ch = config.channels[channelKey];
    if (!ch) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '渠道不存在' }));
      return true;
    }
    const allKeys = getChannelKeys(ch);
    const index = allKeys.indexOf(oldKey);
    if (index < 0) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: 'Key 不存在' }));
      return true;
    }
    if (!newKey) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '新 Key 不能为空' }));
      return true;
    }
    if (allKeys.some((key, keyIndex) => key === newKey && keyIndex !== index)) {
      res.writeHead(409, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '新 Key 已存在' }));
      return true;
    }
    const nextKeys = [...allKeys];
    nextKeys[index] = newKey;
    const wasDisabled = isKeyDisabled(ch, oldKey);
    const wasForceEnabled = getForceEnabledChannelKeys(ch).includes(oldKey);
    const disabledKeys = (ch.disabled_keys || []).filter(key => key !== oldKey);
    if (wasDisabled) disabledKeys.push(newKey);
    const disabledMeta = { ...(ch.disabled_key_meta || {}) };
    if (disabledMeta[oldKey]) {
      disabledMeta[newKey] = disabledMeta[oldKey];
      delete disabledMeta[oldKey];
    }
    config.channels[channelKey] = normalizeChannelForSave({
      ...ch,
      key: nextKeys[0],
      keys: nextKeys,
      disabled_keys: disabledKeys,
      disabled_key_meta: disabledMeta,
      force_enabled_keys: getForceEnabledChannelKeys(ch).filter(key => key !== oldKey).concat(wasForceEnabled ? [newKey] : []),
    });
    channelKeyCursors.delete(channelKey);
    saveConfig();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, id: fingerprintKey(newKey, channelKey) }));
    return true;
  }

  if (url === '/api/config/channel-keys/delete-batch' && req.method === 'POST') {
    const d = JSON.parse(body || '{}');
    const channelKey = String(d.channelKey || '');
    const ch = config.channels[channelKey];
    if (!ch) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '渠道不存在' }));
      return true;
    }
    const allKeys = getChannelKeys(ch);
    const deleteSet = new Set(normalizeKeyList(Array.isArray(d.keys) ? d.keys : []));
    const remainingKeys = allKeys.filter(key => !deleteSet.has(key));
    const deleted = allKeys.length - remainingKeys.length;
    if (!deleted) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true, deleted: 0, total: allKeys.length }));
      return true;
    }
    if (!remainingKeys.length) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '至少保留一个 Key' }));
      return true;
    }
    config.channels[channelKey] = normalizeChannelForSave({ ...ch, key: remainingKeys[0], keys: remainingKeys, force_enabled_keys: getForceEnabledChannelKeys(ch).filter(key => remainingKeys.includes(key)) });
    channelKeyCursors.delete(channelKey);
    saveConfig();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, deleted, total: remainingKeys.length }));
    return true;
  }

  if (url === '/api/config/channel-keys/delete-disabled' && req.method === 'POST') {
    const d = JSON.parse(body || '{}');
    const channelKey = String(d.channelKey || '');
    const ch = config.channels[channelKey];
    if (!ch) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '渠道不存在' }));
      return true;
    }
    const allKeys = getChannelKeys(ch);
    const disabledSet = new Set(ch.disabled_keys || []);
    const remainingKeys = allKeys.filter(key => !disabledSet.has(key));
    if (!remainingKeys.length && allKeys.length) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '不能删除全部 Key，请先恢复至少一个' }));
      return true;
    }
    config.channels[channelKey] = normalizeChannelForSave({ ...ch, key: remainingKeys[0] || '', keys: remainingKeys, force_enabled_keys: getForceEnabledChannelKeys(ch).filter(key => remainingKeys.includes(key)) });
    channelKeyCursors.delete(channelKey);
    saveConfig();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, deleted: allKeys.length - remainingKeys.length, total: remainingKeys.length }));
    return true;
  }

  if (url === '/api/config/channel-keys/delete' && req.method === 'POST') {
    const d = JSON.parse(body || '{}');
    const { channelKey, key } = d;
    const ch = config.channels[channelKey];
    if (!ch) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '渠道不存在' }));
      return true;
    }
    const trimmedKey = String(key || '').trim();
    const allKeys = getChannelKeys(ch);
    if (!allKeys.includes(trimmedKey)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: 'Key 不存在' }));
      return true;
    }
    if (allKeys.length <= 1) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '至少保留一个 Key' }));
      return true;
    }
    const remainingKeys = allKeys.filter(k => k !== trimmedKey);
    config.channels[channelKey] = normalizeChannelForSave({
      ...ch,
      key: remainingKeys[0],
      keys: remainingKeys,
      force_enabled_keys: getForceEnabledChannelKeys(ch).filter(key => remainingKeys.includes(key)),
    });
    rebuildModelMap();
    saveConfig();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
    return true;
  }

  if (url === '/api/config/channel-keys/import' && req.method === 'POST') {
    const d = JSON.parse(body || '{}');
    const { channelKey, keys: keysText } = d;
    const ch = config.channels[channelKey];
    if (!ch) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '渠道不存在' }));
      return true;
    }
    const existingKeys = getChannelKeys(ch);
    const newKeys = normalizeKeyList([
      ...existingKeys,
      ...String(keysText || '').split(/[\r\n,]+/).map(k => k.trim()).filter(Boolean),
    ]);
    const addedCount = newKeys.length - existingKeys.length;
    config.channels[channelKey] = normalizeChannelForSave({
      ...ch,
      key: newKeys[0],
      keys: newKeys,
    });
    rebuildModelMap();
    saveConfig();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, added: addedCount, total: newKeys.length }));
    return true;
  }

  return false;
}

async function handleResponses(req, res, body, requestId) {
  // Override req.url so downstream proxying calls use /v1/chat/completions
  const originalUrl = req.url;
  req.url = '/v1/chat/completions';

  let data;
  try {
    data = JSON.parse(body);
  } catch (err) {
    req.url = originalUrl;
    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: { message: 'Invalid JSON body', type: 'invalid_request' } }));
    return;
  }
  const wantsStream = data.stream === true;
  let messages = convertResponsesInputToMessages(data.input);
  if (data.instructions && !data.previous_response_id) {
    messages.unshift({ role: 'system', content: data.instructions });
  }
  messages = hydrateResponsesMessages(data, messages);
  const chatData = {
    model: data.model,
    messages,
    stream: wantsStream,
  };
  if (data.temperature != null) chatData.temperature = data.temperature;
  if (data.top_p != null) chatData.top_p = data.top_p;
  if (data.max_output_tokens != null) chatData.max_tokens = data.max_output_tokens;
  if (data.max_tokens != null) chatData.max_tokens = data.max_tokens;
  const tools = convertResponsesToolsToOpenAI(data.tools);
  if (tools?.length) chatData.tools = tools;
  if (data.tool_choice) chatData.tool_choice = convertResponsesToolChoiceToOpenAI(data.tool_choice);
  if (data.parallel_tool_calls != null) chatData.parallel_tool_calls = data.parallel_tool_calls;

  if (wantsStream) {
    const chatBody = JSON.stringify(chatData);
    let headersSent = false;
    let isFirst = true;
    let finalized = false;
    let outputText = '';
    let inputTokens = 0;
    let outputTokens = 0;
    let outputStarted = false;
    let outputKind = '';
    let toolCallId = '';
    let toolItemId = '';
    let toolName = '';
    let toolArguments = '';
    let upstreamSseBuffer = '';
    const toolCallsByIndex = new Map();
    const responseId = 'resp_' + newRequestId();
    const msgId = 'msg_' + newRequestId();
    const modelName = data.model || '';
    const messageItem = () => ({
      type: 'message', id: msgId, role: 'assistant', status: 'completed',
      content: [{ type: 'output_text', text: outputText, annotations: [] }],
    });
    const responseObject = (status = 'completed', error = null) => ({
      id: responseId, object: 'response', created_at: Math.floor(Date.now() / 1000),
      status, error, incomplete_details: null, instructions: data.instructions || null,
      max_output_tokens: data.max_output_tokens ?? null, model: modelName,
      output: status === 'completed' ? [messageItem()] : [], parallel_tool_calls: data.parallel_tool_calls ?? true,
      previous_response_id: data.previous_response_id || null, reasoning: { effort: null, summary: null }, store: false,
      temperature: data.temperature ?? 1, text: { format: { type: 'text' } },
      tool_choice: data.tool_choice || 'auto', tools: Array.isArray(data.tools) ? data.tools : [],
      top_p: data.top_p ?? 1, truncation: 'disabled',
      usage: { input_tokens: inputTokens, input_tokens_details: { cached_tokens: 0 },
        output_tokens: outputTokens, output_tokens_details: { reasoning_tokens: 0 },
        total_tokens: inputTokens + outputTokens },
      user: data.user || null, metadata: data.metadata || {},
    });
    const emit = event => res.write('data: ' + JSON.stringify(event) + '\n\n');
    const startTextOutput = () => {
      if (outputStarted) return;
      outputStarted = true;
      outputKind = 'text';
      emit({ type: 'response.output_item.added', output_index: 0,
        item: { type: 'message', id: msgId, role: 'assistant', status: 'in_progress', content: [] } });
      emit({ type: 'response.content_part.added', item_id: msgId, output_index: 0, content_index: 0,
        part: { type: 'output_text', text: '', annotations: [] } });
    };
    const startToolOutput = (toolCall, index = 0) => {
      outputStarted = true;
      outputKind = 'tool';
      let state = toolCallsByIndex.get(index);
      if (!state) {
        state = {
          callId: toolCall.id || ('call_' + newRequestId()),
          itemId: 'fc_' + newRequestId(),
          name: toolCall.function?.name || '',
          arguments: '',
        };
        toolCallsByIndex.set(index, state);
        emit({ type: 'response.output_item.added', output_index: index,
          item: { type: 'function_call', id: state.itemId, call_id: state.callId,
            name: state.name, arguments: '', status: 'in_progress' } });
      }
      if (toolCall.function?.name) state.name = toolCall.function.name;
      if (index === 0) {
        toolCallId = state.callId;
        toolItemId = state.itemId;
        toolName = state.name;
        toolArguments = state.arguments;
      }
      return state;
    };
    const finalizeCompleted = () => {
      if (finalized) return;
      finalized = true;
      if (outputKind === 'tool') {
        const output = [...toolCallsByIndex.entries()].sort((a, b) => a[0] - b[0]).map(([index, state]) => {
          emit({ type: 'response.function_call_arguments.done', item_id: state.itemId,
            output_index: index, arguments: state.arguments });
          const item = { type: 'function_call', id: state.itemId, call_id: state.callId,
            name: state.name, arguments: state.arguments, status: 'completed' };
          emit({ type: 'response.output_item.done', output_index: index, item });
          return item;
        });
        const response = responseObject('completed');
        response.output = output;
        rememberResponseState(responseId, messages, output);
        emit({ type: 'response.completed', response });
        return;
      }
      startTextOutput();
      emit({ type: 'response.output_text.done', output_index: 0, content_index: 0, text: outputText });
      emit({ type: 'response.content_part.done', output_index: 0, content_index: 0,
        part: { type: 'output_text', text: outputText, annotations: [] } });
      const textItem = messageItem();
      emit({ type: 'response.output_item.done', output_index: 0, item: textItem });
      const response = responseObject('completed');
      rememberResponseState(responseId, messages, [textItem]);
      emit({ type: 'response.completed', response });
    };
    const finalizeFailed = message => {
      if (finalized) return;
      finalized = true;
      const error = { type: 'server_error', code: 'upstream_stream_error', message };
      emit({ type: 'response.failed', response: responseObject('failed', error) });
    };
    const fakeRes = {
      headersSent: false,
      writableEnded: false,
      writeHead(status, headers) {
        if (!headersSent) {
          if (status >= 400) {
            res.writeHead(status, { 'Content-Type': 'application/json' });
          } else {
            res.writeHead(200, {
              'Content-Type': 'text/event-stream; charset=utf-8',
              'Cache-Control': 'no-cache',
              'Connection': 'keep-alive',
            });
          }
          headersSent = true;
        }
        fakeRes.headersSent = true;
      },
      setHeader() { },
      getHeader(name) { return res.getHeader(name); },
      write(chunk) {
        upstreamSseBuffer += typeof chunk === 'string' ? chunk : chunk.toString();
        const lines = upstreamSseBuffer.split('\n');
        upstreamSseBuffer = lines.pop() || '';
        for (const line of lines) {
          const trimmed = line.trim();
          if (!trimmed.startsWith('data:')) continue;
          const payload = trimmed.slice(5).trimStart();
          if (payload === '[DONE]') {
            finalizeCompleted();
            return;
          }
          try {
            const chatChunk = JSON.parse(payload);
            if (isFirst) {
              isFirst = false;
              res.write('data: ' + JSON.stringify({
                type: 'response.created',
                response: { id: responseId, object: 'response', status: 'in_progress', model: data.model || chatChunk.model || '', output: [] },
              }) + '\n\n');
              emit({ type: 'response.in_progress', response: { ...responseObject('in_progress'), output: [] } });
            }
            const usage = chatChunk.usage;
            if (usage) {
              inputTokens = usage.prompt_tokens || usage.input_tokens || inputTokens;
              outputTokens = usage.completion_tokens || usage.output_tokens || outputTokens;
            }
            const delta = chatChunk.choices?.[0]?.delta;
            if (delta?.content) {
              startTextOutput();
              outputText += delta.content;
              emit({ type: 'response.output_text.delta', item_id: msgId,
                output_index: 0, content_index: 0, delta: delta.content });
            }
            if (Array.isArray(delta?.tool_calls)) {
              for (const toolCall of delta.tool_calls) {
                const index = Number.isInteger(toolCall.index) ? toolCall.index : 0;
                const state = startToolOutput(toolCall, index);
                const argsDelta = toolCall.function?.arguments || '';
                if (argsDelta) {
                  state.arguments += argsDelta;
                  if (index === 0) toolArguments = state.arguments;
                  emit({ type: 'response.function_call_arguments.delta', item_id: state.itemId,
                    output_index: index, delta: argsDelta });
                }
              }
            }
          } catch { }
        }
      },
      end(endData) {
        if (endData) fakeRes.write(endData);
        if (upstreamSseBuffer.trim()) fakeRes.write('\n');
        if (!finalized && headersSent) {
          if (!isFirst) finalizeCompleted();
          else finalizeFailed('Upstream stream ended without a response');
        } else if (!finalized && !headersSent) {
          res.writeHead(502, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: { message: 'Upstream stream ended without a response', type: 'upstream_error' } }));
        }
        if (!res.writableEnded) res.end();
        fakeRes.writableEnded = true;
      },
    };
    await handleChatCompletions(req, fakeRes, chatBody, requestId);
    return;
  }

  // Non-streaming
  const chatBody = JSON.stringify(chatData);
  let capturedStatus = 200;
  let capturedBody = '';
  const fakeRes = {
    headersSent: false,
    writableEnded: false,
    writeHead(status) { capturedStatus = status; fakeRes.headersSent = true; },
    setHeader() { },
    getHeader(name) { return res.getHeader(name); },
    write(chunk) { capturedBody += (typeof chunk === 'string' ? chunk : chunk.toString()); },
    end(endData) {
      if (endData) capturedBody += (typeof endData === 'string' ? endData : endData.toString());
      fakeRes.writableEnded = true;
    },
  };
  await handleChatCompletions(req, fakeRes, chatBody, requestId);
  if (capturedStatus >= 400) {
    res.writeHead(capturedStatus, { 'Content-Type': 'application/json' });
    res.end(capturedBody);
    return;
  }
  try {
    const chatResult = JSON.parse(capturedBody);
    const responsesResult = convertChatCompletionToResponsesFormat(chatResult, data.model);
    rememberResponseState(responsesResult.id, messages, responsesResult.output);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(responsesResult));
  } catch {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(capturedBody);
  }
}

async function handleAnthropicMessages(req, res, body, requestId) {
  // Override req.url so downstream proxying calls use /v1/chat/completions
  const originalUrl = req.url;
  req.url = '/v1/chat/completions';

  let data;
  try {
    data = JSON.parse(body);
  } catch (err) {
    req.url = originalUrl;
    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ type: 'error', error: { type: 'invalid_request_error', message: 'Invalid JSON body' } }));
    return;
  }
  const wantsStream = data.stream === true;
  const messages = convertAnthropicMessagesToOpenAIChat(data);
  const chatData = {
    model: data.model,
    messages,
    stream: wantsStream,
  };
  if (data.max_tokens != null) chatData.max_tokens = data.max_tokens;
  if (data.temperature != null) chatData.temperature = data.temperature;
  if (data.top_p != null) chatData.top_p = data.top_p;
  if (data.stop_sequences) chatData.stop = data.stop_sequences;
  if (data.thinking) chatData.thinking = data.thinking;
  if (Array.isArray(data.tools)) {
    chatData.tools = data.tools.map(tool => ({
      type: 'function',
      function: {
        name: tool.name || '',
        description: tool.description || '',
        parameters: tool.input_schema || { type: 'object', properties: {} },
      },
    }));
  }
  if (data.tool_choice) {
    if (data.tool_choice.type === 'auto') chatData.tool_choice = 'auto';
    else if (data.tool_choice.type === 'none') chatData.tool_choice = 'none';
    else if (data.tool_choice.type === 'any') chatData.tool_choice = 'required';
    else if (data.tool_choice.type === 'tool' && data.tool_choice.name) {
      chatData.tool_choice = { type: 'function', function: { name: data.tool_choice.name } };
    }
  }

  if (wantsStream) {
    const chatBody = JSON.stringify(chatData);
    let headersSent = false;
    let isFirst = true;
    const msgId = (data.model || '').replace('chatcmpl-', 'msg_') || ('msg_' + newRequestId());
    const fakeRes = {
      headersSent: false,
      writableEnded: false,
      writeHead(status, headers) {
        if (!headersSent) {
          if (status >= 400) {
            res.writeHead(status, { 'Content-Type': 'application/json' });
          } else {
            res.writeHead(200, {
              'Content-Type': 'text/event-stream; charset=utf-8',
              'Cache-Control': 'no-cache',
              'Connection': 'keep-alive',
            });
          }
          headersSent = true;
        }
        fakeRes.headersSent = true;
      },
      setHeader() { },
      getHeader(name) { return res.getHeader(name); },
      write(chunk) {
        const str = typeof chunk === 'string' ? chunk : chunk.toString();
        for (const line of str.split('\n')) {
          const trimmed = line.trim();
          if (!trimmed.startsWith('data: ')) continue;
          const payload = trimmed.slice(6);
          if (payload === '[DONE]') {
            res.write('event: content_block_stop\ndata: ' + JSON.stringify({ type: 'content_block_stop', index: 0 }) + '\n\n');
            res.write('event: message_delta\ndata: ' + JSON.stringify({
              type: 'message_delta', delta: { stop_reason: 'end_turn', stop_sequence: null }, usage: { output_tokens: 0 },
            }) + '\n\n');
            res.write('event: message_stop\ndata: ' + JSON.stringify({ type: 'message_stop' }) + '\n\n');
            return;
          }
          try {
            const chatChunk = JSON.parse(payload);
            if (isFirst) {
              isFirst = false;
              const startMsg = {
                id: (chatChunk.id || '').replace('chatcmpl-', 'msg_') || ('msg_' + newRequestId()),
                type: 'message', role: 'assistant', model: data.model || chatChunk.model || '',
                content: [], stop_reason: null, stop_sequence: null,
                usage: { input_tokens: 0, output_tokens: 0 },
              };
              res.write('event: message_start\ndata: ' + JSON.stringify({ type: 'message_start', message: startMsg }) + '\n\n');
              res.write('event: content_block_start\ndata: ' + JSON.stringify({
                type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' },
              }) + '\n\n');
              res.write('event: ping\ndata: ' + JSON.stringify({ type: 'ping' }) + '\n\n');
            }
            const delta = chatChunk.choices?.[0]?.delta;
            if (delta?.content) {
              res.write('event: content_block_delta\ndata: ' + JSON.stringify({
                type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: delta.content },
              }) + '\n\n');
            }
            if (chatChunk.choices?.[0]?.finish_reason) {
              let sr = 'end_turn';
              if (chatChunk.choices[0].finish_reason === 'length') sr = 'max_tokens';
              else if (chatChunk.choices[0].finish_reason === 'tool_calls') sr = 'tool_use';
              res.write('event: content_block_stop\ndata: ' + JSON.stringify({ type: 'content_block_stop', index: 0 }) + '\n\n');
              res.write('event: message_delta\ndata: ' + JSON.stringify({
                type: 'message_delta', delta: { stop_reason: sr, stop_sequence: null }, usage: { output_tokens: 0 },
              }) + '\n\n');
              res.write('event: message_stop\ndata: ' + JSON.stringify({ type: 'message_stop' }) + '\n\n');
            }
          } catch { }
        }
      },
      end(endData) {
        if (endData) fakeRes.write(endData);
        if (!res.writableEnded) res.end();
        fakeRes.writableEnded = true;
      },
    };
    await handleChatCompletions(req, fakeRes, chatBody, requestId);
    return;
  }

  // Non-streaming
  const chatBody = JSON.stringify(chatData);
  let capturedStatus = 200;
  let capturedBody = '';
  const fakeRes = {
    headersSent: false,
    writableEnded: false,
    writeHead(status) { capturedStatus = status; fakeRes.headersSent = true; },
    setHeader() { },
    getHeader(name) { return res.getHeader(name); },
    write(chunk) { capturedBody += (typeof chunk === 'string' ? chunk : chunk.toString()); },
    end(endData) {
      if (endData) capturedBody += (typeof endData === 'string' ? endData : endData.toString());
      fakeRes.writableEnded = true;
    },
  };
  await handleChatCompletions(req, fakeRes, chatBody, requestId);
  if (capturedStatus >= 400) {
    try {
      const errData = JSON.parse(capturedBody);
      res.writeHead(capturedStatus, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        type: 'error',
        error: { type: errData.error?.type || 'api_error', message: errData.error?.message || 'Unknown error' },
      }));
    } catch {
      res.writeHead(capturedStatus, { 'Content-Type': 'application/json' });
      res.end(capturedBody);
    }
    return;
  }
  try {
    const chatResult = JSON.parse(capturedBody);
    const anthropicResult = convertOpenAIChatResultToAnthropicMessages(chatResult, data.model);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(anthropicResult));
  } catch (err) {
    console.error('Failed to parse chat completion for Anthropic converter:', err.message, 'Captured Body:', capturedBody);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(capturedBody);
  }
}


export {
  handleChatCompletions,
  handleModels,
  handleConfigAPI,
  handleResponses,
  handleAnthropicMessages,
};
