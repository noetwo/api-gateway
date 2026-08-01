import http from 'node:http';
import fs from 'node:fs';
import { config, modelMap, stats, SERVE_UI, UI_FILE, LOGIN_FILE } from './state.mjs';
import {
  writeGatewayLog, newRequestId, saveStats,
  backfillHourlyStatsFromRecentLogs, backfillUsageCacheFromRecentLogs,
  backfillUpstreamKeyUsageFromRecentLogs,
} from './logger.mjs';
import { rebuildModelMap } from './config.mjs';
import {
  adminAuth, clientAuth,
  isAdminRequestAuthenticated, handleAdminLogin, handleAdminLogout,
} from './auth.mjs';
import {
  handleChatCompletions, handleModels, handleConfigAPI,
  handleResponses, handleAnthropicMessages,
} from './handlers.mjs';

const LISTEN_HOST = String(process.env.GATEWAY_HOST || config.host || '127.0.0.1').trim();

// 初始化
rebuildModelMap();
backfillHourlyStatsFromRecentLogs();
backfillUsageCacheFromRecentLogs();
backfillUpstreamKeyUsageFromRecentLogs();

const server = http.createServer(async (req, res) => {
  const requestId = newRequestId();
  res.setHeader('X-Request-Id', requestId);
  writeGatewayLog('http_request', {
    requestId,
    method: req.method,
    url: req.url,
    client: req.socket.remoteAddress,
  });

  // CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', '*');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  const url = req.url;

  // Web UI
  if (SERVE_UI && (url === '/' || url === '/ui')) {
    const authenticated = isAdminRequestAuthenticated(req);
    const pageFile = authenticated ? UI_FILE : LOGIN_FILE;
    res.writeHead(200, {
      'Content-Type': 'text/html; charset=utf-8',
      'Cache-Control': 'no-store, no-cache, must-revalidate',
      'Pragma': 'no-cache',
      'X-Frame-Options': 'DENY',
      'Referrer-Policy': 'same-origin',
    });
    res.end(fs.readFileSync(pageFile, 'utf-8'));
    return;
  }

  // Health check
  if (url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      name: '聚合渠道',
      channels: Object.keys(config.channels).length,
      models: [...modelMap.keys()].length,
      totalRequests: stats.totalRequests
    }));
    return;
  }

  // Collect body for POST
  let body = '';
  if (req.method === 'POST') {
    body = await new Promise((resolve) => {
      const chunks = [];
      req.on('data', c => chunks.push(c));
      req.on('end', () => resolve(Buffer.concat(chunks).toString()));
    });
  }

  if (url === '/api/login' && req.method === 'POST') {
    handleAdminLogin(req, res, body);
    return;
  }

  if (url === '/api/logout' && req.method === 'POST') {
    handleAdminLogout(req, res);
    return;
  }

  // Route
  if (url.startsWith('/api/')) {
    res.setHeader('Cache-Control', 'no-store');
    if (!adminAuth(req, res)) return;
    const handled = await handleConfigAPI(req, res, url, req.method === 'POST' ? body : '');
    if (!handled) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: { message: 'API not found' } }));
    }
    return;
  }

  if (url.startsWith('/v1/')) {
    if (!clientAuth(req, res)) return;
  }

  if (url === '/v1/models' && req.method === 'GET') {
    await handleModels(req, res);
  } else if (url === '/v1/chat/completions' && req.method === 'POST') {
    await handleChatCompletions(req, res, body, requestId);
  } else if (url === '/v1/responses' && req.method === 'POST') {
    await handleResponses(req, res, body, requestId);
  } else if (url === '/v1/messages' && req.method === 'POST') {
    await handleAnthropicMessages(req, res, body, requestId);
  } else if (url.startsWith('/v1/')) {
    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: { message: `Not supported: ${url}`, type: 'not_implemented' } }));
  } else {
    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: { message: 'Not found' } }));
  }
});

// 保存统计数据（程序退出时）
process.on('SIGINT', () => {
  console.log('\n保存统计数据...');
  saveStats();
  process.exit();
});

process.on('SIGTERM', () => {
  saveStats();
  process.exit();
});

server.listen(config.port, LISTEN_HOST, () => {
  console.log(`========================================`);
  console.log(`聚合渠道 (API Gateway) v2.0`);
  console.log(`========================================`);
  console.log(`端口: ${config.port}`);
  console.log(`监听地址: ${LISTEN_HOST}`);
  console.log(`渠道数: ${Object.keys(config.channels).length}`);
  console.log(`模型总数: ${[...modelMap.keys()].length}`);
  console.log(`统计功能: 已启用`);
  console.log(`========================================`);

  // 显示已有统计
  if (stats.totalRequests > 0) {
    console.log(`历史请求数: ${stats.totalRequests}`);
    const topModels = Object.entries(stats.modelUsage)
      .sort((a, b) => b[1].count - a[1].count)
      .slice(0, 3);
    if (topModels.length > 0) {
      console.log(`热门模型:`);
      topModels.forEach(([model, data]) => {
        console.log(`  - ${model}: ${data.count} 次`);
      });
    }
    console.log(`========================================`);
  }
});
