'use strict';

const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || '0.0.0.0';
const PUBLIC_DIR = path.join(__dirname, 'public');
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map(v => v.trim()).filter(Boolean).map(v => { try { return new URL(v).origin; } catch { return ''; } }).filter(Boolean);
const MAX_LISTENERS_PER_SESSION = Math.max(1, Number(process.env.MAX_LISTENERS_PER_SESSION || 10));
const sessions = new Map();
const mimeTypes = {
  '.html': 'text/html; charset=utf-8', '.css': 'text/css; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8', '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml'
};

const server = http.createServer((req, res) => {
  let pathname;
  try { pathname = decodeURIComponent(new URL(req.url, `http://${req.headers.host || 'localhost'}`).pathname); }
  catch { res.writeHead(400).end('Bad request'); return; }
  if (pathname === '/') pathname = '/transmit';
  if (pathname === '/transmit') pathname = '/transmit.html';
  if (pathname === '/listen') pathname = '/listen.html';
  const file = path.resolve(PUBLIC_DIR, `.${pathname}`);
  if (!file.startsWith(PUBLIC_DIR + path.sep)) {
    res.writeHead(403, { 'Content-Type': 'text/plain; charset=utf-8' }).end('Forbidden'); return;
  }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }).end('Not found'); return; }
    res.writeHead(200, {
      'Content-Type': mimeTypes[path.extname(file)] || 'application/octet-stream',
      'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'no-referrer', 'Cache-Control': 'no-store'
    });
    res.end(data);
  });
});

const wss = new WebSocketServer({ noServer: true, maxPayload: 128 * 1024, perMessageDeflate: false });

function send(ws, message) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(message));
}
function safeSession(session) {
  return {
    id: session.id,
    name: session.name,
    createdAt: session.createdAt,
    listenerCount: session.listeners.size
  };
}
function sessionList() {
  return [...sessions.values()]
    .filter(s => s.host && s.host.readyState === WebSocket.OPEN)
    .sort((a, b) => a.createdAt - b.createdAt)
    .map(safeSession);
}
function sendSessionList(ws) { send(ws, { type: 'sessions', sessions: sessionList() }); }
function broadcastSessionList() {
  for (const client of wss.clients) sendSessionList(client);
}
function notifyError(ws, message) { send(ws, { type: 'error', message }); }

function detach(ws, reason = 'left') {
  if (!ws.role) return;
  const role = ws.role;
  const session = sessions.get(ws.sessionId);
  ws.role = null;
  ws.sessionId = null;

  if (!session) return;
  if (role === 'host' && session.host === ws) {
    sessions.delete(session.id);
    console.log(`Session ${session.id} ended (${reason}).`);
    for (const listener of session.listeners.values()) {
      send(listener, { type: 'session-ended', sessionId: session.id });
      listener.role = null;
      listener.sessionId = null;
    }
    session.listeners.clear();
    broadcastSessionList();
    return;
  }
  if (role === 'listener' && session.listeners.delete(ws.id)) {
    send(session.host, { type: 'listener-left', listenerId: ws.id });
    console.log(`Listener left session ${session.id}.`);
    broadcastSessionList();
  }
}

server.on('upgrade', (req, socket, head) => {
  let pathname;
  try { pathname = new URL(req.url, `http://${req.headers.host || 'localhost'}`).pathname; }
  catch { socket.destroy(); return; }
  if (pathname !== '/signal') { socket.destroy(); return; }
  const origin = req.headers.origin;
  let originAllowed = false;
  if (origin) {
    try {
      const parsedOrigin = new URL(origin);
      const requestHost = String(req.headers.host || '').toLowerCase();
      // Default: accept same-origin browser requests only. If ALLOWED_ORIGINS is
      // configured, use that explicit allowlist (useful for a separate listener site).
      originAllowed = ALLOWED_ORIGINS.length
        ? ALLOWED_ORIGINS.includes(parsedOrigin.origin)
        : parsedOrigin.host.toLowerCase() === requestHost;
    } catch (_) {
      originAllowed = false;
    }
  }
  if (!originAllowed) {
    socket.write('HTTP/1.1 403 Forbidden\r\n\r\n'); socket.destroy(); return;
  }
  wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req));
});

wss.on('connection', ws => {
  ws.id = crypto.randomBytes(8).toString('hex');
  ws.role = null;
  ws.sessionId = null;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  sendSessionList(ws);

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); }
    catch { notifyError(ws, 'Получено некорректное сообщение.'); return; }
    if (!msg || typeof msg.type !== 'string') return;

    if (msg.type === 'list-sessions') { sendSessionList(ws); return; }

    if (msg.type === 'create-session') {
      if (ws.role) { notifyError(ws, 'Это соединение уже используется. Перезагрузите страницу.'); return; }
      const id = crypto.randomBytes(6).toString('hex');
      const providedName = typeof msg.name === 'string' ? msg.name.trim().slice(0, 64) : '';
      const name = providedName || `Голосовая сессия ${new Date().toLocaleTimeString('ru-RU')}`;
      const session = { id, name, createdAt: Date.now(), host: ws, listeners: new Map() };
      sessions.set(id, session);
      ws.role = 'host';
      ws.sessionId = id;
      console.log(`Session ${id} started: ${name}`);
      send(ws, { type: 'session-created', session: safeSession(session) });
      broadcastSessionList();
      return;
    }

    if (msg.type === 'join-session') {
      if (ws.role) { notifyError(ws, 'Вы уже подключены к сессии.'); return; }
      if (typeof msg.sessionId !== 'string') { notifyError(ws, 'Не выбрана сессия.'); return; }
      const session = sessions.get(msg.sessionId);
      if (!session || session.host.readyState !== WebSocket.OPEN) {
        send(ws, { type: 'join-failed', message: 'Эта сессия уже завершена. Выберите другую.' });
        sendSessionList(ws); return;
      }
      if (session.listeners.size >= MAX_LISTENERS_PER_SESSION) {
        send(ws, { type: 'join-failed', message: 'В этой сессии уже максимальное число слушателей.' }); return;
      }
      ws.role = 'listener';
      ws.sessionId = session.id;
      session.listeners.set(ws.id, ws);
      send(ws, { type: 'joined-session', session: safeSession(session) });
      send(session.host, { type: 'listener-joined', listenerId: ws.id });
      console.log(`Listener ${ws.id} joined session ${session.id}.`);
      broadcastSessionList();
      return;
    }

    if (msg.type === 'leave') {
      detach(ws, 'user stopped');
      send(ws, { type: 'left' });
      return;
    }

    if (msg.type === 'signal') {
      if (!ws.role || !ws.sessionId || !msg.signal || !['offer', 'answer', 'candidate'].includes(msg.signal.kind)) return;
      const session = sessions.get(ws.sessionId);
      if (!session) return;
      if (ws.role === 'host') {
        if (typeof msg.targetId !== 'string') return;
        const target = session.listeners.get(msg.targetId);
        if (target) send(target, { type: 'signal', from: ws.id, signal: msg.signal });
      } else if (ws.role === 'listener' && session.host.readyState === WebSocket.OPEN) {
        send(session.host, { type: 'signal', from: ws.id, signal: msg.signal });
      }
    }
  });

  ws.on('close', () => detach(ws, 'connection closed'));
  ws.on('error', err => console.error('WebSocket error:', err.message));
});

// Keep WebSocket sessions alive through proxies that would otherwise silently drop idle connections.
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch { ws.terminate(); }
  }
}, 30000);

function listenWithFallback(port, attempts = 10) {
  const onListening = () => {
    server.removeListener('error', onError);
    const address = server.address();
    const actualPort = address && typeof address === 'object' ? address.port : port;
    console.log(`Voice signaling server: http://localhost:${actualPort}`);
    console.log(`Transmitter: http://localhost:${actualPort}/transmit`);
    console.log(`Session selector: http://localhost:${actualPort}/listen`);
    if (actualPort !== PORT) console.log(`Note: requested port ${PORT} was busy; using ${actualPort} instead.`);
    console.log('WebRTC uses trickle ICE for faster setup; restrictive networks may still need TURN.');
    if (!ALLOWED_ORIGINS.length) console.log('Warning: set ALLOWED_ORIGINS before exposing this server publicly.');
  };
  const onError = (err) => {
    server.removeListener('listening', onListening);
    server.removeListener('error', onError);
    if (err.code === 'EADDRINUSE' && attempts > 1) {
      const nextPort = port + 1;
      console.warn(`Port ${port} is already in use. Trying port ${nextPort}...`);
      listenWithFallback(nextPort, attempts - 1);
      return;
    }
    console.error(`Could not start server on ${HOST}:${port}: ${err.message}`);
    if (err.code === 'EADDRINUSE') {
      console.error('Ports are busy. Stop an older server or set PORT=3100 before running npm start.');
    }
    process.exitCode = 1;
  };
  server.once('listening', onListening);
  server.once('error', onError);
  server.listen(port, HOST);
}

listenWithFallback(PORT);
server.on('close', () => clearInterval(heartbeat));
process.on('SIGINT', () => { clearInterval(heartbeat); server.close(() => process.exit(0)); });
process.on('SIGTERM', () => { clearInterval(heartbeat); server.close(() => process.exit(0)); });
