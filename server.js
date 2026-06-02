const http = require('http');
const WebSocket = require('ws');

// ─── Configuración ───────────────────────────────────────────────────────────
const MAX_PLAYERS        = parseInt(process.env.MAX_PLAYERS)  || 8;
const HEARTBEAT_INTERVAL = parseInt(process.env.HEARTBEAT_MS) || 25_000;
const PING_TIMEOUT       = 10_000;
const REJOIN_WINDOW      = parseInt(process.env.REJOIN_MS)    || 60_000;
const MAX_CANVAS_STROKES = 500;
const RATE_LIMIT_MAX     = 50;    // mensajes por segundo por cliente
const RATE_LIMIT_WINDOW  = 1_000;
const SPAM_MAX           = 10;    // mensajes de chat máximos en SPAM_WINDOW
const SPAM_WINDOW        = 8_000; // ventana anti-spam en ms
const EMPTY_ROOM_TTL     = 10 * 60_000;

// ─── Logger estructurado ─────────────────────────────────────────────────────
const log = {
  info:  (room, msg) => console.log(`[${new Date().toISOString()}] [INFO]  [${room}] ${msg}`),
  warn:  (room, msg) => console.warn(`[${new Date().toISOString()}] [WARN]  [${room}] ${msg}`),
  error: (room, msg) => console.error(`[${new Date().toISOString()}] [ERROR] [${room}] ${msg}`),
};

// ─── Servidor HTTP ───────────────────────────────────────────────────────────
const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    const stats = Object.entries(rooms).map(([code, r]) => ({
      code,
      players: Object.keys(r.players).length,
      canvas: r.canvas.length,
    }));
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify({ status: 'ok', rooms: stats }));
    return;
  }
  res.writeHead(200, { 'Access-Control-Allow-Origin': '*', 'Content-Type': 'text/plain' });
  res.end('Pincel WS Server OK');
});

const wss = new WebSocket.Server({ server });

// ─── Estado global ───────────────────────────────────────────────────────────
const rooms = {};

function getRoom(code) {
  if (!rooms[code]) {
    rooms[code] = { players: {}, canvas: [], gameState: null, emptyAt: null };
  }
  return rooms[code];
}

// ─── Utilidades ──────────────────────────────────────────────────────────────
function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(obj)); } catch (_) {}
  }
}

function broadcast(room, obj, excludeId = null) {
  const raw = JSON.stringify(obj);
  for (const [id, p] of Object.entries(room.players)) {
    if (id === excludeId) continue;
    if (p.ws && p.ws.readyState === WebSocket.OPEN) {
      try { p.ws.send(raw); } catch (_) {}
    }
  }
}

function playerList(room) {
  return Object.entries(room.players).map(([pid, p]) => ({
    id: pid, name: p.name, score: p.score, avatar: p.avatar || null,
    connected: !!(p.ws && p.ws.readyState === WebSocket.OPEN),
  }));
}

function removePlayer(roomCode, id, notify = true) {
  const room = rooms[roomCode];
  if (!room) return;
  const p = room.players[id];
  delete room.players[id];
  if (notify && p) broadcast(room, { type: 'playerLeft', id, name: p.name });
  if (Object.keys(room.players).length === 0) {
    room.emptyAt = Date.now();
    log.info(roomCode, `Sala vacía, marcada para limpieza.`);
  }
}

// ─── Rate limiting (mensajes totales/segundo) ────────────────────────────────
function checkRateLimit(p) {
  const now = Date.now();
  if (!p.rateWindow || now - p.rateWindow > RATE_LIMIT_WINDOW) {
    p.rateWindow = now; p.rateCount = 0;
  }
  p.rateCount++;
  return p.rateCount <= RATE_LIMIT_MAX;
}

// ─── Anti-spam de chat (mensajes de tipo guess/chat) ────────────────────────
// Devuelve true si está bien, false si hay que expulsar
function checkSpam(p) {
  const now = Date.now();
  if (!p.spamWindow || now - p.spamWindow > SPAM_WINDOW) {
    p.spamWindow = now; p.spamCount = 0;
  }
  p.spamCount++;
  return p.spamCount <= SPAM_MAX;
}

// ─── Heartbeat ───────────────────────────────────────────────────────────────
setInterval(() => {
  for (const [roomCode, room] of Object.entries(rooms)) {
    for (const [id, p] of Object.entries(room.players)) {
      if (!p.ws || p.ws.readyState !== WebSocket.OPEN) {
        if (p.disconnectedAt && Date.now() - p.disconnectedAt > REJOIN_WINDOW) {
          log.warn(roomCode, `Rejoin expirado para ${p.name} (${id})`);
          removePlayer(roomCode, id);
        }
        continue;
      }
      if (p.lastPing && Date.now() - p.lastPing > HEARTBEAT_INTERVAL + PING_TIMEOUT) {
        log.warn(roomCode, `Timeout de ${p.name} (${id}), cerrando.`);
        p.ws.terminate(); p.ws = null; p.disconnectedAt = Date.now();
        broadcast(room, { type: 'playerDisconnected', id, name: p.name });
        continue;
      }
      try { p.ws.ping(); p.lastPing = Date.now(); } catch (_) {}
    }
  }
}, HEARTBEAT_INTERVAL);

// ─── Limpieza de salas vacías ────────────────────────────────────────────────
setInterval(() => {
  for (const [code, room] of Object.entries(rooms)) {
    if (room.emptyAt && Date.now() - room.emptyAt > EMPTY_ROOM_TTL) {
      delete rooms[code];
      log.info(code, `Sala eliminada por inactividad.`);
    }
  }
}, 60_000);

// ─── Conexión WebSocket ──────────────────────────────────────────────────────
wss.on('connection', (ws, req) => {
  let roomCode, id, name;
  try {
    const params = new URL(req.url, 'http://localhost').searchParams;
    roomCode = params.get('room') || 'default';
    id       = params.get('id')   || `anon_${Date.now()}`;
    name     = params.get('name') || 'Jugador';
  } catch (err) {
    log.error('?', `URL inválida: ${err.message}`);
    ws.close(); return;
  }

  const room = getRoom(roomCode);
  room.emptyAt = null;
  const existingPlayer = room.players[id];

  // ── REJOIN ────────────────────────────────────────────────────────────────
  if (existingPlayer) {
    existingPlayer.ws = ws;
    existingPlayer.disconnectedAt = null;
    existingPlayer.lastPing = null;
    log.info(roomCode, `REJOIN de ${name} (${id}). Puntos: ${existingPlayer.score}`);
    send(ws, { type: 'rejoinOk', score: existingPlayer.score, gameState: room.gameState, players: playerList(room) });
    if (room.canvas.length > 0) send(ws, { type: 'canvasSnapshot', strokes: room.canvas });
    broadcast(room, { type: 'playerRejoined', id, name }, id);
    attachHandlers(ws, roomCode, id, name, room);
    return;
  }

  // ── NUEVA CONEXIÓN ────────────────────────────────────────────────────────
  const activePlayers = Object.keys(room.players).length;
  if (activePlayers >= MAX_PLAYERS) {
    log.warn(roomCode, `Sala llena, rechazando a ${name}`);
    send(ws, { type: 'roomFull', max: MAX_PLAYERS });
    ws.close(); return;
  }

  room.players[id] = {
    ws, name, score: 0, avatar: null,
    lastPing: null, disconnectedAt: null,
    rateWindow: null, rateCount: 0,
    spamWindow: null, spamCount: 0,
  };

  log.info(roomCode, `NUEVO ${name} (${id}). Jugadores: ${activePlayers + 1}/${MAX_PLAYERS}`);
  send(ws, { type: 'welcome', id, players: playerList(room) });
  if (room.canvas.length > 0) send(ws, { type: 'canvasSnapshot', strokes: room.canvas });
  broadcast(room, { type: 'playerJoined', id, name }, id);
  attachHandlers(ws, roomCode, id, name, room);
});

// ─── Handlers ────────────────────────────────────────────────────────────────
function attachHandlers(ws, roomCode, id, name, room) {

  ws.on('pong', () => {
    if (room.players[id]) room.players[id].lastPing = null;
  });

  ws.on('message', (data) => {
    const p = room.players[id];
    if (!p) return;

    // Rate limiting global
    if (!checkRateLimit(p)) {
      log.warn(roomCode, `Rate limit superado por ${name} (${id})`);
      return;
    }

    let msg;
    try { msg = JSON.parse(data.toString()); }
    catch { return; }

    msg.id = id;
    msg.fromName = name;

    switch (msg.type) {

      case 'player-join': {
        if (msg.data?.avatar) p.avatar = msg.data.avatar;
        broadcast(room, { ...msg, data: { ...msg.data, avatar: p.avatar } }, id);
        break;
      }

      case 'draw': {
        room.canvas.push(msg);
        if (room.canvas.length > MAX_CANVAS_STROKES) room.canvas = room.canvas.slice(-MAX_CANVAS_STROKES);
        broadcast(room, msg, id);
        break;
      }

      case 'clear':
      case 'clearCanvas': {
        room.canvas = [];
        broadcast(room, msg);
        break;
      }

      case 'game-start': {
        room.canvas = [];
        room.gameState = { active: true, round: msg.data?.roundNumber || 1 };
        broadcast(room, msg);
        break;
      }

      case 'game-end': {
        room.gameState = null;
        broadcast(room, msg);
        break;
      }

      case 'gameState': {
        room.gameState = msg.state || null;
        broadcast(room, msg, id);
        break;
      }

      // ── Anti-spam: solo aplica a mensajes de chat/adivinanza ─────────────
      case 'guess': {
        if (!checkSpam(p)) {
          log.warn(roomCode, `SPAM detectado de ${name} (${id}), expulsando.`);
          send(ws, {
            type: 'kicked',
            reason: 'Has sido expulsado por enviar demasiados mensajes seguidos.'
          });
          ws.close();
          removePlayer(roomCode, id);
          return;
        }
        broadcast(room, msg);
        break;
      }

      case 'direct': {
        const target = room.players[msg.to];
        if (target?.ws?.readyState === WebSocket.OPEN) {
          try { target.ws.send(JSON.stringify(msg)); } catch (_) {}
        }
        break;
      }

      case 'ping':
        send(ws, { type: 'pong' });
        break;

      default:
        broadcast(room, msg);
        break;
    }
  });

  ws.on('close', (code) => {
    log.info(roomCode, `DESCONEXIÓN ${name} (${id}). Código: ${code}`);
    const p = room.players[id];
    if (p) { p.ws = null; p.disconnectedAt = Date.now(); broadcast(room, { type: 'playerDisconnected', id, name }); }
  });

  ws.on('error', (err) => {
    log.error(roomCode, `Error socket ${name} (${id}): ${err.message}`);
    try { ws.terminate(); } catch (_) {}
    const p = room.players[id];
    if (p) { p.ws = null; p.disconnectedAt = Date.now(); broadcast(room, { type: 'playerDisconnected', id, name }); }
  });
}

// ─── Graceful shutdown ───────────────────────────────────────────────────────
function shutdown(signal) {
  log.info('SERVER', `${signal} recibido, cerrando conexiones...`);
  for (const room of Object.values(rooms)) {
    for (const p of Object.values(room.players)) {
      if (p.ws && p.ws.readyState === WebSocket.OPEN) {
        try {
          p.ws.send(JSON.stringify({ type: 'serverRestart', msg: 'El servidor se reinicia, reconecta en unos segundos.' }));
          p.ws.close();
        } catch (_) {}
      }
    }
  }
  server.close(() => { log.info('SERVER', 'Servidor cerrado limpiamente.'); process.exit(0); });
  setTimeout(() => process.exit(1), 5000);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT',  () => shutdown('SIGINT'));
process.on('uncaughtException',  (err)    => log.error('SERVER', `uncaughtException: ${err.message}`));
process.on('unhandledRejection', (reason) => log.error('SERVER', `unhandledRejection: ${reason}`));

// ─── Arranque ────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  log.info('SERVER', `🎨 Pincel en puerto ${PORT} | MAX=${MAX_PLAYERS} jugadores | REJOIN=${REJOIN_WINDOW/1000}s | RATE=${RATE_LIMIT_MAX}msg/s | SPAM=max ${SPAM_MAX} mensajes/${SPAM_WINDOW/1000}s`);
});
