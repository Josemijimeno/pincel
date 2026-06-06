const http = require('http');
const WebSocket = require('ws');

// ─── Configuración ───────────────────────────────────────────────────────────
const MAX_PLAYERS        = parseInt(process.env.MAX_PLAYERS)  || 8;
const HEARTBEAT_INTERVAL = parseInt(process.env.HEARTBEAT_MS) || 25_000;
const PING_TIMEOUT       = 10_000;
const REJOIN_WINDOW      = parseInt(process.env.REJOIN_MS)    || 60_000;
const MAX_CANVAS_STROKES = 500;
const RATE_LIMIT_MAX     = 50;
const RATE_LIMIT_WINDOW  = 1_000;
const SPAM_MAX           = 10;
const SPAM_WINDOW        = 8_000;
const EMPTY_ROOM_TTL     = 10 * 60_000;

// Salas públicas
const PUBLIC_MAX_PLAYERS  = 8;    // máx jugadores por sala pública
const PUBLIC_START_DELAY  = 30_000; // esperar 30s antes de iniciar si solo hay 1
const PUBLIC_MIN_PLAYERS  = 2;    // mínimo para arrancar antes del delay

// ─── Logger ──────────────────────────────────────────────────────────────────
const log = {
  info:  (room, msg) => console.log(`[${new Date().toISOString()}] [INFO]  [${room}] ${msg}`),
  warn:  (room, msg) => console.warn(`[${new Date().toISOString()}] [WARN]  [${room}] ${msg}`),
  error: (room, msg) => console.error(`[${new Date().toISOString()}] [ERROR] [${room}] ${msg}`),
};

// ─── Servidor HTTP ────────────────────────────────────────────────────────────
const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    const stats = Object.entries(rooms).map(([code, r]) => ({
      code, players: Object.keys(r.players).length,
      public: r.isPublic||false, canvas: r.canvas.length,
    }));
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify({ status: 'ok', rooms: stats }));
    return;
  }
  res.writeHead(200, { 'Access-Control-Allow-Origin': '*', 'Content-Type': 'text/plain' });
  res.end('Pincel WS Server OK');
});

const wss = new WebSocket.Server({ server });

// ─── Estado global ────────────────────────────────────────────────────────────
const rooms = {};

function getRoom(code) {
  if (!rooms[code]) {
    rooms[code] = {
      players: {}, canvas: [], gameState: null,
      emptyAt: null, isPublic: false, startTimer: null,
    };
  }
  return rooms[code];
}

// ─── Encontrar o crear sala pública ──────────────────────────────────────────
function findOrCreatePublicRoom() {
  // Buscar sala pública con hueco y que no haya empezado
  for (const [code, room] of Object.entries(rooms)) {
    if (room.isPublic && !room.gameState && Object.keys(room.players).length < PUBLIC_MAX_PLAYERS) {
      return { code, isNew: false };
    }
  }
  // Crear nueva sala pública
  const code = 'PUB_' + Math.random().toString(36).slice(2,6).toUpperCase();
  rooms[code] = {
    players: {}, canvas: [], gameState: null,
    emptyAt: null, isPublic: true, startTimer: null,
  };
  log.info(code, 'Nueva sala pública creada.');
  return { code, isNew: true };
}

// ─── Iniciar partida pública automáticamente ─────────────────────────────────
function schedulePublicStart(roomCode) {
  const room = rooms[roomCode];
  if (!room || room.startTimer) return;

  room.startTimer = setTimeout(() => {
    const room2 = rooms[roomCode];
    if (!room2) return;
    const playerCount = Object.keys(room2.players).length;
    if (playerCount < PUBLIC_MIN_PLAYERS) {
      log.info(roomCode, `Solo ${playerCount} jugador(es), esperando más...`);
      room2.startTimer = null;
      schedulePublicStart(roomCode); // reintentar
      return;
    }
    // Arrancar partida automáticamente
    log.info(roomCode, `Iniciando partida pública con ${playerCount} jugadores.`);
    room2.gameState = { active: true, round: 1 };
    const turnOrder = Object.keys(room2.players);
    shuffle(turnOrder);
    broadcast(room2, {
      type: 'game-start',
      data: { turnOrder, totalRounds: 3, turnTime: 90, roundNumber: 1, wordPool: null, wordChoiceCount: 3, maxHintsOverride: -1 },
      from: 'server', fromName: 'Servidor'
    });
    room2.startTimer = null;
  }, PUBLIC_START_DELAY);
}

function shuffle(a){for(let i=a.length-1;i>0;i--){const j=Math.floor(Math.random()*(i+1));[a[i],a[j]]=[a[j],a[i]];}}

// ─── Utilidades ───────────────────────────────────────────────────────────────
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
    id: pid, name: p.name, score: p.score,
    avatar: p.avatar || null,
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
    if (room.startTimer) { clearTimeout(room.startTimer); room.startTimer = null; }
    room.emptyAt = Date.now();
    log.info(roomCode, `Sala vacía, marcada para limpieza.`);
  }
}

function checkRateLimit(p) {
  const now = Date.now();
  if (!p.rateWindow || now - p.rateWindow > RATE_LIMIT_WINDOW) { p.rateWindow = now; p.rateCount = 0; }
  p.rateCount++;
  return p.rateCount <= RATE_LIMIT_MAX;
}

function checkSpam(p) {
  const now = Date.now();
  if (!p.spamWindow || now - p.spamWindow > SPAM_WINDOW) { p.spamWindow = now; p.spamCount = 0; }
  p.spamCount++;
  return p.spamCount <= SPAM_MAX;
}

// ─── Heartbeat ────────────────────────────────────────────────────────────────
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

// ─── Limpieza salas vacías ────────────────────────────────────────────────────
setInterval(() => {
  for (const [code, room] of Object.entries(rooms)) {
    if (room.emptyAt && Date.now() - room.emptyAt > EMPTY_ROOM_TTL) {
      delete rooms[code];
      log.info(code, `Sala eliminada por inactividad.`);
    }
  }
}, 60_000);

// ─── Conexión WebSocket ───────────────────────────────────────────────────────
wss.on('connection', (ws, req) => {
  let roomCode, id, name, isPublicRequest;

  try {
    const params = new URL(req.url, 'http://localhost').searchParams;
    roomCode = params.get('room') || 'default';
    id       = params.get('id')   || `anon_${Date.now()}`;
    name     = params.get('name') || 'Jugador';
    isPublicRequest = params.get('public') === '1';
  } catch (err) {
    log.error('?', `URL inválida: ${err.message}`);
    ws.close(); return;
  }

  // ── SALA PÚBLICA: asignar sala automáticamente ────────────────────────────
  if (roomCode === 'PUBLIC' || isPublicRequest) {
    const { code, isNew } = findOrCreatePublicRoom();
    roomCode = code;
    const room = rooms[roomCode];
    room.emptyAt = null;
    const isHost = Object.keys(room.players).length === 0;

    room.players[id] = {
      ws, name, score: 0, avatar: null,
      lastPing: null, disconnectedAt: null,
      rateWindow: null, rateCount: 0,
      spamWindow: null, spamCount: 0,
    };

    log.info(roomCode, `PÚBLICO ${name} (${id}). Jugadores: ${Object.keys(room.players).length}/${PUBLIC_MAX_PLAYERS}`);

    // Notificar al cliente la sala asignada
    send(ws, { type: 'publicAssigned', room: roomCode, isHost });

    // Avisar a los demás
    broadcast(room, { type: 'playerJoined', id, name }, id);

    // Programar inicio automático si hay suficientes jugadores
    if (Object.keys(room.players).length >= PUBLIC_MIN_PLAYERS) {
      schedulePublicStart(roomCode);
    }

    attachHandlers(ws, roomCode, id, name, room);
    return;
  }

  // ── SALA NORMAL ───────────────────────────────────────────────────────────
  const room = getRoom(roomCode);
  room.emptyAt = null;
  const existingPlayer = room.players[id];

  // REJOIN
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

  // NUEVA CONEXIÓN
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

// ─── Handlers ─────────────────────────────────────────────────────────────────
function attachHandlers(ws, roomCode, id, name, room) {

  ws.on('pong', () => { if (room.players[id]) room.players[id].lastPing = null; });

  ws.on('message', (data) => {
    const p = room.players[id];
    if (!p) return;

    if (!checkRateLimit(p)) { log.warn(roomCode, `Rate limit superado por ${name}`); return; }

    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }

    msg.id = id;
    msg.fromName = name;

    switch (msg.type) {

      case 'player-join': {
        if (msg.data?.avatar) p.avatar = msg.data.avatar;
        broadcast(room, { ...msg, data: { ...msg.data, avatar: p.avatar } }, id);
        // En sala pública, programar inicio si hay suficientes jugadores
        if (room.isPublic && Object.keys(room.players).length >= PUBLIC_MIN_PLAYERS && !room.gameState) {
          schedulePublicStart(roomCode);
        }
        break;
      }

      case 'draw': {
        room.canvas.push(msg);
        if (room.canvas.length > MAX_CANVAS_STROKES) room.canvas = room.canvas.slice(-MAX_CANVAS_STROKES);
        broadcast(room, msg, id);
        break;
      }

      case 'shape': {
        // Formas no se guardan en canvas (son eventos únicos)
        broadcast(room, msg, id);
        break;
      }

      case 'fill': {
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
        // Sala pública: reiniciar después de 10s
        if (room.isPublic) {
          setTimeout(() => {
            if (rooms[roomCode] && Object.keys(rooms[roomCode].players).length >= PUBLIC_MIN_PLAYERS) {
              log.info(roomCode, 'Sala pública reiniciando partida...');
              schedulePublicStart(roomCode);
            }
          }, 10_000);
        }
        broadcast(room, msg);
        break;
      }

      case 'gameState': {
        room.gameState = msg.state || null;
        broadcast(room, msg, id);
        break;
      }

      case 'hint': {
        broadcast(room, msg, id);
        break;
      }

      case 'guess': {
        if (!checkSpam(p)) {
          log.warn(roomCode, `SPAM detectado de ${name} (${id}), expulsando.`);
          send(ws, { type: 'kicked', reason: 'Has sido expulsado por enviar demasiados mensajes seguidos.' });
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
    if (p) {
      p.ws = null;
      p.disconnectedAt = Date.now();
      broadcast(room, { type: 'playerDisconnected', id, name });
    }
  });

  ws.on('error', (err) => {
    log.error(roomCode, `Error socket ${name} (${id}): ${err.message}`);
    try { ws.terminate(); } catch (_) {}
    const p = room.players[id];
    if (p) {
      p.ws = null;
      p.disconnectedAt = Date.now();
      broadcast(room, { type: 'playerDisconnected', id, name });
    }
  });
}

// ─── Graceful shutdown ────────────────────────────────────────────────────────
function shutdown(signal) {
  log.info('SERVER', `${signal} recibido, cerrando...`);
  for (const room of Object.values(rooms)) {
    for (const p of Object.values(room.players)) {
      if (p.ws && p.ws.readyState === WebSocket.OPEN) {
        try { p.ws.send(JSON.stringify({ type: 'serverRestart', msg: 'El servidor se reinicia, reconecta en unos segundos.' })); p.ws.close(); } catch (_) {}
      }
    }
  }
  server.close(() => { log.info('SERVER', 'Cerrado limpiamente.'); process.exit(0); });
  setTimeout(() => process.exit(1), 5000);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT',  () => shutdown('SIGINT'));
process.on('uncaughtException',  (err)    => log.error('SERVER', `uncaughtException: ${err.message}`));
process.on('unhandledRejection', (reason) => log.error('SERVER', `unhandledRejection: ${reason}`));

// ─── Arranque ─────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  log.info('SERVER', `🎨 Pincel en puerto ${PORT} | MAX=${MAX_PLAYERS} | REJOIN=${REJOIN_WINDOW/1000}s | SPAM=max ${SPAM_MAX}/${SPAM_WINDOW/1000}s | PUBLIC_START=${PUBLIC_START_DELAY/1000}s`);
});
