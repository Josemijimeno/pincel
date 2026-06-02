const http = require('http');
const WebSocket = require('ws');

// ─── Configuración ───────────────────────────────────────────────────────────
const MAX_PLAYERS        = 8;      // Límite de jugadores por sala
const HEARTBEAT_INTERVAL = 30_000; // Ping cada 30 s
const PING_TIMEOUT       = 10_000; // Tiempo máximo para responder al ping
const REJOIN_WINDOW      = 60_000; // 60 s para reconectarse antes de perder la partida
const MAX_CANVAS_STROKES = 500;    // Máximo de trazos guardados en memoria

// ─── Servidor HTTP ───────────────────────────────────────────────────────────
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Access-Control-Allow-Origin': '*', 'Content-Type': 'text/plain' });
  res.end('Pincel WS Server OK');
});

const wss = new WebSocket.Server({ server });

// ─── Estado global ───────────────────────────────────────────────────────────
// rooms[code] = {
//   players:  { [id]: { ws, name, score, lastPing, disconnectedAt } },
//   canvas:   [ ...strokes ],   ← snapshot de trazos
//   gameState: { active, round, drawerId, word } | null
// }
const rooms = {};

function getRoom(code) {
  if (!rooms[code]) {
    rooms[code] = { players: {}, canvas: [], gameState: null };
  }
  return rooms[code];
}

// ─── Utilidades de broadcast ─────────────────────────────────────────────────
function send(ws, obj) {
  if (ws.readyState === WebSocket.OPEN) {
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

// ─── Eliminar jugador de la sala ─────────────────────────────────────────────
function removePlayer(roomCode, id, notify = true) {
  const room = rooms[roomCode];
  if (!room) return;
  const p = room.players[id];
  delete room.players[id];
  if (notify && p) {
    broadcast(room, { type: 'playerLeft', id, name: p.name });
  }
  if (Object.keys(room.players).length === 0) {
    delete rooms[roomCode];
    console.log(`[${roomCode}] Sala vacía, eliminada.`);
  }
}

// ─── Heartbeat ───────────────────────────────────────────────────────────────
setInterval(() => {
  for (const [roomCode, room] of Object.entries(rooms)) {
    for (const [id, p] of Object.entries(room.players)) {

      // Jugador en estado "desconectado temporalmente" → esperar ventana de rejoin
      if (!p.ws || p.ws.readyState !== WebSocket.OPEN) {
        if (p.disconnectedAt && Date.now() - p.disconnectedAt > REJOIN_WINDOW) {
          console.log(`[${roomCode}] Ventana de rejoin expirada para ${p.name} (${id})`);
          removePlayer(roomCode, id);
        }
        continue;
      }

      // Timeout de ping sin respuesta
      if (p.lastPing && Date.now() - p.lastPing > HEARTBEAT_INTERVAL + PING_TIMEOUT) {
        console.log(`[${roomCode}] Timeout de ${p.name} (${id}), cerrando.`);
        p.ws.terminate();
        p.ws = null;
        p.disconnectedAt = Date.now();
        broadcast(room, { type: 'playerDisconnected', id, name: p.name });
        continue;
      }

      // Enviar ping normal
      try { p.ws.ping(); p.lastPing = Date.now(); } catch (_) {}
    }
  }
}, HEARTBEAT_INTERVAL);

// ─── Conexión WebSocket ──────────────────────────────────────────────────────
wss.on('connection', (ws, req) => {
  let roomCode, id, name;

  try {
    const params = new URL(req.url, 'http://localhost').searchParams;
    roomCode = params.get('room') || 'default';
    id       = params.get('id')   || `anon_${Date.now()}`;
    name     = params.get('name') || 'Jugador';
  } catch (err) {
    console.error('URL inválida:', err.message);
    ws.close();
    return;
  }

  const room = getRoom(roomCode);
  const existingPlayer = room.players[id];

  // ── REJOIN: el jugador ya existía (se cayó y vuelve) ──────────────────────
  if (existingPlayer) {
    existingPlayer.ws = ws;
    existingPlayer.disconnectedAt = null;
    existingPlayer.lastPing = null;
    console.log(`[${roomCode}] 🔄 REJOIN de ${name} (${id}). Puntos recuperados: ${existingPlayer.score}`);

    // Confirmar rejoin con su estado guardado
    send(ws, {
      type: 'rejoinOk',
      score: existingPlayer.score,
      gameState: room.gameState,
      players: Object.entries(room.players).map(([pid, p]) => ({
        id: pid, name: p.name, score: p.score,
        connected: p.ws && p.ws.readyState === WebSocket.OPEN
      }))
    });

    // Mandar snapshot del canvas
    if (room.canvas.length > 0) {
      send(ws, { type: 'canvasSnapshot', strokes: room.canvas });
    }

    // Avisar a los demás
    broadcast(room, { type: 'playerRejoined', id, name }, id);

    attachHandlers(ws, ws, roomCode, id, name, room);
    return;
  }

  // ── NUEVA CONEXIÓN ────────────────────────────────────────────────────────

  // Límite de jugadores
  const activePlayers = Object.keys(room.players).length;
  if (activePlayers >= MAX_PLAYERS) {
    console.log(`[${roomCode}] Sala llena (${MAX_PLAYERS}), rechazando a ${name}`);
    send(ws, { type: 'roomFull', max: MAX_PLAYERS });
    ws.close();
    return;
  }

  // Registrar jugador nuevo
  room.players[id] = { ws, name, score: 0, lastPing: null, disconnectedAt: null };
  console.log(`[${roomCode}] ✅ ${name} (${id}) conectado. Jugadores: ${activePlayers + 1}/${MAX_PLAYERS}`);

  // Enviar estado inicial: lista de jugadores actuales
  send(ws, {
    type: 'welcome',
    id,
    players: Object.entries(room.players).map(([pid, p]) => ({
      id: pid, name: p.name, score: p.score,
      connected: p.ws && p.ws.readyState === WebSocket.OPEN
    }))
  });

  // Enviar snapshot del canvas si hay trazos
  if (room.canvas.length > 0) {
    send(ws, { type: 'canvasSnapshot', strokes: room.canvas });
  }

  // Avisar al resto
  broadcast(room, { type: 'playerJoined', id, name }, id);

  attachHandlers(ws, ws, roomCode, id, name, room);
});

// ─── Handlers de mensajes / desconexión ─────────────────────────────────────
function attachHandlers(ws, _ws, roomCode, id, name, room) {

  ws.on('pong', () => {
    if (room.players[id]) room.players[id].lastPing = null;
  });

  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); }
    catch { return; }

    msg.id = id; // El id siempre viene del servidor, no del cliente

    switch (msg.type) {

      // Trazo de dibujo → guardar en canvas + broadcast
      case 'draw': {
        room.canvas.push(msg);
        if (room.canvas.length > MAX_CANVAS_STROKES) {
          room.canvas = room.canvas.slice(-MAX_CANVAS_STROKES);
        }
        broadcast(room, msg, id);
        break;
      }

      // Limpiar canvas
      case 'clearCanvas': {
        room.canvas = [];
        broadcast(room, msg);
        break;
      }

      // Actualización del estado de partida (lo manda el host)
      case 'gameState': {
        room.gameState = msg.state || null;
        broadcast(room, msg, id);
        break;
      }

      // Mensaje directo a un jugador concreto (ej. datos privados)
      case 'direct': {
        const target = room.players[msg.to];
        if (target?.ws?.readyState === WebSocket.OPEN) {
          try { target.ws.send(JSON.stringify(msg)); } catch (_) {}
        }
        break;
      }

      // Keepalive manual desde el cliente
      case 'ping':
        send(ws, { type: 'pong' });
        break;

      // Resto de mensajes (chat, guess, score, etc.) → broadcast completo
      default:
        broadcast(room, msg);
        break;
    }
  });

  ws.on('close', (code) => {
    console.log(`[${roomCode}] ⏸ ${name} (${id}) desconectado. Código: ${code}`);
    const p = room.players[id];
    if (p) {
      p.ws = null;
      p.disconnectedAt = Date.now();
      // Avisar que está temporalmente fuera (no eliminado aún)
      broadcast(room, { type: 'playerDisconnected', id, name });
    }
  });

  ws.on('error', (err) => {
    console.error(`[${roomCode}] Error socket ${id}:`, err.message);
    try { ws.terminate(); } catch (_) {}
    const p = room.players[id];
    if (p) {
      p.ws = null;
      p.disconnectedAt = Date.now();
      broadcast(room, { type: 'playerDisconnected', id, name });
    }
  });
}

// ─── Arranque ────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🎨 Servidor Pincel en puerto ${PORT} | Límite: ${MAX_PLAYERS} jugadores/sala | Rejoin: ${REJOIN_WINDOW / 1000}s`);
});

process.on('uncaughtException',  (err)    => console.error('uncaughtException:', err.message));
process.on('unhandledRejection', (reason) => console.error('unhandledRejection:', reason));
