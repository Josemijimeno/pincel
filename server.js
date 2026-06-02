const http = require('http');
const WebSocket = require('ws');

const server = http.createServer((req, res) => {
  res.writeHead(200, {'Access-Control-Allow-Origin': '*'});
  res.end('Pincel WS Server OK');
});

const wss = new WebSocket.Server({ server });
const rooms = {};

wss.on('connection', (ws, req) => {
  const params = new URL(req.url, 'http://localhost').searchParams;
  const room = params.get('room');
  const id = params.get('id');
  const name = decodeURIComponent(params.get('name') || 'Jugador');

  if (!rooms[room]) rooms[room] = {};
  rooms[room][id] = { ws, name, id };
  ws.roomId = room;
  ws.playerId = id;
  ws.playerName = name;

  console.log(`[${room}] + ${name} (${Object.keys(rooms[room]).length} jugadores)`);

  // Enviar al recién llegado la lista de jugadores existentes
  const existing = Object.entries(rooms[room])
    .filter(([pid]) => pid !== id)
    .map(([pid, p]) => ({ id: pid, name: p.name }));

  safeSend(ws, { type: 'room-state', data: { players: existing } });

  // Avisar a los demás de la llegada
  broadcastExcept(room, id, {
    type: 'player-join',
    from: id,
    fromName: name,
    data: { name }
  });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch(e) { return; }

    if (msg.type === 'player-join') return; // ya lo mandamos arriba

    if (msg.type === 'draw' || msg.type === 'canvas-sync') {
      // Solo a los demás para no duplicar
      broadcastExcept(room, id, msg);
    } else {
      // Todo lo demás (guess, chat, turn-end, game-start, etc.) a TODOS
      broadcastAll(room, msg);
    }
  });

  ws.on('close', () => {
    if (!rooms[room]) return;
    delete rooms[room][id];
    console.log(`[${room}] - ${name} (${Object.keys(rooms[room]).length} jugadores)`);
    if (Object.keys(rooms[room]).length === 0) { delete rooms[room]; return; }
    broadcastAll(room, { type: 'player-leave', from: id, fromName: name, data: {} });
  });

  ws.on('error', (e) => console.error(`Error ${name}:`, e.message));
});

function safeSend(ws, obj) {
  try {
    if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
  } catch(e) {}
}

function broadcastAll(room, obj) {
  const str = JSON.stringify(obj);
  Object.values(rooms[room] || {}).forEach(p => {
    try { if (p.ws.readyState === WebSocket.OPEN) p.ws.send(str); } catch(e) {}
  });
}

function broadcastExcept(room, excludeId, obj) {
  const str = JSON.stringify(obj);
  Object.entries(rooms[room] || {}).forEach(([pid, p]) => {
    if (pid !== excludeId) {
      try { if (p.ws.readyState === WebSocket.OPEN) p.ws.send(str); } catch(e) {}
    }
  });
}

server.listen(process.env.PORT || 3000, () => {
  console.log('Servidor Pincel OK en puerto', process.env.PORT || 3000);
});
