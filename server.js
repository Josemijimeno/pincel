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
  const name = params.get('name') || 'Jugador';

  if (!rooms[room]) rooms[room] = {};
  rooms[room][id] = ws;

  console.log(`[${room}] ${name} conectado. Jugadores: ${Object.keys(rooms[room]).length}`);

  // Notificar a todos que alguien entró
  broadcast(room, JSON.stringify({
    type: 'player-join',
    from: id,
    fromName: decodeURIComponent(name),
    data: { name: decodeURIComponent(name) }
  }));

  ws.on('message', (data) => {
    // Retransmitir a TODOS en la sala incluyendo al emisor
    broadcastAll(room, data.toString());
  });

  ws.on('close', () => {
    broadcast(room, JSON.stringify({
      type: 'player-leave',
      from: id,
      fromName: decodeURIComponent(name),
      data: {}
    }));
    if (rooms[room]) {
      delete rooms[room][id];
      if (Object.keys(rooms[room]).length === 0) delete rooms[room];
    }
    console.log(`[${room}] ${name} desconectado.`);
  });
});

function broadcast(room, msg) {
  Object.values(rooms[room] || {}).forEach(client => {
    if (client.readyState === WebSocket.OPEN) client.send(msg);
  });
}

function broadcastAll(room, msg) {
  Object.values(rooms[room] || {}).forEach(client => {
    if (client.readyState === WebSocket.OPEN) client.send(msg);
  });
}

server.listen(process.env.PORT || 3000, () => {
  console.log('Servidor Pincel corriendo');
});
