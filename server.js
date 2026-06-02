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

  if (!rooms[room]) rooms[room] = {};
  rooms[room][id] = ws;

  // Enviar a este jugador la lista de jugadores ya conectados
  const existingPlayers = Object.keys(rooms[room])
    .filter(pid => pid !== id)
    .map(pid => rooms[room][pid].playerName || 'Jugador');

  ws.send(JSON.stringify({
    type: 'room-state',
    data: { players: Object.entries(rooms[room])
      .filter(([pid]) => pid !== id)
      .map(([pid, client]) => ({ id: pid, name: client.playerName || 'Jugador' }))
    }
  }));

  ws.on('message', (data) => {
    const msg = data.toString();
    try {
      const parsed = JSON.parse(msg);
      // Guardar nombre del jugador
      if (parsed.type === 'player-join') {
        ws.playerName = parsed.fromName;
        ws.playerId = parsed.from;
        // Reenviar SOLO a los demás, no al emisor
        Object.entries(rooms[room]).forEach(([pid, client]) => {
          if (pid !== id && client.readyState === WebSocket.OPEN) {
            client.send(msg);
          }
        });
      } else {
        // Todo lo demás se manda a todos incluido el emisor
        Object.values(rooms[room]).forEach(client => {
          if (client.readyState === WebSocket.OPEN) client.send(msg);
        });
      }
    } catch(e) {
      Object.values(rooms[room]).forEach(client => {
        if (client.readyState === WebSocket.OPEN) client.send(msg);
      });
    }
  });

  ws.on('close', () => {
    const name = ws.playerName || 'Jugador';
    delete rooms[room][id];
    if (Object.keys(rooms[room]).length === 0) delete rooms[room];
    // Notificar a los demás
    Object.values(rooms[room] || {}).forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(JSON.stringify({
          type: 'player-leave',
          from: id,
          fromName: name,
          data: {}
        }));
      }
    });
  });
});

server.listen(process.env.PORT || 3000, () => {
  console.log('Servidor Pincel corriendo');
});
