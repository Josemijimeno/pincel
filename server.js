const http = require('http');
const WebSocket = require('ws');

const server = http.createServer((req, res) => {
  res.writeHead(200);
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

  ws.on('message', (data) => {
    const msg = data.toString();
    Object.values(rooms[room] || {}).forEach(client => {
      if (client.readyState === WebSocket.OPEN) client.send(msg);
    });
  });

  ws.on('close', () => {
    if (rooms[room]) {
      delete rooms[room][id];
      if (Object.keys(rooms[room]).length === 0) delete rooms[room];
    }
  });
});

server.listen(process.env.PORT || 3000, () => {
  console.log('Servidor Pincel corriendo');
});
