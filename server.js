const http = require('http');
const WebSocket = require('ws');

const server = http.createServer((req, res) => {
  res.writeHead(200, {'Access-Control-Allow-Origin':'*'});
  res.end('OK');
});

const wss = new WebSocket.Server({ server, perMessageDeflate: false });
const rooms = {};

wss.on('connection', (ws, req) => {
  const params = new URL(req.url, 'http://x').searchParams;
  const room = params.get('room');
  const id = params.get('id');
  const name = decodeURIComponent(params.get('name') || 'Jugador');

  if (!room || !id) { ws.close(); return; }
  if (!rooms[room]) rooms[room] = {};
  rooms[room][id] = { ws, name };

  // Decir al recién llegado quién ya está
  send(ws, { type:'room-state', data:{ players: Object.entries(rooms[room]).filter(([pid])=>pid!==id).map(([pid,p])=>({id:pid,name:p.name})) }});

  // Avisar a los demás
  broadcastExcept(room, id, { type:'player-join', from:id, fromName:name, data:{name} });

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch(e) { return; }
    // draw y canvas-sync solo a los demás
    if (msg.type==='draw' || msg.type==='canvas-sync' || msg.type==='player-join') {
      broadcastExcept(room, id, msg);
    } else {
      // Todo lo demás a todos incluido emisor
      broadcastAll(room, msg);
    }
  });

  ws.on('close', () => {
    if (!rooms[room]) return;
    delete rooms[room][id];
    if (Object.keys(rooms[room]).length === 0) { delete rooms[room]; return; }
    broadcastAll(room, { type:'player-leave', from:id, fromName:name, data:{} });
  });

  ws.on('error', ()=>{});

  // Ping para mantener conexión viva
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
});

// Heartbeat cada 25s para evitar desconexiones
setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) { ws.terminate(); return; }
    ws.isAlive = false;
    ws.ping();
  });
}, 25000);

function send(ws, obj) {
  try { if (ws.readyState===WebSocket.OPEN) ws.send(JSON.stringify(obj)); } catch(e){}
}
function broadcastAll(room, obj) {
  const str = JSON.stringify(obj);
  Object.values(rooms[room]||{}).forEach(p => {
    try { if(p.ws.readyState===WebSocket.OPEN) p.ws.send(str); } catch(e){}
  });
}
function broadcastExcept(room, excludeId, obj) {
  const str = JSON.stringify(obj);
  Object.entries(rooms[room]||{}).forEach(([pid,p]) => {
    if (pid!==excludeId) try { if(p.ws.readyState===WebSocket.OPEN) p.ws.send(str); } catch(e){}
  });
}

server.listen(process.env.PORT||3000, ()=>console.log('Pincel server OK'));
