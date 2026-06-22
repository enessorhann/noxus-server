// NOXUS Signaling Server
// ------------------------------------------------------------
// This server does NOT carry any gameplay data (car positions, inputs,
// hits, etc). Its only job is to help two or more browsers find each
// other and exchange the handshake messages WebRTC needs (offers,
// answers, ICE candidates) so they can open a *direct* peer-to-peer
// connection. Once that connection is open, all real race data flows
// browser-to-browser — this server is just the matchmaking phone call
// that happens before that.
//
// Rooms are identified by a short code the host shares with friends.
// Anyone who connects with the same room code is relayed to everyone
// else already in that room.

const WebSocket = require('ws');
const http = require('http');

const PORT = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
  // simple health check endpoint — useful so Render.com (or any host)
  // can verify the service is alive, and so you can sanity-check the
  // deployed URL in a browser before wiring it into the game
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('NOXUS signaling server is running.\n');
});

const wss = new WebSocket.Server({ server });

// rooms[code] = Set of client sockets currently in that room
const rooms = new Map();

function generateRoomCode() {
  // 4 characters, uppercase letters + digits, easy to read aloud/type —
  // collisions are checked against currently-active rooms only
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // no O/0/I/1 to avoid ambiguity
  let code;
  do {
    code = '';
    for (let i = 0; i < 4; i++) code += chars[Math.floor(Math.random() * chars.length)];
  } while (rooms.has(code));
  return code;
}

wss.on('connection', (ws) => {
  ws.roomCode = null;
  ws.playerId = Math.random().toString(36).slice(2, 10);

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch (e) {
      return; // ignore malformed input rather than crashing the connection
    }

    if (msg.type === 'create_room') {
      const code = generateRoomCode();
      rooms.set(code, new Set([ws]));
      ws.roomCode = code;
      ws.send(JSON.stringify({ type: 'room_created', code, playerId: ws.playerId }));
      return;
    }

    if (msg.type === 'join_room') {
      const code = (msg.code || '').toUpperCase();
      const room = rooms.get(code);
      if (!room) {
        ws.send(JSON.stringify({ type: 'join_error', reason: 'Room not found' }));
        return;
      }
      if (room.size >= 4) {
        ws.send(JSON.stringify({ type: 'join_error', reason: 'Room is full (max 4 players)' }));
        return;
      }
      // tell the new player who's already here, so they can initiate a
      // WebRTC connection to each existing peer
      const existingIds = [...room].map(peer => peer.playerId);
      room.add(ws);
      ws.roomCode = code;
      ws.send(JSON.stringify({ type: 'room_joined', code, playerId: ws.playerId, peers: existingIds }));
      // tell everyone already in the room that a new peer arrived, so
      // THEY can also initiate a connection toward the new player
      room.forEach(peer => {
        if (peer !== ws) {
          peer.send(JSON.stringify({ type: 'peer_joined', playerId: ws.playerId }));
        }
      });
      return;
    }

    if (msg.type === 'signal') {
      // relay a WebRTC handshake payload (offer/answer/ICE candidate) to
      // one specific peer in the same room, identified by playerId —
      // this server never reads or interprets the payload itself
      const room = rooms.get(ws.roomCode);
      if (!room) return;
      const target = [...room].find(peer => peer.playerId === msg.targetId);
      if (target) {
        target.send(JSON.stringify({ type: 'signal', fromId: ws.playerId, payload: msg.payload }));
      }
      return;
    }
  });

  ws.on('close', () => {
    if (ws.roomCode && rooms.has(ws.roomCode)) {
      const room = rooms.get(ws.roomCode);
      room.delete(ws);
      room.forEach(peer => {
        peer.send(JSON.stringify({ type: 'peer_left', playerId: ws.playerId }));
      });
      if (room.size === 0) rooms.delete(ws.roomCode);
    }
  });
});

server.listen(PORT, () => {
  console.log('NOXUS signaling server listening on port ' + PORT);
});
