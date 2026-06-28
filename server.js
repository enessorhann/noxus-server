// NOXUS — static file server + WebRTC signaling server, combined.
//
// Serves index.html (and any other files in this folder) over plain
// HTTP/HTTPS, and upgrades the SAME connection to WebSocket for
// signaling. Render terminates TLS for you, so one deployed URL gives
// players both "https://...onrender.com" to load the page and
// "wss://...onrender.com" to connect for multiplayer — which is why
// index.html can auto-fill the signaling field from location.host.
//
// This file ONLY relays room membership and WebRTC handshake messages
// (offer/answer/ICE candidates) between browsers. Actual race data
// (position, speed, hits) never touches this server — it flows directly
// peer-to-peer over the WebRTC DataChannel once two browsers connect.
//
// EKLENEN (Public Plaza): plazaRelay.js modülü 3 noktadan bağlandı —
// (1) require, (2) ws.on('message') içinde switch'ten önce, (3) ws.on('close')
// içinde. Bu, mevcut create_room/join_room/signal sistemine HİÇ dokunmuyor;
// plazaRelay yalnızca kendi mesajlarını (join_public_plaza, list_servers,
// plaza_state, party_* vb.) işler, diğerlerini mevcut switch'e bırakır.

const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

// (1) Public Plaza relay modülü — plazaRelay.js bu dosyayla AYNI klasörde olmalı.
const { handlePlazaMessage, handlePlazaDisconnect } = require('./plazaRelay');

const PORT = process.env.PORT || 3000;
const PUBLIC_DIR = __dirname;
const MAX_PLAYERS_PER_ROOM = 4;

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
};

// ---- Static file server ----
const server = http.createServer((req, res) => {
  let urlPath = decodeURIComponent(req.url.split('?')[0]);
  if (urlPath === '/') urlPath = '/index.html';

  const filePath = path.normalize(path.join(PUBLIC_DIR, urlPath));
  if (!filePath.startsWith(PUBLIC_DIR)) {
    res.writeHead(403);
    res.end('Forbidden');
    return;
  }

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('Not found');
      return;
    }
    const ext = path.extname(filePath).toLowerCase();
    res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream' });
    res.end(data);
  });
});

// ---- WebSocket signaling server, same HTTP server/port ----
const wss = new WebSocket.Server({ server });

const rooms = new Map();   // roomCode -> Set<playerId>
const players = new Map(); // playerId -> { ws, roomCode }

const ROOM_CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // no 0/O/1/I ambiguity

function genPlayerId() {
  return Math.random().toString(36).slice(2, 10);
}

function genRoomCode() {
  let code;
  do {
    code = '';
    for (let i = 0; i < 4; i++) {
      code += ROOM_CODE_CHARS[Math.floor(Math.random() * ROOM_CODE_CHARS.length)];
    }
  } while (rooms.has(code));
  return code;
}

function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function leaveRoom(playerId) {
  const player = players.get(playerId);
  if (!player || !player.roomCode) return;
  const room = rooms.get(player.roomCode);
  if (room) {
    room.delete(playerId);
    room.forEach((peerId) => {
      const peer = players.get(peerId);
      if (peer) send(peer.ws, { type: 'peer_left', playerId });
    });
    if (room.size === 0) rooms.delete(player.roomCode);
  }
  player.roomCode = null;
}

wss.on('connection', (ws) => {
  const playerId = genPlayerId();
  players.set(playerId, { ws, roomCode: null });

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch (e) {
      return;
    }

    // (2) Public Plaza mesajlarını önce plazaRelay'e sor. Kendi mesajıysa
    //     orada işlenir ve true döner -> burada dururuz. Değilse false döner
    //     ve aşağıdaki mevcut switch (create_room/join_room/signal) devam eder.
    //     plazaRelay'in soketi tanıması için ws.playerId'yi ona veriyoruz.
    ws.playerId = playerId;
    if (handlePlazaMessage(ws, msg)) return;

    const player = players.get(playerId);
    if (!player) return;

    if (msg.type === 'create_room') {
      const code = genRoomCode();
      rooms.set(code, new Set([playerId]));
      player.roomCode = code;
      send(ws, { type: 'room_created', playerId, code });
    } else if (msg.type === 'join_room') {
      const code = String(msg.code || '').toUpperCase();
      const room = rooms.get(code);
      if (!room) {
        send(ws, { type: 'join_error', reason: 'Room not found.' });
        return;
      }
      if (room.size >= MAX_PLAYERS_PER_ROOM) {
        send(ws, { type: 'join_error', reason: 'Room is full.' });
        return;
      }
      const existingPeers = Array.from(room);
      room.add(playerId);
      player.roomCode = code;
      send(ws, { type: 'room_joined', playerId, code, peers: existingPeers });
      existingPeers.forEach((peerId) => {
        const peer = players.get(peerId);
        if (peer) send(peer.ws, { type: 'peer_joined', playerId });
      });
    } else if (msg.type === 'signal') {
      const target = players.get(msg.targetId);
      if (target) {
        send(target.ws, { type: 'signal', fromId: playerId, payload: msg.payload });
      }
    }
  });

  ws.on('close', () => {
    // (3) Public Plaza tarafının da bu soketin ayrıldığını bilmesi gerekiyor
    //     (shard'tan çıkar, gruptan çıkar, diğerlerine haber ver).
    handlePlazaDisconnect(ws);

    leaveRoom(playerId);
    players.delete(playerId);
  });
});

server.listen(PORT, () => {
  console.log('NOXUS server (static + signaling + plaza relay) listening on port ' + PORT);
});
