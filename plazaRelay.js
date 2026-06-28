/**
 * plazaRelay.js
 *
 * MEVCUT sinyal sunucunuza (Render'da çalışan dosya) EKLENECEK bir modül.
 * O dosyayı görmediğim için doğrudan düzenleyemedim — bunun yerine bu
 * modülü dışa veriyorum, sen kendi server.js'inde şu 3 yeri bağlarsın:
 *
 *   1. const { handlePlazaMessage, handlePlazaDisconnect } = require('./plazaRelay');
 *   2. ws.on('message', (raw) => {
 *        const msg = JSON.parse(raw);
 *        if (handlePlazaMessage(ws, msg)) return;  // <-- BUNU EKLE (mevcut switch'ten ÖNCE)
 *        // ... senin mevcut create_room/join_room/signal/quick_match switch'in burada kalır
 *      });
 *   3. ws.on('close', () => {
 *        handlePlazaDisconnect(ws);                // <-- BUNU EKLE
 *        // ... senin mevcut peer_left/oda temizliği burada kalır
 *      });
 *
 * Neden ayrı bir modül: mevcut WebRTC mesh sisteminize (create_room/
 * join_room/signal) HİÇ dokunmuyor — küçük özel odalar için o sistem zaten
 * doğru çalışıyor. Public Plaza tamamen farklı bir mesaj tipi seti
 * kullanıyor, o yüzden `handlePlazaMessage` kendi tipi değilse `false`
 * döndürüp mevcut switch'inizin devreye girmesine izin veriyor.
 *
 * Mimari: index.html client'ı bu mod için WebRTC PeerConnection KURMUYOR —
 * herkes zaten açık olan TEK WebSocket'i (mp.ws) kullanıyor, sunucu
 * sadece "aynı shard'taki herkese ilet" yapıyor. 30 kişi = 30 bağlantı,
 * 30×29 RTCPeerConnection değil.
 *
 * Bu sürümde eklenen: tek-kişiye-davet yerine 2-8 kişilik GRUP (party)
 * kurma — bkz. PARTY bölümü altındaki case'ler. Bir oyuncu önce bir grup
 * açar (lider olur), istediği kadar başka oyuncuyu aynı gruba davet eder,
 * herkes kabul ettikten sonra LİDER "yarışı başlat" der ve grubun TÜM
 * üyeleri (shard'taki diğerleri etkilenmeden) yarış pistine geçer.
 */

const PLAZA_SHARD_SIZE = 30;
const MAX_PARTY_SIZE = 8;

/** @type {Map<string, Set<WebSocket>>} shardId -> bu shard'taki socket'ler */
const shards = new Map();
/** @type {Map<WebSocket, {id, shardId, name, vehicleModel, x, z, heading, modelKey, partyId}>} */
const plazaPlayers = new Map();
/** @type {Map<string, {partyId, shardId, hostId, memberIds: Set<string>}>} */
const parties = new Map();

let nextPlayerId = 1;
let nextShardId = 1;
let nextPartyId = 1;

// Sabit, isimli "dünya" havuzu (Kintara'daki seçilebilir sunucu listesi
// gibi). Her dünya bir shard'a karşılık gelir; oyuncu listeden seçer.
// Otomatik (serverId yok) katılımda boş olan ilk dünya kullanılır.
const WORLD_NAMES = ['NEON-1', 'NEON-2', 'NEON-3', 'NEON-4'];
function ensureWorldsExist() {
  for (const name of WORLD_NAMES) {
    if (!shards.has(name)) shards.set(name, new Set());
  }
}
ensureWorldsExist();

function getOrCreateAvailableShard() {
  ensureWorldsExist();
  // Önce isimli dünyalardan boş olan ilkini dene
  for (const name of WORLD_NAMES) {
    if (shards.get(name).size < PLAZA_SHARD_SIZE) return name;
  }
  // Hepsi doluysa taşma shard'ı aç (isimli havuz tükenince)
  for (const [shardId, members] of shards) {
    if (members.size < PLAZA_SHARD_SIZE) return shardId;
  }
  const shardId = 'shard_' + (nextShardId++);
  shards.set(shardId, new Set());
  return shardId;
}

// Belirli bir dünyaya (serverId) katılmaya çalışır; doluysa veya yoksa
// null döner (çağıran taraf otomatik atamaya düşer).
function resolveRequestedShard(serverId) {
  if (!serverId || serverId === 'auto') return null;
  const members = shards.get(serverId);
  if (members && members.size < PLAZA_SHARD_SIZE) return serverId;
  return null;
}

function send(ws, payload) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(payload));
}

function broadcastToShard(shardId, payload, exclude) {
  const members = shards.get(shardId);
  if (!members) return;
  for (const sock of members) {
    if (sock === exclude) continue;
    send(sock, payload);
  }
}

function findSocketByPlayerId(shardId, playerId) {
  const members = shards.get(shardId);
  if (!members) return null;
  for (const sock of members) {
    const p = plazaPlayers.get(sock);
    if (p && p.id === playerId) return sock;
  }
  return null;
}

/** Bir partinin tüm üyelerine güncel üye listesini yollar (isim dahil). */
function broadcastPartyUpdate(party) {
  const memberNames = {};
  const sockets = [];
  for (const memberId of party.memberIds) {
    const sock = findSocketByPlayerId(party.shardId, memberId);
    if (sock) {
      sockets.push(sock);
      memberNames[memberId] = plazaPlayers.get(sock).name;
    }
  }
  const payload = {
    type: 'party_member_update',
    partyId: party.partyId,
    hostId: party.hostId,
    memberIds: [...party.memberIds],
    memberNames,
  };
  sockets.forEach(sock => send(sock, payload));
}

function disbandParty(party, reason) {
  parties.delete(party.partyId);
  for (const memberId of party.memberIds) {
    const sock = findSocketByPlayerId(party.shardId, memberId);
    if (sock) {
      plazaPlayers.get(sock).partyId = null;
      send(sock, { type: 'party_disbanded', reason });
    }
  }
}

/**
 * Mesajı bu modül mü işleyecek, yoksa mevcut server'ınızın switch'ine mi
 * bırakılacak? `true` dönerse mesaj burada tamamen işlendi.
 */
function handlePlazaMessage(ws, msg) {
  switch (msg.type) {

    // Sunucu (dünya) listesi — client mpShowServerList'te bunu ister.
    case 'list_servers': {
      ensureWorldsExist();
      const servers = WORLD_NAMES.map(name => ({
        id: name,
        name: name,
        count: shards.get(name) ? shards.get(name).size : 0,
        capacity: PLAZA_SHARD_SIZE,
      }));
      send(ws, { type: 'server_list', servers });
      return true;
    }

    case 'join_public_plaza': {
      // Oyuncu listeden bir dünya seçtiyse (msg.serverId) ve o dünya
      // doluysa değilse oraya koy; aksi halde boş olan ilk dünyaya ata.
      const shardId = resolveRequestedShard(msg.serverId) || getOrCreateAvailableShard();
      const player = {
        id: 'p' + (nextPlayerId++),
        shardId,
        name: msg.name || 'Racer',
        modelKey: msg.vehicleModel || 'SHARP',
        x: 0, z: 0, heading: 0, speed: 0, hitCount: 0, lap: 1, wrecked: false,
        partyId: null,
      };
      plazaPlayers.set(ws, player);
      shards.get(shardId).add(ws);

      const existingPlayers = [...shards.get(shardId)]
        .filter(s => s !== ws)
        .map(s => plazaPlayers.get(s))
        .map(p => ({ id: p.id, name: p.name, modelKey: p.modelKey, x: p.x, z: p.z, heading: p.heading }));

      send(ws, { type: 'public_plaza_joined', myId: player.id, shardId, players: existingPlayers });
      broadcastToShard(shardId, { type: 'plaza_peer_joined', peerId: player.id, name: player.name }, ws);
      return true;
    }

    case 'plaza_state': {
      const player = plazaPlayers.get(ws);
      if (!player) return true;
      Object.assign(player, { x: msg.x, z: msg.z, heading: msg.heading, speed: msg.speed,
        hitCount: msg.hitCount, lap: msg.lap, wrecked: msg.wrecked, modelKey: msg.modelKey });
      broadcastToShard(player.shardId, Object.assign({ type: 'plaza_peer_state', peerId: player.id }, msg), ws);
      return true;
    }

    // Herkese (chat gibi) — sadece sıradan broadcast.
    case 'plaza_event': {
      const player = plazaPlayers.get(ws);
      if (!player) return true;
      broadcastToShard(player.shardId, Object.assign({ type: 'plaza_peer_event', peerId: player.id }, msg), ws);
      return true;
    }

    // Sadece TEK bir hedefe (davet, kabul/red gibi) — broadcast değil.
    case 'plaza_direct_event': {
      const player = plazaPlayers.get(ws);
      if (!player) return true;
      const target = findSocketByPlayerId(player.shardId, msg.targetId);
      if (target) send(target, Object.assign({ type: 'plaza_peer_event', peerId: player.id }, msg));
      return true;
    }

    // ============================================================
    // PARTY (2-8 kişilik yarış grubu)
    // ============================================================

    case 'create_party': {
      const player = plazaPlayers.get(ws);
      if (!player) return true;
      if (player.partyId && parties.has(player.partyId)) {
        // Zaten bir partinin içinde (lider veya üye) — onu tekrar kullan.
        send(ws, { type: 'party_created', partyId: player.partyId });
        return true;
      }
      const partyId = 'party_' + (nextPartyId++);
      const party = { partyId, shardId: player.shardId, hostId: player.id, memberIds: new Set([player.id]) };
      parties.set(partyId, party);
      player.partyId = partyId;
      send(ws, { type: 'party_created', partyId });
      return true;
    }

    case 'invite_to_party': {
      const player = plazaPlayers.get(ws);
      const party = parties.get(msg.partyId);
      if (!player || !party || party.hostId !== player.id) return true; // sadece lider davet edebilir
      if (party.memberIds.size >= MAX_PARTY_SIZE) {
        send(ws, { type: 'party_invite_failed', reason: 'Grup dolu (maks. ' + MAX_PARTY_SIZE + ')' });
        return true;
      }
      const target = findSocketByPlayerId(party.shardId, msg.targetId);
      if (target) {
        send(target, {
          type: 'plaza_peer_event', peerId: player.id,
          event: 'party_invite', partyId: party.partyId, from: player.name, laps: msg.laps,
        });
      }
      return true;
    }

    case 'accept_party_invite': {
      const player = plazaPlayers.get(ws);
      const party = parties.get(msg.partyId);
      if (!player || !party) {
        send(ws, { type: 'party_invite_failed', reason: 'Grup artık mevcut değil' });
        return true;
      }
      if (party.memberIds.size >= MAX_PARTY_SIZE) {
        send(ws, { type: 'party_invite_failed', reason: 'Grup dolu (maks. ' + MAX_PARTY_SIZE + ')' });
        return true;
      }
      party.memberIds.add(player.id);
      player.partyId = party.partyId;
      broadcastPartyUpdate(party);
      return true;
    }

    case 'leave_party': {
      const player = plazaPlayers.get(ws);
      const party = parties.get(msg.partyId);
      if (!player || !party) return true;
      party.memberIds.delete(player.id);
      player.partyId = null;
      if (party.hostId === player.id || party.memberIds.size === 0) {
        // Lider ayrılırsa grup dağılır (basitleştirme — yeni lider seçip
        // devam ettirmek yerine; küçük gruplar için bu genelde sorun değil).
        disbandParty(party, 'host_left');
      } else {
        broadcastPartyUpdate(party);
      }
      return true;
    }

    // Lider "YARIŞI BAŞLAT"a bastı — grubun TÜM üyelerine aynı anda
    // race_room_assigned gider, shard'taki DİĞER oyuncular hiç etkilenmez
    // (client tarafı zaten katılımcı olmayan uzak araçları kendi başına
    // park ediyor, bkz. mpBeginRaceFromPlaza).
    case 'start_party_race': {
      const player = plazaPlayers.get(ws);
      const party = parties.get(msg.partyId);
      if (!player || !party || party.hostId !== player.id) return true;
      if (party.memberIds.size < 2) return true; // yalnız başına "grup" yarışı anlamsız

      const participantIds = [...party.memberIds];
      for (const memberId of party.memberIds) {
        const sock = findSocketByPlayerId(party.shardId, memberId);
        if (sock) {
          send(sock, { type: 'race_room_assigned', participantIds });
          plazaPlayers.get(sock).partyId = null;
        }
      }
      parties.delete(party.partyId);
      return true;
    }

    case 'return_to_plaza': {
      const player = plazaPlayers.get(ws);
      if (player) player.wrecked = false;
      return true;
    }

    default:
      return false; // bu modülün mesajı değil — ana server switch'iniz devam etsin
  }
}

function handlePlazaDisconnect(ws) {
  const player = plazaPlayers.get(ws);
  if (!player) return;

  if (player.partyId) {
    const party = parties.get(player.partyId);
    if (party) {
      party.memberIds.delete(player.id);
      if (party.hostId === player.id || party.memberIds.size === 0) disbandParty(party, 'host_disconnected');
      else broadcastPartyUpdate(party);
    }
  }

  const members = shards.get(player.shardId);
  if (members) {
    members.delete(ws);
    broadcastToShard(player.shardId, { type: 'plaza_peer_left', peerId: player.id });
    // Sadece dinamik taşma shard'larını sil; sabit isimli dünyalar (NEON-*)
    // boşalsa bile kalır ki sunucu listesi her zaman onları gösterebilsin.
    if (members.size === 0 && !WORLD_NAMES.includes(player.shardId)) shards.delete(player.shardId);
  }
  plazaPlayers.delete(ws);
}

module.exports = { handlePlazaMessage, handlePlazaDisconnect, PLAZA_SHARD_SIZE, MAX_PARTY_SIZE };
