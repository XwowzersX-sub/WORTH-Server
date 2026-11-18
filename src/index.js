import express from 'express';
import cors from 'cors';
import { createServer } from 'node:http';
import { randomUUID } from 'node:crypto';
import { WebSocket, WebSocketServer } from 'ws';
import { createClient } from '@supabase/supabase-js';

const PORT = Number(process.env.PORT || 10000);
const CLIENT_ORIGINS = (process.env.CLIENT_ORIGIN || '*').split(',').map(v => v.trim());
const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_PUBLISHABLE_KEY = process.env.SUPABASE_PUBLISHABLE_KEY || '';
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || '';

const authClient = SUPABASE_URL && SUPABASE_PUBLISHABLE_KEY
  ? createClient(SUPABASE_URL, SUPABASE_PUBLISHABLE_KEY, { auth: { persistSession: false, autoRefreshToken: false } })
  : null;
const adminClient = SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY
  ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false, autoRefreshToken: false } })
  : null;

const app = express();
app.disable('x-powered-by');
app.use(cors({
  origin(origin, callback) {
    callback(null, !origin || CLIENT_ORIGINS.includes('*') || CLIENT_ORIGINS.includes(origin));
  }
}));
app.get('/health', (_req, res) => res.status(200).json({ ok: true, service: 'worth-server', rooms: rooms.size, queued: [...queues.values()].reduce((n, q) => n + q.length, 0) }));
app.get('/', (_req, res) => res.json({ service: 'WORTH multiplayer', status: 'online', websocket: '/ws', maps: MAPS, modes: MODES }));
const http = createServer(app);
const wss = new WebSocketServer({ noServer: true, maxPayload: 4096, perMessageDeflate: false });

const MAPS = ['Neon Foundry','Skybridge','Dustpoint','Glacier Run','Orbit Yard'];
const MODES = ['loadout','addition'];
const rooms = new Map();
const queues = new Map();
const sockets = new Map();
const TICK_MS = 33;
const SNAPSHOT_MS = 90;
const POINT_SECONDS = 5.5;
const WINS_TO_MATCH = 5;
const WINS_TO_ROUND = 5;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function safeName(value) {
  const clean = String(value || 'Rookie').replace(/[^a-zA-Z0-9 _-]/g, '').trim().slice(0, 16);
  return clean.length >= 3 ? clean : 'Rookie';
}
function send(client, payload) {
  if (client?.socket?.readyState === WebSocket.OPEN) {
    try { client.socket.send(JSON.stringify(payload)); } catch {}
  }
}
function validMap(value) { return MAPS.includes(value) ? value : MAPS[0]; }
function validMode(value) { return MODES.includes(value) ? value : 'loadout'; }
function clamp(value, min, max) { return Math.max(min, Math.min(max, value)); }
function safeUpgrades(input) {
  const keys = ['moveSpeed','jump','stamina','recharge','zoneControl'];
  const result = {};
  for (const key of keys) result[key] = clamp(Math.floor(Number(input?.[key]) || 0), 0, 20);
  return result;
}
function safeLoadout(input) {
  const allowed = {
    movement: ['Sprint Soles','Air Wing','Gravity Boots'],
    utility: ['Bounce Core','Time Bubble','Signal Beacon'],
    support: ['Shield Halo','Echo Trail','Boost Ring'],
    style: ['Aurora Trail','Pixel Spark','Comet Wake']
  };
  const result = {};
  for (const [slot, options] of Object.entries(allowed)) {
    result[slot] = options.includes(input?.[slot]) ? input[slot] : options[0];
  }
  return result;
}

async function verifyToken(token) {
  if (!authClient || typeof token !== 'string' || token.length < 20 || token.length > 4096) return null;
  try {
    const { data, error } = await authClient.auth.getUser(token);
    if (error || !data?.user) return null;
    return data.user;
  } catch {
    return null;
  }
}

function queueKey(map, mode) { return map + '::' + mode; }
function removeFromQueue(client) {
  if (!client.queueKey) return;
  const q = queues.get(client.queueKey) || [];
  const next = q.filter(item => item !== client);
  if (next.length) queues.set(client.queueKey, next);
  else queues.delete(client.queueKey);
  client.queueKey = null;
}
function queueClient(client, request) {
  if (client.roomId) return;
  removeFromQueue(client);
  const map = validMap(request.map);
  const mode = validMode(request.mode);
  client.loadout = safeLoadout(request.loadout);
  client.upgrades = safeUpgrades(request.upgrades);
  client.additionPoints = clamp(Math.floor(Number(request.additionPoints) || 0), 0, 1000);
  client.queueKey = queueKey(map, mode);
  let q = queues.get(client.queueKey) || [];
  q = q.filter(item => item.socket.readyState === WebSocket.OPEN && !item.roomId);
  const opponent = q.shift();
  if (opponent) {
    if (q.length) queues.set(client.queueKey, q); else queues.delete(client.queueKey);
    client.queueKey = null;
    opponent.queueKey = null;
    createRoom(opponent, client, map, mode);
  } else {
    q.push(client);
    queues.set(client.queueKey, q);
    send(client, { type: 'queued', position: 0, map, mode });
  }
}

function newPlayer(client, index) {
  return {
    id: client.id,
    name: client.name,
    userId: client.userId,
    client,
    x: index === 0 ? -7 : 7,
    y: 0,
    z: index === 0 ? 7 : -7,
    vx: 0, vy: 0, vz: 0,
    yaw: index === 0 ? Math.PI / 4 : -3 * Math.PI / 4,
    onGround: true,
    input: { keys: [], yaw: 0, roll: false, crouch: false, aim: false },
    previousKeys: new Set(),
    score: 0,
    roundWins: 0,
    additionPoints: client.additionPoints || 0,
    zoneHold: 0,
    lastSnapshot: 0,
    upgrades: client.upgrades || safeUpgrades({}),
    loadout: client.loadout || safeLoadout({})
  };
}
function createRoom(a, b, map, mode) {
  const room = {
    id: randomUUID(),
    map,
    mode,
    players: [newPlayer(a, 0), newPlayer(b, 1)],
    startedAt: Date.now(),
    lastSnapshot: 0,
    phase: 'playing',
    round: 1,
    resetAt: 0,
    zoneOwner: null,
    winnerId: null
  };
  rooms.set(room.id, room);
  a.roomId = room.id; b.roomId = room.id;
  for (const p of room.players) {
    send(p.client, { type: 'matchFound', roomId: room.id, playerId: p.id, map, mode, round: 1, winsToMatch: WINS_TO_MATCH });
  }
  broadcast(room, { type: 'notice', message: 'Match started. Hold the glowing center to score.' });
}
function broadcast(room, payload) { for (const p of room.players) send(p.client, payload); }

function resetRound(room) {
  room.round += 1;
  room.phase = 'playing';
  room.zoneOwner = null;
  for (let i = 0; i < room.players.length; i++) {
    const p = room.players[i];
    p.x = i === 0 ? -7 : 7;
    p.y = 0;
    p.z = i === 0 ? 7 : -7;
    p.vx = 0; p.vy = 0; p.vz = 0;
    p.score = 0;
    p.zoneHold = 0;
    p.previousKeys.clear();
  }
}
async function saveMatchResult(room, winnerId) {
  if (!adminClient) return;
  await Promise.all(room.players.map(async p => {
    if (!p.userId || !UUID_RE.test(p.userId)) return;
    const won = p.id === winnerId;
    const { data, error: readError } = await adminClient
      .from('worth_player_data')
      .select('nickname,credits,match_wins,match_losses,round_wins,addition_points,upgrades,loadout,contracts')
      .eq('user_id', p.userId).maybeSingle();
    if (readError) { console.error('WORTH progress read failed:', readError.message); return; }
    const current = data || {};
    const credits = Math.max(0, Number(current.credits) || 0) + (won ? 350 : 100);
    const payload = {
      user_id: p.userId,
      nickname: p.name,
      credits,
      match_wins: (Number(current.match_wins) || 0) + (won ? 1 : 0),
      match_losses: (Number(current.match_losses) || 0) + (won ? 0 : 1),
      round_wins: (Number(current.round_wins) || 0) + p.roundWins,
      addition_points: p.additionPoints,
      upgrades: p.upgrades,
      loadout: p.loadout,
      contracts: current.contracts || {},
      updated_at: new Date().toISOString()
    };
    const { error } = await adminClient.from('worth_player_data').upsert(payload, { onConflict: 'user_id' });
    if (error) console.error('WORTH progress save failed:', error.message);
    send(p.client, { type: 'creditsAwarded', credits: won ? 350 : 100, totalCredits: credits });
  }));
}

function endRound(room, winner) {
  if (room.phase !== 'playing') return;
  room.phase = 'roundEnd';
  winner.roundWins += 1;
  const [a, b] = room.players;
  for (const p of room.players) {
    if (room.mode === 'addition' && p.id !== winner.id) p.additionPoints += 3;
    send(p.client, {
      type: 'roundEnd',
      winner: p.id === winner.id ? 'self' : 'opponent',
      selfWins: p.roundWins,
      opponentWins: room.players.find(other => other.id !== p.id).roundWins,
      additionPoints: p.additionPoints,
      round: room.round
    });
  }
  if (winner.roundWins >= WINS_TO_MATCH) {
    room.phase = 'matchEnd';
    room.winnerId = winner.id;
    for (const p of room.players) {
      send(p.client, {
        type: 'matchEnd',
        winner: p.id === winner.id ? 'self' : 'opponent',
        selfWins: p.roundWins,
        opponentWins: room.players.find(other => other.id !== p.id).roundWins
      });
    }
    void saveMatchResult(room, winner.id);
    setTimeout(() => {
      for (const p of room.players) {
        p.client.roomId = null;
        if (p.client.socket.readyState === WebSocket.OPEN) send(p.client, { type: 'backToLobby' });
      }
      rooms.delete(room.id);
    }, 1800);
  } else {
    room.resetAt = Date.now() + 1800;
  }
}
function updatePlayer(player, dt) {
  const keys = new Set(player.input.keys || []);
  const forward = { x: Math.sin(player.yaw), z: -Math.cos(player.yaw) };
  const right = { x: Math.cos(player.yaw), z: Math.sin(player.yaw) };
  let ix = 0, iz = 0;
  if (keys.has('KeyW')) { ix += forward.x; iz += forward.z; }
  if (keys.has('KeyS')) { ix -= forward.x; iz -= forward.z; }
  if (keys.has('KeyD')) { ix += right.x; iz += right.z; }
  if (keys.has('KeyA')) { ix -= right.x; iz -= right.z; }
  const length = Math.hypot(ix, iz);
  if (length > 0) { ix /= length; iz /= length; }
  let speed = 6.0 + player.upgrades.moveSpeed * .28;
  if (player.input.crouch) speed *= .55;
  if (player.input.roll) speed *= 1.65;
  const targetX = ix * speed, targetZ = iz * speed;
  const smoothing = 1 - Math.exp(-11 * dt);
  player.vx += (targetX - player.vx) * smoothing;
  player.vz += (targetZ - player.vz) * smoothing;
  if (player.input.roll && length < .01) {
    const d = Math.hypot(player.input.rollX || 0, player.input.rollZ || -1) || 1;
    player.vx = ((player.input.rollX || 0) / d) * speed;
    player.vz = ((player.input.rollZ || -1) / d) * speed;
  }
  if (keys.has('Space') && !player.previousKeys.has('Space') && player.onGround) {
    player.vy = 8.1 + player.upgrades.jump * .26;
    player.onGround = false;
  }
  player.previousKeys = keys;
  player.vy -= 22 * dt;
  player.x = clamp(player.x + player.vx * dt, -17.5, 17.5);
  player.z = clamp(player.z + player.vz * dt, -17.5, 17.5);
  player.y += player.vy * dt;
  if (player.y < 0) { player.y = 0; player.vy = 0; player.onGround = true; }
}
function tickRoom(room, dt, now) {
  if (room.phase !== 'playing') {
    if (room.phase === 'roundEnd' && now >= room.resetAt) resetRound(room);
    return;
  }
  // Focus mode is shared: when either competitor uses the head camera, the whole arena slows evenly.
  const timeScale = room.players.some(p => p.input.aim) ? 0.38 : 1;
  const simulationDt = dt * timeScale;
  for (const p of room.players) updatePlayer(p, simulationDt);
  const [a, b] = room.players;
  const inA = Math.hypot(a.x, a.z) < 2.7;
  const inB = Math.hypot(b.x, b.z) < 2.7;
  if (inA && !inB) { a.zoneHold += simulationDt * (1 + a.upgrades.zoneControl * .06); b.zoneHold = Math.max(0, b.zoneHold - simulationDt * .5); room.zoneOwner = a.id; }
  else if (inB && !inA) { b.zoneHold += simulationDt * (1 + b.upgrades.zoneControl * .06); a.zoneHold = Math.max(0, a.zoneHold - simulationDt * .5); room.zoneOwner = b.id; }
  else if (inA && inB) { a.zoneHold = Math.max(0, a.zoneHold - simulationDt * .8); b.zoneHold = Math.max(0, b.zoneHold - simulationDt * .8); room.zoneOwner = 'contested'; }
  else { a.zoneHold = Math.max(0, a.zoneHold - simulationDt * .2); b.zoneHold = Math.max(0, b.zoneHold - simulationDt * .2); room.zoneOwner = null; }
  for (const p of room.players) {
    if (p.zoneHold >= POINT_SECONDS) {
      p.zoneHold = 0; p.score += 1;
      if (p.score >= WINS_TO_ROUND) { endRound(room, p); return; }
    }
  }
  if (now - room.lastSnapshot >= SNAPSHOT_MS) {
    room.lastSnapshot = now;
    for (const p of room.players) {
      const opponent = room.players.find(other => other.id !== p.id);
      send(p.client, {
        type: 'snapshot',
        timeScale,
        selfId: p.id,
        map: room.map,
        players: room.players.map(q => ({ id: q.id, name: q.name, x: q.x, y: q.y, z: q.z, yaw: q.yaw })),
        score: { self: p.score, opponent: opponent.score },
        roundWins: { self: p.roundWins, opponent: opponent.roundWins },
        zoneOwner: room.zoneOwner === p.id ? 'self' : room.zoneOwner === opponent.id ? 'opponent' : room.zoneOwner === 'contested' ? 'contested' : null,
        serverTime: now
      });
    }
  }
}

async function handleMessage(client, raw) {
  let msg;
  try { msg = JSON.parse(raw.toString()); } catch { send(client, { type: 'error', message: 'Invalid message format.' }); return; }
  if (!msg || typeof msg.type !== 'string') return;
  if (msg.type === 'hello') {
    client.authPromise = (async () => {
      const user = await verifyToken(msg.token);
      client.userId = user?.id || null;
      client.id = user?.id || client.guestId;
      client.name = safeName(msg.nickname || user?.user_metadata?.nickname || 'Rookie');
      send(client, { type: 'hello', playerId: client.id, authenticated: !!user, nickname: client.name });
    })();
    await client.authPromise;
    return;
  }
  if (msg.type === 'queue') {
    await client.authPromise;
    if (client.socket.readyState !== WebSocket.OPEN) return;
    queueClient(client, msg);
    return;
  }
  if (msg.type === 'cancelQueue') { removeFromQueue(client); return; }
  if (msg.type === 'profileUpdate') {
    const room = rooms.get(client.roomId);
    const p = room?.players.find(q => q.client === client);
    if (p) {
      p.loadout = safeLoadout(msg.loadout);
      if (room.mode === 'addition') {
        p.upgrades = safeUpgrades(msg.upgrades);
        p.additionPoints = clamp(Math.floor(Number(msg.additionPoints) || 0), 0, 1000);
      }
    }
    return;
  }
  if (msg.type === 'input') {
    const room = rooms.get(client.roomId);
    const p = room?.players.find(q => q.client === client);
    if (!p || room.phase !== 'playing') return;
    const now = Date.now();
    if (now - client.lastInputAt < 24) return;
    client.lastInputAt = now;
    const allowed = new Set(['KeyW','KeyA','KeyS','KeyD','Space']);
    p.input.keys = Array.isArray(msg.keys) ? msg.keys.filter(k => allowed.has(k)) : [];
    p.input.yaw = Number.isFinite(msg.yaw) ? msg.yaw : p.yaw;
    p.yaw = p.input.yaw;
    p.input.roll = !!msg.roll;
    p.input.crouch = !!msg.crouch;
    p.input.aim = !!msg.aim;
    p.input.rollX = Math.sin(p.yaw);
    p.input.rollZ = -Math.cos(p.yaw);
  }
}

http.on('upgrade', (request, socket, head) => {
  const origin = request.headers.origin;
  if (origin && !CLIENT_ORIGINS.includes('*') && !CLIENT_ORIGINS.includes(origin)) {
    socket.write('HTTP/1.1 403 Forbidden\r\n\r\n'); socket.destroy(); return;
  }
  if (request.url?.split('?')[0] !== '/ws') {
    socket.write('HTTP/1.1 404 Not Found\r\n\r\n'); socket.destroy(); return;
  }
  wss.handleUpgrade(request, socket, head, ws => wss.emit('connection', ws, request));
});

wss.on('connection', socket => {
  const client = {
    socket, id: 'guest', guestId: 'guest_' + randomUUID(),
    userId: null, name: 'Rookie', queueKey: null, roomId: null,
    authPromise: Promise.resolve(), lastInputAt: 0
  };
  sockets.set(socket, client);
  send(client, { type: 'welcome', service: 'WORTH', protocol: 1, maps: MAPS, modes: MODES });
  socket.on('message', raw => { void handleMessage(client, raw).catch(err => console.error('WORTH message error:', err)); });
  socket.on('close', () => {
    removeFromQueue(client);
    const room = rooms.get(client.roomId);
    if (room && room.phase !== 'matchEnd') {
      const remaining = room.players.find(p => p.client !== client);
      if (remaining) {
        send(remaining.client, { type: 'matchEnd', winner: 'self', reason: 'opponent-disconnected', selfWins: remaining.roundWins, opponentWins: 0 });
        if (remaining.userId) void saveMatchResult(room, remaining.id);
        remaining.client.roomId = null;
      }
      rooms.delete(room.id);
    }
    sockets.delete(socket);
  });
  socket.on('error', err => console.error('WORTH socket error:', err.message));
});

let previousTick = Date.now();
setInterval(() => {
  const now = Date.now();
  const dt = clamp((now - previousTick) / 1000, 0, .08);
  previousTick = now;
  for (const room of rooms.values()) tickRoom(room, dt, now);
}, TICK_MS);

http.listen(PORT, '0.0.0.0', () => {
  console.log('WORTH server listening on', PORT);
  if (!adminClient) console.warn('SUPABASE_SERVICE_ROLE_KEY not configured; cloud progress writes are disabled.');
  if (!authClient) console.warn('Supabase auth verification is disabled; matches will use guest IDs.');
});

process.on('SIGTERM', () => {
  for (const client of sockets.values()) {
    send(client, { type: 'serverShutdown', message: 'Server restarting.' });
    try { client.socket.close(1001, 'Server shutdown'); } catch {}
  }
  http.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000).unref();
});
