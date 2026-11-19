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
const MATCH_FORMATS = {
  '1v1': { size: 2, sides: 2, ffa: false },
  '2v2': { size: 4, sides: 2, ffa: false },
  '3v3': { size: 6, sides: 2, ffa: false },
  '5v5': { size: 10, sides: 2, ffa: false },
  '1v1v1': { size: 3, sides: 3, ffa: true }
};
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
function validMatchFormat(value) { return MATCH_FORMATS[value] ? value : '1v1'; }
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

function queueKey(format, mode) { return format + '::' + mode; }
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
  const format = validMatchFormat(request.matchFormat);
  const mode = validMode(request.mode);
  const required = MATCH_FORMATS[format].size;
  client.matchFormat = format;
  client.loadout = safeLoadout(request.loadout);
  client.upgrades = safeUpgrades(request.upgrades);
  client.additionPoints = clamp(Math.floor(Number(request.additionPoints) || 0), 0, 1000);
  client.queueKey = queueKey(format, mode);
  let q = queues.get(client.queueKey) || [];
  q = q.filter(item => item.socket.readyState === WebSocket.OPEN && !item.roomId && item !== client);
  q.push(client);
  if (q.length >= required) {
    const players = q.splice(0, required);
    for (const item of players) item.queueKey = null;
    if (q.length) {
      queues.set(queueKey(format, mode), q);
      for (const item of q) send(item, { type: 'queued', playersQueued: q.length, required, format, mode });
    } else queues.delete(queueKey(format, mode));
    createRoom(players, format, mode);
  } else {
    queues.set(client.queueKey, q);
    for (const item of q) send(item, { type: 'queued', playersQueued: q.length, required, format, mode });
  }
}

function spawnFor(index, format) {
  const config = MATCH_FORMATS[format];
  if (config.ffa) {
    const angle = index / config.size * Math.PI * 2;
    return { x: Math.sin(angle) * 12, z: Math.cos(angle) * 12, yaw: angle + Math.PI };
  }
  const side = index % 2;
  const slot = Math.floor(index / 2);
  const teamSize = config.size / 2;
  const z = (slot - (teamSize - 1) / 2) * 2.5;
  return { x: side === 0 ? -11 : 11, z, yaw: side === 0 ? Math.PI / 2 : -Math.PI / 2 };
}

function newPlayer(client, index, format) {
  const config = MATCH_FORMATS[format];
  const spawn = spawnFor(index, format);
  return {
    id: client.id,
    name: client.name,
    userId: client.userId,
    client,
    side: config.ffa ? index : index % 2,
    x: spawn.x,
    y: 0,
    z: spawn.z,
    vx: 0, vy: 0, vz: 0,
    yaw: spawn.yaw,
    onGround: true,
    input: { keys: [], yaw: spawn.yaw, roll: false, crouch: false, sprint: false, sprinting: false, aim: false },
    stamina: 100,
    previousKeys: new Set(),
    additionPoints: client.additionPoints || 0,
    lastSnapshot: 0,
    upgrades: client.upgrades || safeUpgrades({}),
    loadout: client.loadout || safeLoadout({})
  };
}

function sideVoteCounts(room) {
  const counts = Object.fromEntries(MAPS.map(map => [map, 0]));
  for (const map of room.votes.values()) if (Object.hasOwn(counts, map)) counts[map] += 1;
  return counts;
}

function startRoom(room) {
  if (room.phase !== 'mapVote') return;
  const counts = sideVoteCounts(room);
  const high = Math.max(0, ...Object.values(counts));
  const finalists = high === 0 ? MAPS : MAPS.filter(map => counts[map] === high);
  room.map = finalists[Math.floor(Math.random() * finalists.length)];
  room.phase = 'playing';
  room.startedAt = Date.now();
  broadcast(room, { type: 'voteEnd', map: room.map, counts });
  for (const p of room.players) {
    send(p.client, {
      type: 'matchFound',
      roomId: room.id,
      playerId: p.id,
      map: room.map,
      mode: room.mode,
      format: room.format,
      side: p.side,
      playerCount: room.players.length,
      round: 1,
      winsToMatch: WINS_TO_MATCH
    });
  }
  broadcast(room, { type: 'notice', message: 'Match started. Hold the glowing center to score.' });
}

function createRoom(clients, format, mode) {
  const config = MATCH_FORMATS[format];
  const room = {
    id: randomUUID(),
    map: null,
    mode,
    format,
    players: clients.map((client, index) => newPlayer(client, index, format)),
    sideCount: config.sides,
    sideScores: Array(config.sides).fill(0),
    sideRoundWins: Array(config.sides).fill(0),
    sideHolds: Array(config.sides).fill(0),
    votes: new Map(),
    voteDeadline: Date.now() + 15000,
    startedAt: Date.now(),
    lastSnapshot: 0,
    phase: 'mapVote',
    round: 1,
    resetAt: 0,
    zoneOwner: null,
    winnerSide: null
  };
  rooms.set(room.id, room);
  for (const p of room.players) p.client.roomId = room.id;
  const voteStart = {
    type: 'voteStart',
    maps: MAPS,
    counts: sideVoteCounts(room),
    playerCount: room.players.length,
    seconds: 15,
    format
  };
  broadcast(room, voteStart);
}

function broadcast(room, payload) {
  for (const p of room.players) send(p.client, payload);
}

function resetRound(room) {
  room.round += 1;
  room.phase = 'playing';
  room.zoneOwner = null;
  room.sideScores.fill(0);
  room.sideHolds.fill(0);
  for (let i = 0; i < room.players.length; i++) {
    const p = room.players[i];
    const spawn = spawnFor(i, room.format);
    p.x = spawn.x;
    p.y = 0;
    p.z = spawn.z;
    p.vx = 0; p.vy = 0; p.vz = 0;
    p.yaw = spawn.yaw;
    p.score = 0;
    p.stamina = 100;
    p.input.keys = [];
    p.input.roll = false;
    p.input.crouch = false;
    p.input.sprint = false;
    p.input.sprinting = false;
    p.input.aim = false;
    p.previousKeys.clear();
  }
}

async function saveMatchResult(room, winnerSide) {
  if (!adminClient) return;
  await Promise.all(room.players.map(async p => {
    if (!p.userId || !UUID_RE.test(p.userId)) return;
    const won = p.side === winnerSide;
    const { data, error: readError } = await adminClient
      .from('worth_player_data')
      .select('nickname,credits,match_wins,match_losses,round_wins,addition_points,upgrades,loadout,contracts,inventory')
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
      round_wins: (Number(current.round_wins) || 0) + (room.sideRoundWins[p.side] || 0),
      addition_points: p.additionPoints,
      upgrades: p.upgrades,
      loadout: p.loadout,
      contracts: current.contracts || {},
      inventory: Array.isArray(current.inventory) ? current.inventory : [],
      updated_at: new Date().toISOString()
    };
    const { error } = await adminClient.from('worth_player_data').upsert(payload, { onConflict: 'user_id' });
    if (error) console.error('WORTH progress save failed:', error.message);
    send(p.client, { type: 'creditsAwarded', credits: won ? 350 : 100, totalCredits: credits });
  }));
}

function finishMatch(room, winnerSide, reason = null) {
  if (room.phase === 'matchEnd') return;
  room.phase = 'matchEnd';
  room.winnerSide = winnerSide;
  for (const p of room.players) {
    const opponentWins = Math.max(0, ...room.sideRoundWins.filter((_, index) => index !== p.side));
    send(p.client, {
      type: 'matchEnd',
      winner: p.side === winnerSide ? 'self' : 'opponent',
      selfWins: room.sideRoundWins[p.side] || 0,
      opponentWins,
      reason
    });
  }
  void saveMatchResult(room, winnerSide);
  setTimeout(() => {
    for (const p of room.players) {
      p.client.roomId = null;
      if (p.client.socket.readyState === WebSocket.OPEN) send(p.client, { type: 'backToLobby' });
    }
    rooms.delete(room.id);
  }, 1800);
}

function endRound(room, winnerSide) {
  if (room.phase !== 'playing') return;
  room.phase = 'roundEnd';
  room.sideRoundWins[winnerSide] += 1;
  for (const p of room.players) {
    const opponentWins = Math.max(0, ...room.sideRoundWins.filter((_, index) => index !== p.side));
    if (room.mode === 'addition' && p.side !== winnerSide) p.additionPoints += 3;
    send(p.client, {
      type: 'roundEnd',
      winner: p.side === winnerSide ? 'self' : 'opponent',
      selfWins: room.sideRoundWins[p.side] || 0,
      opponentWins,
      additionPoints: p.additionPoints,
      round: room.round
    });
  }
  if (room.sideRoundWins[winnerSide] >= WINS_TO_MATCH) finishMatch(room, winnerSide);
  else room.resetAt = Date.now() + 1800;
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
  let length = Math.hypot(ix, iz);
  if (player.input.roll) {
    const rollLength = Math.hypot(player.input.rollX || 0, player.input.rollZ || -1) || 1;
    ix = (player.input.rollX || 0) / rollLength;
    iz = (player.input.rollZ || -1) / rollLength;
    length = 1;
  } else if (length > 0) {
    ix /= length; iz /= length;
  }
  const moving = length > .01;
  const sprinting = !!(player.input.sprint && moving && !player.input.crouch && !player.input.roll && player.stamina > 0);
  player.stamina = clamp(player.stamina + (sprinting ? -32 : 22) * dt, 0, 100);
  player.input.sprinting = sprinting;
  let speed = 6.1 + player.upgrades.moveSpeed * .35;
  if (sprinting) speed *= 1.48;
  if (player.input.crouch) speed *= .55;
  if (player.input.roll) speed *= 1.65;
  const targetX = ix * speed, targetZ = iz * speed;
  const smoothing = 1 - Math.exp(-11 * dt);
  player.vx += (targetX - player.vx) * smoothing;
  player.vz += (targetZ - player.vz) * smoothing;
  if (player.input.roll) {
    player.vx += (targetX - player.vx) * .22;
    player.vz += (targetZ - player.vz) * .22;
  }
  if (keys.has('Space') && !player.previousKeys.has('Space') && player.onGround) {
    player.vy = 8.1 + player.upgrades.jump * .35;
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
  if (room.phase === 'mapVote') {
    if (now >= room.voteDeadline) startRoom(room);
    return;
  }
  if (room.phase !== 'playing') {
    if (room.phase === 'roundEnd' && now >= room.resetAt) resetRound(room);
    return;
  }
  const timeScale = room.players.some(p => p.input.aim) ? 0.38 : 1;
  const simulationDt = dt * timeScale;
  for (const p of room.players) updatePlayer(p, simulationDt);

  const occupiedSides = new Set();
  for (const p of room.players) if (Math.hypot(p.x, p.z) < 2.7) occupiedSides.add(p.side);
  if (occupiedSides.size === 1) {
    const owningSide = [...occupiedSides][0];
    room.zoneOwner = owningSide;
    for (let side = 0; side < room.sideCount; side++) {
      if (side === owningSide) room.sideHolds[side] += simulationDt;
      else room.sideHolds[side] = Math.max(0, room.sideHolds[side] - simulationDt * 0.5);
    }
  } else if (occupiedSides.size > 1) {
    room.zoneOwner = 'contested';
    for (let side = 0; side < room.sideCount; side++) room.sideHolds[side] = Math.max(0, room.sideHolds[side] - simulationDt * 0.8);
  } else {
    room.zoneOwner = null;
    for (let side = 0; side < room.sideCount; side++) room.sideHolds[side] = Math.max(0, room.sideHolds[side] - simulationDt * 0.2);
  }

  for (let side = 0; side < room.sideCount; side++) {
    if (room.sideHolds[side] >= POINT_SECONDS) {
      room.sideHolds[side] = 0;
      room.sideScores[side] += 1;
      if (room.sideScores[side] >= WINS_TO_ROUND) {
        endRound(room, side);
        return;
      }
    }
  }

  if (now - room.lastSnapshot >= SNAPSHOT_MS) {
    room.lastSnapshot = now;
    for (const p of room.players) {
      const otherScores = room.sideScores.filter((_, index) => index !== p.side);
      const otherRoundWins = room.sideRoundWins.filter((_, index) => index !== p.side);
      send(p.client, {
        type: 'snapshot',
        timeScale,
        selfId: p.id,
        selfSide: p.side,
        format: room.format,
        map: room.map,
        players: room.players.map(q => ({ id: q.id, name: q.name, side: q.side, x: q.x, y: q.y, z: q.z, yaw: q.yaw, stamina: q.stamina, sprinting: q.input.sprinting, rolling: q.input.roll })),
        score: { self: room.sideScores[p.side] || 0, opponent: Math.max(0, ...otherScores) },
        roundWins: { self: room.sideRoundWins[p.side] || 0, opponent: Math.max(0, ...otherRoundWins) },
        zoneOwner: room.zoneOwner === p.side ? 'self' : room.zoneOwner === 'contested' ? 'contested' : room.zoneOwner === null ? null : 'opponent',
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
      // A player ID belongs to this live connection, not the account.
      // This allows the same account to open a second test session safely.
      client.id = client.guestId;
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
  if (msg.type === 'mapVote') {
    const room = rooms.get(client.roomId);
    if (!room || room.phase !== 'mapVote' || !MAPS.includes(msg.map)) return;
    room.votes.set(client.id, msg.map);
    broadcast(room, { type: 'voteUpdate', counts: sideVoteCounts(room), voted: room.votes.size, playerCount: room.players.length });
    if (room.votes.size >= room.players.length) startRoom(room);
    return;
  }
  if (msg.type === 'cancelQueue') { removeFromQueue(client); return; }
  if (msg.type === 'profileUpdate') {
    const room = rooms.get(client.roomId);
    const p = room?.players.find(q => q.client === client);
    if (p) {
      p.loadout = safeLoadout(msg.loadout);
      if (typeof msg.nickname === 'string') {
        client.name = safeName(msg.nickname);
        p.name = client.name;
      }
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
    p.input.sprint = !!msg.sprint;
    p.input.rollX = Number.isFinite(msg.rollX) ? clamp(msg.rollX, -1, 1) : Math.sin(p.yaw);
    p.input.rollZ = Number.isFinite(msg.rollZ) ? clamp(msg.rollZ, -1, 1) : -Math.cos(p.yaw);
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
      room.players = room.players.filter(p => p.client !== client);
      if (room.phase === 'mapVote') {
        for (const p of room.players) {
          p.client.roomId = null;
          send(p.client, { type: 'matchCancelled', message: 'A player left during map voting. Requeue when ready.' });
        }
        rooms.delete(room.id);
      } else if (!room.players.length) {
        rooms.delete(room.id);
      } else {
        const sides = [...new Set(room.players.map(p => p.side))];
        if (room.format === '1v1v1') {
          if (room.players.length === 1) finishMatch(room, room.players[0].side, 'opponent-disconnected');
        } else if (sides.length === 1) {
          finishMatch(room, sides[0], 'opponent-disconnected');
        } else {
          broadcast(room, { type: 'notice', message: 'A player disconnected. The match continues.' });
        }
      }
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
