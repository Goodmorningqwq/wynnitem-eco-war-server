const express = require('express');
const http = require('http');
const crypto = require('crypto');
const { Server } = require('socket.io');

const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 3001;
const TICK_INTERVAL_MS = process.env.TICK_INTERVAL_MS
  ? parseInt(process.env.TICK_INTERVAL_MS, 10)
  : 6000;
const RATE_LIMIT_WINDOW_MS = process.env.RATE_LIMIT_WINDOW_MS
  ? parseInt(process.env.RATE_LIMIT_WINDOW_MS, 10)
  : 10000;
const RATE_LIMIT_MAX_SOCKET = process.env.RATE_LIMIT_MAX_SOCKET
  ? parseInt(process.env.RATE_LIMIT_MAX_SOCKET, 10)
  : 24;
const RATE_LIMIT_MAX_IP = process.env.RATE_LIMIT_MAX_IP
  ? parseInt(process.env.RATE_LIMIT_MAX_IP, 10)
  : 60;
const DISCONNECT_GRACE_MS = process.env.DISCONNECT_GRACE_MS
  ? parseInt(process.env.DISCONNECT_GRACE_MS, 10)
  : 60000;
const ECO_WAR_SHARED_TOKEN = typeof process.env.ECO_WAR_SHARED_TOKEN === 'string'
  ? process.env.ECO_WAR_SHARED_TOKEN.trim()
  : '';
const PREP_SECONDS = 60;
const TERRITORIES_URL =
  'https://raw.githubusercontent.com/jakematt123/Wynncraft-Territory-Info/main/territories.json';

const UPGRADE_COSTS = {
  damage: [0, 50, 120, 250, 450, 700, 1000, 1400, 1900, 2500, 3200, 4000], // ore
  attackSpeed: [0, 40, 100, 220, 400, 650, 950, 1350, 1800, 2400, 3100, 3900], // crops
  health: [0, 60, 140, 280, 500, 780, 1100, 1550, 2100, 2800, 3600, 4500], // wood
  defense: [0, 30, 80, 170, 320, 520, 780, 1100, 1500, 2000, 2600, 3300] // fish
};
const UPGRADE_CATEGORIES = ['damage', 'attackSpeed', 'health', 'defense'];
const UPGRADE_RESOURCE_BY_CATEGORY = {
  damage: 'ore',
  attackSpeed: 'crops',
  health: 'wood',
  defense: 'fish'
};

function buildAllowedOrigins() {
  const raw = typeof process.env.ALLOWED_ORIGINS === 'string' ? process.env.ALLOWED_ORIGINS : '';
  const fromEnv = raw
    .split(',')
    .map(function (value) {
      return value.trim();
    })
    .filter(function (value) {
      return value.length > 0;
    });
  const defaults = [
    'https://wynnitem-territory.vercel.app',
    'https://wynnitem-territories.vercel.app',
    'http://localhost:3000',
    'http://localhost:5173',
    'http://127.0.0.1:3000',
    'http://127.0.0.1:5173'
  ];
  return Array.from(new Set(fromEnv.concat(defaults)));
}

const ALLOWED_ORIGINS = buildAllowedOrigins();

function isOriginAllowed(origin) {
  if (!origin) return true;
  return ALLOWED_ORIGINS.indexOf(origin) !== -1;
}

const metrics = {
  startedAt: Date.now(),
  connectionsTotal: 0,
  disconnectionsTotal: 0,
  roomCreateTotal: 0,
  roomJoinTotal: 0,
  roomDeleteTotal: 0,
  resumeSuccessTotal: 0,
  resumeFailTotal: 0,
  graceExpiryTotal: 0,
  rateLimitRejectTotal: 0,
  authRejectTotal: 0,
  originRejectTotal: 0
};

/** @type {Map<string, { windowStart: number, counts: Record<string, number> }>} */
const socketRateState = new Map();
/** @type {Map<string, { windowStart: number, counts: Record<string, number> }>} */
const ipRateState = new Map();

function logEvent(event, detail) {
  const payload = {
    ts: new Date().toISOString(),
    event,
    detail: detail || {}
  };
  console.log(JSON.stringify(payload));
}

function getClientIp(socket) {
  const fwd = socket && socket.handshake && socket.handshake.headers
    ? socket.handshake.headers['x-forwarded-for']
    : '';
  if (typeof fwd === 'string' && fwd.trim()) {
    return fwd.split(',')[0].trim();
  }
  return socket && socket.handshake && socket.handshake.address
    ? String(socket.handshake.address)
    : 'unknown';
}

function tokenFromHandshake(socket) {
  const authToken = socket && socket.handshake && socket.handshake.auth
    ? socket.handshake.auth.token
    : '';
  if (typeof authToken === 'string' && authToken.trim()) {
    return authToken.trim();
  }
  const headerToken = socket && socket.handshake && socket.handshake.headers
    ? socket.handshake.headers['x-eco-war-token']
    : '';
  if (typeof headerToken === 'string' && headerToken.trim()) {
    return headerToken.trim();
  }
  return '';
}

function isPrivilegedAuthorized(socket) {
  if (!ECO_WAR_SHARED_TOKEN) return true;
  return tokenFromHandshake(socket) === ECO_WAR_SHARED_TOKEN;
}

function requirePrivilegedAccess(socket, ack, eventName) {
  if (isPrivilegedAuthorized(socket)) return true;
  metrics.authRejectTotal += 1;
  logEvent('auth_reject', {
    event: eventName,
    socketId: socket.id,
    ip: socket.data && socket.data.clientIp ? socket.data.clientIp : 'unknown'
  });
  if (typeof ack === 'function') {
    ack({ ok: false, error: 'Unauthorized action.' });
  }
  return false;
}

function consumeBucket(map, key, eventName, limit) {
  const now = Date.now();
  const current = map.get(key) || { windowStart: now, counts: {} };
  if (now - current.windowStart >= RATE_LIMIT_WINDOW_MS) {
    current.windowStart = now;
    current.counts = {};
  }
  const nextCount = (current.counts[eventName] || 0) + 1;
  current.counts[eventName] = nextCount;
  map.set(key, current);
  return nextCount <= limit;
}

function allowEventRate(socket, eventName) {
  const socketOk = consumeBucket(socketRateState, socket.id, eventName, RATE_LIMIT_MAX_SOCKET);
  const ip = socket.data && socket.data.clientIp ? socket.data.clientIp : 'unknown';
  const ipOk = consumeBucket(ipRateState, ip, eventName, RATE_LIMIT_MAX_IP);
  if (socketOk && ipOk) return true;
  metrics.rateLimitRejectTotal += 1;
  logEvent('rate_limit_reject', { event: eventName, socketId: socket.id, ip });
  return false;
}

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: function (origin, callback) {
      if (isOriginAllowed(origin)) {
        callback(null, true);
        return;
      }
      metrics.originRejectTotal += 1;
      logEvent('origin_reject', { origin: origin || '' });
      callback(new Error('Origin not allowed'));
    }
  }
});

/** @type {Map<string, any>} */
const rooms = new Map();
/** @type {Map<string, any>} */
let territoryByName = new Map();
/** @type {Map<string, string[]>} */
let routeGraph = new Map();
let territoryCacheLoadedAt = 0;
let territoryCacheLoadError = '';

function createResources() {
  return { emeralds: 0, wood: 0, ore: 0, crops: 0, fish: 0 };
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function createRoomState(roomId, defenderSocketId) {
  return {
    id: roomId,
    status: 'lobby',
    defenderSocketId,
    defenderSessionToken: crypto.randomBytes(16).toString('hex'),
    defenderPendingDisconnectAt: null,
    defenderDisconnectTimer: null,
    attackerSocketId: null,
    attackerSessionToken: '',
    attackerPendingDisconnectAt: null,
    attackerDisconnectTimer: null,
    defenderReady: false,
    attackerReady: false,
    selectedTerritories: [],
    defenderResources: createResources(),
    attackerResources: createResources(),
    territoryUpgrades: {},
    prepSecondsRemaining: null,
    hqTerritory: '',
    taxRates: {},
    maxStorage: 1000000,
    tickIntervalMs: TICK_INTERVAL_MS,
    tickTimer: null,
    prepTimer: null,
    nextTickAt: null,
    tickCount: 0
  };
}

function publicRoomState(room) {
  return {
    id: room.id,
    status: room.status,
    defenderSocketId: room.defenderSocketId,
    attackerSocketId: room.attackerSocketId,
    defenderReady: room.defenderReady,
    attackerReady: room.attackerReady,
    selectedTerritories: room.selectedTerritories,
    defenderResources: room.defenderResources,
    attackerResources: room.attackerResources,
    territoryUpgrades: room.territoryUpgrades,
    prepSecondsRemaining: room.prepSecondsRemaining,
    hqTerritory: room.hqTerritory,
    maxStorage: room.maxStorage,
    tickIntervalMs: room.tickIntervalMs,
    nextTickAt: room.nextTickAt,
    tickCount: room.tickCount
  };
}

function hasRolePresence(room, role) {
  if (role === 'defender') {
    return !!room.defenderSocketId || !!room.defenderPendingDisconnectAt;
  }
  if (role === 'attacker') {
    return !!room.attackerSocketId || !!room.attackerPendingDisconnectAt;
  }
  return false;
}

async function loadTerritoryCache() {
  const res = await fetch(TERRITORIES_URL, { headers: { Accept: 'application/json' } });
  if (!res.ok) {
    throw new Error('territories.json fetch failed: ' + res.status);
  }
  const data = await res.json();
  const byName = new Map();
  const graph = new Map();
  Object.keys(data || {}).forEach(function (name) {
    const row = data[name] || {};
    const resources = row.resources || {};
    const tradeRoutes = Array.isArray(row.trade_routes)
      ? row.trade_routes.map(String)
      : Array.isArray(row['Trading Routes'])
        ? row['Trading Routes'].map(String)
        : [];
    byName.set(name, {
      name,
      resources: {
        emeralds: parseInt(resources.emeralds || '0', 10) || 0,
        wood: parseInt(resources.wood || '0', 10) || 0,
        ore: parseInt(resources.ore || '0', 10) || 0,
        crops: parseInt(resources.crops || '0', 10) || 0,
        fish: parseInt(resources.fish || '0', 10) || 0
      },
      tradeRoutes
    });
    graph.set(name, tradeRoutes);
  });
  territoryByName = byName;
  routeGraph = graph;
  territoryCacheLoadedAt = Date.now();
  territoryCacheLoadError = '';
}

async function ensureTerritoryCacheLoaded() {
  if (territoryByName.size > 0) return;
  try {
    await loadTerritoryCache();
  } catch (e) {
    territoryCacheLoadError = e instanceof Error ? e.message : 'Unknown territory cache error';
    throw new Error(territoryCacheLoadError);
  }
}

function emitRoomState(roomId) {
  const room = rooms.get(roomId);
  if (!room) return;
  io.to(roomId).emit('roomState', publicRoomState(room));
}

function emitRoomError(roomId, message) {
  io.to(roomId).emit('roomError', { error: message });
}

function generateRoomId() {
  let tries = 0;
  while (tries < 20) {
    const id = String(Math.floor(100000 + Math.random() * 900000));
    if (!rooms.has(id)) return id;
    tries += 1;
  }
  return null;
}

function roleForSocket(room, socketId) {
  if (room.defenderSocketId === socketId) return 'defender';
  if (room.attackerSocketId === socketId) return 'attacker';
  return null;
}

function stopPrepTicker(room) {
  if (room.prepTimer) {
    clearInterval(room.prepTimer);
    room.prepTimer = null;
  }
}

function stopTickLoop(room) {
  if (room.tickTimer) {
    clearInterval(room.tickTimer);
    room.tickTimer = null;
  }
  room.nextTickAt = null;
}

/**
 * BFS shortest-path search from a territory to the current HQ.
 * Returns a path array including both endpoints, or null if no route exists.
 */
function findRouteToHq(fromName, hqName, graph) {
  if (!fromName || !hqName) return null;
  if (fromName === hqName) return [hqName];
  const queue = [fromName];
  const visited = new Set([fromName]);
  const parent = new Map();
  while (queue.length > 0) {
    const current = queue.shift();
    const neighbors = graph.get(current) || [];
    for (let i = 0; i < neighbors.length; i++) {
      const next = neighbors[i];
      if (visited.has(next)) continue;
      visited.add(next);
      parent.set(next, current);
      if (next === hqName) {
        const path = [hqName];
        let cursor = hqName;
        while (parent.has(cursor)) {
          cursor = parent.get(cursor);
          path.push(cursor);
          if (cursor === fromName) break;
        }
        path.reverse();
        return path;
      }
      queue.push(next);
    }
  }
  return null;
}

function applyUpgradeDrain(room, territoryNames, messages) {
  const tickHours = room.tickIntervalMs / 3600000;
  for (let i = 0; i < territoryNames.length; i++) {
    const territoryName = territoryNames[i];
    const upgrades = ensureTerritoryUpgradeShape(room, territoryName);
    const active = {};
    const inactive = {};
    for (let c = 0; c < UPGRADE_CATEGORIES.length; c++) {
      const category = UPGRADE_CATEGORIES[c];
      const level = clamp(parseInt(upgrades[category] || 0, 10) || 0, 0, 11);
      if (!level) continue;
      const resourceKey = UPGRADE_RESOURCE_BY_CATEGORY[category];
      const hourlyCost = UPGRADE_COSTS[category][level] || 0;
      const perTickCost = hourlyCost * tickHours;
      const current = Number(room.defenderResources[resourceKey] || 0);
      if (perTickCost <= 0) {
        active[category] = level;
        continue;
      }
      if (current >= perTickCost) {
        room.defenderResources[resourceKey] = current - perTickCost;
        active[category] = level;
      } else if (current > 0) {
        room.defenderResources[resourceKey] = 0;
        inactive[category] = level;
      } else {
        inactive[category] = level;
      }
    }
    const activeParts = [];
    const inactiveParts = [];
    if (active.damage) activeParts.push('D' + active.damage);
    if (active.attackSpeed) activeParts.push('AS' + active.attackSpeed);
    if (active.health) activeParts.push('H' + active.health);
    if (active.defense) activeParts.push('DEF' + active.defense);
    if (inactive.damage) inactiveParts.push('D' + inactive.damage);
    if (inactive.attackSpeed) inactiveParts.push('AS' + inactive.attackSpeed);
    if (inactive.health) inactiveParts.push('H' + inactive.health);
    if (inactive.defense) inactiveParts.push('DEF' + inactive.defense);
    if (activeParts.length || inactiveParts.length) {
      let line = territoryName + ' upgrades';
      if (activeParts.length) line += ' active(' + activeParts.join(',') + ')';
      if (inactiveParts.length) line += ' inactive(' + inactiveParts.join(',') + ')';
      messages.push(line);
    }
  }
}

function clampStorageAndDrain(room, messages) {
  const resourceKeys = ['emeralds', 'wood', 'ore', 'crops', 'fish'];
  for (let i = 0; i < resourceKeys.length; i++) {
    const key = resourceKeys[i];
    if (room.defenderResources[key] > room.maxStorage) {
      const overflow = room.defenderResources[key] - room.maxStorage;
      room.defenderResources[key] = room.maxStorage;
      messages.push('VOID: ' + key + ' overflowed by ' + Math.floor(overflow));
    }
    if (room.defenderResources[key] < 0) {
      messages.push('DRAIN: ' + key + ' went negative and was clamped to 0');
      room.defenderResources[key] = 0;
    }
    room.defenderResources[key] = Math.floor(room.defenderResources[key]);
  }
}

function runEcoTick(roomId) {
  const room = rooms.get(roomId);
  if (!room) return;
  if (room.status !== 'prep' && room.status !== 'playing') return;
  const messages = [];
  const selected = Array.isArray(room.selectedTerritories) ? room.selectedTerritories : [];
  if (!selected.length) {
    messages.push('No selected territories to simulate.');
    io.to(roomId).emit('tick:update', {
      defenderResources: room.defenderResources,
      messages,
      nextTickInMs: room.tickIntervalMs,
      serverNow: Date.now(),
      hqTerritory: room.hqTerritory || '',
      tickCount: room.tickCount
    });
    return;
  }
  if (!room.hqTerritory || selected.indexOf(room.hqTerritory) === -1) {
    room.hqTerritory = selected[0];
  }

  for (let i = 0; i < selected.length; i++) {
    const territoryName = selected[i];
    const territory = territoryByName.get(territoryName);
    if (!territory) {
      messages.push('Missing territory data: ' + territoryName);
      continue;
    }
    const path = findRouteToHq(territoryName, room.hqTerritory, routeGraph);
    if (!path) {
      messages.push('No trade route path from ' + territoryName + ' to HQ ' + room.hqTerritory);
      continue;
    }
    const routeNodesAfterStart = path.slice(1);
    let multiplier = 1;
    for (let p = 0; p < routeNodesAfterStart.length; p++) {
      const hop = routeNodesAfterStart[p];
      const hasOwnedTax = Object.prototype.hasOwnProperty.call(room.taxRates, hop);
      const rawTax = hasOwnedTax ? room.taxRates[hop] : 0.2;
      const taxRate = clamp(Number(rawTax) || 0, 0, 0.6);
      multiplier *= (1 - taxRate);
    }
    room.defenderResources.emeralds += territory.resources.emeralds * multiplier;
    room.defenderResources.wood += territory.resources.wood * multiplier;
    room.defenderResources.ore += territory.resources.ore * multiplier;
    room.defenderResources.crops += territory.resources.crops * multiplier;
    room.defenderResources.fish += territory.resources.fish * multiplier;
  }

  applyUpgradeDrain(room, selected, messages);
  clampStorageAndDrain(room, messages);
  room.tickCount += 1;
  room.nextTickAt = Date.now() + room.tickIntervalMs;

  io.to(roomId).emit('tick:update', {
    defenderResources: room.defenderResources,
    messages,
    nextTickInMs: room.tickIntervalMs,
    serverNow: Date.now(),
    hqTerritory: room.hqTerritory,
    tickCount: room.tickCount
  });
  emitRoomState(roomId);
}

function startTickLoop(roomId) {
  const room = rooms.get(roomId);
  if (!room || room.tickTimer) return;
  room.nextTickAt = Date.now() + room.tickIntervalMs;
  room.tickTimer = setInterval(function () {
    runEcoTick(roomId);
  }, room.tickIntervalMs);
}

function startPrepCountdown(roomId) {
  const room = rooms.get(roomId);
  if (!room) return;
  stopPrepTicker(room);
  room.status = 'prep';
  room.prepSecondsRemaining = PREP_SECONDS;
  io.to(roomId).emit('statusChanged', { status: room.status });
  io.to(roomId).emit('prepTick', { secondsRemaining: room.prepSecondsRemaining });
  emitRoomState(roomId);
  startTickLoop(roomId);
  room.prepTimer = setInterval(function () {
    const activeRoom = rooms.get(roomId);
    if (!activeRoom) return;
    activeRoom.prepSecondsRemaining -= 1;
    io.to(roomId).emit('prepTick', { secondsRemaining: activeRoom.prepSecondsRemaining });
    emitRoomState(roomId);
    if (activeRoom.prepSecondsRemaining <= 0) {
      stopPrepTicker(activeRoom);
      activeRoom.status = 'playing';
      activeRoom.prepSecondsRemaining = 0;
      io.to(roomId).emit('statusChanged', { status: activeRoom.status });
      emitRoomState(roomId);
    }
  }, 1000);
}

function cleanupRoomIfEmpty(roomId) {
  const room = rooms.get(roomId);
  if (!room) return;
  if (!hasRolePresence(room, 'defender') && !hasRolePresence(room, 'attacker')) {
    stopPrepTicker(room);
    stopTickLoop(room);
    if (room.defenderDisconnectTimer) {
      clearTimeout(room.defenderDisconnectTimer);
      room.defenderDisconnectTimer = null;
    }
    if (room.attackerDisconnectTimer) {
      clearTimeout(room.attackerDisconnectTimer);
      room.attackerDisconnectTimer = null;
    }
    rooms.delete(roomId);
    metrics.roomDeleteTotal += 1;
    logEvent('room_deleted', { roomId });
  }
}

function roleTokenField(role) {
  return role === 'defender' ? 'defenderSessionToken' : 'attackerSessionToken';
}

function roleSocketField(role) {
  return role === 'defender' ? 'defenderSocketId' : 'attackerSocketId';
}

function rolePendingField(role) {
  return role === 'defender' ? 'defenderPendingDisconnectAt' : 'attackerPendingDisconnectAt';
}

function roleTimerField(role) {
  return role === 'defender' ? 'defenderDisconnectTimer' : 'attackerDisconnectTimer';
}

function clearDisconnectGrace(room, role) {
  const timerField = roleTimerField(role);
  const pendingField = rolePendingField(role);
  if (room[timerField]) {
    clearTimeout(room[timerField]);
    room[timerField] = null;
  }
  room[pendingField] = null;
}

function scheduleDisconnectGrace(roomId, role) {
  const room = rooms.get(roomId);
  if (!room) return;
  const pendingField = rolePendingField(role);
  const timerField = roleTimerField(role);
  clearDisconnectGrace(room, role);
  room[pendingField] = Date.now();
  room[timerField] = setTimeout(function () {
    const activeRoom = rooms.get(roomId);
    if (!activeRoom) return;
    const activeSocketField = roleSocketField(role);
    const activePendingField = rolePendingField(role);
    const activeTimerField = roleTimerField(role);
    if (activeRoom[activeSocketField]) return;
    activeRoom[activePendingField] = null;
    activeRoom[activeTimerField] = null;
    if (role === 'attacker') {
      activeRoom.attackerSessionToken = '';
    }
    metrics.graceExpiryTotal += 1;
    logEvent('grace_expired', { roomId, role });
    resetRoomOnPlayerLeave(activeRoom);
    emitRoomState(roomId);
    cleanupRoomIfEmpty(roomId);
  }, DISCONNECT_GRACE_MS);
}

function resetRoomOnPlayerLeave(room) {
  room.defenderReady = false;
  room.attackerReady = false;
  if (room.status === 'prep' || room.status === 'playing') {
    stopPrepTicker(room);
    stopTickLoop(room);
    room.status = 'lobby';
    room.prepSecondsRemaining = null;
    io.to(room.id).emit('statusChanged', { status: room.status });
  }
}

function normalizeUpgradeLevel(value) {
  return clamp(parseInt(value || 0, 10) || 0, 0, 11);
}

function ensureTerritoryUpgradeShape(room, territoryName) {
  if (!room.territoryUpgrades[territoryName]) {
    room.territoryUpgrades[territoryName] = {
      damage: 0,
      attackSpeed: 0,
      health: 0,
      defense: 0
    };
  } else {
    const row = room.territoryUpgrades[territoryName];
    row.damage = normalizeUpgradeLevel(row.damage);
    row.attackSpeed = normalizeUpgradeLevel(row.attackSpeed);
    row.health = normalizeUpgradeLevel(row.health);
    row.defense = normalizeUpgradeLevel(row.defense);
  }
  return room.territoryUpgrades[territoryName];
}

function applyUpgrade(room, territoryName, category) {
  const resourceKey = UPGRADE_RESOURCE_BY_CATEGORY[category];
  if (!resourceKey) {
    return { ok: false, error: 'Invalid upgrade category.' };
  }
  const upgrades = ensureTerritoryUpgradeShape(room, territoryName);
  const currentLevel = normalizeUpgradeLevel(upgrades[category]);
  if (currentLevel >= 11) {
    return { ok: false, error: category + ' is already at max level.' };
  }
  const nextLevel = currentLevel + 1;
  const hourlyCost = UPGRADE_COSTS[category][nextLevel] || 0;
  upgrades[category] = nextLevel;
  return {
    ok: true,
    territoryName,
    category,
    level: nextLevel,
    resourceKey,
    hourlyCost
  };
}

io.on('connection', function (socket) {
  metrics.connectionsTotal += 1;
  socket.data.clientIp = getClientIp(socket);
  logEvent('socket_connected', {
    socketId: socket.id,
    ip: socket.data.clientIp
  });

  socket.on('createRoom', function (_, ack) {
    if (!requirePrivilegedAccess(socket, ack, 'createRoom')) return;
    if (!allowEventRate(socket, 'createRoom')) {
      if (typeof ack === 'function') ack({ ok: false, error: 'Too many requests. Try again shortly.' });
      return;
    }
    const roomId = generateRoomId();
    if (!roomId) {
      if (typeof ack === 'function') ack({ ok: false, error: 'Failed to allocate room code.' });
      return;
    }
    const room = createRoomState(roomId, socket.id);
    rooms.set(roomId, room);
    socket.join(roomId);
    socket.data.roomId = roomId;
    socket.data.role = 'defender';
    metrics.roomCreateTotal += 1;
    logEvent('room_created', { roomId, socketId: socket.id, ip: socket.data.clientIp });
    emitRoomState(roomId);
    if (typeof ack === 'function') {
      ack({ ok: true, roomId, role: 'defender', playerToken: room.defenderSessionToken });
    }
  });

  socket.on('joinRoom', function (payload, ack) {
    if (!allowEventRate(socket, 'joinRoom')) {
      if (typeof ack === 'function') ack({ ok: false, error: 'Too many requests. Try again shortly.' });
      return;
    }
    const roomId = payload && typeof payload.roomId === 'string' ? payload.roomId.trim() : '';
    if (!/^\d{6}$/.test(roomId)) {
      if (typeof ack === 'function') ack({ ok: false, error: 'Room code must be 6 digits.' });
      return;
    }
    const room = rooms.get(roomId);
    if (!room) {
      if (typeof ack === 'function') ack({ ok: false, error: 'Room not found.' });
      return;
    }
    if (room.attackerSocketId && room.attackerSocketId !== socket.id) {
      if (typeof ack === 'function') ack({ ok: false, error: 'Room already has an attacker.' });
      return;
    }
    room.attackerSocketId = socket.id;
    room.attackerSessionToken = crypto.randomBytes(16).toString('hex');
    clearDisconnectGrace(room, 'attacker');
    room.attackerReady = false;
    socket.join(roomId);
    socket.data.roomId = roomId;
    socket.data.role = 'attacker';
    metrics.roomJoinTotal += 1;
    logEvent('room_joined', { roomId, socketId: socket.id, ip: socket.data.clientIp });
    emitRoomState(roomId);
    if (typeof ack === 'function') {
      ack({ ok: true, roomId, role: 'attacker', playerToken: room.attackerSessionToken });
    }
  });

  socket.on('resumeRoom', function (payload, ack) {
    if (!requirePrivilegedAccess(socket, ack, 'resumeRoom')) return;
    if (!allowEventRate(socket, 'resumeRoom')) {
      if (typeof ack === 'function') ack({ ok: false, error: 'Too many requests. Try again shortly.' });
      return;
    }
    const roomId = payload && typeof payload.roomId === 'string' ? payload.roomId.trim() : '';
    const playerToken = payload && typeof payload.playerToken === 'string' ? payload.playerToken.trim() : '';
    if (!/^\d{6}$/.test(roomId) || !playerToken) {
      metrics.resumeFailTotal += 1;
      if (typeof ack === 'function') ack({ ok: false, error: 'Invalid resume payload.' });
      return;
    }
    const room = rooms.get(roomId);
    if (!room) {
      metrics.resumeFailTotal += 1;
      if (typeof ack === 'function') ack({ ok: false, error: 'Room not found.' });
      return;
    }
    let role = null;
    if (room.defenderSessionToken && room.defenderSessionToken === playerToken) {
      role = 'defender';
    } else if (room.attackerSessionToken && room.attackerSessionToken === playerToken) {
      role = 'attacker';
    }
    if (!role) {
      metrics.resumeFailTotal += 1;
      if (typeof ack === 'function') ack({ ok: false, error: 'Resume token invalid.' });
      return;
    }
    room[roleSocketField(role)] = socket.id;
    clearDisconnectGrace(room, role);
    socket.join(roomId);
    socket.data.roomId = roomId;
    socket.data.role = role;
    metrics.resumeSuccessTotal += 1;
    logEvent('room_resumed', { roomId, role, socketId: socket.id, ip: socket.data.clientIp });
    emitRoomState(roomId);
    if (typeof ack === 'function') ack({ ok: true, roomId, role });
  });

  socket.on('updateSelection', function (payload, ack) {
    if (!allowEventRate(socket, 'updateSelection')) {
      if (typeof ack === 'function') ack({ ok: false, error: 'Too many requests. Try again shortly.' });
      return;
    }
    const roomId = socket.data.roomId;
    const room = roomId ? rooms.get(roomId) : null;
    if (!room) {
      if (typeof ack === 'function') ack({ ok: false, error: 'Not in a room.' });
      return;
    }
    if (roleForSocket(room, socket.id) !== 'defender') {
      if (typeof ack === 'function') ack({ ok: false, error: 'Only defender can update selection.' });
      return;
    }
    if (room.status !== 'lobby') {
      if (typeof ack === 'function') ack({ ok: false, error: 'Selection can only be edited in lobby.' });
      return;
    }
    const selected = payload && Array.isArray(payload.selectedTerritories)
      ? payload.selectedTerritories
        .filter(function (name) {
          return typeof name === 'string' && name.trim().length > 0;
        })
        .map(function (name) {
          return name.trim();
        })
      : [];
    room.selectedTerritories = Array.from(new Set(selected));
    room.hqTerritory = room.selectedTerritories[0] || '';
    room.defenderReady = false;
    room.attackerReady = false;
    emitRoomState(roomId);
    if (typeof ack === 'function') ack({ ok: true });
  });

  socket.on('setReady', async function (payload, ack) {
    if (!requirePrivilegedAccess(socket, ack, 'setReady')) return;
    if (!allowEventRate(socket, 'setReady')) {
      if (typeof ack === 'function') ack({ ok: false, error: 'Too many requests. Try again shortly.' });
      return;
    }
    const roomId = socket.data.roomId;
    const room = roomId ? rooms.get(roomId) : null;
    if (!room) {
      if (typeof ack === 'function') ack({ ok: false, error: 'Not in a room.' });
      return;
    }
    const role = roleForSocket(room, socket.id);
    if (!role) {
      if (typeof ack === 'function') ack({ ok: false, error: 'Invalid player role.' });
      return;
    }
    if (room.status !== 'lobby') {
      if (typeof ack === 'function') ack({ ok: false, error: 'Room is not in lobby state.' });
      return;
    }
    const ready = !!(payload && payload.ready);
    if (role === 'defender') room.defenderReady = ready;
    if (role === 'attacker') room.attackerReady = ready;
    emitRoomState(roomId);

    if (room.defenderReady && room.attackerReady) {
      try {
        await ensureTerritoryCacheLoaded();
      } catch (e) {
        room.defenderReady = false;
        room.attackerReady = false;
        emitRoomState(roomId);
        emitRoomError(roomId, 'Territory cache unavailable: ' + (e instanceof Error ? e.message : String(e)));
        if (typeof ack === 'function') ack({ ok: false, error: 'Territory cache unavailable.' });
        return;
      }
      startPrepCountdown(roomId);
    }
    if (typeof ack === 'function') ack({ ok: true });
  });

  socket.on('upgrade:apply', function (payload, ack) {
    if (!requirePrivilegedAccess(socket, ack, 'upgrade:apply')) return;
    if (!allowEventRate(socket, 'upgrade:apply')) {
      if (typeof ack === 'function') ack({ ok: false, error: 'Too many requests. Try again shortly.' });
      return;
    }
    const roomId = socket.data.roomId;
    const room = roomId ? rooms.get(roomId) : null;
    if (!room) {
      if (typeof ack === 'function') ack({ ok: false, error: 'Not in a room.' });
      return;
    }
    if (room.status !== 'playing' && room.status !== 'prep') {
      if (typeof ack === 'function') ack({ ok: false, error: 'Upgrades are only available during prep/playing.' });
      return;
    }
    const role = roleForSocket(room, socket.id);
    if (role !== 'defender') {
      if (typeof ack === 'function') ack({ ok: false, error: 'Only defender can apply upgrades.' });
      return;
    }
    const territoryName = payload && typeof payload.territoryName === 'string'
      ? payload.territoryName.trim()
      : '';
    const category = payload && typeof payload.category === 'string'
      ? payload.category.trim()
      : '';
    if (!territoryName || room.selectedTerritories.indexOf(territoryName) === -1) {
      if (typeof ack === 'function') ack({ ok: false, error: 'Territory is not defender-owned in this room.' });
      return;
    }
    const result = applyUpgrade(room, territoryName, category);
    if (!result.ok) {
      if (typeof ack === 'function') ack(result);
      return;
    }
    io.to(roomId).emit('upgrade:applied', {
      territoryName: result.territoryName,
      category: result.category,
      level: result.level,
      categoryCost: result.categoryCost,
      emeraldCost: result.emeraldCost,
      resourceKey: result.resourceKey
    });
    emitRoomState(roomId);
    if (typeof ack === 'function') ack({ ok: true, ...result });
  });

  socket.on('leaveRoom', function (_, ack) {
    const roomId = socket.data.roomId;
    const room = roomId ? rooms.get(roomId) : null;
    if (!room) {
      if (typeof ack === 'function') ack({ ok: true });
      return;
    }
    const role = roleForSocket(room, socket.id);
    if (role === 'defender') {
      room.defenderSocketId = null;
      clearDisconnectGrace(room, 'defender');
    }
    if (role === 'attacker') {
      room.attackerSocketId = null;
      room.attackerSessionToken = '';
      clearDisconnectGrace(room, 'attacker');
    }
    resetRoomOnPlayerLeave(room);
    socket.leave(roomId);
    socket.data.roomId = null;
    socket.data.role = null;
    emitRoomState(roomId);
    cleanupRoomIfEmpty(roomId);
    if (typeof ack === 'function') ack({ ok: true });
  });

  socket.on('disconnect', function () {
    metrics.disconnectionsTotal += 1;
    const roomId = socket.data.roomId;
    const room = roomId ? rooms.get(roomId) : null;
    socketRateState.delete(socket.id);
    logEvent('socket_disconnected', {
      socketId: socket.id,
      ip: socket.data && socket.data.clientIp ? socket.data.clientIp : 'unknown',
      roomId: roomId || ''
    });
    if (!room) return;
    const role = roleForSocket(room, socket.id);
    if (!role) return;
    room[roleSocketField(role)] = null;
    scheduleDisconnectGrace(roomId, role);
    emitRoomState(roomId);
  });
});

app.get('/health', function (_req, res) {
  res.json({
    ok: true,
    rooms: rooms.size,
    territoryCacheSize: territoryByName.size,
    territoryCacheLoadedAt,
    territoryCacheLoadError,
    uptimeMs: Date.now() - metrics.startedAt,
    tokenAuthEnabled: !!ECO_WAR_SHARED_TOKEN,
    allowedOrigins: ALLOWED_ORIGINS,
    rateLimit: {
      windowMs: RATE_LIMIT_WINDOW_MS,
      maxPerSocket: RATE_LIMIT_MAX_SOCKET,
      maxPerIp: RATE_LIMIT_MAX_IP
    },
    roomLifecycle: {
      disconnectGraceMs: DISCONNECT_GRACE_MS
    },
    metrics: {
      connectionsTotal: metrics.connectionsTotal,
      disconnectionsTotal: metrics.disconnectionsTotal,
      roomCreateTotal: metrics.roomCreateTotal,
      roomJoinTotal: metrics.roomJoinTotal,
      roomDeleteTotal: metrics.roomDeleteTotal,
      resumeSuccessTotal: metrics.resumeSuccessTotal,
      resumeFailTotal: metrics.resumeFailTotal,
      graceExpiryTotal: metrics.graceExpiryTotal,
      rateLimitRejectTotal: metrics.rateLimitRejectTotal,
      authRejectTotal: metrics.authRejectTotal,
      originRejectTotal: metrics.originRejectTotal
    }
  });
});

loadTerritoryCache()
  .then(function () {
    console.log('Territory cache loaded. Entries: ' + territoryByName.size);
  })
  .catch(function (e) {
    territoryCacheLoadError = e instanceof Error ? e.message : String(e);
    console.error('Territory cache startup load failed:', territoryCacheLoadError);
  })
  .finally(function () {
    server.listen(PORT, function () {
      console.log('Socket room server listening on port ' + PORT);
    });
  });
