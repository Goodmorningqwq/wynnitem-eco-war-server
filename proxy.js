const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 3001;
const TICK_INTERVAL_MS = process.env.TICK_INTERVAL_MS
  ? parseInt(process.env.TICK_INTERVAL_MS, 10)
  : 6000;
const PREP_SECONDS = 60;
const TERRITORIES_URL =
  'https://raw.githubusercontent.com/jakematt123/Wynncraft-Territory-Info/main/territories.json';

const UPGRADE_COSTS = {
  damage: [0, 50, 120, 250, 450, 700, 1000, 1400, 1900, 2500, 3200, 4000], // ore
  attackSpeed: [0, 40, 100, 220, 400, 650, 950, 1350, 1800, 2400, 3100, 3900], // crops
  health: [0, 60, 140, 280, 500, 780, 1100, 1550, 2100, 2800, 3600, 4500], // wood
  defense: [0, 30, 80, 170, 320, 520, 780, 1100, 1500, 2000, 2600, 3300] // fish
};

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' }
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
    attackerSocketId: null,
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
  for (let i = 0; i < territoryNames.length; i++) {
    const territoryName = territoryNames[i];
    const upgrades = room.territoryUpgrades[territoryName] || {};
    const damageLevel = clamp(parseInt(upgrades.damage || 0, 10) || 0, 0, 11);
    const speedLevel = clamp(parseInt(upgrades.attackSpeed || 0, 10) || 0, 0, 11);
    const healthLevel = clamp(parseInt(upgrades.health || 0, 10) || 0, 0, 11);
    const defenseLevel = clamp(parseInt(upgrades.defense || 0, 10) || 0, 0, 11);

    room.defenderResources.ore -= UPGRADE_COSTS.damage[damageLevel];
    room.defenderResources.crops -= UPGRADE_COSTS.attackSpeed[speedLevel];
    room.defenderResources.wood -= UPGRADE_COSTS.health[healthLevel];
    room.defenderResources.fish -= UPGRADE_COSTS.defense[defenseLevel];

    if (damageLevel || speedLevel || healthLevel || defenseLevel) {
      messages.push(
        'Drain on ' + territoryName + ': d' + damageLevel +
          ' as' + speedLevel + ' h' + healthLevel + ' def' + defenseLevel
      );
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
  if (!room.defenderSocketId && !room.attackerSocketId) {
    stopPrepTicker(room);
    stopTickLoop(room);
    rooms.delete(roomId);
  }
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
  const resourceByCategory = {
    damage: 'ore',
    attackSpeed: 'crops',
    health: 'wood',
    defense: 'fish'
  };
  const resourceKey = resourceByCategory[category];
  if (!resourceKey) {
    return { ok: false, error: 'Invalid upgrade category.' };
  }
  const upgrades = ensureTerritoryUpgradeShape(room, territoryName);
  const currentLevel = normalizeUpgradeLevel(upgrades[category]);
  if (currentLevel >= 11) {
    return { ok: false, error: category + ' is already at max level.' };
  }
  const nextLevel = currentLevel + 1;
  const categoryCost = UPGRADE_COSTS[category][nextLevel] || 0;
  const emeraldCost = categoryCost;
  if (room.defenderResources.emeralds < emeraldCost) {
    return { ok: false, error: 'Not enough emeralds.' };
  }
  if (room.defenderResources[resourceKey] < categoryCost) {
    return { ok: false, error: 'Not enough ' + resourceKey + '.' };
  }
  room.defenderResources.emeralds -= emeraldCost;
  room.defenderResources[resourceKey] -= categoryCost;
  upgrades[category] = nextLevel;
  return {
    ok: true,
    territoryName,
    category,
    level: nextLevel,
    categoryCost,
    emeraldCost,
    resourceKey
  };
}

io.on('connection', function (socket) {
  socket.on('createRoom', function (_, ack) {
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
    emitRoomState(roomId);
    if (typeof ack === 'function') ack({ ok: true, roomId, role: 'defender' });
  });

  socket.on('joinRoom', function (payload, ack) {
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
    room.attackerReady = false;
    socket.join(roomId);
    socket.data.roomId = roomId;
    socket.data.role = 'attacker';
    emitRoomState(roomId);
    if (typeof ack === 'function') ack({ ok: true, roomId, role: 'attacker' });
  });

  socket.on('updateSelection', function (payload, ack) {
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
    const roomId = socket.data.roomId;
    const room = roomId ? rooms.get(roomId) : null;
    if (!room) {
      if (typeof ack === 'function') ack({ ok: false, error: 'Not in a room.' });
      return;
    }
    if (room.status !== 'playing') {
      if (typeof ack === 'function') ack({ ok: false, error: 'Upgrades are only available during playing state.' });
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
    if (role === 'defender') room.defenderSocketId = null;
    if (role === 'attacker') room.attackerSocketId = null;
    resetRoomOnPlayerLeave(room);
    socket.leave(roomId);
    socket.data.roomId = null;
    socket.data.role = null;
    emitRoomState(roomId);
    cleanupRoomIfEmpty(roomId);
    if (typeof ack === 'function') ack({ ok: true });
  });

  socket.on('disconnect', function () {
    const roomId = socket.data.roomId;
    const room = roomId ? rooms.get(roomId) : null;
    if (!room) return;
    const role = roleForSocket(room, socket.id);
    if (role === 'defender') room.defenderSocketId = null;
    if (role === 'attacker') room.attackerSocketId = null;
    resetRoomOnPlayerLeave(room);
    emitRoomState(roomId);
    cleanupRoomIfEmpty(roomId);
  });
});

app.get('/health', function (_req, res) {
  res.json({
    ok: true,
    rooms: rooms.size,
    territoryCacheSize: territoryByName.size,
    territoryCacheLoadedAt,
    territoryCacheLoadError
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
