'use strict';

const express = require('express');
const http = require('http');
const crypto = require('crypto');
const { Server } = require('socket.io');

// ─── Server Config ────────────────────────────────────────────────────────────
const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 3001;
const TICK_INTERVAL_MS = process.env.TICK_INTERVAL_MS ? parseInt(process.env.TICK_INTERVAL_MS, 10) : 60000;
const RATE_LIMIT_WINDOW_MS = process.env.RATE_LIMIT_WINDOW_MS ? parseInt(process.env.RATE_LIMIT_WINDOW_MS, 10) : 10000;
const RATE_LIMIT_MAX_SOCKET = process.env.RATE_LIMIT_MAX_SOCKET ? parseInt(process.env.RATE_LIMIT_MAX_SOCKET, 10) : 24;
const RATE_LIMIT_MAX_IP = process.env.RATE_LIMIT_MAX_IP ? parseInt(process.env.RATE_LIMIT_MAX_IP, 10) : 60;
const DISCONNECT_GRACE_MS = process.env.DISCONNECT_GRACE_MS ? parseInt(process.env.DISCONNECT_GRACE_MS, 10) : 60000;
const ECO_WAR_SHARED_TOKEN = typeof process.env.ECO_WAR_SHARED_TOKEN === 'string' ? process.env.ECO_WAR_SHARED_TOKEN.trim() : '';
const PREP_SECONDS = 60;
const TERRITORIES_URL = 'https://raw.githubusercontent.com/jakematt123/Wynncraft-Territory-Info/main/territories.json';

// ─── Game Constants (Real Wynncraft Values) ───────────────────────────────────
/**
 * Tower upgrades: same hourly drain table for all 4 stat categories. Index = level.
 */
const UPGRADE_COSTS_PER_LEVEL = [0, 100, 300, 600, 1200, 2400, 4800, 8400, 12000, 15600, 19200, 22800];

// Storage upgrades are ONE-TIME purchases (not hourly drain).
// Larger Emerald Storage → costs Wood per level
const EMERALD_STORAGE_COSTS_WOOD = [0, 100, 220, 400, 650, 950, 1350, 1850, 2500, 3300, 4300, 5500];
// Larger Resource Storage → costs Emeralds per level
const RESOURCE_STORAGE_COSTS_EM  = [0, 300, 650, 1100, 1700, 2500, 3500, 4700, 6200, 8000, 10100, 12500];

/**
 * BONUS_DEFS — official Wynncraft bonus upgrades with correct resource costs.
 * Tower bonuses:  strongerMobs, multiAttack, aura, volley
 * Economy bonuses: efficientEmeralds (+%em/tick, costs Crops),
 *                  efficientResources (+%res/tick, costs Emeralds),
 *                  emeraldRate (faster em tick, costs Ore),
 *                  resourceRate (faster res tick, costs Emeralds)
 */
const BONUS_DEFS = {
  // ── Tower bonuses ──
  strongerMobs:       { resource: 'wood',     costs: [0, 1200, 2400, 4800], maxLevel: 3 },
  multiAttack:        { resource: 'fish',     costs: [0, 3200],             maxLevel: 1 },
  aura:               { resource: 'crops',    costs: [0, 2000],             maxLevel: 1 },
  volley:             { resource: 'ore',      costs: [0, 2000],             maxLevel: 1 },
  // ── Economy bonuses ──
  efficientEmeralds:  { resource: 'crops',    costs: [0, 800,  1600, 3200], maxLevel: 3 },
  efficientResources: { resource: 'emeralds', costs: [0, 800,  1600, 3200], maxLevel: 3 },
  emeraldRate:        { resource: 'ore',      costs: [0, 1000, 2000, 4000], maxLevel: 3 },
  resourceRate:       { resource: 'emeralds', costs: [0, 1000, 2000, 4000], maxLevel: 3 },
};

// Tower upgrade categories and their hourly drain resources
const UPGRADE_CATEGORIES = ['damage', 'attackSpeed', 'health', 'defense'];
const UPGRADE_RESOURCE_BY_CATEGORY = {
  damage: 'ore', attackSpeed: 'crops', health: 'wood', defense: 'fish'
};
// Storage upgrade categories — separate from tower upgrades, charged one-time not hourly
const STORAGE_CATEGORIES = ['largerEmeraldStorage', 'largerResourceStorage'];
const STORAGE_RESOURCE_BY_CATEGORY = {
  largerEmeraldStorage: 'wood',
  largerResourceStorage: 'emeralds'
};
const STORAGE_COSTS_BY_CATEGORY = {
  largerEmeraldStorage:  EMERALD_STORAGE_COSTS_WOOD,
  largerResourceStorage: RESOURCE_STORAGE_COSTS_EM
};
const RESOURCE_KEYS = ['emeralds', 'wood', 'ore', 'crops', 'fish'];

// HQ storage caps (same as live Wynncraft)
const HQ_BASE_STORAGE_RESOURCE = 1500;
const HQ_BASE_STORAGE_EMERALDS = 5000;
const HQ_MAX_STORAGE_RESOURCE  = 120000;
const HQ_MAX_STORAGE_EMERALDS  = 400000;

// Treasury bonus — TIERED (not linear): +0% <1hr, +10% >=1hr, +20% >=24hr, +25% >=5 days
const TREASURY_TIER_MS    = [3600000, 86400000, 432000000]; // 1hr, 24hr, 5 days
const TREASURY_TIER_BONUS = [0.10,    0.20,     0.25];      // corresponding bonus values

// Production bonus per economy bonus level (applied multiplicatively)
const EFFICIENT_EM_PER_LEVEL  = 0.10; // +10% emerald output per efficientEmeralds level
const EFFICIENT_RES_PER_LEVEL = 0.10; // +10% resource output per efficientResources level
const EM_RATE_PER_LEVEL       = 0.10; // +10% emerald tick rate per emeraldRate level
const RES_RATE_PER_LEVEL      = 0.10; // +10% resource tick rate per resourceRate level

// Combat
const TOWER_BASE_HP    = 1000000; // base HP at level 0, no connections
const TOWER_BASE_DPS   = 500000;  // base tower DPS at level 0, no connections
const WAR_GRACE_SECONDS = 30;
const WAR_ATTACK_TYPES  = {
  solo:   { label: 'Solo Warrer',      dps: 150000  },
  normal: { label: 'Normal War Team',  dps: 2000000 },
  elite:  { label: 'Elite War Team',   dps: 4000000 }
};
const PRODUCTION_MULT_MIN = 0.5;
const PRODUCTION_MULT_MAX = 1.5;

// ─── Origin / Auth ────────────────────────────────────────────────────────────
function buildAllowedOrigins() {
  const raw = typeof process.env.ALLOWED_ORIGINS === 'string' ? process.env.ALLOWED_ORIGINS : '';
  const fromEnv = raw.split(',').map(v => v.trim()).filter(v => v.length > 0);
  const defaults = [
    'https://wynnitem-territory.vercel.app',
    'https://wynnitem-territories.vercel.app',
    'http://localhost:3000', 'http://localhost:5173',
    'http://127.0.0.1:3000', 'http://127.0.0.1:5173'
  ];
  return Array.from(new Set(fromEnv.concat(defaults)));
}
const ALLOWED_ORIGINS = buildAllowedOrigins();
function isOriginAllowed(origin) {
  if (!origin) return true;
  return ALLOWED_ORIGINS.indexOf(origin) !== -1;
}

// ─── Metrics ──────────────────────────────────────────────────────────────────
const metrics = {
  startedAt: Date.now(),
  connectionsTotal: 0, disconnectionsTotal: 0,
  roomCreateTotal: 0, roomJoinTotal: 0, roomDeleteTotal: 0,
  resumeSuccessTotal: 0, resumeFailTotal: 0,
  graceExpiryTotal: 0, rateLimitRejectTotal: 0,
  authRejectTotal: 0, originRejectTotal: 0
};

// ─── Rate Limiting ────────────────────────────────────────────────────────────
const socketRateState = new Map();
const ipRateState = new Map();

function logEvent(event, detail) {
  console.log(JSON.stringify({ ts: new Date().toISOString(), event, detail: detail || {} }));
}
function getClientIp(socket) {
  const fwd = socket && socket.handshake && socket.handshake.headers
    ? socket.handshake.headers['x-forwarded-for'] : '';
  if (typeof fwd === 'string' && fwd.trim()) return fwd.split(',')[0].trim();
  return socket && socket.handshake && socket.handshake.address ? String(socket.handshake.address) : 'unknown';
}
function tokenFromHandshake(socket) {
  const authToken = socket && socket.handshake && socket.handshake.auth ? socket.handshake.auth.token : '';
  if (typeof authToken === 'string' && authToken.trim()) return authToken.trim();
  const headerToken = socket && socket.handshake && socket.handshake.headers
    ? socket.handshake.headers['x-eco-war-token'] : '';
  if (typeof headerToken === 'string' && headerToken.trim()) return headerToken.trim();
  return '';
}
function isPrivilegedAuthorized(socket) {
  if (!ECO_WAR_SHARED_TOKEN) return true;
  return tokenFromHandshake(socket) === ECO_WAR_SHARED_TOKEN;
}
function requirePrivilegedAccess(socket, ack, eventName) {
  if (isPrivilegedAuthorized(socket)) return true;
  metrics.authRejectTotal += 1;
  logEvent('auth_reject', { event: eventName, socketId: socket.id });
  if (typeof ack === 'function') ack({ ok: false, error: 'Unauthorized.' });
  return false;
}
function consumeBucket(map, key, eventName, limit) {
  const now = Date.now();
  const current = map.get(key) || { windowStart: now, counts: {} };
  if (now - current.windowStart >= RATE_LIMIT_WINDOW_MS) { current.windowStart = now; current.counts = {}; }
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
  return false;
}

// ─── Express / Socket.io ──────────────────────────────────────────────────────
const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: function (origin, callback) {
      if (isOriginAllowed(origin)) { callback(null, true); return; }
      metrics.originRejectTotal += 1;
      logEvent('origin_reject', { origin: origin || '' });
      callback(new Error('Origin not allowed'));
    }
  }
});

// ─── Data Stores ──────────────────────────────────────────────────────────────
const rooms = new Map();
let territoryByName = new Map();
let routeGraph = new Map();
let territoryCacheLoadedAt = 0;
let territoryCacheLoadError = '';

// ─── Helper: create empty resources ───────────────────────────────────────────
function createResources() { return { emeralds: 0, wood: 0, ore: 0, crops: 0, fish: 0 }; }
function clamp(value, min, max) { return Math.max(min, Math.min(max, value)); }
function createTerritoryBonuses() {
  return {
    strongerMobs: 0, multiAttack: 0, aura: 0, volley: 0,
    efficientEmeralds: 0, efficientResources: 0, emeraldRate: 0, resourceRate: 0
  };
}

// ─── Room State ───────────────────────────────────────────────────────────────
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
    perTerritoryStorage: {},
    territoryUpgrades: {},
    territoryBonuses: {},
    territoryTaxRates: {},      // { territoryName: { enemy: 0.30, ally: 0.10 } }
    territoryRouteMode: {},     // { territoryName: 'fastest'|'cheapest' }
    territoryHeldSince: {},     // { territoryName: timestamp_ms }
    prepSecondsRemaining: null,
    hqTerritory: '',
    routeMode: 'fastest',
    productionMultiplier: 1,
    tickIntervalMs: TICK_INTERVAL_MS,
    tickTimer: null,
    prepTimer: null,
    nextTickAt: null,
    tickCount: 0,
    attackerWarType: 'normal',
    // Manual attack flow
    attackerCapturedTerritories: [], // territories the attacker has taken
    currentAttack: null,             // { territory, phase:'queue'|'battle', endsAt, battleEndsAt, towerStats }
    attackQueueTimer: null,
    attackBattleTimer: null,
    warResult: null,
    warTowerStats: null              // stats from last territory battle
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
    perTerritoryStorage: room.perTerritoryStorage,
    territoryUpgrades: room.territoryUpgrades,
    territoryBonuses: room.territoryBonuses,
    territoryTaxRates: room.territoryTaxRates,
    territoryRouteMode: room.territoryRouteMode,
    territoryHeldSince: room.territoryHeldSince,
    prepSecondsRemaining: room.prepSecondsRemaining,
    hqTerritory: room.hqTerritory,
    routeMode: room.routeMode || 'fastest',
    productionMultiplier: room.productionMultiplier != null ? room.productionMultiplier : 1,
    tickIntervalMs: room.tickIntervalMs,
    nextTickAt: room.nextTickAt,
    tickCount: room.tickCount,
    attackerWarType: room.attackerWarType,
    attackerCapturedTerritories: room.attackerCapturedTerritories || [],
    currentAttack: room.currentAttack,
    warResult: room.warResult,
    warTowerStats: room.warTowerStats
  };
}

// ─── Territory Cache ──────────────────────────────────────────────────────────
function readTradeRoutes(row) {
  const tr = row['Trading Routes'] || row.tradingRoutes || row.trade_routes;
  return Array.isArray(tr) ? tr.map(String) : [];
}

async function loadTerritoryCache() {
  const res = await fetch(TERRITORIES_URL, { headers: { Accept: 'application/json' } });
  if (!res.ok) throw new Error('territories.json fetch failed: ' + res.status);
  const data = await res.json();
  const byName = new Map();
  const graph = new Map();
  Object.keys(data || {}).forEach(function (name) {
    const row = data[name] || {};
    const resources = row.resources || {};
    const tradeRoutes = readTradeRoutes(row);
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
  try { await loadTerritoryCache(); }
  catch (e) {
    territoryCacheLoadError = e instanceof Error ? e.message : String(e);
    throw new Error(territoryCacheLoadError);
  }
}

// ─── Room State Helpers ───────────────────────────────────────────────────────
function emitRoomState(roomId) {
  const room = rooms.get(roomId);
  if (!room) return;
  io.to(roomId).emit('roomState', publicRoomState(room));
}
function emitRoomError(roomId, message) { io.to(roomId).emit('roomError', { error: message }); }
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

// ─── Route Finding ────────────────────────────────────────────────────────────
function buildSelectedAdjacency(selected) {
  const selectedSet = new Set(selected);
  const adjacency = new Map();
  selected.forEach(function (name) { adjacency.set(name, new Set()); });
  selected.forEach(function (fromName) {
    const outgoing = routeGraph.get(fromName) || [];
    for (let i = 0; i < outgoing.length; i++) {
      const toName = outgoing[i];
      if (!selectedSet.has(toName)) continue;
      adjacency.get(fromName).add(toName);
      adjacency.get(toName).add(fromName);
    }
  });
  return adjacency;
}

function findSelectedOnlyPathToHq(fromName, hqName, selectedAdjacency) {
  if (!fromName || !hqName) return null;
  if (fromName === hqName) return [hqName];
  if (!selectedAdjacency.has(fromName) || !selectedAdjacency.has(hqName)) return null;
  const queue = [fromName];
  const visited = new Set([fromName]);
  const parent = new Map();
  while (queue.length > 0) {
    const current = queue.shift();
    const neighbors = Array.from(selectedAdjacency.get(current) || []);
    for (let i = 0; i < neighbors.length; i++) {
      const next = neighbors[i];
      if (visited.has(next)) continue;
      visited.add(next);
      parent.set(next, current);
      if (next === hqName) {
        const path = [hqName];
        let cursor = hqName;
        while (parent.has(cursor)) { cursor = parent.get(cursor); path.push(cursor); if (cursor === fromName) break; }
        return path.reverse();
      }
      queue.push(next);
    }
  }
  return null;
}

function findSelectedOnlyPathToHqCheapest(fromName, hqName, selectedAdjacency) {
  // Dijkstra with uniform edge weight (same as BFS in topology sense,
  // but lets individual territories choose a distinct algorithm path for future tax logic)
  return findSelectedOnlyPathToHq(fromName, hqName, selectedAdjacency);
}

// ─── Storage Helpers ──────────────────────────────────────────────────────────
function ensurePerTerritoryStorage(room, territoryName) {
  if (!room.perTerritoryStorage[territoryName]) {
    room.perTerritoryStorage[territoryName] = createResources();
  }
  const row = room.perTerritoryStorage[territoryName];
  for (let i = 0; i < RESOURCE_KEYS.length; i++) {
    const key = RESOURCE_KEYS[i];
    row[key] = Number(row[key] || 0);
    if (!Number.isFinite(row[key]) || row[key] < 0) row[key] = 0;
  }
  return row;
}

/**
 * HQ storage capacity.
 * - Emerald cap depends on largerEmeraldStorage level
 * - Resource cap depends on largerResourceStorage level
 * Non-HQ territories have unlimited pass-through storage (Infinity).
 */
function hqEmeraldStorageCap(emeraldStorLevel) {
  const lv = clamp(emeraldStorLevel, 0, 11);
  return Math.floor(HQ_BASE_STORAGE_EMERALDS + (lv / 11) * (HQ_MAX_STORAGE_EMERALDS - HQ_BASE_STORAGE_EMERALDS));
}
function hqResourceStorageCap(resourceStorLevel) {
  const lv = clamp(resourceStorLevel, 0, 11);
  return Math.floor(HQ_BASE_STORAGE_RESOURCE + (lv / 11) * (HQ_MAX_STORAGE_RESOURCE - HQ_BASE_STORAGE_RESOURCE));
}

function getUpgradeLevel(room, territoryName, field) {
  const row = room.territoryUpgrades && room.territoryUpgrades[territoryName];
  return clamp(parseInt((row && row[field]) || 0, 10) || 0, 0, 11);
}

/**
 * Apply storage caps — ONLY to HQ territory.
 * Emerald cap from largerEmeraldStorage level; resource cap from largerResourceStorage level.
 * Non-HQ territories are unlimited pass-through (real Wynncraft behavior).
 */
function applyHqStorageCap(room, messages) {
  const hq = room.hqTerritory || '';
  if (!hq) return;
  const store = room.perTerritoryStorage[hq];
  if (!store) return;
  const emStorLv  = getUpgradeLevel(room, hq, 'largerEmeraldStorage');
  const resStorLv = getUpgradeLevel(room, hq, 'largerResourceStorage');
  const emCap  = hqEmeraldStorageCap(emStorLv);
  const resCap = hqResourceStorageCap(resStorLv);
  for (let r = 0; r < RESOURCE_KEYS.length; r++) {
    const key = RESOURCE_KEYS[r];
    const cap = key === 'emeralds' ? emCap : resCap;
    if (store[key] > cap) {
      const overflow = Math.floor(store[key] - cap);
      store[key] = cap;
      if (overflow > 0) messages.push('HQ overflow: voided ' + overflow.toLocaleString() + ' ' + key);
    }
  }
}

// ─── Treasury Bonus (tiered) ──────────────────────────────────────────────────
function getTreasuryBonus(room, territoryName) {
  const heldSince = room.territoryHeldSince && room.territoryHeldSince[territoryName];
  if (!heldSince) return 0;
  const ms = Date.now() - heldSince;
  // Walk tiers from highest to lowest
  for (let i = TREASURY_TIER_MS.length - 1; i >= 0; i--) {
    if (ms >= TREASURY_TIER_MS[i]) return TREASURY_TIER_BONUS[i];
  }
  return 0; // < 1 hour
}

// ─── Upgrade Shape Normalisation ─────────────────────────────────────────────
function normalizeUpgradeLevel(value) { return clamp(parseInt(value || 0, 10) || 0, 0, 11); }

function ensureTerritoryUpgradeShape(room, territoryName) {
  if (!room.territoryUpgrades[territoryName]) {
    room.territoryUpgrades[territoryName] = {
      damage: 0, attackSpeed: 0, health: 0, defense: 0,
      largerEmeraldStorage: 0, largerResourceStorage: 0
    };
  } else {
    const row = room.territoryUpgrades[territoryName];
    row.damage  = normalizeUpgradeLevel(row.damage);
    row.attackSpeed = normalizeUpgradeLevel(row.attackSpeed);
    row.health  = normalizeUpgradeLevel(row.health);
    row.defense = normalizeUpgradeLevel(row.defense);
    // Migrate old single 'storage' field → split into two
    if (row.storage != null) {
      row.largerEmeraldStorage  = normalizeUpgradeLevel(row.largerEmeraldStorage || row.storage);
      row.largerResourceStorage = normalizeUpgradeLevel(row.largerResourceStorage || row.storage);
      delete row.storage;
    } else {
      row.largerEmeraldStorage  = normalizeUpgradeLevel(row.largerEmeraldStorage);
      row.largerResourceStorage = normalizeUpgradeLevel(row.largerResourceStorage);
    }
  }
  return room.territoryUpgrades[territoryName];
}

function ensureTerritoryBonusShape(room, territoryName) {
  if (!room.territoryBonuses[territoryName]) {
    room.territoryBonuses[territoryName] = createTerritoryBonuses();
  } else {
    const row = room.territoryBonuses[territoryName];
    Object.keys(BONUS_DEFS).forEach(function (key) {
      row[key] = clamp(parseInt(row[key] || 0, 10) || 0, 0, BONUS_DEFS[key].maxLevel);
    });
  }
  return room.territoryBonuses[territoryName];
}

// ─── Upgrade/Bonus Drain ──────────────────────────────────────────────────────
function applyUpgradeDrain(room, territoryNames, messages) {
  const tickHours = room.tickIntervalMs / 3600000;
  for (let i = 0; i < territoryNames.length; i++) {
    const name = territoryNames[i];
    const upgrades = ensureTerritoryUpgradeShape(room, name);
    const storage = ensurePerTerritoryStorage(room, name);
    const active = [], inactive = [];
    for (let c = 0; c < UPGRADE_CATEGORIES.length; c++) {
      const category = UPGRADE_CATEGORIES[c];
      if (category === 'storage') continue;
      const level = clamp(parseInt(upgrades[category] || 0, 10) || 0, 0, 11);
      if (!level) continue;
      const resourceKey = UPGRADE_RESOURCE_BY_CATEGORY[category];
      const hourlyCost = UPGRADE_COSTS_PER_LEVEL[level] || 0;
      const perTickCost = hourlyCost * tickHours;
      const current = Number(storage[resourceKey] || 0);
      if (perTickCost <= 0) { active.push(category[0].toUpperCase() + level); continue; }
      if (current >= perTickCost) {
        storage[resourceKey] = current - perTickCost;
        active.push(category[0].toUpperCase() + level);
      } else {
        storage[resourceKey] = 0;
        inactive.push(category[0].toUpperCase() + level);
      }
    }
    let line = name;
    if (active.length) line += ' ✓(' + active.join(',') + ')';
    if (inactive.length) line += ' ✗INACTIVE(' + inactive.join(',') + ')';
    if (active.length || inactive.length) messages.push(line);
  }
}

function applyBonusDrain(room, territoryNames, messages) {
  const tickHours = room.tickIntervalMs / 3600000;
  const bonusKeys = Object.keys(BONUS_DEFS);
  for (let i = 0; i < territoryNames.length; i++) {
    const name = territoryNames[i];
    const bonuses = ensureTerritoryBonusShape(room, name);
    const storage = ensurePerTerritoryStorage(room, name);
    const failed = [];
    for (let b = 0; b < bonusKeys.length; b++) {
      const bonusKey = bonusKeys[b];
      const level = bonuses[bonusKey] || 0;
      if (!level) continue;
      const def = BONUS_DEFS[bonusKey];
      const hourlyCost = def.costs[level] || 0;
      const perTickCost = hourlyCost * tickHours;
      const resourceKey = def.resource;
      const current = Number(storage[resourceKey] || 0);
      if (perTickCost <= 0) continue;
      if (current >= perTickCost) {
        storage[resourceKey] = current - perTickCost;
      } else {
        storage[resourceKey] = 0;
        failed.push(bonusKey);
      }
    }
    if (failed.length) {
      messages.push(name + ' bonus drain failed: ' + failed.join(', '));
    }
  }
}

// ─── HQ Resource Sync ─────────────────────────────────────────────────────────
function syncDefenderResourcesFromHq(room) {
  const hq = room.hqTerritory || '';
  if (!hq || !room.perTerritoryStorage[hq]) { room.defenderResources = createResources(); return; }
  const src = room.perTerritoryStorage[hq];
  room.defenderResources = {
    emeralds: Math.floor(src.emeralds || 0),
    wood:     Math.floor(src.wood     || 0),
    ore:      Math.floor(src.ore      || 0),
    crops:    Math.floor(src.crops    || 0),
    fish:     Math.floor(src.fish     || 0)
  };
}

// ─── Resource Movement ────────────────────────────────────────────────────────
function moveOneHopPackets(room, selected, messages) {
  const hq = room.hqTerritory || '';
  if (!hq) return;
  const selectedAdjacency = buildSelectedAdjacency(selected);
  const arrivals = {};
  for (let i = 0; i < selected.length; i++) {
    const name = selected[i];
    if (name === hq) continue;
    const mode = (room.territoryRouteMode && room.territoryRouteMode[name]) || room.routeMode || 'fastest';
    const findPath = mode === 'cheapest' ? findSelectedOnlyPathToHqCheapest : findSelectedOnlyPathToHq;
    const path = findPath(name, hq, selectedAdjacency);
    if (!path || path.length < 2) { messages.push('No route from ' + name + ' to HQ ' + hq); continue; }
    const hop = path[1];
    const sourceStore = ensurePerTerritoryStorage(room, name);
    let anyAmount = false;
    for (let r = 0; r < RESOURCE_KEYS.length; r++) { if (sourceStore[RESOURCE_KEYS[r]] > 0) { anyAmount = true; break; } }
    if (!anyAmount) continue;
    if (!arrivals[hop]) arrivals[hop] = createResources();
    for (let r = 0; r < RESOURCE_KEYS.length; r++) {
      const key = RESOURCE_KEYS[r];
      arrivals[hop][key] += Number(sourceStore[key]) || 0;
      sourceStore[key] = 0;
    }
  }
  Object.keys(arrivals).forEach(function (dest) {
    const store = ensurePerTerritoryStorage(room, dest);
    const packet = arrivals[dest];
    for (let r = 0; r < RESOURCE_KEYS.length; r++) { store[RESOURCE_KEYS[r]] += packet[RESOURCE_KEYS[r]]; }
  });
}

// ─── Combat System ────────────────────────────────────────────────────────────

/**
 * Calculate tower stats for ANY territory.
 * Connection bonus (+30% per adjacent defender territory) applies to BOTH HP and DPS.
 */
function calcTerritoryTowerStats(room, territoryName) {
  const upgrades = (room.territoryUpgrades && room.territoryUpgrades[territoryName]) || {};
  const healthLevel  = clamp(parseInt(upgrades.health  || 0, 10) || 0, 0, 11);
  const defenseLevel = clamp(parseInt(upgrades.defense || 0, 10) || 0, 0, 11);
  const damageLevel  = clamp(parseInt(upgrades.damage  || 0, 10) || 0, 0, 11);
  const selected = room.selectedTerritories || [];
  const selectedSet = new Set(selected);
  const tradeRoutes = (territoryByName.get(territoryName) || {}).tradeRoutes || [];
  const connections = tradeRoutes.filter(function (t) { return t !== territoryName && selectedSet.has(t); }).length;
  const connectionBonus  = 1 + (0.3 * connections);   // +30% per connection
  // HP: base × health upgrade × connection bonus
  const healthMultiplier = 1 + (healthLevel * 0.25);  // +25% per health level
  const towerHP     = Math.floor(TOWER_BASE_HP * healthMultiplier * connectionBonus);
  const defenseReduction = Math.min(0.80, defenseLevel * 0.05);
  const effectiveHP = Math.floor(towerHP / (1 - defenseReduction));
  // DPS: base × damage upgrade × connection bonus (same +30% per connection)
  const damageMultiplier = 1 + (damageLevel * 0.25);  // +25% per damage level
  const towerDPS    = Math.floor(TOWER_BASE_DPS * damageMultiplier * connectionBonus);
  return { towerHP, effectiveHP, towerDPS, connections, healthLevel, defenseLevel, damageLevel, territory: territoryName };
}

// Legacy alias used by prepTick estimates (shows HQ stats)
function calcTowerStats(room) {
  const hq = room.hqTerritory || (room.selectedTerritories && room.selectedTerritories[0]) || '';
  if (!hq) return { towerHP: TOWER_BASE_HP, effectiveHP: TOWER_BASE_HP, connections: 0, healthLevel: 0, defenseLevel: 0, hq };
  const stats = calcTerritoryTowerStats(room, hq);
  return { ...stats, hq };
}

function calcWarTime(effectiveHP, dps) {
  const rawSeconds = Math.ceil(effectiveHP / dps);
  return rawSeconds + WAR_GRACE_SECONDS;
}

function calcAllWarEstimates(room) {
  const towerStats = calcTowerStats(room);
  const estimates = {};
  Object.keys(WAR_ATTACK_TYPES).forEach(function (type) {
    const dps = WAR_ATTACK_TYPES[type].dps;
    const warTimeSeconds = calcWarTime(towerStats.effectiveHP, dps);
    estimates[type] = { warTimeSeconds, dps };
  });
  return { towerStats, estimates };
}

/**
 * Queue time (seconds) before a war battle begins.
 *   - No adjacent capture → 120s (2 min)
 *   - Adjacent capture exists → (hops_from_target_to_HQ + 1) × 60s
 */
function calcQueueTime(room, targetTerritory) {
  const captured = new Set(room.attackerCapturedTerritories || []);
  const targetData = territoryByName.get(targetTerritory);
  const tradeRoutes = (targetData && targetData.tradeRoutes) || [];
  const hasAdjCapture = tradeRoutes.some(function (t) { return captured.has(t); });
  if (!hasAdjCapture) return 120; // 2-min base
  // Find hops from target territory to defender HQ
  const hq = room.hqTerritory || '';
  const selected = room.selectedTerritories || [];
  const adj = buildSelectedAdjacency(selected);
  const path = findSelectedOnlyPathToHq(targetTerritory, hq, adj);
  const hops = path ? Math.max(0, path.length - 1) : 1;
  return (hops + 1) * 60;
}

/**
 * Is this territory eligible to be attacked?
 * Rules: must still be owned by defender AND either:
 *   1. Attacker has no captures yet (can attack any territory)
 *   2. Attacker owns an adjacent territory (captured previously)
 */
function isAttackable(room, targetTerritory) {
  const defTerrStr = room.selectedTerritories || [];
  if (!defTerrStr.includes(targetTerritory)) return false;
  const captured = room.attackerCapturedTerritories || [];
  if (captured.length === 0) return true; // First attack: any territory
  const capturedSet = new Set(captured);
  const targetData = territoryByName.get(targetTerritory);
  const tradeRoutes = (targetData && targetData.tradeRoutes) || [];
  return tradeRoutes.some(function (t) { return capturedSet.has(t); });
}

/**
 * Stop any in-progress attack timers.
 */
function stopAttackTimers(room) {
  if (room.attackQueueTimer)  { clearTimeout(room.attackQueueTimer);  room.attackQueueTimer  = null; }
  if (room.attackBattleTimer) { clearTimeout(room.attackBattleTimer); room.attackBattleTimer = null; }
}

/**
 * Start an attack on a territory:
 *   1. Queue phase (calcQueueTime)
 *   2. Battle phase (calcWarTime with attacker DPS)
 *   3. Capture: remove from defender, add to attacker
 *   4. If HQ captured: war ends
 */
function startAttackOnTerritory(roomId, territoryName) {
  const room = rooms.get(roomId);
  if (!room || room.status !== 'playing') return;
  stopAttackTimers(room);

  const queueSeconds  = calcQueueTime(room, territoryName);
  const queueEndsAt   = Date.now() + queueSeconds * 1000;
  const towerStats    = calcTerritoryTowerStats(room, territoryName);
  const warType       = room.attackerWarType || 'normal';
  const attackDef     = WAR_ATTACK_TYPES[warType] || WAR_ATTACK_TYPES.normal;
  const battleSeconds = calcWarTime(towerStats.effectiveHP, attackDef.dps);
  const battleEndsAt  = queueEndsAt + battleSeconds * 1000;

  room.currentAttack = {
    territory: territoryName,
    phase: 'queue',
    queueSeconds, queueEndsAt,
    battleSeconds, battleEndsAt,
    towerStats: {
      ...towerStats,
      warType, attackerDPS: attackDef.dps,
      queueSeconds, battleSeconds, gracePeriod: WAR_GRACE_SECONDS
    }
  };

  logEvent('attack_queued', { roomId, territoryName, queueSeconds, battleSeconds, warType });
  io.to(roomId).emit('attack:queued', {
    territory: territoryName,
    queueSeconds, queueEndsAt,
    battleSeconds, battleEndsAt,
    towerStats: room.currentAttack.towerStats
  });
  emitRoomState(roomId);

  // After queue: transition to battle phase
  room.attackQueueTimer = setTimeout(function () {
    const ar = rooms.get(roomId);
    if (!ar || ar.status !== 'playing' || !ar.currentAttack || ar.currentAttack.territory !== territoryName) return;
    ar.currentAttack.phase = 'battle';
    logEvent('attack_battle', { roomId, territoryName, battleSeconds });
    io.to(roomId).emit('attack:battle', {
      territory: territoryName,
      battleSeconds,
      battleEndsAt: ar.currentAttack.battleEndsAt,
      towerStats: ar.currentAttack.towerStats
    });
    emitRoomState(roomId);

    // After battle: capture
    ar.attackBattleTimer = setTimeout(function () {
      const br = rooms.get(roomId);
      if (!br || br.status !== 'playing' || !br.currentAttack || br.currentAttack.territory !== territoryName) return;
      // Remove from defender
      br.selectedTerritories = (br.selectedTerritories || []).filter(function (t) { return t !== territoryName; });
      // Add to attacker
      if (!br.attackerCapturedTerritories.includes(territoryName)) br.attackerCapturedTerritories.push(territoryName);
      br.currentAttack = null;
      br.warTowerStats = room.currentAttack ? room.currentAttack.towerStats : null;

      logEvent('attack_captured', { roomId, territoryName });
      io.to(roomId).emit('attack:captured', { territory: territoryName });

      // Check win: HQ captured or all territories captured
      const hq = br.hqTerritory || '';
      if (territoryName === hq || br.selectedTerritories.length === 0) {
        br.warResult = 'attacker_wins';
        io.to(roomId).emit('war:ended', { result: 'attacker_wins', capturedTerritory: territoryName, warTowerStats: br.warTowerStats });
        logEvent('war_ended', { roomId, result: 'attacker_wins' });
      }
      emitRoomState(roomId);
    }, battleSeconds * 1000);
  }, queueSeconds * 1000);
}

// ─── Eco Tick ─────────────────────────────────────────────────────────────────
function runEcoTick(roomId) {
  const room = rooms.get(roomId);
  if (!room) return;
  if (room.status !== 'prep' && room.status !== 'playing') return;
  const messages = [];
  const selected = Array.isArray(room.selectedTerritories) ? room.selectedTerritories : [];
  if (!selected.length) {
    syncDefenderResourcesFromHq(room);
    io.to(roomId).emit('tick:update', {
      defenderResources: room.defenderResources, perTerritoryStorage: room.perTerritoryStorage,
      messages: ['No territories selected.'], nextTickInMs: room.tickIntervalMs,
      serverNow: Date.now(), hqTerritory: room.hqTerritory || '', tickCount: room.tickCount
    });
    return;
  }
  if (!room.hqTerritory || selected.indexOf(room.hqTerritory) === -1) {
    room.hqTerritory = selected[0];
  }
  const selectedSet = new Set(selected);
  // Remove storage for deselected territories
  Object.keys(room.perTerritoryStorage).forEach(function (name) {
    if (!selectedSet.has(name)) delete room.perTerritoryStorage[name];
  });
  for (let i = 0; i < selected.length; i++) { ensurePerTerritoryStorage(room, selected[i]); }

  // 1. Produce resources per territory (with treasury + production bonuses)
  for (let i = 0; i < selected.length; i++) {
    const name = selected[i];
    const territory = territoryByName.get(name);
    if (!territory) { messages.push('No data for: ' + name); continue; }
    const localStore = ensurePerTerritoryStorage(room, name);
    const bonuses = ensureTerritoryBonusShape(room, name);
    const treasuryBonus = getTreasuryBonus(room, name);
    // Efficient bonuses: +% per level; Rate bonuses: additional +% (same effect in tick model)
    const emProdBonus  = bonuses.efficientEmeralds  * EFFICIENT_EM_PER_LEVEL
                       + bonuses.emeraldRate        * EM_RATE_PER_LEVEL;
    const resProdBonus = bonuses.efficientResources * EFFICIENT_RES_PER_LEVEL
                       + bonuses.resourceRate       * RES_RATE_PER_LEVEL;
    const emMult  = 1 + treasuryBonus + emProdBonus;
    const resMult = 1 + treasuryBonus + resProdBonus;
    localStore.emeralds += Math.floor(territory.resources.emeralds * emMult);
    localStore.wood     += Math.floor(territory.resources.wood     * resMult);
    localStore.ore      += Math.floor(territory.resources.ore      * resMult);
    localStore.crops    += Math.floor(territory.resources.crops    * resMult);
    localStore.fish     += Math.floor(territory.resources.fish     * resMult);
  }

  // 2. Move resources one hop toward HQ (no tax — all defender's own territories)
  moveOneHopPackets(room, selected, messages);

  // 3. Cap HQ only
  applyHqStorageCap(room, messages);

  // 4. Upgrade drain
  applyUpgradeDrain(room, selected, messages);

  // 5. Bonus drain
  applyBonusDrain(room, selected, messages);

  // 6. Final HQ cap after drains
  applyHqStorageCap(room, messages);

  // 7. Sync defender HQ resources
  syncDefenderResourcesFromHq(room);

  room.tickCount += 1;
  room.nextTickAt = Date.now() + room.tickIntervalMs;
  io.to(roomId).emit('tick:update', {
    defenderResources: room.defenderResources,
    perTerritoryStorage: room.perTerritoryStorage,
    messages, nextTickInMs: room.tickIntervalMs,
    serverNow: Date.now(), hqTerritory: room.hqTerritory, tickCount: room.tickCount
  });
  emitRoomState(roomId);
}

// ─── Timers ───────────────────────────────────────────────────────────────────
function stopPrepTicker(room) { if (room.prepTimer) { clearInterval(room.prepTimer); room.prepTimer = null; } }
function stopTickLoop(room) { if (room.tickTimer) { clearInterval(room.tickTimer); room.tickTimer = null; } room.nextTickAt = null; }

function startTickLoop(roomId) {
  const room = rooms.get(roomId);
  if (!room || room.tickTimer) return;
  room.nextTickAt = Date.now() + room.tickIntervalMs;
  room.tickTimer = setInterval(function () { runEcoTick(roomId); }, room.tickIntervalMs);
}

function startPrepCountdown(roomId) {
  const room = rooms.get(roomId);
  if (!room) return;
  stopPrepTicker(room);
  room.status = 'prep';
  room.prepSecondsRemaining = PREP_SECONDS;
  io.to(roomId).emit('statusChanged', { status: room.status });
  emitRoomState(roomId);
  startTickLoop(roomId);
  room.prepTimer = setInterval(function () {
    const activeRoom = rooms.get(roomId);
    if (!activeRoom) return;
    activeRoom.prepSecondsRemaining -= 1;
    // Broadcast live war estimates during prep so attacker panel can update
    const { towerStats, estimates } = calcAllWarEstimates(activeRoom);
    io.to(roomId).emit('prepTick', {
      secondsRemaining: activeRoom.prepSecondsRemaining,
      warEstimates: { towerStats, estimates }
    });
    emitRoomState(roomId);
    if (activeRoom.prepSecondsRemaining <= 0) {
      stopPrepTicker(activeRoom);
      activeRoom.status = 'playing';
      activeRoom.prepSecondsRemaining = 0;
      io.to(roomId).emit('statusChanged', { status: activeRoom.status });
      // No auto-war: attacker manually selects territory to attack
      io.to(roomId).emit('playing:started', {
        message: 'Prep complete! Attacker: click a territory on the map to begin your assault.',
        attackerWarType: activeRoom.attackerWarType
      });
      emitRoomState(roomId);
    }
  }, 1000);
}

// ─── Room Lifecycle ────────────────────────────────────────────────────────────
function hasRolePresence(room, role) {
  if (role === 'defender') return !!room.defenderSocketId || !!room.defenderPendingDisconnectAt;
  if (role === 'attacker') return !!room.attackerSocketId || !!room.attackerPendingDisconnectAt;
  return false;
}
function cleanupRoomIfEmpty(roomId) {
  const room = rooms.get(roomId);
  if (!room) return;
  if (!hasRolePresence(room, 'defender') && !hasRolePresence(room, 'attacker')) {
    stopPrepTicker(room); stopTickLoop(room); stopAttackTimers(room);
    if (room.defenderDisconnectTimer) { clearTimeout(room.defenderDisconnectTimer); room.defenderDisconnectTimer = null; }
    if (room.attackerDisconnectTimer) { clearTimeout(room.attackerDisconnectTimer); room.attackerDisconnectTimer = null; }
    rooms.delete(roomId);
    metrics.roomDeleteTotal += 1;
    logEvent('room_deleted', { roomId });
  }
}
function roleSocketField(role)  { return role === 'defender' ? 'defenderSocketId' : 'attackerSocketId'; }
function rolePendingField(role) { return role === 'defender' ? 'defenderPendingDisconnectAt' : 'attackerPendingDisconnectAt'; }
function roleTimerField(role)   { return role === 'defender' ? 'defenderDisconnectTimer' : 'attackerDisconnectTimer'; }
function roleTokenField(role)   { return role === 'defender' ? 'defenderSessionToken' : 'attackerSessionToken'; }
function clearDisconnectGrace(room, role) {
  const timerField = roleTimerField(role);
  if (room[timerField]) { clearTimeout(room[timerField]); room[timerField] = null; }
  room[rolePendingField(role)] = null;
}
function scheduleDisconnectGrace(roomId, role) {
  const room = rooms.get(roomId);
  if (!room) return;
  clearDisconnectGrace(room, role);
  room[rolePendingField(role)] = Date.now();
  room[roleTimerField(role)] = setTimeout(function () {
    const activeRoom = rooms.get(roomId);
    if (!activeRoom) return;
    if (activeRoom[roleSocketField(role)]) return;
    activeRoom[rolePendingField(role)] = null;
    activeRoom[roleTimerField(role)] = null;
    if (role === 'attacker') activeRoom.attackerSessionToken = '';
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
    stopPrepTicker(room); stopTickLoop(room); stopAttackTimers(room);
    room.status = 'lobby';
    room.prepSecondsRemaining = null;
    room.attackerCapturedTerritories = [];
    room.currentAttack = null;
    room.warResult = null; room.warTowerStats = null;
    io.to(room.id).emit('statusChanged', { status: room.status });
  }
}

// ─── Upgrade / Bonus Application ──────────────────────────────────────────────
function applyUpgrade(room, territoryName, category) {
  const resourceKey = UPGRADE_RESOURCE_BY_CATEGORY[category];
  if (!resourceKey) return { ok: false, error: 'Invalid upgrade category.' };
  const upgrades = ensureTerritoryUpgradeShape(room, territoryName);
  const currentLevel = normalizeUpgradeLevel(upgrades[category]);
  if (currentLevel >= 11) return { ok: false, error: category + ' is already at max level.' };
  const nextLevel = currentLevel + 1;
  upgrades[category] = nextLevel;
  return { ok: true, territoryName, category, level: nextLevel, resourceKey, hourlyCost: UPGRADE_COSTS_PER_LEVEL[nextLevel] || 0 };
}

/**
 * Apply a one-time storage upgrade (Emerald Storage or Resource Storage).
 * Immediately deducts cost from the HQ storage, then raises the level cap.
 */
function applyStorageUpgrade(room, territoryName, category) {
  if (!STORAGE_CATEGORIES.includes(category)) return { ok: false, error: 'Invalid storage category.' };
  const upgrades = ensureTerritoryUpgradeShape(room, territoryName);
  const currentLevel = normalizeUpgradeLevel(upgrades[category]);
  if (currentLevel >= 11) return { ok: false, error: category + ' already at max.' };
  const nextLevel = currentLevel + 1;
  const resourceKey = STORAGE_RESOURCE_BY_CATEGORY[category];
  const cost = STORAGE_COSTS_BY_CATEGORY[category][nextLevel] || 0;
  // Deduct cost from HQ storage
  const hq = room.hqTerritory || '';
  const hqStore = hq ? room.perTerritoryStorage[hq] : null;
  if (cost > 0) {
    const available = Number((hqStore && hqStore[resourceKey]) || 0);
    if (available < cost) return { ok: false, error: 'Not enough ' + resourceKey + ' in HQ. Need ' + cost + ', have ' + Math.floor(available) + '.' };
    hqStore[resourceKey] = available - cost;
  }
  upgrades[category] = nextLevel;
  return { ok: true, territoryName, category, level: nextLevel, resourceKey, cost };
}

function applyBonus(room, territoryName, bonusKey) {
  const def = BONUS_DEFS[bonusKey];
  if (!def) return { ok: false, error: 'Invalid bonus.' };
  const bonuses = ensureTerritoryBonusShape(room, territoryName);
  const currentLevel = bonuses[bonusKey] || 0;
  if (currentLevel >= def.maxLevel) return { ok: false, error: bonusKey + ' already at max.' };
  const nextLevel = currentLevel + 1;
  bonuses[bonusKey] = nextLevel;
  return { ok: true, territoryName, bonusKey, level: nextLevel, resource: def.resource, hourlyCost: def.costs[nextLevel] || 0 };
}

// ─── Socket Events ────────────────────────────────────────────────────────────
io.on('connection', function (socket) {
  metrics.connectionsTotal += 1;
  socket.data.clientIp = getClientIp(socket);
  logEvent('socket_connected', { socketId: socket.id, ip: socket.data.clientIp });

  // ── createRoom ──────────────────────────────────────────────────────────────
  socket.on('createRoom', function (_, ack) {
    if (!requirePrivilegedAccess(socket, ack, 'createRoom')) return;
    if (!allowEventRate(socket, 'createRoom')) { if (typeof ack === 'function') ack({ ok: false, error: 'Rate limited.' }); return; }
    const roomId = generateRoomId();
    if (!roomId) { if (typeof ack === 'function') ack({ ok: false, error: 'Failed to allocate room.' }); return; }
    const room = createRoomState(roomId, socket.id);
    rooms.set(roomId, room);
    socket.join(roomId);
    socket.data.roomId = roomId;
    socket.data.role = 'defender';
    metrics.roomCreateTotal += 1;
    logEvent('room_created', { roomId, socketId: socket.id });
    emitRoomState(roomId);
    if (typeof ack === 'function') ack({ ok: true, roomId, role: 'defender', playerToken: room.defenderSessionToken });
  });

  // ── joinRoom ────────────────────────────────────────────────────────────────
  socket.on('joinRoom', function (payload, ack) {
    if (!allowEventRate(socket, 'joinRoom')) { if (typeof ack === 'function') ack({ ok: false, error: 'Rate limited.' }); return; }
    const roomId = payload && typeof payload.roomId === 'string' ? payload.roomId.trim() : '';
    if (!/^\d{6}$/.test(roomId)) { if (typeof ack === 'function') ack({ ok: false, error: 'Room code must be 6 digits.' }); return; }
    const room = rooms.get(roomId);
    if (!room) { if (typeof ack === 'function') ack({ ok: false, error: 'Room not found.' }); return; }
    if (room.attackerSocketId && room.attackerSocketId !== socket.id) {
      if (typeof ack === 'function') ack({ ok: false, error: 'Room already full.' }); return;
    }
    room.attackerSocketId = socket.id;
    room.attackerSessionToken = crypto.randomBytes(16).toString('hex');
    clearDisconnectGrace(room, 'attacker');
    room.attackerReady = false;
    socket.join(roomId);
    socket.data.roomId = roomId;
    socket.data.role = 'attacker';
    metrics.roomJoinTotal += 1;
    logEvent('room_joined', { roomId, socketId: socket.id });
    emitRoomState(roomId);
    if (typeof ack === 'function') ack({ ok: true, roomId, role: 'attacker', playerToken: room.attackerSessionToken });
  });

  // ── resumeRoom ──────────────────────────────────────────────────────────────
  socket.on('resumeRoom', function (payload, ack) {
    if (!requirePrivilegedAccess(socket, ack, 'resumeRoom')) return;
    if (!allowEventRate(socket, 'resumeRoom')) { if (typeof ack === 'function') ack({ ok: false, error: 'Rate limited.' }); return; }
    const roomId = payload && typeof payload.roomId === 'string' ? payload.roomId.trim() : '';
    const playerToken = payload && typeof payload.playerToken === 'string' ? payload.playerToken.trim() : '';
    if (!/^\d{6}$/.test(roomId) || !playerToken) { metrics.resumeFailTotal += 1; if (typeof ack === 'function') ack({ ok: false, error: 'Invalid payload.' }); return; }
    const room = rooms.get(roomId);
    if (!room) { metrics.resumeFailTotal += 1; if (typeof ack === 'function') ack({ ok: false, error: 'Room not found.' }); return; }
    let role = null;
    if (room.defenderSessionToken && room.defenderSessionToken === playerToken) role = 'defender';
    else if (room.attackerSessionToken && room.attackerSessionToken === playerToken) role = 'attacker';
    if (!role) { metrics.resumeFailTotal += 1; if (typeof ack === 'function') ack({ ok: false, error: 'Invalid token.' }); return; }
    room[roleSocketField(role)] = socket.id;
    clearDisconnectGrace(room, role);
    socket.join(roomId);
    socket.data.roomId = roomId;
    socket.data.role = role;
    metrics.resumeSuccessTotal += 1;
    logEvent('room_resumed', { roomId, role, socketId: socket.id });
    emitRoomState(roomId);
    if (typeof ack === 'function') ack({ ok: true, roomId, role });
  });

  // ── updateSelection ─────────────────────────────────────────────────────────
  socket.on('updateSelection', function (payload, ack) {
    if (!allowEventRate(socket, 'updateSelection')) { if (typeof ack === 'function') ack({ ok: false, error: 'Rate limited.' }); return; }
    const roomId = socket.data.roomId;
    const room = roomId ? rooms.get(roomId) : null;
    if (!room) { if (typeof ack === 'function') ack({ ok: false, error: 'Not in a room.' }); return; }
    if (roleForSocket(room, socket.id) !== 'defender') { if (typeof ack === 'function') ack({ ok: false, error: 'Defender only.' }); return; }
    if (room.status !== 'lobby') { if (typeof ack === 'function') ack({ ok: false, error: 'Lobby only.' }); return; }
    const selected = payload && Array.isArray(payload.selectedTerritories)
      ? payload.selectedTerritories.filter(n => typeof n === 'string' && n.trim()).map(n => n.trim())
      : [];
    const newSelected = Array.from(new Set(selected));
    const now = Date.now();
    const prevSelected = new Set(room.selectedTerritories || []);
    if (!room.territoryHeldSince) room.territoryHeldSince = {};
    // Track hold-time: newly added gets now; removed gets deleted
    newSelected.forEach(function (name) { if (!prevSelected.has(name)) room.territoryHeldSince[name] = now; });
    Object.keys(room.territoryHeldSince).forEach(function (name) {
      if (!newSelected.includes(name)) delete room.territoryHeldSince[name];
    });
    room.selectedTerritories = newSelected;
    room.hqTerritory = newSelected[0] || '';
    room.defenderReady = false;
    room.attackerReady = false;
    emitRoomState(roomId);
    if (typeof ack === 'function') ack({ ok: true });
  });

  // ── setReady ────────────────────────────────────────────────────────────────
  socket.on('setReady', async function (payload, ack) {
    if (!requirePrivilegedAccess(socket, ack, 'setReady')) return;
    if (!allowEventRate(socket, 'setReady')) { if (typeof ack === 'function') ack({ ok: false, error: 'Rate limited.' }); return; }
    const roomId = socket.data.roomId;
    const room = roomId ? rooms.get(roomId) : null;
    if (!room) { if (typeof ack === 'function') ack({ ok: false, error: 'Not in a room.' }); return; }
    const role = roleForSocket(room, socket.id);
    if (!role) { if (typeof ack === 'function') ack({ ok: false, error: 'Invalid role.' }); return; }
    if (room.status !== 'lobby') { if (typeof ack === 'function') ack({ ok: false, error: 'Lobby only.' }); return; }
    const ready = !!(payload && payload.ready);
    if (role === 'defender') room.defenderReady = ready;
    if (role === 'attacker') room.attackerReady = ready;
    emitRoomState(roomId);
    if (room.defenderReady && room.attackerReady) {
      try { await ensureTerritoryCacheLoaded(); }
      catch (e) {
        room.defenderReady = false; room.attackerReady = false;
        emitRoomState(roomId);
        emitRoomError(roomId, 'Territory cache unavailable: ' + (e instanceof Error ? e.message : String(e)));
        if (typeof ack === 'function') ack({ ok: false, error: 'Territory cache unavailable.' }); return;
      }
      startPrepCountdown(roomId);
    }
    if (typeof ack === 'function') ack({ ok: true });
  });

  // ── upgrade:apply ───────────────────────────────────────────────────────────
  socket.on('upgrade:apply', function (payload, ack) {
    if (!requirePrivilegedAccess(socket, ack, 'upgrade:apply')) return;
    if (!allowEventRate(socket, 'upgrade:apply')) { if (typeof ack === 'function') ack({ ok: false, error: 'Rate limited.' }); return; }
    const roomId = socket.data.roomId;
    const room = roomId ? rooms.get(roomId) : null;
    if (!room) { if (typeof ack === 'function') ack({ ok: false, error: 'Not in a room.' }); return; }
    if (room.status !== 'playing' && room.status !== 'prep') { if (typeof ack === 'function') ack({ ok: false, error: 'Prep/playing only.' }); return; }
    if (roleForSocket(room, socket.id) !== 'defender') { if (typeof ack === 'function') ack({ ok: false, error: 'Defender only.' }); return; }
    const territoryName = payload && typeof payload.territoryName === 'string' ? payload.territoryName.trim() : '';
    const category     = payload && typeof payload.category === 'string' ? payload.category.trim() : '';
    if (!territoryName || room.selectedTerritories.indexOf(territoryName) === -1) {
      if (typeof ack === 'function') ack({ ok: false, error: 'Territory not in room.' }); return;
    }
    const result = applyUpgrade(room, territoryName, category);
    if (!result.ok) { if (typeof ack === 'function') ack(result); return; }
    io.to(roomId).emit('upgrade:applied', result);
    emitRoomState(roomId);
    if (typeof ack === 'function') ack({ ok: true, ...result });
  });

  // ── bonus:apply ─────────────────────────────────────────────────────────────
  socket.on('bonus:apply', function (payload, ack) {
    if (!requirePrivilegedAccess(socket, ack, 'bonus:apply')) return;
    if (!allowEventRate(socket, 'bonus:apply')) { if (typeof ack === 'function') ack({ ok: false, error: 'Rate limited.' }); return; }
    const roomId = socket.data.roomId;
    const room = roomId ? rooms.get(roomId) : null;
    if (!room) { if (typeof ack === 'function') ack({ ok: false, error: 'Not in a room.' }); return; }
    if (room.status !== 'playing' && room.status !== 'prep') { if (typeof ack === 'function') ack({ ok: false, error: 'Prep/playing only.' }); return; }
    if (roleForSocket(room, socket.id) !== 'defender') { if (typeof ack === 'function') ack({ ok: false, error: 'Defender only.' }); return; }
    const territoryName = payload && typeof payload.territoryName === 'string' ? payload.territoryName.trim() : '';
    const bonusKey      = payload && typeof payload.bonusKey === 'string' ? payload.bonusKey.trim() : '';
    if (!territoryName || room.selectedTerritories.indexOf(territoryName) === -1) {
      if (typeof ack === 'function') ack({ ok: false, error: 'Territory not in room.' }); return;
    }
    const result = applyBonus(room, territoryName, bonusKey);
    if (!result.ok) { if (typeof ack === 'function') ack(result); return; }
    io.to(roomId).emit('bonus:applied', result);
    emitRoomState(roomId);
    if (typeof ack === 'function') ack({ ok: true, ...result });
  });

  // ── storage:apply ────────────────────────────────────────────────────────────
  socket.on('storage:apply', function (payload, ack) {
    if (!requirePrivilegedAccess(socket, ack, 'storage:apply')) return;
    if (!allowEventRate(socket, 'storage:apply')) { if (typeof ack === 'function') ack({ ok: false, error: 'Rate limited.' }); return; }
    const roomId = socket.data.roomId;
    const room = roomId ? rooms.get(roomId) : null;
    if (!room) { if (typeof ack === 'function') ack({ ok: false, error: 'Not in a room.' }); return; }
    if (room.status !== 'playing' && room.status !== 'prep') { if (typeof ack === 'function') ack({ ok: false, error: 'Prep/playing only.' }); return; }
    if (roleForSocket(room, socket.id) !== 'defender') { if (typeof ack === 'function') ack({ ok: false, error: 'Defender only.' }); return; }
    const territoryName = payload && typeof payload.territoryName === 'string' ? payload.territoryName.trim() : '';
    const category      = payload && typeof payload.category === 'string' ? payload.category.trim() : '';
    if (!territoryName || room.selectedTerritories.indexOf(territoryName) === -1) {
      if (typeof ack === 'function') ack({ ok: false, error: 'Territory not in room.' }); return;
    }
    const result = applyStorageUpgrade(room, territoryName, category);
    if (!result.ok) { if (typeof ack === 'function') ack(result); return; }
    io.to(roomId).emit('storage:applied', result);
    emitRoomState(roomId);
    if (typeof ack === 'function') ack({ ok: true, ...result });
  });

  // ── setTaxRate ──────────────────────────────────────────────────────────────
  socket.on('setTaxRate', function (payload, ack) {
    if (!requirePrivilegedAccess(socket, ack, 'setTaxRate')) return;
    if (!allowEventRate(socket, 'setTaxRate')) { if (typeof ack === 'function') ack({ ok: false, error: 'Rate limited.' }); return; }
    const roomId = socket.data.roomId;
    const room = roomId ? rooms.get(roomId) : null;
    if (!room) { if (typeof ack === 'function') ack({ ok: false, error: 'Not in a room.' }); return; }
    if (roleForSocket(room, socket.id) !== 'defender') { if (typeof ack === 'function') ack({ ok: false, error: 'Defender only.' }); return; }
    const territoryName = payload && typeof payload.territoryName === 'string' ? payload.territoryName.trim() : '';
    if (!territoryName || room.selectedTerritories.indexOf(territoryName) === -1) {
      if (typeof ack === 'function') ack({ ok: false, error: 'Territory not in room.' }); return;
    }
    const enemy = typeof payload.enemy === 'number' ? clamp(payload.enemy, 0.05, 0.40) : 0.30;
    const ally  = typeof payload.ally  === 'number' ? clamp(payload.ally,  0.05, 0.40) : 0.10;
    if (!room.territoryTaxRates) room.territoryTaxRates = {};
    room.territoryTaxRates[territoryName] = { enemy, ally };
    emitRoomState(roomId);
    if (typeof ack === 'function') ack({ ok: true, territoryName, enemy, ally });
  });

  // ── setTerritoryRouteMode ───────────────────────────────────────────────────
  socket.on('setTerritoryRouteMode', function (payload, ack) {
    if (!requirePrivilegedAccess(socket, ack, 'setTerritoryRouteMode')) return;
    if (!allowEventRate(socket, 'setTerritoryRouteMode')) { if (typeof ack === 'function') ack({ ok: false, error: 'Rate limited.' }); return; }
    const roomId = socket.data.roomId;
    const room = roomId ? rooms.get(roomId) : null;
    if (!room) { if (typeof ack === 'function') ack({ ok: false, error: 'Not in a room.' }); return; }
    if (roleForSocket(room, socket.id) !== 'defender') { if (typeof ack === 'function') ack({ ok: false, error: 'Defender only.' }); return; }
    const territoryName = payload && typeof payload.territoryName === 'string' ? payload.territoryName.trim() : '';
    if (!territoryName || room.selectedTerritories.indexOf(territoryName) === -1) {
      if (typeof ack === 'function') ack({ ok: false, error: 'Territory not in room.' }); return;
    }
    const mode = payload && payload.routeMode === 'cheapest' ? 'cheapest' : 'fastest';
    if (!room.territoryRouteMode) room.territoryRouteMode = {};
    room.territoryRouteMode[territoryName] = mode;
    emitRoomState(roomId);
    if (typeof ack === 'function') ack({ ok: true, territoryName, routeMode: mode });
  });

  // ── attacker:selectWarType ──────────────────────────────────────────────────
  // Can be changed any time before or during playing (before an attack is queued)
  socket.on('attacker:selectWarType', function (payload, ack) {
    if (!allowEventRate(socket, 'attacker:selectWarType')) { if (typeof ack === 'function') ack({ ok: false, error: 'Rate limited.' }); return; }
    const roomId = socket.data.roomId;
    const room = roomId ? rooms.get(roomId) : null;
    if (!room) { if (typeof ack === 'function') ack({ ok: false, error: 'Not in a room.' }); return; }
    if (roleForSocket(room, socket.id) !== 'attacker') { if (typeof ack === 'function') ack({ ok: false, error: 'Attacker only.' }); return; }
    if (room.status === 'playing' && room.currentAttack) { if (typeof ack === 'function') ack({ ok: false, error: 'Cannot change war type while attack is in progress.' }); return; }
    const warType = payload && WAR_ATTACK_TYPES[payload.warType] ? payload.warType : 'normal';
    room.attackerWarType = warType;
    const { towerStats, estimates } = calcAllWarEstimates(room);
    io.to(roomId).emit('war:estimates', { warType, towerStats, estimates });
    emitRoomState(roomId);
    if (typeof ack === 'function') ack({ ok: true, warType, estimates });
  });

  // ── attacker:attack ─────────────────────────────────────────────────────────
  socket.on('attacker:attack', function (payload, ack) {
    if (!allowEventRate(socket, 'attacker:attack')) { if (typeof ack === 'function') ack({ ok: false, error: 'Rate limited.' }); return; }
    const roomId = socket.data.roomId;
    const room = roomId ? rooms.get(roomId) : null;
    if (!room) { if (typeof ack === 'function') ack({ ok: false, error: 'Not in a room.' }); return; }
    if (roleForSocket(room, socket.id) !== 'attacker') { if (typeof ack === 'function') ack({ ok: false, error: 'Attacker only.' }); return; }
    if (room.status !== 'playing') { if (typeof ack === 'function') ack({ ok: false, error: 'Playing phase only.' }); return; }
    if (room.currentAttack) { if (typeof ack === 'function') ack({ ok: false, error: 'An attack is already in progress.' }); return; }
    const territoryName = payload && typeof payload.territoryName === 'string' ? payload.territoryName.trim() : '';
    if (!territoryName) { if (typeof ack === 'function') ack({ ok: false, error: 'territoryName required.' }); return; }
    if (!isAttackable(room, territoryName)) {
      if (typeof ack === 'function') ack({ ok: false, error: 'Territory is not attackable. Must be a defender territory adjacent to a captured territory (or first attack).' }); return;
    }
    startAttackOnTerritory(roomId, territoryName);
    if (typeof ack === 'function') ack({ ok: true, territory: territoryName });
  });

  // ── setHqTerritory ──────────────────────────────────────────────────────────
  socket.on('setHqTerritory', function (payload, ack) {
    if (!requirePrivilegedAccess(socket, ack, 'setHqTerritory')) return;
    if (!allowEventRate(socket, 'setHqTerritory')) { if (typeof ack === 'function') ack({ ok: false, error: 'Rate limited.' }); return; }
    const roomId = socket.data.roomId;
    const room = roomId ? rooms.get(roomId) : null;
    if (!room) { if (typeof ack === 'function') ack({ ok: false, error: 'Not in a room.' }); return; }
    if (room.status !== 'prep' && room.status !== 'playing') { if (typeof ack === 'function') ack({ ok: false, error: 'Prep/playing only.' }); return; }
    if (roleForSocket(room, socket.id) !== 'defender') { if (typeof ack === 'function') ack({ ok: false, error: 'Defender only.' }); return; }
    const territoryName = payload && typeof payload.territoryName === 'string' ? payload.territoryName.trim() : '';
    if (!territoryName || room.selectedTerritories.indexOf(territoryName) === -1) {
      if (typeof ack === 'function') ack({ ok: false, error: 'Must be one of selected territories.' }); return;
    }
    room.hqTerritory = territoryName;
    emitRoomState(roomId);
    if (typeof ack === 'function') ack({ ok: true, hqTerritory: room.hqTerritory });
  });

  // ── eco:setRouteMode (global fallback, kept for compatibility) ───────────────
  socket.on('eco:setRouteMode', function (payload, ack) {
    if (!requirePrivilegedAccess(socket, ack, 'eco:setRouteMode')) return;
    const roomId = socket.data.roomId;
    const room = roomId ? rooms.get(roomId) : null;
    if (!room) { if (typeof ack === 'function') ack({ ok: false, error: 'Not in a room.' }); return; }
    if (roleForSocket(room, socket.id) !== 'defender') { if (typeof ack === 'function') ack({ ok: false, error: 'Defender only.' }); return; }
    room.routeMode = payload && payload.routeMode === 'cheapest' ? 'cheapest' : 'fastest';
    emitRoomState(roomId);
    if (typeof ack === 'function') ack({ ok: true, routeMode: room.routeMode });
  });

  // ── eco:setProductionMultiplier (kept for compatibility) ────────────────────
  socket.on('eco:setProductionMultiplier', function (payload, ack) {
    if (!requirePrivilegedAccess(socket, ack, 'eco:setProductionMultiplier')) return;
    const roomId = socket.data.roomId;
    const room = roomId ? rooms.get(roomId) : null;
    if (!room) { if (typeof ack === 'function') ack({ ok: false, error: 'Not in a room.' }); return; }
    if (roleForSocket(room, socket.id) !== 'defender') { if (typeof ack === 'function') ack({ ok: false, error: 'Defender only.' }); return; }
    const raw = payload && payload.multiplier != null ? Number(payload.multiplier) : NaN;
    if (!Number.isFinite(raw)) { if (typeof ack === 'function') ack({ ok: false, error: 'Invalid multiplier.' }); return; }
    room.productionMultiplier = clamp(raw, PRODUCTION_MULT_MIN, PRODUCTION_MULT_MAX);
    emitRoomState(roomId);
    if (typeof ack === 'function') ack({ ok: true, productionMultiplier: room.productionMultiplier });
  });

  // ── attacker:selectWarType ──────────────────────────────────────────────────
  socket.on('attacker:selectWarType', function (payload, ack) {
    if (!allowEventRate(socket, 'attacker:selectWarType')) { if (typeof ack === 'function') ack({ ok: false, error: 'Rate limited.' }); return; }
    const roomId = socket.data.roomId;
    const room = roomId ? rooms.get(roomId) : null;
    if (!room) { if (typeof ack === 'function') ack({ ok: false, error: 'Not in a room.' }); return; }
    if (roleForSocket(room, socket.id) !== 'attacker') { if (typeof ack === 'function') ack({ ok: false, error: 'Attacker only.' }); return; }
    if (room.status !== 'lobby' && room.status !== 'prep') { if (typeof ack === 'function') ack({ ok: false, error: 'Lobby/prep only.' }); return; }
    const warType = payload && WAR_ATTACK_TYPES[payload.warType] ? payload.warType : 'normal';
    room.attackerWarType = warType;
    const { towerStats, estimates } = calcAllWarEstimates(room);
    io.to(roomId).emit('war:estimates', { warType, towerStats, estimates });
    emitRoomState(roomId);
    if (typeof ack === 'function') ack({ ok: true, warType, estimates });
  });

  // ── war:getEstimates ────────────────────────────────────────────────────────
  socket.on('war:getEstimates', function (_, ack) {
    const roomId = socket.data.roomId;
    const room = roomId ? rooms.get(roomId) : null;
    if (!room) { if (typeof ack === 'function') ack({ ok: false, error: 'Not in a room.' }); return; }
    const { towerStats, estimates } = calcAllWarEstimates(room);
    if (typeof ack === 'function') ack({ ok: true, towerStats, estimates });
  });

  // ── leaveRoom ───────────────────────────────────────────────────────────────
  socket.on('leaveRoom', function (_, ack) {
    const roomId = socket.data.roomId;
    const room = roomId ? rooms.get(roomId) : null;
    if (!room) { if (typeof ack === 'function') ack({ ok: true }); return; }
    const role = roleForSocket(room, socket.id);
    if (role === 'defender') { room.defenderSocketId = null; clearDisconnectGrace(room, 'defender'); }
    if (role === 'attacker') { room.attackerSocketId = null; room.attackerSessionToken = ''; clearDisconnectGrace(room, 'attacker'); }
    resetRoomOnPlayerLeave(room);
    socket.leave(roomId);
    socket.data.roomId = null;
    socket.data.role = null;
    emitRoomState(roomId);
    cleanupRoomIfEmpty(roomId);
    if (typeof ack === 'function') ack({ ok: true });
  });

  // ── disconnect ──────────────────────────────────────────────────────────────
  socket.on('disconnect', function () {
    metrics.disconnectionsTotal += 1;
    const roomId = socket.data.roomId;
    const room = roomId ? rooms.get(roomId) : null;
    socketRateState.delete(socket.id);
    logEvent('socket_disconnected', { socketId: socket.id, roomId: roomId || '' });
    if (!room) return;
    const role = roleForSocket(room, socket.id);
    if (!role) return;
    room[roleSocketField(role)] = null;
    scheduleDisconnectGrace(roomId, role);
    emitRoomState(roomId);
  });
});

// ─── HTTP Endpoints ───────────────────────────────────────────────────────────
app.get('/health', function (_req, res) {
  res.json({
    ok: true, rooms: rooms.size,
    territoryCacheSize: territoryByName.size,
    territoryCacheLoadedAt, territoryCacheLoadError,
    uptimeMs: Date.now() - metrics.startedAt,
    tokenAuthEnabled: !!ECO_WAR_SHARED_TOKEN,
    allowedOrigins: ALLOWED_ORIGINS,
    rateLimit: { windowMs: RATE_LIMIT_WINDOW_MS, maxPerSocket: RATE_LIMIT_MAX_SOCKET, maxPerIp: RATE_LIMIT_MAX_IP },
    roomLifecycle: { disconnectGraceMs: DISCONNECT_GRACE_MS },
    metrics
  });
});

// ─── Startup ──────────────────────────────────────────────────────────────────
loadTerritoryCache()
  .then(function () { console.log('Territory cache loaded. Entries: ' + territoryByName.size); })
  .catch(function (e) {
    territoryCacheLoadError = e instanceof Error ? e.message : String(e);
    console.error('Territory cache startup load failed:', territoryCacheLoadError);
  })
  .finally(function () {
    server.listen(PORT, function () { console.log('Socket room server listening on port ' + PORT); });
  });
