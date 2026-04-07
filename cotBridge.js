const WebSocket = require('ws');
const axios = require('axios');
const { Builder } = require('xml2js');
const { v4: uuidv4 } = require('uuid');

// ---------------------------------------------------------------------------
// Configuration (env vars with defaults)
// ---------------------------------------------------------------------------
const config = {
  PEAT_WS_URL:    process.env.PEAT_WS_URL    || 'ws://localhost:8090/ws',
  PEAT_ROOM_NAME: process.env.PEAT_ROOM_NAME  || 'cop-bridge',
  COP_HTTP_URL:   process.env.COP_HTTP_URL    || 'http://localhost:3000',
  COP_WS_URL:     process.env.COP_WS_URL      || 'ws://localhost:8080',
  BRIDGE_NAME:    process.env.BRIDGE_NAME      || 'COP-Bridge',
  RECONNECT_MS:   parseInt(process.env.RECONNECT_MS || '3000', 10),
};

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
const peatOrigins    = new Set();   // UIDs that came from peat-chat
const copOrigins     = new Set();   // UIDs that came from COP
const copEntityCache = new Map();   // uid -> { type, lat, lon, hae, ce, le }
const copMarkersSent = new Set();   // COP marker UIDs already forwarded

let peatSelfId = null;   // assigned by peat-chat on connect
let peatRoomId = null;   // hex room ID after join_room
let peatWs     = null;
let copWs      = null;

const xmlBuilder = new Builder({ headless: true });

// ---------------------------------------------------------------------------
// Translation helpers
// ---------------------------------------------------------------------------

function contactToXml(contact) {
  const timeStr  = new Date(contact.time).toISOString();
  const staleStr = new Date(contact.stale).toISOString();

  return xmlBuilder.buildObject({
    event: {
      $: {
        uid:   contact.uid,
        type:  contact.cot_type || 'a-f-G-U-C',
        how:   'h-e',
        time:  timeStr,
        start: timeStr,
        stale: staleStr,
      },
      point: {
        $: {
          lat: String(contact.lat),
          lon: String(contact.lon),
          hae: String(contact.hae || 0),
          ce:  String(contact.ce  || 999999),
          le:  '999999',
        },
      },
    },
  });
}

function markerToXml(marker) {
  const timeStr  = new Date(marker.created_at).toISOString();
  const staleStr = new Date(marker.stale).toISOString();

  return xmlBuilder.buildObject({
    event: {
      $: {
        uid:   marker.id,
        type:  marker.cot_type || 'b-m-p-s-m',
        how:   marker.how || 'h-e',
        time:  timeStr,
        start: timeStr,
        stale: staleStr,
      },
      point: {
        $: {
          lat: String(marker.lat),
          lon: String(marker.lon),
          hae: String(marker.hae || 0),
          ce:  String(marker.ce  || 999999),
          le:  String(marker.le  || 999999),
        },
      },
    },
  });
}

function cotTypeToIcon(cotType) {
  if (cotType === 'b-m-p-w')     return 'waypoint';
  if (cotType === 'b-m-p-s-p-i') return 'info';
  return 'rally';
}

// ---------------------------------------------------------------------------
// peat-chat -> COP  (forward contacts & markers as CoT XML)
// ---------------------------------------------------------------------------

async function forwardContactToCop(contact) {
  if (contact.uid === peatSelfId)           return;
  if (copOrigins.has(contact.uid))          return;
  if (contact.lat === 0 && contact.lon === 0) return;

  peatOrigins.add(contact.uid);

  try {
    const xml = contactToXml(contact);
    await axios.post(`${config.COP_HTTP_URL}/cot`, xml, {
      headers: { 'Content-Type': 'application/xml' },
    });
  } catch (err) {
    console.error('[bridge] POST contact to COP failed:', err.message);
  }
}

async function forwardMarkerToCop(marker) {
  if (copOrigins.has(marker.id)) return;

  peatOrigins.add(marker.id);

  try {
    const xml = markerToXml(marker);
    await axios.post(`${config.COP_HTTP_URL}/cot`, xml, {
      headers: { 'Content-Type': 'application/xml' },
    });
  } catch (err) {
    console.error('[bridge] POST marker to COP failed:', err.message);
  }
}

// ---------------------------------------------------------------------------
// COP -> peat-chat  (forward deltas as cot_position / create_marker)
// ---------------------------------------------------------------------------

function sendToPeat(envelope) {
  if (peatWs && peatWs.readyState === WebSocket.OPEN) {
    peatWs.send(JSON.stringify(envelope));
  }
}

function forwardNewEntityToPeat(delta) {
  const attrs = delta.event && delta.event.$;
  const point = delta.event && delta.event.point && delta.event.point[0] && delta.event.point[0].$;
  if (!attrs || !point) return;

  const uid     = attrs.uid;
  const cotType = attrs.type || 'a-f-G-U-C';

  if (peatOrigins.has(uid)) return;
  copOrigins.add(uid);

  // Cache the entity
  copEntityCache.set(uid, {
    type: cotType,
    lat:  parseFloat(point.lat),
    lon:  parseFloat(point.lon),
    hae:  parseFloat(point.hae || 0),
    ce:   parseFloat(point.ce  || 999999),
    le:   parseFloat(point.le  || 999999),
  });

  if (!peatRoomId) return;

  if (cotType.startsWith('a-')) {
    sendToPeat({
      type: 'cot_position',
      data: {
        room_id:  peatRoomId,
        lat:      parseFloat(point.lat),
        lon:      parseFloat(point.lon),
        hae:      parseFloat(point.hae || 0),
        ce:       parseFloat(point.ce  || 999999),
        cot_type: cotType,
      },
    });
  } else if (cotType.startsWith('b-') && !copMarkersSent.has(uid)) {
    copMarkersSent.add(uid);
    sendToPeat({
      type: 'create_marker',
      data: {
        room_id:  peatRoomId,
        lat:      parseFloat(point.lat),
        lon:      parseFloat(point.lon),
        name:     uid,
        icon:     cotTypeToIcon(cotType),
        color:    '#ffff00',
        cot_type: cotType,
        remarks:  '',
      },
    });
  }
}

function forwardUpdateToPeat(update) {
  const uid = update.uid;
  if (!uid) return;
  if (peatOrigins.has(uid)) return;

  copOrigins.add(uid);

  // Merge changes into cache
  let cached = copEntityCache.get(uid) || {};
  Object.assign(cached, update.changes);
  copEntityCache.set(uid, cached);

  if (!peatRoomId) return;
  if (cached.lat === undefined || cached.lon === undefined) return;

  const cotType = cached.type || 'a-f-G-U-C';

  if (cotType.startsWith('a-')) {
    sendToPeat({
      type: 'cot_position',
      data: {
        room_id:  peatRoomId,
        lat:      parseFloat(cached.lat),
        lon:      parseFloat(cached.lon),
        hae:      parseFloat(cached.hae || 0),
        ce:       parseFloat(cached.ce  || 999999),
        cot_type: cotType,
      },
    });
  }
}

// ---------------------------------------------------------------------------
// peat-chat WebSocket connection
// ---------------------------------------------------------------------------

function connectPeat() {
  console.log(`[peat] connecting to ${config.PEAT_WS_URL} ...`);
  const ws = new WebSocket(config.PEAT_WS_URL);

  ws.on('open', () => {
    console.log('[peat] connected');
    peatWs = ws;
  });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {
      case 'identity':
        peatSelfId = msg.data.id;
        console.log(`[peat] identity: ${msg.data.short_id}`);
        // Complete handshake
        ws.send(JSON.stringify({ type: 'set_name', data: { name: config.BRIDGE_NAME } }));
        ws.send(JSON.stringify({ type: 'join_room', data: { name: config.PEAT_ROOM_NAME } }));
        break;

      case 'room_joined':
        peatRoomId = msg.data.room_id;
        console.log(`[peat] joined room "${msg.data.name}" (${peatRoomId})`);
        break;

      case 'cot_state': {
        const { contacts = [], markers = [] } = msg.data;
        for (const contact of contacts) {
          forwardContactToCop(contact);
        }
        for (const marker of markers) {
          forwardMarkerToCop(marker);
        }
        break;
      }
    }
  });

  ws.on('close', () => {
    console.log(`[peat] disconnected, reconnecting in ${config.RECONNECT_MS}ms`);
    peatWs = null;
    peatRoomId = null;
    setTimeout(connectPeat, config.RECONNECT_MS);
  });

  ws.on('error', (err) => {
    console.error('[peat] error:', err.message);
  });
}

// ---------------------------------------------------------------------------
// COP WebSocket connection
// ---------------------------------------------------------------------------

function connectCop() {
  console.log(`[cop] connecting to ${config.COP_WS_URL} ...`);
  const ws = new WebSocket(config.COP_WS_URL);

  ws.on('open', async () => {
    console.log('[cop] connected');
    copWs = ws;

    // Seed entity cache from current COP state
    try {
      const res = await axios.get(`${config.COP_HTTP_URL}/cot`);
      const entities = res.data || [];
      for (const entity of entities) {
        const uid   = entity.event && entity.event.$ && entity.event.$.uid;
        const point = entity.event && entity.event.point && entity.event.point[0] && entity.event.point[0].$;
        if (uid && point) {
          copEntityCache.set(uid, {
            type: entity.event.$.type,
            lat:  point.lat,
            lon:  point.lon,
            hae:  point.hae || '0',
            ce:   point.ce  || '999999',
            le:   point.le  || '999999',
          });
        }
      }
      console.log(`[cop] seeded cache with ${copEntityCache.size} entities`);
    } catch (err) {
      console.error('[cop] failed to seed cache:', err.message);
    }
  });

  ws.on('message', (raw) => {
    let delta;
    try { delta = JSON.parse(raw); } catch { return; }

    // Skip the welcome message
    if (delta.message) return;

    // Determine if this is a new entity or an update
    if (delta.event && delta.event.$) {
      forwardNewEntityToPeat(delta);
    } else if (delta.uid && delta.changes) {
      forwardUpdateToPeat(delta);
    }
  });

  ws.on('close', () => {
    console.log(`[cop] disconnected, reconnecting in ${config.RECONNECT_MS}ms`);
    copWs = null;
    setTimeout(connectCop, config.RECONNECT_MS);
  });

  ws.on('error', (err) => {
    console.error('[cop] error:', err.message);
  });
}

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

console.log('=== CoT Bridge ===');
console.log(`  peat-chat : ${config.PEAT_WS_URL} (room: ${config.PEAT_ROOM_NAME})`);
console.log(`  COP HTTP  : ${config.COP_HTTP_URL}`);
console.log(`  COP WS    : ${config.COP_WS_URL}`);
console.log(`  Bridge ID : ${config.BRIDGE_NAME}`);
console.log('');

connectPeat();
connectCop();
