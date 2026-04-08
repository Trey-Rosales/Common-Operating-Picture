const WebSocket = require('ws');
const axios = require('axios');
const { Builder, parseStringPromise } = require('xml2js');
const { randomUUID } = require('crypto');
const dgram = require('dgram');
const { spawn } = require('child_process');

// ---------------------------------------------------------------------------
// Configuration (env vars with defaults)
// ---------------------------------------------------------------------------
const config = {
  PEAT_WS_URL:      process.env.PEAT_WS_URL    || '',  // empty = use mDNS
  PEAT_ROOM_NAME:   process.env.PEAT_ROOM_NAME  || 'cop-bridge',
  COP_HTTP_URL:     process.env.COP_HTTP_URL    || 'http://localhost:3000',
  COP_WS_URL:       process.env.COP_WS_URL      || 'ws://localhost:8080',
  BRIDGE_NAME:      process.env.BRIDGE_NAME      || 'COP-Bridge',
  RECONNECT_MS:     parseInt(process.env.RECONNECT_MS || '3000', 10),
  MDNS_TIMEOUT_MS:  parseInt(process.env.MDNS_TIMEOUT_MS || '10000', 10),
  UDP_MULTICAST_ADDR: process.env.UDP_MULTICAST_ADDR || '239.2.3.1',
  UDP_MULTICAST_PORT: parseInt(process.env.UDP_MULTICAST_PORT || '6969', 10),
  UDP_ENABLED:        process.env.UDP_ENABLED !== '0',  // default on
};

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
const peatOrigins    = new Set();   // UIDs that came from peat-chat
const copOrigins     = new Set();   // UIDs that came from COP
const udpOrigins     = new Set();   // UIDs that came from multicast UDP
const copEntityCache = new Map();   // uid -> { type, lat, lon, hae, ce, le }
const copMarkersSent = new Set();   // COP marker UIDs already forwarded

let peatSelfId       = null;  // assigned by peat-chat on connect
let peatRoomId       = null;  // hex room ID after join_room
let peatWs           = null;
let copWs            = null;
let discoveredPeatUrl = null; // set by mDNS or config
let udpSocket        = null;

const xmlBuilder = new Builder({ headless: true });

// ---------------------------------------------------------------------------
// mDNS discovery for PeatLink (_peatlink._tcp)
// ---------------------------------------------------------------------------

// Get this machine's local IPv4 address (first non-loopback)
function getLocalIp() {
  const os = require('os');
  const ifaces = os.networkInterfaces();
  for (const name of Object.keys(ifaces)) {
    for (const iface of ifaces[name]) {
      if (iface.family === 'IPv4' && !iface.internal) {
        return iface.address;
      }
    }
  }
  return '127.0.0.1';
}

// Run a command and capture combined stdout+stderr, resolve on first match or timeout
function runCapture(cmd, args, matchFn, timeoutMs) {
  return new Promise((resolve) => {
    const proc = spawn(cmd, args);
    let output = '';
    let done = false;

    const finish = (result) => {
      if (done) return;
      done = true;
      proc.kill();
      resolve(result);
    };

    const onData = (data) => {
      output += data.toString();
      const result = matchFn(output);
      if (result) finish(result);
    };

    proc.stdout.on('data', onData);
    proc.stderr.on('data', onData);
    proc.on('error', () => finish(null));
    setTimeout(() => finish(null), timeoutMs);
  });
}

function discoverPeatLink() {
  return new Promise(async (resolve) => {
    if (config.PEAT_WS_URL) {
      console.log(`[mdns] skipping discovery — using configured URL: ${config.PEAT_WS_URL}`);
      resolve(config.PEAT_WS_URL);
      return;
    }

    console.log('[mdns] browsing for _peatlink._tcp ...');

    const isMac = process.platform === 'darwin';

    if (isMac) {
      // Step 1: Browse — find the instance name
      const instance = await runCapture('dns-sd', ['-B', '_peatlink._tcp', 'local.'], (out) => {
        const m = out.match(/Add\s+\S+\s+\d+\s+(\S+)\s+_peatlink\._tcp\.\s+(.+)/);
        return m ? m[2].trim() : null;
      }, config.MDNS_TIMEOUT_MS);

      if (!instance) {
        console.log(`[mdns] no PeatLink found after ${config.MDNS_TIMEOUT_MS}ms, using fallback`);
        resolve('ws://localhost:8090/ws');
        return;
      }

      console.log(`[mdns] found service: ${instance}`);

      // Step 2: Lookup — get host:port
      const hostPort = await runCapture('dns-sd', ['-L', instance, '_peatlink._tcp', 'local.'], (out) => {
        const m = out.match(/can be reached at\s+(\S+?):(\d+)/);
        if (m) return { host: m[1].replace(/\.$/, ''), port: parseInt(m[2], 10) };
        return null;
      }, 5000);

      if (!hostPort) {
        console.log('[mdns] could not resolve service, using fallback');
        resolve('ws://localhost:8090/ws');
        return;
      }

      // Step 3: Resolve .local hostname to IP
      // dns-sd -G v4 writes to stderr on macOS, so we capture both streams
      let ip = await runCapture('dns-sd', ['-G', 'v4', hostPort.host], (out) => {
        const m = out.match(/\s(\d+\.\d+\.\d+\.\d+)\s/);
        return m ? m[1] : null;
      }, 5000);

      if (!ip) {
        // .local hostname on the same machine — use our own IP
        ip = getLocalIp();
        console.log(`[mdns] hostname ${hostPort.host} unresolvable, using local IP: ${ip}`);
      }

      const url = `ws://${ip}:${hostPort.port}/ws`;
      console.log(`[mdns] resolved PeatLink: ${ip}:${hostPort.port}`);
      resolve(url);

    } else {
      // Linux: avahi-browse gives us everything in one shot
      const result = await runCapture('avahi-browse', ['-rpt', '_peatlink._tcp'], (out) => {
        const lines = out.split('\n');
        for (const line of lines) {
          if (line.startsWith('=')) {
            const parts = line.split(';');
            if (parts.length >= 9) {
              return { ip: parts[7], port: parseInt(parts[8], 10) };
            }
          }
        }
        return null;
      }, config.MDNS_TIMEOUT_MS);

      if (result) {
        const url = `ws://${result.ip}:${result.port}/ws`;
        console.log(`[mdns] resolved PeatLink: ${result.ip}:${result.port}`);
        resolve(url);
      } else {
        console.log('[mdns] no PeatLink found, using fallback');
        resolve('ws://localhost:8090/ws');
      }
    }
  });
}

// Background watcher — re-discovers if PeatLink moves
function startMdnsWatcher() {
  if (config.PEAT_WS_URL) return;

  setInterval(async () => {
    if (peatWs && peatWs.readyState === WebSocket.OPEN) return;

    console.log('[mdns] re-scanning for PeatLink ...');
    const url = await discoverPeatLink();
    if (url !== discoveredPeatUrl) {
      console.log(`[mdns] PeatLink address changed: ${url}`);
      discoveredPeatUrl = url;
      connectPeat();
    }
  }, 30000);
}

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

async function forwardContactToCop(contact, source = 'peat') {
  if (contact.uid === peatSelfId)             return;
  if (copOrigins.has(contact.uid))            return;
  if (udpOrigins.has(contact.uid))            return;
  if (contact.lat === 0 && contact.lon === 0) return;

  peatOrigins.add(contact.uid);

  try {
    const xml = contactToXml(contact);
    await axios.post(`${config.COP_HTTP_URL}/cot`, xml, {
      headers: {
        'Content-Type': 'application/xml',
        'X-CoT-Source': source,
      },
    });
  } catch (err) {
    console.error(`[bridge] POST contact to COP failed: ${err.message}`);
  }
}

async function forwardMarkerToCop(marker, source = 'peat') {
  if (copOrigins.has(marker.id)) return;
  if (udpOrigins.has(marker.id)) return;

  peatOrigins.add(marker.id);

  try {
    const xml = markerToXml(marker);
    await axios.post(`${config.COP_HTTP_URL}/cot`, xml, {
      headers: {
        'Content-Type': 'application/xml',
        'X-CoT-Source': source,
      },
    });
  } catch (err) {
    console.error(`[bridge] POST marker to COP failed: ${err.message}`);
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
// Multicast UDP -> COP + peat-chat
// ---------------------------------------------------------------------------

async function forwardUdpToCop(xml, uid) {
  try {
    await axios.post(`${config.COP_HTTP_URL}/cot`, xml, {
      headers: {
        'Content-Type': 'application/xml',
        'X-CoT-Source': 'udp-multicast',
      },
    });
  } catch (err) {
    console.error(`[udp] POST to COP failed: ${err.message}`);
  }
}

function forwardUdpToPeat(attrs, point) {
  if (!peatRoomId) return;

  const uid     = attrs.uid;
  const cotType = attrs.type || 'a-f-G-U-C';
  const lat     = parseFloat(point.lat || 0);
  const lon     = parseFloat(point.lon || 0);
  const hae     = parseFloat(point.hae || 0);
  const ce      = parseFloat(point.ce  || 999999);

  if (lat === 0 && lon === 0) return;

  if (cotType.startsWith('a-')) {
    sendToPeat({
      type: 'cot_position',
      data: {
        room_id:  peatRoomId,
        lat, lon, hae, ce,
        cot_type: cotType,
      },
    });
  } else if (cotType.startsWith('b-') && !copMarkersSent.has(uid)) {
    copMarkersSent.add(uid);
    sendToPeat({
      type: 'create_marker',
      data: {
        room_id:  peatRoomId,
        lat, lon,
        name:     uid,
        icon:     cotTypeToIcon(cotType),
        color:    '#ffff00',
        cot_type: cotType,
        remarks:  '',
      },
    });
  }
}

function startUdpListener() {
  if (!config.UDP_ENABLED) {
    console.log('[udp] multicast listener disabled (UDP_ENABLED=0)');
    return;
  }

  const addr = config.UDP_MULTICAST_ADDR;
  const port = config.UDP_MULTICAST_PORT;

  udpSocket = dgram.createSocket({ type: 'udp4', reuseAddr: true });

  udpSocket.on('listening', () => {
    try {
      udpSocket.addMembership(addr);
      console.log(`[udp] listening on ${addr}:${port}`);
    } catch (err) {
      console.error(`[udp] addMembership failed: ${err.message}`);
    }
  });

  udpSocket.on('message', async (buf, rinfo) => {
    const xml = buf.toString('utf-8').trim();
    if (!xml.startsWith('<')) return; // not XML

    try {
      const parsed = await parseStringPromise(xml);
      const attrs  = parsed.event && parsed.event.$;
      const pointArr = parsed.event && parsed.event.point;
      const point  = Array.isArray(pointArr) ? pointArr[0].$ : (pointArr && pointArr.$) || {};

      if (!attrs || !attrs.uid) return;

      const uid = attrs.uid;

      // Skip if this UID already originated from peat or COP
      if (peatOrigins.has(uid) || copOrigins.has(uid)) return;

      udpOrigins.add(uid);

      console.log(`[udp] received ${attrs.type || '?'} uid=${uid.slice(0, 12)} from ${rinfo.address}`);

      // Forward raw XML to COP
      await forwardUdpToCop(xml, uid);

      // Forward parsed data to PeatLink
      forwardUdpToPeat(attrs, point);

    } catch (err) {
      // Silently ignore malformed XML — common with partial UDP packets
    }
  });

  udpSocket.on('error', (err) => {
    console.error(`[udp] socket error: ${err.message}`);
  });

  udpSocket.bind(port);
}

// ---------------------------------------------------------------------------
// peat-chat WebSocket connection
// ---------------------------------------------------------------------------

function connectPeat() {
  const url = discoveredPeatUrl;
  if (!url) {
    console.log('[peat] no PeatLink URL yet, retrying after discovery...');
    setTimeout(connectPeat, config.RECONNECT_MS);
    return;
  }

  console.log(`[peat] connecting to ${url} ...`);
  const ws = new WebSocket(url);

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
        const contacts = msg.data.contacts || [];
        const markers  = msg.data.markers  || [];
        for (const contact of contacts) {
          forwardContactToCop(contact, 'peatlink');
        }
        for (const marker of markers) {
          forwardMarkerToCop(marker, 'peatlink');
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

async function main() {
  console.log('=== CoT Bridge ===');
  console.log(`  COP HTTP  : ${config.COP_HTTP_URL}`);
  console.log(`  COP WS    : ${config.COP_WS_URL}`);
  console.log(`  Bridge ID : ${config.BRIDGE_NAME}`);
  console.log(`  Room      : ${config.PEAT_ROOM_NAME}`);
  console.log(`  UDP Mcast : ${config.UDP_ENABLED ? `${config.UDP_MULTICAST_ADDR}:${config.UDP_MULTICAST_PORT}` : 'disabled'}`);
  console.log('');

  // Discover PeatLink via mDNS (or use configured URL)
  discoveredPeatUrl = await discoverPeatLink();
  console.log(`  PeatLink  : ${discoveredPeatUrl}`);
  console.log('');

  // Start all connections
  connectPeat();
  connectCop();
  startUdpListener();

  // Keep watching for PeatLink service changes
  startMdnsWatcher();
}

main().catch((err) => {
  console.error('Fatal:', err);
  process.exit(1);
});
