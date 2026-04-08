const WebSocket = require('ws');
const axios = require('axios');
const { Builder, parseStringPromise } = require('xml2js');
const { randomUUID } = require('crypto');
const dgram = require('dgram');
const { spawn } = require('child_process');
const takproto = require('./takproto');

// ---------------------------------------------------------------------------
// Configuration (env vars with defaults)
// ---------------------------------------------------------------------------
const config = {
  PEAT_WS_URL:      process.env.PEAT_WS_URL    || '',  // empty = use mDNS
  PEAT_ROOM_NAME:   process.env.PEAT_ROOM_NAME  || 'general',
  COP_HTTP_URL:     process.env.COP_HTTP_URL    || 'http://localhost:3000',
  COP_WS_URL:       process.env.COP_WS_URL      || 'ws://localhost:8080',
  BRIDGE_NAME:      process.env.BRIDGE_NAME      || 'COP-Bridge',
  RECONNECT_MS:     parseInt(process.env.RECONNECT_MS || '3000', 10),
  MDNS_TIMEOUT_MS:  parseInt(process.env.MDNS_TIMEOUT_MS || '10000', 10),
  UDP_MULTICAST_ADDR: process.env.UDP_MULTICAST_ADDR || '239.2.3.1',
  UDP_MULTICAST_PORT: parseInt(process.env.UDP_MULTICAST_PORT || '6969', 10),
  UDP_ENABLED:        process.env.UDP_ENABLED !== '0',  // default on
  TCP_COT_PORT:       parseInt(process.env.TCP_COT_PORT || '4242', 10),
};

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
const peatOrigins    = new Set();   // UIDs that came from peat-chat
const copOrigins     = new Set();   // UIDs that came from COP
const udpOrigins     = new Set();   // UIDs that came from multicast UDP
const copEntityCache  = new Map();   // uid -> { type, lat, lon, hae, ce, le, callsign }
const peatEntityCache = new Map();   // uid -> "lat,lon,type" — dedup PeatLink cot_state repeats
const copMarkersSent  = new Set();   // COP marker UIDs already forwarded
const peatMarkersSent = new Set();   // PeatLink marker IDs already forwarded to COP
let udpSendSocket     = null;        // separate socket for outbound multicast
const tcpClients      = new Set();   // connected ATAK TCP clients

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
  const callsign = contact.callsign || contact.uid;

  const event = {
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
    detail: {
      contact: { $: { callsign } },
    },
  };

  return xmlBuilder.buildObject({ event });
}

function markerToXml(marker) {
  const timeStr  = new Date(marker.created_at).toISOString();
  const staleStr = new Date(marker.stale).toISOString();
  const name     = marker.name || marker.id;

  const event = {
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
    detail: {
      contact: { $: { callsign: name } },
      remarks: marker.remarks || '',
    },
  };

  return xmlBuilder.buildObject({ event });
}

// Build CoT XML from a cached entity (for UDP broadcast of COP/PeatLink data)
function entityToXml(uid, entity) {
  const now      = new Date().toISOString();
  const stale    = new Date(Date.now() + 300000).toISOString(); // 5 min stale
  const callsign = entity.callsign || uid;

  const event = {
    $: {
      uid,
      type:  entity.type || 'a-f-G-U-C',
      how:   'h-e',
      time:  now,
      start: now,
      stale: stale,
    },
    point: {
      $: {
        lat: String(entity.lat),
        lon: String(entity.lon),
        hae: String(entity.hae || 0),
        ce:  String(entity.ce  || 999999),
        le:  String(entity.le  || 999999),
      },
    },
    detail: {
      contact: { $: { callsign } },
    },
  };

  return xmlBuilder.buildObject({ event });
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

  // Always broadcast so ATAK gets periodic SA refreshes (UDP + TCP)
  const xml = contactToXml(contact);
  broadcastUdp(xml);
  broadcastTcp(xml);
  broadcastUdpProto({
    uid:      contact.uid,
    type:     contact.cot_type || 'a-f-G-U-C',
    callsign: contact.callsign || contact.uid,
    lat:      contact.lat,
    lon:      contact.lon,
    hae:      contact.hae || 0,
    ce:       contact.ce  || 999999,
    le:       999999,
    team:     'Cyan',
    role:     'Team Member',
  });

  // Only POST to COP if position changed (dedup to avoid spamming server)
  const fingerprint = `${contact.lat},${contact.lon},${contact.cot_type}`;
  if (peatEntityCache.get(contact.uid) === fingerprint) return;
  peatEntityCache.set(contact.uid, fingerprint);

  console.log(`[peat→cop] ${contact.callsign || contact.uid.slice(0,12)} ${contact.cot_type || 'a-f-G-U-C'} ${contact.lat.toFixed(4)},${contact.lon.toFixed(4)}`);

  try {
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

  // Always broadcast markers (UDP + TCP for ATAK)
  const mxml = markerToXml(marker);
  broadcastUdp(mxml);
  broadcastTcp(mxml);
  broadcastUdpProto({
    uid:      marker.id,
    type:     marker.cot_type || 'b-m-p-s-m',
    callsign: marker.name || marker.id,
    lat:      marker.lat,
    lon:      marker.lon,
    hae:      marker.hae || 0,
    ce:       marker.ce  || 999999,
    le:       marker.le  || 999999,
  });

  // Only POST to COP once per marker
  if (peatMarkersSent.has(marker.id)) return;
  peatMarkersSent.add(marker.id);

  console.log(`[peat→cop] marker "${marker.name || marker.id}" ${marker.cot_type || 'b-m-p-s-m'} ${marker.lat.toFixed(4)},${marker.lon.toFixed(4)}`);

  try {
    await axios.post(`${config.COP_HTTP_URL}/cot`, mxml, {
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

  // Extract callsign from detail if present (e.g. from direct POST)
  const detail   = delta.event.detail;
  const callsign = detail && detail[0] && detail[0].contact && detail[0].contact[0] && detail[0].contact[0].$.callsign;

  // Cache the entity
  const cached = {
    type: cotType,
    lat:  parseFloat(point.lat),
    lon:  parseFloat(point.lon),
    hae:  parseFloat(point.hae || 0),
    ce:   parseFloat(point.ce  || 999999),
    le:   parseFloat(point.le  || 999999),
    callsign: callsign || uid,
  };
  copEntityCache.set(uid, cached);

  // Broadcast to UDP multicast (skip if this entity came from UDP)
  if (!udpOrigins.has(uid)) {
    const _xml = entityToXml(uid, cached);
    broadcastUdp(_xml);
    broadcastTcp(_xml);
    broadcastUdpProto({ uid, ...cached });
  }

  if (!peatRoomId) return;

  if (cotType.startsWith('a-')) {
    sendToPeat({
      type: 'cot_position',
      data: {
        room_id:  peatRoomId,
        lat:      cached.lat,
        lon:      cached.lon,
        hae:      cached.hae,
        ce:       cached.ce,
        cot_type: cotType,
      },
    });
  } else if (cotType.startsWith('b-') && !copMarkersSent.has(uid)) {
    copMarkersSent.add(uid);
    sendToPeat({
      type: 'create_marker',
      data: {
        room_id:  peatRoomId,
        lat:      cached.lat,
        lon:      cached.lon,
        name:     cached.callsign,
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

  if (cached.lat === undefined || cached.lon === undefined) return;

  const cotType = cached.type || 'a-f-G-U-C';

  // Broadcast updated entity to UDP multicast (skip if from UDP)
  if (!udpOrigins.has(uid)) {
    const _xml = entityToXml(uid, cached);
    broadcastUdp(_xml);
    broadcastTcp(_xml);
    broadcastUdpProto({ uid, ...cached });
  }

  if (!peatRoomId) return;

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
// UDP multicast broadcast (outbound)
// ---------------------------------------------------------------------------

function initUdpSendSocket() {
  if (!config.UDP_ENABLED) return;

  const localIp = getLocalIp();

  udpSendSocket = dgram.createSocket({ type: 'udp4', reuseAddr: true });
  udpSendSocket.bind(() => {
    udpSendSocket.setMulticastTTL(32);
    udpSendSocket.setMulticastInterface(localIp);
    udpSendSocket.setMulticastLoopback(false);
    console.log(`[udp] send socket ready (interface: ${localIp})`);
  });
}

function broadcastUdp(xml) {
  if (!udpSendSocket || !config.UDP_ENABLED) return;

  const buf = Buffer.from(xml, 'utf-8');
  udpSendSocket.send(buf, 0, buf.length, config.UDP_MULTICAST_PORT, config.UDP_MULTICAST_ADDR, (err) => {
    if (err) console.error(`[udp] xml broadcast failed: ${err.message}`);
  });
}

// Broadcast as TAK protobuf (for ATAK devices that use mesh SA protobuf)
function broadcastUdpProto(cot) {
  if (!udpSendSocket || !config.UDP_ENABLED) return;

  try {
    const buf = takproto.encode(cot);
    udpSendSocket.send(buf, 0, buf.length, config.UDP_MULTICAST_PORT, config.UDP_MULTICAST_ADDR, (err) => {
      if (err) console.error(`[udp] proto broadcast failed: ${err.message}`);
    });
  } catch (err) {
    console.error(`[udp] proto encode failed: ${err.message}`);
  }
}

// ---------------------------------------------------------------------------
// TCP CoT server (direct connection for ATAK when multicast is blocked)
// ---------------------------------------------------------------------------

function startTcpCotServer() {
  const net = require('net');
  const port = config.TCP_COT_PORT;

  const server = net.createServer((sock) => {
    const addr = `${sock.remoteAddress}:${sock.remotePort}`;
    console.log(`[tcp] ATAK connected: ${addr}`);
    tcpClients.add(sock);

    // Send current entity snapshot on connect
    for (const [uid, ent] of copEntityCache) {
      if (ent.lat && ent.lon && !(ent.lat === 0 && ent.lon === 0)) {
        const xml = entityToXml(uid, ent);
        sock.write(xml + '\n');
      }
    }

    // Receive CoT from ATAK and forward to COP + PeatLink
    let buffer = '';
    sock.on('data', async (data) => {
      buffer += data.toString();

      // Extract complete <event>...</event> elements
      let match;
      while ((match = buffer.match(/<event[\s\S]*?<\/event>/)) !== null) {
        const xml = match[0];
        buffer = buffer.slice(match.index + xml.length);

        try {
          const parsed = await parseStringPromise(xml);
          const attrs = parsed.event?.$;
          const pointArr = parsed.event?.point;
          const point = Array.isArray(pointArr) ? pointArr[0].$ : {};
          if (!attrs?.uid) continue;

          const uid = attrs.uid;
          if (peatOrigins.has(uid) || copOrigins.has(uid)) continue;

          const detailArr = parsed.event.detail;
          const contactEl = detailArr?.[0]?.contact;
          const callsign = contactEl?.[0]?.$?.callsign;

          console.log(`[tcp] recv ${attrs.type} cs=${callsign || '?'} uid=${uid.slice(0, 16)} from ${addr}`);

          udpOrigins.add(uid);

          copEntityCache.set(uid, {
            type:     attrs.type || 'a-f-G-U-C',
            lat:      parseFloat(point.lat || 0),
            lon:      parseFloat(point.lon || 0),
            hae:      parseFloat(point.hae || 0),
            ce:       parseFloat(point.ce || 999999),
            le:       parseFloat(point.le || 999999),
            callsign: callsign || uid,
          });

          await forwardUdpToCop(xml, uid);
          forwardUdpToPeat(attrs, point, callsign);
        } catch {}
      }
    });

    sock.on('close', () => {
      console.log(`[tcp] ATAK disconnected: ${addr}`);
      tcpClients.delete(sock);
    });

    sock.on('error', (err) => {
      console.error(`[tcp] error from ${addr}: ${err.message}`);
      tcpClients.delete(sock);
    });
  });

  server.listen(port, '0.0.0.0', () => {
    console.log(`[tcp] CoT server listening on :${port} — add ${getLocalIp()}:${port} as TCP input in ATAK`);
  });
}

// Send CoT XML to all connected ATAK TCP clients
function broadcastTcp(xml) {
  for (const sock of tcpClients) {
    if (!sock.destroyed) {
      sock.write(xml + '\n');
    }
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

function forwardUdpToPeat(attrs, point, callsign) {
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
        room_id:     peatRoomId,
        lat, lon, hae, ce,
        cot_type:    cotType,
        sender_name: callsign || uid,
        sender_id:   uid,
      },
    });
  } else if (cotType.startsWith('b-') && !copMarkersSent.has(uid)) {
    copMarkersSent.add(uid);
    sendToPeat({
      type: 'create_marker',
      data: {
        room_id:     peatRoomId,
        lat, lon,
        name:        callsign || uid,
        icon:        cotTypeToIcon(cotType),
        color:       '#ffff00',
        cot_type:    cotType,
        remarks:     '',
        sender_name: callsign || uid,
        sender_id:   uid,
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
    // Handle TAK protobuf (ATAK mesh SA — starts with BF 01 BF)
    if (takproto.isTakProtobuf(buf)) {
      const cot = takproto.decode(buf);
      if (!cot || !cot.uid) return;
      if (cot.lat === 0 && cot.lon === 0) return; // no GPS lock

      const uid = cot.uid;
      if (peatOrigins.has(uid) || copOrigins.has(uid)) return;

      // Deduplicate — skip if position hasn't changed
      const fingerprint = `${cot.lat.toFixed(6)},${cot.lon.toFixed(6)},${cot.type}`;
      const prev = peatEntityCache.get(uid);
      if (prev === fingerprint) return;
      peatEntityCache.set(uid, fingerprint);

      udpOrigins.add(uid);

      console.log(`[udp] TAK proto ${cot.type} cs=${cot.callsign} uid=${uid.slice(0, 16)} from ${rinfo.address}`);

      // Cache the entity
      copEntityCache.set(uid, {
        type:     cot.type,
        lat:      cot.lat,
        lon:      cot.lon,
        hae:      cot.hae,
        ce:       cot.ce,
        le:       cot.le,
        callsign: cot.callsign,
      });

      // Build XML for COP (COP server expects XML)
      const xml = entityToXml(uid, copEntityCache.get(uid));
      await forwardUdpToCop(xml, uid);

      // Forward to PeatLink
      forwardUdpToPeat(
        { uid, type: cot.type },
        { lat: cot.lat, lon: cot.lon, hae: cot.hae, ce: cot.ce },
        cot.callsign
      );
      return;
    }

    // Handle standard CoT XML
    const xml = buf.toString('utf-8').trim();
    if (!xml.startsWith('<')) return;

    try {
      const parsed = await parseStringPromise(xml);
      const attrs  = parsed.event && parsed.event.$;
      const pointArr = parsed.event && parsed.event.point;
      const point  = Array.isArray(pointArr) ? pointArr[0].$ : (pointArr && pointArr.$) || {};

      if (!attrs || !attrs.uid) return;

      const uid = attrs.uid;

      if (peatOrigins.has(uid) || copOrigins.has(uid)) return;

      // Deduplicate
      const fingerprint = `${point.lat},${point.lon},${attrs.type}`;
      if (peatEntityCache.get(uid) === fingerprint) return;
      peatEntityCache.set(uid, fingerprint);

      udpOrigins.add(uid);

      const detailArr = parsed.event.detail;
      const contactEl = detailArr && detailArr[0] && detailArr[0].contact;
      const callsign  = contactEl && contactEl[0] && contactEl[0].$ && contactEl[0].$.callsign;

      console.log(`[udp] XML ${attrs.type || '?'} cs=${callsign || '?'} uid=${uid.slice(0, 16)} from ${rinfo.address}`);

      copEntityCache.set(uid, {
        type:     attrs.type || 'a-f-G-U-C',
        lat:      parseFloat(point.lat || 0),
        lon:      parseFloat(point.lon || 0),
        hae:      parseFloat(point.hae || 0),
        ce:       parseFloat(point.ce  || 999999),
        le:       parseFloat(point.le  || 999999),
        callsign: callsign || uid,
      });

      await forwardUdpToCop(xml, uid);
      forwardUdpToPeat(attrs, point, callsign);

    } catch (err) {
      // Silently ignore malformed XML
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
  initUdpSendSocket();
  startUdpListener();
  startTcpCotServer();

  // Keep watching for PeatLink service changes
  startMdnsWatcher();
}

main().catch((err) => {
  console.error('Fatal:', err);
  process.exit(1);
});
