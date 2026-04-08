/**
 * TAK Protocol Buffer decoder/encoder for ATAK mesh SA messages.
 *
 * ATAK broadcasts SA (Situational Awareness) on multicast UDP using
 * protobuf-encoded TakMessage, prefixed with a 3-byte magic header (BF 01 BF).
 * This module decodes those messages into plain CoT-like objects and encodes
 * CoT objects back into the TAK protobuf wire format.
 *
 * Proto definitions sourced from:
 * https://github.com/TAK-Product-Center/Server/tree/main/src/takserver-protobuf/src/main/proto
 */

const protobuf = require('protobufjs');

// Official TAK protobuf schema (field numbers match TAK-Product-Center/Server)
const PROTO_DEF = `
syntax = "proto3";

message TakMessage {
  TakControl takControl = 1;
  CotEvent cotEvent = 2;
  uint64 submissionTime = 3;
  uint64 creationTime = 4;
}

message TakControl {
  uint32 minProtoVersion = 1;
  uint32 maxProtoVersion = 2;
}

message CotEvent {
  string type = 1;
  string access = 2;
  string qos = 3;
  string opex = 4;
  string uid = 5;
  uint64 sendTime = 6;
  uint64 startTime = 7;
  uint64 staleTime = 8;
  string how = 9;
  double lat = 10;
  double lon = 11;
  double hae = 12;
  double ce = 13;
  double le = 14;
  Detail detail = 15;
  string caveat = 16;
  string releaseableTo = 17;
}

message Detail {
  string xmlDetail = 1;
  Contact contact = 2;
  Group group = 3;
  PrecisionLocation precisionLocation = 4;
  Status status = 5;
  Takv takv = 6;
  Track track = 7;
}

message Contact {
  string endpoint = 1;
  string callsign = 2;
}

message Group {
  string name = 1;
  string role = 2;
}

message PrecisionLocation {
  string geopointsrc = 1;
  string altsrc = 2;
}

message Status {
  uint32 battery = 1;
}

message Takv {
  string device = 1;
  string platform = 2;
  string os = 3;
  string version = 4;
}

message Track {
  double speed = 1;
  double course = 2;
}
`;

const root = protobuf.parse(PROTO_DEF).root;
const TakMessage = root.lookupType('TakMessage');

const TAK_MAGIC = Buffer.from([0xBF, 0x01, 0xBF]);

/**
 * Decode a raw UDP buffer (with or without the BF 01 BF header) into a
 * plain object: { uid, type, callsign, lat, lon, hae, ce, le, how, team, role }
 * Returns null if the buffer is not a valid TAK protobuf or has no CotEvent.
 */
function decode(buf) {
  let offset = 0;
  if (buf.length > 3 && buf[0] === 0xBF && buf[1] === 0x01 && buf[2] === 0xBF) {
    offset = 3;
  }

  try {
    const msg = TakMessage.decode(buf.slice(offset));
    const e = msg.cotEvent;
    if (!e || !e.uid) return null;

    // Extract callsign — prefer Droid attribute from xmlDetail (ATAK's display name),
    // then structured contact.callsign, skipping "GPS" which ATAK uses for endpoint type
    let callsign = '';

    if (e.detail?.xmlDetail) {
      const droidMatch = e.detail.xmlDetail.match(/Droid="([^"]+)"/);
      if (droidMatch) callsign = droidMatch[1];
      if (!callsign) {
        const csMatch = e.detail.xmlDetail.match(/callsign="([^"]+)"/);
        if (csMatch) callsign = csMatch[1];
      }
    }

    // Fall back to structured contact field
    if (!callsign) {
      const structCs = e.detail?.contact?.callsign;
      if (structCs && structCs !== 'GPS') callsign = structCs;
    }

    const team = e.detail?.group?.name || '';
    const role = e.detail?.group?.role || '';

    return {
      uid:      e.uid,
      type:     e.type || 'a-f-G-U-C',
      callsign: callsign || e.uid,
      how:      e.how || 'h-e',
      lat:      e.lat || 0,
      lon:      e.lon || 0,
      hae:      e.hae || 0,
      ce:       e.ce  || 999999,
      le:       e.le  || 999999,
      team,
      role,
    };
  } catch {
    return null;
  }
}

/**
 * Encode a CoT-like object into a TAK protobuf buffer with the BF 01 BF header.
 * Input: { uid, type, callsign, lat, lon, hae, ce, le, how, team, role }
 */
function encode(cot) {
  const now = Date.now();
  const stale = now + 120000; // 2 min stale

  const msg = TakMessage.create({
    cotEvent: {
      type:      cot.type || 'a-f-G-U-C',
      uid:       cot.uid,
      sendTime:  now,
      startTime: now,
      staleTime: stale,
      how:       cot.how || 'h-e',
      lat:       cot.lat,
      lon:       cot.lon,
      hae:       cot.hae || 0,
      ce:        cot.ce  || 999999,
      le:        cot.le  || 999999,
      detail: {
        xmlDetail: `<uid Droid="${cot.callsign || cot.uid}"/>`,
        contact:   { callsign: cot.callsign || cot.uid },
        group:     cot.team ? { name: cot.team, role: cot.role || 'Team Member' } : undefined,
      },
    },
  });

  const payload = TakMessage.encode(msg).finish();
  return Buffer.concat([TAK_MAGIC, payload]);
}

/**
 * Check if a buffer looks like a TAK protobuf message (starts with BF 01 BF)
 * as opposed to XML (starts with '<').
 */
function isTakProtobuf(buf) {
  return buf.length > 3 && buf[0] === 0xBF && buf[1] === 0x01 && buf[2] === 0xBF;
}

module.exports = { decode, encode, isTakProtobuf };
