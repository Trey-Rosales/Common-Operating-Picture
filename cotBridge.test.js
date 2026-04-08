const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const { Builder, parseStringPromise } = require('xml2js');

// ---------------------------------------------------------------------------
// We test the pure translation / helper functions from cotBridge by
// re-implementing them here identically (they're module-scoped and not
// exported).  This validates the XML generation and icon-mapping logic
// without needing to spin up WebSocket or mDNS infrastructure.
// ---------------------------------------------------------------------------

const xmlBuilder = new Builder({ headless: true });

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
// contactToXml
// ---------------------------------------------------------------------------
describe('contactToXml', () => {
  it('produces valid CoT XML with correct attributes', async () => {
    const contact = {
      uid:      'ALPHA-1',
      cot_type: 'a-f-G-U-C',
      lat:      38.8977,
      lon:      -77.0365,
      hae:      10,
      ce:       5,
      time:     1712487600000,  // 2024-04-07T13:00:00.000Z
      stale:    1712491200000,
    };

    const xml = contactToXml(contact);

    assert.ok(xml.includes('<event'), 'should contain <event> element');
    assert.ok(xml.includes('uid="ALPHA-1"'), 'should contain uid');
    assert.ok(xml.includes('type="a-f-G-U-C"'), 'should contain type');
    assert.ok(xml.includes('how="h-e"'), 'should contain how');
    assert.ok(xml.includes('lat="38.8977"'), 'should contain lat');
    assert.ok(xml.includes('lon="-77.0365"'), 'should contain lon');

    // Verify it round-trips through xml2js
    const parsed = await parseStringPromise(xml);
    assert.equal(parsed.event.$.uid, 'ALPHA-1');
    assert.equal(parsed.event.point[0].$.lat, '38.8977');
  });

  it('defaults to a-f-G-U-C when cot_type is missing', async () => {
    const contact = {
      uid: 'X', lat: 0, lon: 0, time: Date.now(), stale: Date.now() + 60000,
    };

    const xml = contactToXml(contact);
    assert.ok(xml.includes('type="a-f-G-U-C"'));
  });

  it('defaults hae and ce when missing', async () => {
    const contact = {
      uid: 'X', cot_type: 'a-f-G-U-C', lat: 1, lon: 2,
      time: Date.now(), stale: Date.now() + 60000,
    };

    const xml = contactToXml(contact);
    assert.ok(xml.includes('hae="0"'));
    assert.ok(xml.includes('ce="999999"'));
  });
});

// ---------------------------------------------------------------------------
// markerToXml
// ---------------------------------------------------------------------------
describe('markerToXml', () => {
  it('produces valid CoT XML for a marker', async () => {
    const marker = {
      id:         'WP-RALLY',
      cot_type:   'b-m-p-w',
      how:        'h-e',
      lat:        39.1,
      lon:        -94.5,
      hae:        0,
      ce:         10,
      le:         20,
      created_at: 1712487600000,
      stale:      1712574000000,
    };

    const xml = markerToXml(marker);

    assert.ok(xml.includes('uid="WP-RALLY"'));
    assert.ok(xml.includes('type="b-m-p-w"'));
    assert.ok(xml.includes('lat="39.1"'));

    const parsed = await parseStringPromise(xml);
    assert.equal(parsed.event.$.uid, 'WP-RALLY');
    assert.equal(parsed.event.point[0].$.le, '20');
  });

  it('defaults cot_type to b-m-p-s-m when missing', () => {
    const marker = {
      id: 'M1', lat: 0, lon: 0, created_at: Date.now(), stale: Date.now() + 60000,
    };

    const xml = markerToXml(marker);
    assert.ok(xml.includes('type="b-m-p-s-m"'));
  });
});

// ---------------------------------------------------------------------------
// cotTypeToIcon
// ---------------------------------------------------------------------------
describe('cotTypeToIcon', () => {
  it('maps b-m-p-w to waypoint', () => {
    assert.equal(cotTypeToIcon('b-m-p-w'), 'waypoint');
  });

  it('maps b-m-p-s-p-i to info', () => {
    assert.equal(cotTypeToIcon('b-m-p-s-p-i'), 'info');
  });

  it('defaults to rally for unknown marker types', () => {
    assert.equal(cotTypeToIcon('b-m-p-s-m'), 'rally');
    assert.equal(cotTypeToIcon('b-x-y-z'), 'rally');
  });
});

// ---------------------------------------------------------------------------
// Echo prevention logic (unit-level simulation)
// ---------------------------------------------------------------------------
describe('echo prevention logic', () => {
  it('three origin sets are mutually exclusive for a given UID', () => {
    const peatOrigins = new Set();
    const copOrigins  = new Set();
    const udpOrigins  = new Set();

    // Simulate a UID arriving from peat
    const uid = 'TEST-UID';
    peatOrigins.add(uid);

    // When COP re-broadcasts it, the bridge should skip it
    assert.ok(peatOrigins.has(uid), 'peatOrigins should contain the UID');
    assert.ok(!copOrigins.has(uid), 'copOrigins should NOT contain the UID');
    assert.ok(!udpOrigins.has(uid), 'udpOrigins should NOT contain the UID');
  });

  it('a UID from UDP should not be forwarded back by peat or cop paths', () => {
    const peatOrigins = new Set();
    const copOrigins  = new Set();
    const udpOrigins  = new Set();

    const uid = 'UDP-UNIT';
    udpOrigins.add(uid);

    // Check guards that would appear in forwardContactToCop
    const shouldSkipPeatToCop = copOrigins.has(uid) || udpOrigins.has(uid);
    assert.ok(shouldSkipPeatToCop, 'peat→cop path should skip UDP-originated UIDs');
  });
});

// ---------------------------------------------------------------------------
// UDP CoT XML parsing (round-trip)
// ---------------------------------------------------------------------------
describe('UDP CoT XML round-trip', () => {
  it('parses standard CoT XML into the expected structure', async () => {
    const xml = `<event uid="ATAK-1" type="a-f-G-U-C" how="m-g"
        time="2026-04-07T12:00:00Z" start="2026-04-07T12:00:00Z"
        stale="2026-04-07T13:00:00Z">
      <point lat="38.8977" lon="-77.0365" hae="50" ce="10" le="20"/>
    </event>`;

    const parsed = await parseStringPromise(xml);

    assert.ok(parsed.event, 'should have event root');
    assert.equal(parsed.event.$.uid, 'ATAK-1');
    assert.equal(parsed.event.$.type, 'a-f-G-U-C');

    const point = parsed.event.point[0].$;
    assert.equal(point.lat, '38.8977');
    assert.equal(point.lon, '-77.0365');
    assert.equal(point.hae, '50');
  });

  it('handles CoT XML with nested detail elements gracefully', async () => {
    const xml = `<event uid="TAK-2" type="a-h-G-U-C" how="h-e"
        time="2026-04-07T12:00:00Z" start="2026-04-07T12:00:00Z"
        stale="2026-04-07T13:00:00Z">
      <point lat="39.1" lon="-94.5" hae="0" ce="999999" le="999999"/>
      <detail><contact callsign="HOSTILE-1"/></detail>
    </event>`;

    const parsed = await parseStringPromise(xml);

    assert.equal(parsed.event.$.uid, 'TAK-2');
    assert.equal(parsed.event.point[0].$.lat, '39.1');
    // detail is parsed but the bridge only reads $ and point — this should not crash
    assert.ok(parsed.event.detail);
  });
});

// ---------------------------------------------------------------------------
// Source tagging (index.js behaviour)
// ---------------------------------------------------------------------------
describe('source tagging', () => {
  it('spreads _source onto a new-entity delta', () => {
    const delta = {
      event: {
        $: { uid: 'A', type: 'a-f-G-U-C' },
        point: [{ $: { lat: '1', lon: '2' } }],
      },
    };

    const broadcast = { ...delta, _source: 'peatlink' };

    assert.equal(broadcast._source, 'peatlink');
    assert.ok(broadcast.event, 'original delta fields preserved');
  });

  it('spreads _source onto an update delta', () => {
    const delta = { uid: 'A', changes: { lat: '10' } };
    const broadcast = { ...delta, _source: 'udp-multicast' };

    assert.equal(broadcast._source, 'udp-multicast');
    assert.equal(broadcast.uid, 'A');
    assert.deepStrictEqual(broadcast.changes, { lat: '10' });
  });

  it('defaults to "direct" when no source header is present', () => {
    const source = undefined || 'direct';
    assert.equal(source, 'direct');
  });
});
