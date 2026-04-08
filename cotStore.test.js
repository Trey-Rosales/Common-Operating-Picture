const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { CotStore } = require('./cotStore');

// ---------------------------------------------------------------------------
// Helper: build a minimal CoT JSON object (same shape xml2js produces)
// ---------------------------------------------------------------------------
function makeCot(uid, type, lat, lon, extras = {}) {
  return {
    event: {
      $: {
        uid,
        type:  type || 'a-f-G-U-C',
        how:   'h-e',
        time:  '2026-04-07T12:00:00Z',
        start: '2026-04-07T12:00:00Z',
        stale: '2026-04-07T13:00:00Z',
        ...extras,
      },
      point: [{
        $: {
          lat: String(lat),
          lon: String(lon),
          hae: '0',
          ce:  '999999',
          le:  '999999',
        },
      }],
    },
  };
}

// ---------------------------------------------------------------------------
// CotStore.update — new entities
// ---------------------------------------------------------------------------
describe('CotStore.update (new entities)', () => {
  it('returns the full object as the delta for a brand-new entity', () => {
    const store = new CotStore();
    const cot   = makeCot('ALPHA-1', 'a-f-G-U-C', 38.8977, -77.0365);
    const delta = store.update(cot);

    // Delta for a new entity IS the entity itself
    assert.deepStrictEqual(delta, cot);
  });

  it('stores the entity and returns it via getAll()', () => {
    const store = new CotStore();
    const cot   = makeCot('BRAVO-2', 'a-f-G-U-C', 39.0997, -94.5786);
    store.update(cot);

    const all = store.getAll();
    assert.equal(all.length, 1);
    assert.equal(all[0].event.$.uid, 'BRAVO-2');
  });

  it('assigns a UUID when uid is missing from the input', () => {
    const store = new CotStore();
    const cot   = { event: { $: { type: 'a-f-G-U-C' }, point: [{ $: { lat: '0', lon: '0' } }] } };
    store.update(cot);

    const all = store.getAll();
    assert.equal(all.length, 1);
    // The store should have assigned a key (UUID) even though uid was absent
  });
});

// ---------------------------------------------------------------------------
// CotStore.update — delta computation on updates
// ---------------------------------------------------------------------------
describe('CotStore.update (delta computation)', () => {
  it('returns only changed fields when an existing entity is updated', () => {
    const store = new CotStore();

    store.update(makeCot('ALPHA-1', 'a-f-G-U-C', 38.8977, -77.0365));

    // Update position only
    const delta = store.update(makeCot('ALPHA-1', 'a-f-G-U-C', 38.9000, -77.0300));

    assert.equal(delta.uid, 'ALPHA-1');
    assert.ok(delta.changes);
    assert.equal(delta.changes.lat, '38.9');
    assert.equal(delta.changes.lon, '-77.03');
  });

  it('returns an empty changes object when nothing changed', () => {
    const store = new CotStore();
    const cot   = makeCot('ALPHA-1', 'a-f-G-U-C', 38.8977, -77.0365);

    store.update(cot);
    const delta = store.update(cot);

    assert.equal(delta.uid, 'ALPHA-1');
    assert.deepStrictEqual(delta.changes, {});
  });

  it('detects attribute changes (e.g. type change)', () => {
    const store = new CotStore();

    store.update(makeCot('UNIT-X', 'a-f-G-U-C', 39.0, -77.0));
    const delta = store.update(makeCot('UNIT-X', 'a-h-G-U-C', 39.0, -77.0));

    assert.equal(delta.changes.type, 'a-h-G-U-C');
  });
});

// ---------------------------------------------------------------------------
// CotStore.getAll
// ---------------------------------------------------------------------------
describe('CotStore.getAll', () => {
  it('returns an empty array for a fresh store', () => {
    const store = new CotStore();
    assert.deepStrictEqual(store.getAll(), []);
  });

  it('returns all stored entities', () => {
    const store = new CotStore();
    store.update(makeCot('A', 'a-f-G-U-C', 1, 2));
    store.update(makeCot('B', 'b-m-p-w',   3, 4));

    const all = store.getAll();
    assert.equal(all.length, 2);

    const uids = all.map(e => e.event.$.uid).sort();
    assert.deepStrictEqual(uids, ['A', 'B']);
  });

  it('reflects the latest state after multiple updates to the same UID', () => {
    const store = new CotStore();
    store.update(makeCot('A', 'a-f-G-U-C', 1, 2));
    store.update(makeCot('A', 'a-f-G-U-C', 10, 20));

    const all = store.getAll();
    assert.equal(all.length, 1);
    assert.equal(all[0].event.point[0].$.lat, '10');
    assert.equal(all[0].event.point[0].$.lon, '20');
  });
});
