const { randomUUID } = require('crypto');

class CotStore {
  constructor() {
    this.entities = new Map();
  }

  // Update or add a CoT entity
  update(cotJson) {
    const entityId = cotJson.event?.$.uid || randomUUID();
    const oldEntity = this.entities.get(entityId);

    // Save new state
    this.entities.set(entityId, cotJson);

    // Compute delta
    const delta = this.computeDelta(oldEntity, cotJson);
    return delta;
  }

  // Improved delta logic (includes position changes)
  computeDelta(oldEntity, newEntity) {
    if (!oldEntity) {
      return newEntity; // New entity, return full object
    }

    const changes = {};

    // Compare event attributes (uid, type, how, etc.)
    const oldAttrs = oldEntity.event?.$ || {};
    const newAttrs = newEntity.event?.$ || {};

    for (const key in newAttrs) {
      if (!oldAttrs[key] || oldAttrs[key] !== newAttrs[key]) {
        changes[key] = newAttrs[key];
      }
    }

    // Compare point data (lat, lon, hae, etc.)
    const oldPoint = oldEntity.event?.point?.[0]?.$ || {};
    const newPoint = newEntity.event?.point?.[0]?.$ || {};

    for (const key in newPoint) {
      if (!oldPoint[key] || oldPoint[key] !== newPoint[key]) {
        changes[key] = newPoint[key];
      }
    }

    return {
      uid: newEntity.event?.$.uid,
      changes
    };
  }

  // Get all stored entities
  getAll() {
    return Array.from(this.entities.values());
  }
}

module.exports = { CotStore };