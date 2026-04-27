/**
 * finraCache.js
 * Simple cache wrapper: prefer Redis when available (REDIS_URL), fallback to
 * an in-memory Map with TTL for local development.
 */
import { setTimeout as delay } from "node:timers/promises";

let client = null;
let hasRedis = false;

async function tryInitRedis() {
  if (client !== null) return;
  try {
    const IORedis = (await import("ioredis")).default;
    const url = process.env.REDIS_URL || "redis://127.0.0.1:6379";
    client = new IORedis(url);
    // test connection
    await client.ping();
    hasRedis = true;
  } catch (e) {
    hasRedis = false;
    client = new Map();
  }
}

function memSet(map, key, value, ttlSeconds) {
  const expiresAt = Date.now() + ttlSeconds * 1000;
  map.set(key, { value, expiresAt });
  // schedule deletion after ttl
  void delay(ttlSeconds * 1000).then(() => {
    const cur = map.get(key);
    if (cur && cur.expiresAt <= Date.now()) map.delete(key);
  });
}

function memGet(map, key) {
  const item = map.get(key);
  if (!item) return null;
  if (item.expiresAt && item.expiresAt <= Date.now()) {
    map.delete(key);
    return null;
  }
  return item.value;
}

export async function cachedFetch(key, ttlSeconds, fetcher) {
  await tryInitRedis();

  if (hasRedis) {
    try {
      const raw = await client.get(key);
      if (raw) return JSON.parse(raw);
      const value = await fetcher();
      if (value !== undefined) {
        await client.set(key, JSON.stringify(value), "EX", ttlSeconds);
      }
      return value;
    } catch (e) {
      // fall through to in-memory on Redis errors
    }
  }

  // memory fallback
  const mem = client;
  const hit = memGet(mem, key);
  if (hit) return hit;
  const value = await fetcher();
  if (value !== undefined) memSet(mem, key, value, ttlSeconds);
  return value;
}

export async function clearCache(key) {
  await tryInitRedis();
  if (hasRedis) return client.del(key);
  return client.delete(key);
}

export default { cachedFetch, clearCache };
