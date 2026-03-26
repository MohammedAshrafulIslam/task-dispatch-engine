import { redis } from '../redis.js';

// ─── Configuration ───
export const LEASE_TTL_MS = 60000; // 60 seconds

/**
 * Try to claim an order using Redis.
 * Returns true if we got it, false if someone else has it.
 */
export async function tryClaimOrder(orderId: string, workerId: string): Promise<boolean> {
  // SET key value NX PX milliseconds
  // NX = only set if Not eXists
  // PX = expire in milliseconds (60 seconds = 60000)
  const result = await redis.set(
    `lock:order:${orderId}`,
    workerId,
    'PX',
    LEASE_TTL_MS,
    'NX'
  );

  // result is 'OK' if we got the lock, null if someone else has it
  return result === 'OK';
}

/**
 * Release the Redis lease after processing or skip.
 * Only deletes if this worker still owns it (token check).
 * If another worker somehow owns it, we leave it alone.
 */
export async function releaseLease(orderId: string, workerId: string): Promise<void> {
  // Lua script: only delete the key if its value matches our worker ID
  // This prevents us from accidentally deleting another worker's lease
  const script = `
    if redis.call("GET", KEYS[1]) == ARGV[1] then
      return redis.call("DEL", KEYS[1])
    else
      return 0
    end
  `;
  await redis.eval(script, 1, `lock:order:${orderId}`, workerId);
}
