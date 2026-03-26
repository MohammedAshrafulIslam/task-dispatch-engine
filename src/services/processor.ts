import type { Order } from '../types.js';

// ─── Configuration ───
export const MAX_RETRIES = 3;

/**
 * Simulates real work that sometimes fails.
 * In production this would be a DB write, API call, payment processing, etc.
 *
 * The 40% failure rate is intentionally high so you can see retries
 * happening frequently during testing. In a real system, failures
 * would be much rarer (network timeouts, DB connection drops, etc).
 */
async function processOrder(order: Order): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 2000)); // 2 second simulated work

  // 40% chance of failure — simulates downstream service flakiness
  if (Math.random() < 0.4) {
    throw new Error(`Downstream service timeout for order ${order.order_id}`);
  }
}

/**
 * Retries processing with exponential backoff.
 * Returns success status and how many retries were needed.
 */
export async function processWithRetry(order: Order, workerId: string): Promise<{ success: boolean; retries: number }> {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      await processOrder(order);
      return { success: true, retries: attempt - 1 }; // attempt 1 = 0 retries
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.log(
        `[${workerId}] Attempt ${attempt}/${MAX_RETRIES} failed for order ${order.order_id}: ${message}`
      );

      if (attempt < MAX_RETRIES) {
        const backoffMs = 1000 * attempt; // 1s, 2s, 3s
        console.log(`[${workerId}] Retrying in ${backoffMs}ms...`);
        await new Promise((r) => setTimeout(r, backoffMs));
      }
    }
  }
  return { success: false, retries: MAX_RETRIES };
}
