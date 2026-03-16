import { Kafka, type Consumer, type Producer } from 'kafkajs';
import { redis } from './redis.js';
import { pool } from './db.js';

type Order = {
  order_id: string;
  user: string;
  item: string;
  quantity: number;
  createdAt: string;
};

const kafka = new Kafka({
  clientId: 'task-consumer',
  brokers: ['localhost:9092'],
});

// unlike producers, in consumers we must specify which group our consumer belongs into by passing a ConsumerConfig argument( which is an object with certain instructions for the consumer which includes groupId ) and we do that by giving groupId.
// so that groupId tells which team of workers is this specific consumer part of.
// by doing this, kafka gets the information needed to manage offsets and co-ordinate partition assignment.

const consumer: Consumer = kafka.consumer({
  groupId: 'order-tracker',
});

// DLQ producer — sends permanently failed orders to the dead-letter topic
const dlqProducer: Producer = kafka.producer();

// Giving this worker a unique ID
const WORKER_ID = `worker-${process.pid}`;

// ─── Configuration ───
const MAX_RETRIES = 3;
const LEASE_TTL_MS = 60000; // 60 seconds

/**
 * Try to claim an order using Redis.
 * Returns true if we got it, false if someone else has it.
 */
async function tryClaimOrder(orderId: string): Promise<boolean> {
  // SET key value NX PX milliseconds
  // NX = only set if Not eXists
  // PX = expire in milliseconds (60 seconds = 60000)
  const result = await redis.set(
    `lock:order:${orderId}`,
    WORKER_ID,
    'PX',
    LEASE_TTL_MS,
    'NX'
  );

  // result is 'OK' if we got the lock, null if someone else has it
  return result === 'OK';
}

/**
 * Release the Redis lease after successful processing.
 * Only deletes if this worker still owns it (token check).
 * If another worker somehow owns it, we leave it alone.
 */
async function releaseLease(orderId: string): Promise<void> {
  // Lua script: only delete the key if its value matches our worker ID
  // This prevents us from accidentally deleting another worker's lease
  const script = `
    if redis.call("GET", KEYS[1]) == ARGV[1] then
      return redis.call("DEL", KEYS[1])
    else
      return 0
    end
  `;
  await redis.eval(script, 1, `lock:order:${orderId}`, WORKER_ID);
}

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
async function processWithRetry(order: Order): Promise<{ success: boolean; retries: number }> {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      await processOrder(order);
      return { success: true, retries: attempt - 1 }; // attempt 1 = 0 retries
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.log(
        `[${WORKER_ID}] Attempt ${attempt}/${MAX_RETRIES} failed for order ${order.order_id}: ${message}`
      );

      if (attempt < MAX_RETRIES) {
        const backoffMs = 1000 * attempt; // 1s, 2s, 3s
        console.log(`[${WORKER_ID}] Retrying in ${backoffMs}ms...`);
        await new Promise((r) => setTimeout(r, backoffMs));
      }
    }
  }
  return { success: false, retries: MAX_RETRIES };
}

/**
 * Sends a permanently failed order to the dead-letter topic.
 * Includes the original order plus error metadata so someone
 * can inspect why it failed and potentially reprocess it.
 */
async function sendToDLQ(order: Order): Promise<void> {
  await dlqProducer.send({
    topic: 'orders-dlq',
    messages: [
      {
        key: order.order_id,
        value: JSON.stringify({
          originalOrder: order,
          reason: 'Max retries exceeded',
          attempts: MAX_RETRIES,
          worker: WORKER_ID,
          failedAt: new Date().toISOString(),
        }),
      },
    ],
  });
}

async function main() {
  // Connect the consumer, DLQ producer
  await dlqProducer.connect();
  await consumer.connect();

  //  order tracker (consumer) is subscribing to the topic to get updates whenever there is a new order (event/message)
  await consumer.subscribe({
    topic: 'orders',
    fromBeginning: true,
  });

  console.log(`[${WORKER_ID}] Consumer running, subscribed to orders topic`);

  // now we will write an instruction which will continously pings or "ask" kafka if there is any event in the topic that it's subscribed to or not

  await consumer.run({
    autoCommit: false,

    eachMessage: async ({ topic, partition, message }) => {
      // 1) Convert raw bytes to string
      const raw = message.value?.toString('utf8');
      if (!raw) {
        console.log('Got message with empty value, skipping');
        return;
      }

      // 2) Parse JSON into Order object
      let order: Order;
      try {
        order = JSON.parse(raw) as Order;
      } catch (err) {
        console.error('Failed to parse order:', err);
        return;
      }

      // 3) Redis coordination — try to claim this order
      const claimed = await tryClaimOrder(order.order_id);
      if (!claimed) {
        console.log(`[${WORKER_ID}] Order ${order.order_id} already claimed, skipping`);
        // Commit offset — the lease-holding worker is responsible for this task
        await consumer.commitOffsets([
          {
            topic,
            partition,
            offset: (parseInt(message.offset, 10) + 1).toString(),
          },
        ]);
        return;
      }

      // 4) Process with retry logic
      console.log(`[${WORKER_ID}] Processing order ${order.order_id} for ${order.user}...`);
      const { success, retries: retryCount } = await processWithRetry(order);

      if (success) {
        console.log(`[${WORKER_ID}] ✓ Completed order ${order.order_id}`);

        // Write to PostgreSQL — unique constraint on task_id prevents duplicates
        // created_at uses the original order timestamp from the producer
        // processed_at uses NOW() to capture when processing finished
        // The difference gives us end-to-end latency
        await pool.query(
          `INSERT INTO task_outcomes (task_id, worker_id, status, retries, created_at, processed_at)
           VALUES ($1, $2, 'COMPLETED', $3, $4, NOW())
           ON CONFLICT (task_id) DO NOTHING`,
          [order.order_id, WORKER_ID, retryCount, order.createdAt]
        );

        await redis.hset('orders:processed_by', order.order_id, WORKER_ID);
        await releaseLease(order.order_id);
      } else {
        console.log(
          `[${WORKER_ID}] ✗ All retries failed for order ${order.order_id}, sending to DLQ`
        );

        // Record failure in PostgreSQL before sending to DLQ
        await pool.query(
          `INSERT INTO task_outcomes (task_id, worker_id, status, retries, error_message, created_at, processed_at)
           VALUES ($1, $2, 'DLQ', $3, $4, $5, NOW())
           ON CONFLICT (task_id) DO NOTHING`,
          [order.order_id, WORKER_ID, MAX_RETRIES, 'Max retries exceeded', order.createdAt]
        );

        await sendToDLQ(order);
      }

      // 5) Commit offset — whether success or DLQ, we're done with this message
      await consumer.commitOffsets([
        {
          topic,
          partition,
          offset: (parseInt(message.offset, 10) + 1).toString(),
        },
      ]);
    },
  });
}

main().catch((err) => {
  console.error('FATAL:', err instanceof Error ? err.stack : err);
  process.exitCode = 1;
});