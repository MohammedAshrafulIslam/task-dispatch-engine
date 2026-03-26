import { redis } from './redis.js';
import { consumer, dlqProducer } from './kafka.js';
import type { Order } from './types.js';
import { tryClaimOrder, releaseLease, LEASE_TTL_MS } from './services/lock.js';
import { claimTask, getTaskState, markCompleted, markDlq } from './services/store.js';
import { processWithRetry, MAX_RETRIES } from './services/processor.js';

// Giving this worker a unique ID
const WORKER_ID = `worker-${process.pid}`;

async function commitOffset(topic: string, partition: number, offset: string): Promise<void> {
  await consumer.commitOffsets([
    {
      topic,
      partition,
      offset: (Number(offset) + 1).toString(),
    },
  ]);
}

function scheduleRetry(
  topic: string,
  partition: number,
  offset: string,
  leaseUntil: string | null,
  orderId: string,
): void {
  const now = Date.now();
  const leaseExpiry = leaseUntil ? new Date(leaseUntil).getTime() : NaN;
  const leaseMs = Number.isFinite(leaseExpiry) ? leaseExpiry - now : LEASE_TTL_MS;
  const delayMs = Math.max(leaseMs + 1000, 1000);

  console.log(
    `[${WORKER_ID}] Pausing partition ${partition} for ${delayMs}ms (order ${orderId})`,
  );
  consumer.pause([{ topic, partitions: [partition] }]);

  setTimeout(() => {
    consumer.seek({ topic, partition, offset });
    console.log(`[${WORKER_ID}] Resuming partition ${partition} (order ${orderId})`);
    consumer.resume([{ topic, partitions: [partition] }]);
  }, delayMs);
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
    fromBeginning: false,
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
        await commitOffset(topic, partition, message.offset);
        return;
      }

      // 2) Parse JSON into Order object
      let order: Order;
      try {
        order = JSON.parse(raw) as Order;
      } catch (err) {
        console.error('Failed to parse order:', err);
        await commitOffset(topic, partition, message.offset);
        return;
      }

      // 3) Redis coordination — try to claim this order
      const lockAcquired = await tryClaimOrder(order.order_id, WORKER_ID);
      if (!lockAcquired) {
        const taskState = await getTaskState(order.order_id);
        if (taskState?.status === 'COMPLETED' || taskState?.status === 'DLQ') {
          console.log(`[${WORKER_ID}] Order ${order.order_id} already finalized, skipping`);
          await commitOffset(topic, partition, message.offset);
          return;
        }

        console.log(`[${WORKER_ID}] Order ${order.order_id} is in-flight, waiting for lease`);
        scheduleRetry(topic, partition, message.offset, taskState?.lease_until ?? null, order.order_id);
        return;
      }

      try {
        const claimedInDb = await claimTask(order, WORKER_ID);
        if (!claimedInDb) {
          const taskState = await getTaskState(order.order_id);
          if (taskState?.status === 'COMPLETED' || taskState?.status === 'DLQ') {
            console.log(`[${WORKER_ID}] Order ${order.order_id} already finalized, skipping`);
            await commitOffset(topic, partition, message.offset);
            return;
          }

          console.log(`[${WORKER_ID}] Order ${order.order_id} is in-flight, waiting for lease`);
          scheduleRetry(topic, partition, message.offset, taskState?.lease_until ?? null, order.order_id);
          return;
        }

        // 4) Process with retry logic
        console.log(`[${WORKER_ID}] Processing order ${order.order_id} for ${order.user}...`);
        const { success, retries: retryCount } = await processWithRetry(order, WORKER_ID);

        if (success) {
          console.log(`[${WORKER_ID}] ✓ Completed order ${order.order_id}`);

          await markCompleted(order.order_id, WORKER_ID, retryCount);

          await redis.hset('orders:processed_by', order.order_id, WORKER_ID);
        } else {
          console.log(
            `[${WORKER_ID}] ✗ All retries failed for order ${order.order_id}, sending to DLQ`,
          );

          await markDlq(order.order_id, WORKER_ID, MAX_RETRIES);

          await sendToDLQ(order);
        }

        // 5) Commit offset — whether success or DLQ, we're done with this message
        await commitOffset(topic, partition, message.offset);
      } finally {
        await releaseLease(order.order_id, WORKER_ID);
      }
    },
  });
}

main().catch((err) => {
  console.error('FATAL:', err instanceof Error ? err.stack : err);
  process.exitCode = 1;
});
