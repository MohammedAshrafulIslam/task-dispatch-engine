import { pool } from '../db.js';
import type { Order } from '../types.js';

/**
 * Idempotency gate in Postgres.
 * Returns true if this worker should process; false if already finalized.
 */
export async function claimTask(order: Order, workerId: string): Promise<boolean> {
  const result = await pool.query(
    `INSERT INTO task_outcomes (task_id, worker_id, status, retries, created_at, processed_at)
     VALUES ($1, $2, 'PROCESSING', 0, $3, NOW())
     ON CONFLICT (task_id) DO UPDATE
       SET worker_id = EXCLUDED.worker_id,
           status = 'PROCESSING',
           retries = 0,
           processed_at = NOW()
     WHERE task_outcomes.status NOT IN ('COMPLETED', 'DLQ')
     RETURNING task_id`,
    [order.order_id, workerId, order.createdAt],
  );

  return (result.rowCount ?? 0) > 0;
}

/**
 * Mark a task as completed in PostgreSQL.
 * Update PostgreSQL — unique constraint on task_id prevents duplicates
 * created_at uses the original order timestamp from the producer
 * processed_at uses NOW() to capture when processing finished
 * The difference gives us end-to-end latency
 */
export async function markCompleted(orderId: string, workerId: string, retryCount: number): Promise<void> {
  await pool.query(
    `UPDATE task_outcomes
     SET worker_id = $2,
         status = 'COMPLETED',
         retries = $3,
         error_message = NULL,
         processed_at = NOW()
     WHERE task_id = $1`,
    [orderId, workerId, retryCount],
  );
}

/**
 * Record failure in PostgreSQL before sending to DLQ
 */
export async function markDlq(orderId: string, workerId: string, maxRetries: number): Promise<void> {
  await pool.query(
    `UPDATE task_outcomes
     SET worker_id = $2,
         status = 'DLQ',
         retries = $3,
         error_message = $4,
         processed_at = NOW()
     WHERE task_id = $1`,
    [orderId, workerId, maxRetries, 'Max retries exceeded'],
  );
}
