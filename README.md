# Task Dispatch Engine

An event-driven task dispatch system built with TypeScript. Tasks arrive as events through Kafka, get assigned to workers via Redis-coordinated locking, and all outcomes are logged to PostgreSQL. The system guarantees at-least-once delivery with idempotent processing — each task results in exactly one outcome, even if messages are re-delivered.

Built as a capstone project for EECS4080.

## Motivation

I wanted a small system that still captures the hard parts of distributed systems: messages can be duplicated, workers can crash, and yet the result needs to be consistent. Keeping the domain simple (orders) lets me focus on reliability, recovery, and observability instead of app‑specific complexity.

## High‑Level Idea

Kafka is the inbox, Redis is the coordination lock, and Postgres is the source of truth for outcomes. Workers are stateless, so scaling is just adding more workers. Retries and a DLQ make failures visible instead of silent. The dashboard is there to show throughput, latency, and backlog as the system runs.

## How It Works

```
Producer  -->  Kafka ("orders" topic)  -->  Worker(s)
                                              |
                                    1. Claim lock (Redis)
                                    2. Claim in DB (PostgreSQL)
                                    3. Process with retries
                                    4. Mark COMPLETED or send to DLQ
                                    5. Release lock
```

1. The **producer** generates order events and publishes them to a Kafka topic.
2. One or more **workers** consume from that topic as part of the same consumer group. Kafka distributes partitions across workers automatically.
3. Before processing, a worker tries to **claim the order with a Redis lock** (`SET NX PX`). If another worker already has it, this one skips it.
4. The worker then does an **atomic claim in PostgreSQL** — inserting a `PROCESSING` row. If the order was already completed or sent to the DLQ, it skips. This is the idempotency gate.
5. The worker processes the order with **exponential backoff retries** (up to 3 attempts, with 1s/2s/3s delays).
6. If all retries fail, the order goes to a **dead letter queue** (`orders-dlq` topic) and gets recorded with a `DLQ` status. Nothing is silently dropped.

## Project Structure

```
src/
  types.ts                 -- shared Order type
  kafka.ts                 -- Kafka client, consumer, DLQ producer
  db.ts                    -- PostgreSQL connection pool
  redis.ts                 -- Redis client
  producer.ts              -- generates and sends fake orders
  tracker.ts               -- main worker orchestration
  services/
    lock.ts                -- Redis lock acquisition and release
    store.ts               -- PostgreSQL queries (claim, complete, dlq)
    processor.ts           -- order processing with retry logic
```

## Prerequisites

- Node.js 18+
- Docker and Docker Compose

## Setup

Start the infrastructure:

```bash
docker compose up -d
```

This spins up:
- **Kafka** on port 9092 (KRaft mode, no Zookeeper)
- **Redis** on port 6379
- **PostgreSQL** on port 5433 (user: `ashraf`, db: `taskengine`)
- **Grafana** on port 3000
- **Kafka Exporter** on port 9308 (Prometheus scrape target)
- **Prometheus** on port 9090

Install dependencies:

```bash
npm install
```

Create the `task_outcomes` table in PostgreSQL:

```sql
CREATE TABLE task_outcomes (
  id            SERIAL PRIMARY KEY,
  task_id       TEXT NOT NULL UNIQUE,
  worker_id     TEXT NOT NULL,
  status        TEXT NOT NULL,
  retries       INTEGER DEFAULT 0,
  error_message TEXT,
  created_at    TIMESTAMPTZ DEFAULT NOW(),
  processed_at  TIMESTAMPTZ DEFAULT NOW(),
  lease_until   TIMESTAMPTZ
);
```

## Running

Start one or more workers in separate terminals:

```bash
# Terminal 1
node --loader ts-node/esm src/tracker.ts 2>&1 | tee worker1.log

# Terminal 2
node --loader ts-node/esm src/tracker.ts 2>&1 | tee worker2.log
```

Or using npm scripts:

```bash
npm run worker 2>&1 | tee worker1.log
```

Send orders:

```bash
# Terminal 3
node --loader ts-node/esm src/producer.ts 2>&1 | tee producer.log
```

The producer sends 200 orders with 100ms delays. Workers will pick them up, and you'll see the processing logs in real time.

## What to Look For

**Duplicate prevention** — Run two workers and send the same order multiple times. Only one worker processes it, and only one row ends up in the database.

**Retry behavior** — Watch the worker logs for lines like:
```
[worker-1234] Attempt 1/3 failed: Downstream service timeout
[worker-1234] Retrying in 1000ms...
[worker-1234] Attempt 2/3 failed: Downstream service timeout
[worker-1234] Retrying in 2000ms...
[worker-1234] ✓ Completed order abc-123
```

**Dead letter queue** — When all 3 retries fail, the order is sent to `orders-dlq` and marked as `DLQ` in the database. You can verify with:
```sql
SELECT * FROM task_outcomes WHERE status = 'DLQ';
```

## Monitoring

Open [localhost:3000](http://localhost:3000) to view the Grafana dashboard. It tracks throughput, latency, consumer lag, retry distribution, and work distribution by worker — all updated in real time.

## Tech Stack

| Component | Tech | Role |
|-----------|------|------|
| Language | TypeScript (ES modules) | Type safety, modern JS |
| Message Broker | Apache Kafka | Event streaming, consumer groups |
| Coordination | Redis | Distributed locking (SET NX PX) |
| Persistence | PostgreSQL | Durable task outcome storage |
| Containers | Docker Compose | Local infrastructure |
| Monitoring | Grafana | Dashboard (port 3000) |
| Metrics | Prometheus + Kafka Exporter | Lag + topic depth |

## Data Model

**Event schema (Kafka payload)**  
The `Order` event is the contract between producer and workers:

- `order_id` (string, unique task ID)
- `user` (string)
- `item` (string)
- `quantity` (number)
- `createdAt` (ISO timestamp)

**Outcome ledger (PostgreSQL)**  
`task_outcomes` stores the durable result of each task:

- `task_id` (unique constraint for idempotency)
- `status` (`PROCESSING`, `COMPLETED`, `DLQ`)
- `retries`, `error_message`
- `created_at`, `processed_at` (latency measurement)
- `lease_until` (DB lease for crash recovery)

## Kafka Metrics (Lag + Topic Depth)

Prometheus scrapes Kafka Exporter for accurate queue depth and consumer lag metrics.

Add Prometheus as a Grafana data source:
- URL: `http://prometheus:9090`

Example Prometheus queries:

**Total consumer lag (queue depth)**:
```
sum(kafka_consumergroup_lag{consumergroup="order-tracker"})
```

**DLQ topic depth (approximate total messages)**:
```
sum(kafka_topic_partition_current_offset{topic="orders-dlq"})
```

If you want depth relative to retention:
```
sum(kafka_topic_partition_current_offset{topic="orders-dlq"})
- sum(kafka_topic_partition_oldest_offset{topic="orders-dlq"})
```

## Idempotency

The system uses a layered approach:

1. **Redis lock** — fast coordination layer. Prevents most duplicate processing across workers. Short-lived lease (60s) prevents deadlocks if a worker crashes.
2. **PostgreSQL atomic claim** — correctness layer. An `INSERT ... ON CONFLICT DO UPDATE WHERE status NOT IN ('COMPLETED', 'DLQ')` ensures that once a task is finalized, no worker can reprocess it.
3. **Offset management** — offsets are committed only after the outcome is persisted, so a crash before the DB write means the message gets redelivered and handled by the layers above.
