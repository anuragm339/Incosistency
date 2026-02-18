# InconsistencyService

A Java Micronaut microservice that tracks message delivery across a distributed POS network, detects delivery inconsistencies, and feeds real-time and historical dashboards in New Relic.

---

## Table of Contents

1. [Purpose](#purpose)
2. [System Topology](#system-topology)
3. [Quick Start](#quick-start)
4. [Configuration](#configuration)
5. [API Reference](#api-reference)
6. [Data Model](#data-model)
7. [How It Works](#how-it-works)
8. [Inconsistency Types](#inconsistency-types)
9. [New Relic Dashboards](#new-relic-dashboards)
10. [PostgreSQL Design](#postgresql-design)
11. [New Relic Volume Analysis](#new-relic-volume-analysis)
12. [Integrating with Existing Services](#integrating-with-existing-services)
13. [Project Structure](#project-structure)

---

## Purpose

The existing `track_requests` table grows unboundedly and is read/updated by many services, making it unsuitable for real-time delivery tracking. InconsistencyService solves this with:

- A **dedicated, self-cleaning PostgreSQL table** (`mts_summary` + `mts_store`) that holds only in-flight messages
- **Event-driven state tracking**: the table row for a message is deleted the moment it is fully settled
- **Three New Relic event types** that answer every operational question about message delivery
- A **live status API** for in-flight messages not yet visible in New Relic

### What it tracks

A consumer sends a message to a cluster. That cluster maps to N stores (10 to 1,000+). Each store has ~40 POS machines that must receive and acknowledge the message. InconsistencyService tracks the complete delivery chain:

```
Consumer → Publisher → Cluster → [Store-1, Store-2, ..., Store-N] → [POS-1 ... POS-40 per store]
```

For each message it answers:
- Did all POS machines receive it?
- Which specific machines are missing?
- Did consumers acknowledge receipt?
- What is the end-to-end latency?
- Was the message re-published (superseded)?

---

## System Topology

```
External Consumer
    └── calls Publisher Service (HTTP)

Publisher Service
    ├── creates row in track_requests (existing — unchanged)
    ├── distributes message via Registry Service (existing — unchanged)
    └── [NEW] POST /api/message-tracking/publish → InconsistencyService

POS Machine
    ├── calls PATCH on MessageTrackerController (existing Spring Boot — unchanged)
    │       └── [NEW] forwards to PATCH /api/message-tracking/{key}/pos-response
    └── payload includes: delivered, consumerAckTimestamp (if ACK'd)

InconsistencyService
    ├── POST /api/message-tracking/publish
    ├── PATCH /api/message-tracking/{messageKey}/pos-response
    ├── GET  /api/message-tracking/{messageKey}/status
    ├── PostgreSQL: mts_summary + mts_store (partitioned, self-cleaning)
    └── New Relic Events API
```

**Scale**: 4,000 stores × 40 POS = 160,000 machines, 1.9M messages/day

---

## Quick Start

### Prerequisites

- Java 21
- Maven 3.8+
- PostgreSQL 14+ with schema `aq_registry_ppe`
- New Relic account with Events API access (Enterprise for production volumes)

### Run locally

```bash
# Set required environment variables
export DB_URL=jdbc:postgresql://localhost:5432/aq_registry_ppe
export DB_USER=postgres
export DB_PASSWORD=postgres
export NR_ACCOUNT_ID=your_account_id
export NR_INSERT_KEY=your_insert_key

# Build and run
mvn package -DskipTests
java -jar target/inconsistency-service-1.0.0-SNAPSHOT.jar
```

Flyway will automatically create the `mts_summary` and `mts_store` tables on first startup.

### Health check

```bash
curl http://localhost:8090/health
# {"status":"UP"}
```

---

## Configuration

All settings are externalized via environment variables.

| Environment Variable | Default | Description |
|---|---|---|
| `DB_URL` | `jdbc:postgresql://localhost:5432/aq_registry_ppe` | JDBC connection URL |
| `DB_USER` | `postgres` | Database username |
| `DB_PASSWORD` | `postgres` | Database password |
| `NR_ACCOUNT_ID` | `0` | New Relic account ID |
| `NR_INSERT_KEY` | `changeme` | New Relic Insert (Ingest) Key |

Additional tuning via `application.yml`:

| Config Key | Default | Description |
|---|---|---|
| `inconsistency.late-pos-threshold-minutes` | `60` | Minutes after publish before a POS response is flagged as LATE |
| `inconsistency.checkpoint-thresholds` | `1,50,100` | % completion thresholds that trigger a `StoreTrackingResult` NR event |
| `inconsistency.cleanup-batch-size` | `500` | Max rows processed per cleanup scheduler run |
| `micronaut.server.port` | `8090` | HTTP server port |
| `datasources.default.maximum-pool-size` | `20` | HikariCP max connection pool size |

---

## API Reference

### POST /api/message-tracking/publish

Called by the Publisher Service for every message it sends. Fire-and-forget — Publisher must not block waiting for this.

**Request body:**
```json
{
  "messageKey": "623506cf-e2d1-4584-af06-7255833f80a5_en_GB",
  "clusterId": "cluster-uk-north",
  "msgOffset": 1144353810,
  "expireAt": "2026-02-17T11:00:00Z",
  "stores": [
    {
      "storeNumber": "9750",
      "locationId": "25ea26e3-4cf0-4823-830b-33ea7c3ecb7c",
      "expectedPosHosts": [
        "uk-9750-00012e8844dd.global.tesco.org",
        "uk-9750-04d9f555b1ff.global.tesco.org"
      ]
    }
  ]
}
```

**Responses:**
- `202 Accepted` — tracking started
- First publish → creates 1 `mts_summary` row + N `mts_store` rows (one per store)
- Re-publish of same key → old tracking replaced: emits `REPLACED` event to NR, deletes old rows, starts fresh

---

### PATCH /api/message-tracking/{messageKey}/pos-response

Called by the existing `MessageTrackerController` (Spring Boot) as a fire-and-forget forward. Never causes a POS-visible error — returns 200 even for stale/orphaned responses.

**Path parameter:** `messageKey` — the message key

**Request body:**
```json
{
  "posHostname": "uk-9750-00012e8844dd.global.tesco.org",
  "storeNumber": "9750",
  "delivered": true,
  "consumerAckTimestamp": "2026-02-17T10:05:23Z",
  "status": "DONE"
}
```

| Field | Required | Description |
|---|---|---|
| `posHostname` | Yes | The POS machine's hostname |
| `storeNumber` | Yes | Store identifier |
| `delivered` | Yes | Whether the POS has the message data |
| `consumerAckTimestamp` | No | ISO-8601 timestamp when consumer ACK'd. Null = Case 2 (delivered but unacknowledged) |
| `status` | Yes | `DONE` or `DELIVERED_NO_ACK` |

**Responses:** `200 OK` always (including for stale responses after a re-publish)

---

### GET /api/message-tracking/{messageKey}/status

Live drilldown for in-flight messages. Queries PostgreSQL directly — use this when the message is not yet finalized and NR doesn't have complete data.

**Performance:** < 5ms (indexed lookup on `message_key`)

**Response (200):**
```json
{
  "messageKey": "623506cf-e2d1-4584-af06-7255833f80a5_en_GB",
  "clusterId": "cluster-uk-north",
  "state": "PARTIAL",
  "clusterCompletionPct": 62.5,
  "totalStores": 100,
  "storesDone": 62,
  "storesPartial": 35,
  "storesPending": 3,
  "storesTimedOut": 0,
  "firstPublishedAt": "2026-02-17T10:00:00Z",
  "expireAt": "2026-02-17T12:00:00Z",
  "inconsistencies": ["LATE_POS_RESPONSE"],
  "stores": [
    {
      "storeNumber": "9750",
      "state": "PARTIAL",
      "posCompletionPct": 75.0,
      "respondedPosCount": 30,
      "expectedPosCount": 40,
      "missingPosCount": 10,
      "missingPos": ["uk-9750-pos38.tesco.org", "uk-9750-pos39.tesco.org"],
      "inconsistencies": []
    }
  ]
}
```

**Response (404):** message not found (already finalized and deleted, or never tracked)

---

### GET /health

```json
{"status": "UP"}
```

---

## Data Model

### Why two tables instead of one

With large clusters (e.g. 500 stores × 40 POS = 20,000 POS machines), a single row per `message_key` would:
- Hold a 20,000-entry JSONB array (large, slow to update)
- Require all 20,000 POS responses to lock and update the same row (`SELECT FOR UPDATE`) — severe contention

**Solution:** Split by store. Each store gets its own row with only its 40 POS machines. Lock contention is bounded to 40 per row regardless of cluster size.

### mts_summary

One row per in-flight `message_key`. Uses integer counters (not JSONB) for store completion tracking — fast atomic increments.

| Column | Type | Description |
|---|---|---|
| `message_key` | VARCHAR UNIQUE | The message identifier |
| `cluster_id` | VARCHAR | Which cluster this message was sent to |
| `msg_offset` | BIGINT | Kafka/event stream offset |
| `first_published_at` | TIMESTAMP | When Publisher called `/publish` |
| `expire_at` | TIMESTAMP | Deadline — used for partition key |
| `total_stores` | INT | How many stores in this cluster |
| `stores_done` | INT | Stores where all POS responded |
| `stores_partial` | INT | Stores with partial POS responses |
| `stores_timed_out` | INT | Stores that expired with missing POS |
| `publish_count` | INT | > 1 = key was re-published |
| `state` | VARCHAR | `PENDING` → `PARTIAL` → `DONE` / `DEGRADED` |
| `inconsistencies` | JSONB | List of detected inconsistency type strings |

### mts_store

One row per `(message_key, store_number)`. Holds JSONB arrays of POS hostnames.

| Column | Type | Description |
|---|---|---|
| `message_key` | VARCHAR | Links to mts_summary |
| `store_number` | VARCHAR | Store identifier |
| `expected_pos` | JSONB | List of hostnames expected to respond |
| `responded_pos` | JSONB | Hostnames that responded |
| `missing_pos` | JSONB | expected - responded |
| `pos_statuses` | JSONB | `{hostname: {status, consumerAckTimestamp, patchReceivedAt}}` |
| `state` | VARCHAR | `PENDING` → `PARTIAL` → `DONE` / `TIMED_OUT` |
| `last_checkpoint_pct` | INT | Last NR checkpoint emitted (0, 1, 50, 100) |
| `expire_at` | TIMESTAMP | Partition key |

### Row lifecycle

Rows are hard-deleted after finalization — the tables hold **only in-flight messages**:

- Resolved (DONE) messages: deleted immediately after NR emit
- Expired (TIMED_OUT) messages: deleted by cleanup scheduler within `expire_at + 1 minute`
- Old date partitions: dropped nightly in milliseconds (no VACUUM needed)

**Estimated steady-state size** (100-store avg cluster, 10-minute avg resolution):
- `mts_summary`: ~13,200 rows
- `mts_store`: ~1.32 million rows

---

## How It Works

### Publish flow

```
Publisher sends K1 to cluster-uk-north (100 stores)
    ↓
POST /api/message-tracking/publish
    ↓
If new key:
  INSERT 1 mts_summary row (state=PENDING, total_stores=100)
  Bulk INSERT 100 mts_store rows (one per store, state=PENDING)

If re-publish (key already exists):
  Emit MessageTrackingResult(state=REPLACED) to NR
  DELETE all 100 old mts_store rows
  DELETE old mts_summary row
  INSERT fresh rows (same as new key case)
```

### POS response flow

```
POS machine at store-9750 calls PATCH on MessageTrackerController
    ↓ (forwarded)
PATCH /api/message-tracking/K1/pos-response
    ↓
Look up mts_store for (K1, store-9750)
  Not found → stale/orphaned response → log + return 200 OK
  Found → SELECT FOR UPDATE

Update pos_statuses, move host from missing_pos to responded_pos

Check: lag > 60 min → flag LATE_POS_RESPONSE

Recalculate posCompletionPct:
  Crossed 1% threshold?  → emit StoreTrackingResult(checkpointPct=1)
  Crossed 50% threshold? → emit StoreTrackingResult(checkpointPct=50)

Advance store state:
  PENDING → PARTIAL (first POS responds)
  PARTIAL → DONE    (all 40 POS responded)

If DONE or expire_at passed:
  → finalizeStore()
```

### Store finalization

```
finalizeStore(store-9750 row):
  Detect: CONFLICTING_STATUS, MISSING_POS, CONSUMER_ACK_MISSING
  Set state = DONE or TIMED_OUT

  Emit StoreTrackingResult(state=DONE/TIMED_OUT, posCompletionPct=final)
  Emit PosTrackingResult for each MISSING/FAILED/LATE/DELIVERED_NO_ACK machine
    (healthy DONE machines are NOT emitted — volume stays proportional to problem rate)

  UPDATE mts_summary: stores_done++ or stores_timed_out++
  DELETE mts_store row

  If stores_done + stores_timed_out == total_stores:
    → finalizeMessage()
```

### Message finalization

```
finalizeMessage(K1):
  If stores_timed_out > 0: state = DEGRADED (else DONE)
  If any store had zero POS responses: flag MISSING_STORE

  Emit MessageTrackingResult (final verdict, all latency metrics)
  DELETE mts_summary row
  ✓ Zero rows remain for K1
```

### Cleanup scheduler

Runs every 1 minute. Handles messages where `expire_at` has passed but no POS response ever triggered `finalizeStore()` (e.g. a store's POS machines are all offline):

```
Find: mts_store WHERE expire_at < NOW() AND state IN ('PENDING', 'PARTIAL') LIMIT 500
For each row:
  → finalizeStore() (marks as TIMED_OUT, emits NR, deletes row)
```

---

## Inconsistency Types

A message is **inconsistent** if any POS machine either didn't receive it, or received it but the consumer didn't acknowledge receipt.

| Type | Detected When | Trigger |
|---|---|---|
| `DUPLICATE_KEY` | At publish | Same key published more than once |
| `LATE_POS_RESPONSE` | At POS PATCH | POS responded > 60 min after `first_published_at` |
| `CONFLICTING_STATUS` | At store finalization | Store has both DONE and DELIVERED_NO_ACK machines |
| `MISSING_POS` | At store finalization | Some POS machines never responded before `expire_at` |
| `CONSUMER_ACK_MISSING` | At store finalization | POS has `delivered=true` but no `consumerAckTimestamp` |
| `MISSING_STORE` | At message finalization | An entire store timed out with zero POS responses |
| `OFFSET_MISMATCH` | At re-publish | Same key re-published with a different `msg_offset` |
| `PREMATURE_REPUBLISH` | At re-publish | Key replaced while previous delivery had partial POS responses |

### The 3 POS response cases

| Case | PATCH sent? | `delivered` | `consumerAckTimestamp` | Inconsistency |
|---|---|---|---|---|
| **Case 1: Fully ACK'd** | Yes | true | Present | None — full success |
| **Case 2: Delivered, no ACK** | Yes | true | null | `CONSUMER_ACK_MISSING` |
| **Case 3: Missing/Offline** | No | — | — | `MISSING_POS` (cleanup scheduler) |

---

## New Relic Dashboards

### Three event types in New Relic

| Event Type | Emitted when | Volume (100-store avg cluster) |
|---|---|---|
| `MessageTrackingResult` | Once per message at finalization | ~1.9M/day |
| `StoreTrackingResult` | At completion checkpoints (1%, 50%, 100% per store) | ~570M/day |
| `PosTrackingResult` | At store finalization — problem machines only | ~5–28M/day |

> **NR Enterprise is required** for production volumes (~600M events/day total).

### Dashboard 1 — Complete State

```sql
-- Delivery state breakdown over time
SELECT count(*) FROM MessageTrackingResult FACET state SINCE 1 day ago TIMESERIES 1 hour

-- Overall inconsistency rate
SELECT percentage(count(*), WHERE hasInconsistency = true) FROM MessageTrackingResult SINCE 1 day ago

-- Inconsistency breakdown by type
SELECT
  filter(count(*), WHERE inconsistencies LIKE '%MISSING_POS%')          AS 'Missing POS',
  filter(count(*), WHERE inconsistencies LIKE '%CONSUMER_ACK_MISSING%') AS 'Unacknowledged',
  filter(count(*), WHERE inconsistencies LIKE '%MISSING_STORE%')        AS 'Missing Store',
  filter(count(*), WHERE inconsistencies LIKE '%DUPLICATE_KEY%')        AS 'Duplicate Keys',
  filter(count(*), WHERE inconsistencies LIKE '%LATE_POS_RESPONSE%')    AS 'Late Responses'
FROM MessageTrackingResult SINCE 1 day ago

-- Re-published (superseded) messages
SELECT count(*) FROM MessageTrackingResult WHERE state = 'REPLACED' SINCE 1 day ago TIMESERIES 1 hour
```

### Dashboard 2 — By Cluster

```sql
-- Real-time cluster completion (use 30-min window for LATEST queries)
SELECT latest(posCompletionPct) FROM StoreTrackingResult FACET clusterId, messageKey SINCE 30 minutes ago

-- Clusters with most degraded messages today
SELECT count(*) FROM MessageTrackingResult WHERE state = 'DEGRADED' FACET clusterId SINCE 1 day ago

-- Unacknowledged delivery rate per cluster
SELECT percentage(count(*), WHERE inconsistencies LIKE '%CONSUMER_ACK_MISSING%')
FROM MessageTrackingResult FACET clusterId SINCE 1 day ago
```

### Dashboard 3 — By Store

```sql
-- Current store completion % (real-time via checkpoint events)
SELECT latest(posCompletionPct), latest(state)
FROM StoreTrackingResult FACET messageKey, storeNumber SINCE 30 minutes ago

-- Stores with most missing POS today
SELECT sum(missingPosCount) FROM StoreTrackingResult WHERE state IN ('DONE', 'TIMED_OUT')
FACET storeNumber SINCE 1 day ago ORDER BY sum(missingPosCount) DESC LIMIT 20

-- Problem POS machines per store
SELECT uniqueCount(posHostname) FROM PosTrackingResult WHERE status = 'MISSING'
FACET storeNumber SINCE 1 day ago ORDER BY uniqueCount(posHostname) DESC LIMIT 20

-- Lookup specific message + store
SELECT latest(state), latest(posCompletionPct), latest(missingPosCount)
FROM StoreTrackingResult
WHERE messageKey = '{{messageKey}}' AND storeNumber = '{{storeNumber}}'
SINCE 4 hours ago
```

### Dashboard 4 — E2E Latency

```sql
-- True E2E: publish → last consumer ACK
SELECT average(lastConsumerAckMs)/1000 AS 'Avg E2E (sec)',
       percentile(lastConsumerAckMs, 50)/1000 AS 'P50',
       percentile(lastConsumerAckMs, 95)/1000 AS 'P95',
       percentile(lastConsumerAckMs, 99)/1000 AS 'P99'
FROM MessageTrackingResult SINCE 1 day ago TIMESERIES 30 minutes

-- Delivery window (publish → last POS PATCH, without ACK)
SELECT average(publishToLastPosAckMs)/1000 AS 'Delivery Window (sec)'
FROM MessageTrackingResult SINCE 1 day ago TIMESERIES 30 minutes

-- Unacknowledged + missing POS trends
SELECT sum(unackedPosCount) AS 'Unacked POS', sum(missingPosCount) AS 'Missing POS'
FROM MessageTrackingResult SINCE 1 day ago TIMESERIES 1 hour

-- Latency by cluster (slowest clusters)
SELECT average(lastConsumerAckMs)/1000 FROM MessageTrackingResult
FACET clusterId SINCE 1 day ago ORDER BY average(lastConsumerAckMs) DESC LIMIT 10
```

### Business questions answered

| Question | Tool | Query |
|---|---|---|
| Did K1 reach all POS? | NR or Status API | `StoreTrackingResult LATEST` or `GET /status` |
| Which POS machines are missing? | NR (post-finalization) or Status API (live) | `PosTrackingResult WHERE status='MISSING'` |
| How many POS are pending RIGHT NOW? | Status API | `GET /{key}/status → stores[].missingPos` |
| What is E2E latency? | NR | `AVG/P95(lastConsumerAckMs)` |
| Was K1 re-published? | NR | `WHERE messageKey='K1' AND state='REPLACED'` |
| Which stores have chronic missing POS? | NR | `SUM(missingPosCount) FACET storeNumber` |

> **Note:** For in-flight messages not yet finalized, use `GET /{key}/status`. New Relic only shows data after the first checkpoint (1% completion) is crossed.

---

## PostgreSQL Design

### Why PostgreSQL handles 160,000 POS machines

**Concurrency model:** Each `PATCH /pos-response` call does `SELECT FOR UPDATE` on the `mts_store` row for `(messageKey, storeNumber)`. This means:
- Only 40 POS machines at the **same store** ever contend for the same row
- POS machines at different stores are never blocked by each other

**Write rate reality check:** 7.2B total POS responses/day sounds extreme, but POS machines run on a **5-minute health check cycle** — they respond in waves, not continuously. Realistic concurrent write rate: 5,000–20,000 updates/second during business hours.

| PostgreSQL instance | JSONB UPDATE/sec capacity |
|---|---|
| Standard (8 vCPU, 64GB RAM) | 5,000–15,000 |
| Large (32 vCPU, 256GB RAM) | 20,000–50,000 |

### Anti-bloat: two-layer protection

**Layer 1 — Partitioning by `expire_at` date**

Both tables are partitioned by `expire_at`. One partition is created per day. Old partitions are dropped nightly:

```sql
DROP TABLE IF EXISTS mts_store_2026_02_14;  -- instant, no VACUUM needed
```

**Layer 2 — Cleanup scheduler (every 1 minute)**

For rows that expire within today's partition (offline POS that never respond):
- Runs every 1 minute
- Finds rows where `expire_at < NOW()` and `state IN ('PENDING', 'PARTIAL')`
- Finalizes them as `TIMED_OUT`, emits to NR, deletes the row
- Processes max 500 rows per run

**Result:** No row is ever stuck for more than `expire_at + 1 minute`.

### Recommended PostgreSQL tuning

| Parameter | Value | Reason |
|---|---|---|
| `shared_buffers` | 25% of RAM | Hot `mts_store` rows stay in memory |
| `max_connections` | 200 | Use HikariCP pool (max 20 connections) |
| `synchronous_commit` | `off` for mts_store | 3–5× throughput gain. Safe: cleanup scheduler recovers any missed updates |
| `work_mem` | 4–8MB | Sorting in cleanup scheduler queries |

### Scalability

| Store count | Assessment |
|---|---|
| 4,000 (current) | Standard cloud instance, comfortable headroom |
| 8,000 (2×) | Large instance (32 vCPU, 256GB RAM) |
| 20,000+ | Consider sharding `mts_store` by `cluster_id` across 2–4 instances |

---

## New Relic Volume Analysis

### Daily events (100-store avg cluster)

| Event type | Calculation | Events/day |
|---|---|---|
| `MessageTrackingResult` | 1.9M × 1 | 1.9M |
| `StoreTrackingResult` | 1.9M × 100 stores × 3 checkpoints | ~570M |
| `PosTrackingResult` | 5% problem rate × 1.9M × 100 × 3 machines | ~28M |
| **Total** | | **~600M/day** |
| NR avg ingest | 600M / 1440 min | ~417K events/min |
| NR peak (4× spike) | | ~1.7M events/min |

### Why NRQL queries stay fast

- **Real-time widgets** use `SINCE 30 minutes ago` → scan ~12M events (not 570M)
- **Historical summaries** use `SINCE 1 day ago` with COUNT/AVERAGE/PERCENTAGE — columnar scan is fast
- **Never use** `LATEST() FACET messageKey SINCE 1 day ago` — too many unique keys. Use `SINCE 30 minutes ago` for LATEST queries.

### Volume at scale

| Store count | StoreTrackingResult/day | Total NR/day |
|---|---|---|
| 4,000 (current) | ~570M | ~600M |
| 8,000 | ~1.14B | ~1.2B |
| 20,000 | ~2.85B | ~3B |

All within NR Enterprise capabilities. Negotiate ingest quota with your NR account team before production rollout at full scale.

---

## Integrating with Existing Services

### 1. Publisher Service

Add a fire-and-forget HTTP call after the existing message distribution logic:

```java
// Fire-and-forget — must NOT block the publisher
CompletableFuture.runAsync(() -> {
    try {
        restTemplate.postForEntity(
            inconsistencyServiceUrl + "/api/message-tracking/publish",
            publishEvent,
            Void.class
        );
    } catch (Exception e) {
        log.warn("Failed to notify InconsistencyService for key={}: {}",
                 publishEvent.getMessageKey(), e.getMessage());
    }
});
```

The `PublishEvent` must include all stores in the cluster and their expected POS hostnames.

### 2. MessageTrackerController (existing Spring Boot service)

Add a fire-and-forget forward inside the existing PATCH handler, **after** the `track_requests` update:

```java
// Add to application.properties:
// inconsistency-service.base-url=http://inconsistency-service:8090
// inconsistency-service.enabled=${INCONSISTENCY_SERVICE_ENABLED:false}

@Value("${inconsistency-service.base-url}")
private String inconsistencyServiceUrl;

@Value("${inconsistency-service.enabled:false}")
private boolean inconsistencyServiceEnabled;

// In PATCH handler, after existing logic:
if (inconsistencyServiceEnabled) {
    forwardToInconsistencyService(messageKey, posHostname, storeNumber,
                                  delivered, consumerAckTimestamp, status);
}

private void forwardToInconsistencyService(String messageKey, String posHostname,
        String storeNumber, boolean delivered, String consumerAckTimestamp, String status) {
    String url = inconsistencyServiceUrl
        + "/api/message-tracking/" + messageKey + "/pos-response";
    Map<String, Object> body = Map.of(
        "posHostname",           posHostname,
        "storeNumber",           storeNumber,
        "delivered",             delivered,
        "consumerAckTimestamp",  consumerAckTimestamp != null ? consumerAckTimestamp : "",
        "status",                status
    );
    try {
        restTemplate.patchForObject(url, body, Void.class);
    } catch (Exception e) {
        // NEVER fail the POS response — fire-and-forget
        log.warn("Failed to forward to InconsistencyService key={}: {}",
                 messageKey, e.getMessage());
    }
}
```

**Critical rules:**
- Always wrap in try/catch — POS machines must never receive an error because InconsistencyService is down
- Use `INCONSISTENCY_SERVICE_ENABLED=false` as a feature flag during rollout; set to `true` to enable

---

## Project Structure

```
inconsistency-service/
├── pom.xml
└── src/main/
    ├── java/com/pos/inconsistency/
    │   ├── Application.java
    │   ├── controller/
    │   │   ├── MessageTrackingController.java    POST /publish, PATCH /pos-response, GET /status
    │   │   └── HealthController.java             GET /health
    │   ├── model/
    │   │   ├── InconsistencyType.java            Enum: 8 types
    │   │   ├── MtsSummary.java                   DB entity: mts_summary
    │   │   ├── MtsStore.java                     DB entity: mts_store
    │   │   ├── PosStatus.java                    Value in pos_statuses JSONB map
    │   │   ├── PublishEvent.java                 Inbound DTO for POST /publish
    │   │   ├── StoreDetail.java                  Nested in PublishEvent
    │   │   ├── PosResponseEvent.java             Inbound DTO for PATCH /pos-response
    │   │   ├── StatusResponse.java               Response DTO for GET /status
    │   │   └── StoreStatusDetail.java            Nested in StatusResponse
    │   ├── repository/
    │   │   ├── MtsSummaryRepository.java         Plain JDBC for mts_summary
    │   │   └── MtsStoreRepository.java           Plain JDBC + SELECT FOR UPDATE
    │   ├── service/
    │   │   ├── MessageTrackingService.java        Core orchestration
    │   │   ├── InconsistencyDetectionService.java 8 detection rules
    │   │   └── NewRelicEmitService.java           3 NR event types, batched POST
    │   └── scheduler/
    │       └── CleanupScheduler.java              @Scheduled every 1 min
    └── resources/
        ├── application.yml
        └── db/migration/
            └── V1__create_mts_tables.sql          Partitioned tables + indexes
```
