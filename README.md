# WorkOS Events consumer

A small Node consumer that polls the WorkOS Events API (`GET /events`, via `@workos-inc/node`) and writes each event as one NDJSON line. Point it at Vector (stdout or HTTP) for near-real-time delivery, or run it in daily-file mode for a data-lake drop.

This is a standalone sample, not an official WorkOS project.

## Setup

Requires Node 22+.

```sh
npm install
npm run build
WORKOS_API_KEY=sk_... npm start
npm test        # offline tests against a fake Events API
```

| Variable | Default | Purpose |
| --- | --- | --- |
| `WORKOS_API_KEY` | required | API key for the environment to read |
| `EVENTS` | auth, session, user, organization and membership events | Comma-separated event types. The API requires at least one. |
| `ORGANIZATION_ID` | all | Only events for this organization |
| `SINK` | `stdout` | `stdout`, `http` or `daily-file` |
| `VECTOR_HTTP_URL` | | Vector `http_server` URL (`SINK=http`) |
| `VECTOR_HTTP_AUTHORIZATION` | | Optional `Authorization` header value for Vector |
| `OUTPUT_DIR` | `./out` | Directory for `events-YYYY-MM-DD.ndjson` (`SINK=daily-file`) |
| `CURSOR_STORE` | `memory` | `memory` or `file` |
| `CURSOR_FILE` | `./state/cursor.json` | Cursor path when `CURSOR_STORE=file` |
| `START_FROM` | 90 days ago | ISO 8601 start of the first backfill; clamped to no older than 90 days |
| `POLL_INTERVAL_MS` | `5000` | Wait between polls once caught up |
| `PAGE_SIZE` | `100` | 1 to 100 |
| `RUN_ONCE` | `false` | Drain until caught up, then exit (for cron or a scheduled job) |

Logs go to stderr, so stdout carries only NDJSON.

## How it reads events

The Events API serves the last 90 days. It has two ways to page, with different limits, so the consumer uses both.

**Timestamp mode** (first run, or when the saved cursor is more than 29 days old). Requests use `range_start` and `range_end` in windows of at most 30 days, starting at `START_FROM` (default: 90 days ago). A single request from 90 days ago fails, because the API caps a range at 30 days and `range_end` defaults to now. Within a window the consumer pages forward by timestamp and remembers the ids already delivered at the boundary instant, so ties are not duplicated. If a full page of events shares one timestamp and has all been delivered, it logs a warning and skips past that instant; events beyond the first 100 at that exact instant can be missed in that case.

**Cursor mode** (steady state). Once caught up, the consumer switches to `after=<last event id>`. Cursor reads only reach back about 31 days, and an older cursor silently skips events, so if the consumer has been stopped for more than 29 days it drops back to timestamp mode from the cursor's timestamp. It logs a warning when the cursor lags by more than 7 days.

`range_start` and `after` cannot be combined, and results are always ascending. Events become visible about 5 seconds after they occur.

### Backfill cost

A 90-day backfill takes at least 3 windows plus one request per 100 events. For example, 1,000,000 events is about 10,000 requests. The limit is [6,000 requests per 60 seconds per API key](https://workos.com/docs/reference/rate-limits), counted per key, so every consumer using the same key shares it. This consumer issues requests one at a time, so it is bounded by request latency and typically stays well under the limit; steady state when idle is about 12 requests per minute at the default poll interval. If you run several backfills at once on one key, stagger them. On HTTP 429 the consumer waits for the `Retry-After` value.

## Cursor storage

The cursor is `{ id, createdAt }` of the last event written. It is saved only after a successful write.

The default store is in memory. With it, every restart re-runs the 90-day backfill and re-delivers events, so downstream must dedupe on the event `id`. Use `CURSOR_STORE=file`, Redis or Postgres in production.

Any store implements:

```ts
interface CursorStore {
  load(): Promise<Cursor | undefined>;
  save(cursor: Cursor): Promise<void>;
}
```

Redis (`ioredis`):

```ts
import { Redis } from 'ioredis';

export class RedisCursorStore implements CursorStore {
  constructor(private readonly redis: Redis, private readonly key: string) {}

  async load(): Promise<Cursor | undefined> {
    const raw = await this.redis.get(this.key);
    if (!raw) return undefined;
    const parsed: unknown = JSON.parse(raw);
    if (
      typeof parsed === 'object' && parsed !== null &&
      'id' in parsed && typeof parsed.id === 'string' &&
      'createdAt' in parsed && typeof parsed.createdAt === 'string'
    ) {
      return { id: parsed.id, createdAt: parsed.createdAt };
    }
    throw new Error('malformed cursor');
  }

  async save(cursor: Cursor): Promise<void> {
    await this.redis.set(this.key, JSON.stringify(cursor));
  }
}
```

Postgres (`pg`):

```sql
CREATE TABLE workos_event_cursors (
  consumer   text PRIMARY KEY,
  event_id   text NOT NULL,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);
```

```ts
import type { Pool } from 'pg';

export class PostgresCursorStore implements CursorStore {
  constructor(private readonly pool: Pool, private readonly consumer: string) {}

  async load(): Promise<Cursor | undefined> {
    const { rows } = await this.pool.query<{ event_id: string; created_at: Date }>(
      'SELECT event_id, created_at FROM workos_event_cursors WHERE consumer = $1',
      [this.consumer],
    );
    const row = rows[0];
    return row ? { id: row.event_id, createdAt: row.created_at.toISOString() } : undefined;
  }

  async save(cursor: Cursor): Promise<void> {
    await this.pool.query(
      `INSERT INTO workos_event_cursors (consumer, event_id, created_at)
       VALUES ($1, $2, $3)
       ON CONFLICT (consumer) DO UPDATE
       SET event_id = EXCLUDED.event_id, created_at = EXCLUDED.created_at, updated_at = now()`,
      [this.consumer, cursor.id, cursor.createdAt],
    );
  }
}
```

Then construct it in `buildStore()` in `src/consumer.ts`.

## Vector

Receive over HTTP (`SINK=http`, `VECTOR_HTTP_URL=http://vector:8080`):

```toml
[sources.workos]
type = "http_server"
address = "0.0.0.0:8080"
decoding.codec = "json"
framing.method = "newline_delimited"
```

or read the consumer's stdout (`SINK=stdout`):

```toml
[sources.workos]
type = "stdin"
decoding.codec = "json"
```

Then fan out:

```toml
[sinks.splunk]
type = "splunk_hec_logs"
inputs = ["workos_fields"]
endpoint = "https://splunk.example.com:8088"
default_token = "${SPLUNK_HEC_TOKEN}"
encoding.codec = "json"

[sinks.s3]
type = "aws_s3"
inputs = ["workos_fields"]
bucket = "your-data-lake-bucket"
region = "us-east-1"
key_prefix = "workos/events/date=%F/"
compression = "gzip"
encoding.codec = "json"
```

For a daily batch without Vector, use `SINK=daily-file`. Events are appended to one file per UTC day of `createdAt`. `uploadDayFile` in `src/consumer.ts` is called when a day's file is complete (and for the last open day on shutdown); put your S3 upload there, with a deterministic object key per day so a re-upload overwrites rather than duplicates.

### Field names

The Node SDK returns camelCase fields, including nested ones (`createdAt`, `organizationId`, and so on), while the REST API and webhooks use snake_case. Either write your Splunk extractions against camelCase, or rename the top-level fields in Vector (nested `data` keys stay camelCase):

```toml
[transforms.workos_fields]
type = "remap"
inputs = ["workos"]
source = '''
.created_at = del(.createdAt)
'''
```

## Delivery semantics

At-least-once. A page is written to the sink first, and the cursor saved only after the write succeeds. A crash or sink failure between the two means the same page is delivered again on restart. Dedupe downstream on the event `id`.

Errors back off exponentially (1s doubling to a 60s cap, with jitter). On HTTP 429 the consumer waits for `Retry-After`. An invalid API key (401) stops the process.

## Event types

`EVENTS` is checked against the event names the SDK accepts, and the consumer exits at startup on anything else. See the [events reference](https://workos.com/docs/events) for each type's payload.

## Impersonation

There is no dedicated impersonation event. `session.created` and `session.revoked` carry an optional `impersonator` object when the session was started through impersonation. Filter or tag on its presence in Vector:

```toml
[transforms.tag_impersonation]
type = "remap"
inputs = ["workos"]
source = '''
.impersonated = exists(.data.impersonator) && .data.impersonator != null
'''
```

## Multiple consumers

Each consumer keeps its own cursor, so they are independent: give each its own cursor (a separate `CURSOR_FILE`, Redis key or Postgres `consumer` value) and settings.

```sh
# security pipeline: near real time, to Vector, auth and session events
CURSOR_STORE=file CURSOR_FILE=./state/security.json SINK=http VECTOR_HTTP_URL=http://vector:8080 \
  EVENTS=authentication.sso_failed,authentication.password_failed,session.created,session.revoked npm start

# data lake: daily files, run from a scheduler
CURSOR_STORE=file CURSOR_FILE=./state/lake.json SINK=daily-file RUN_ONCE=true npm start
```

All consumers on one API key share its rate limit.

## Retention

The Events API serves the last 90 days. A consumer that is stopped longer than that has a gap it cannot recover from the API. To replay, clear the stored cursor and set `START_FROM`.
