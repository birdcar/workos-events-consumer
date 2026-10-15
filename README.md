# WorkOS Events consumer

An example of consuming the [WorkOS Events API](https://workos.com/docs/events/data-syncing/events-api) with the Node SDK. It polls `workos.events.listEvents`, keeps a cursor, and writes each event as one JSON line to stdout, where a log shipper such as [Vector](https://vector.dev) can route it to Splunk, S3 or anywhere else.

This is a standalone example, not an official WorkOS project.

## Run it

Requires Node 22+.

```sh
npm install
npm run build
WORKOS_API_KEY=sk_test_... npm start
```

| Variable | Default | Purpose |
| --- | --- | --- |
| `WORKOS_API_KEY` | required | API key for the environment to read |
| `START_FROM` | 30 days ago | ISO 8601 timestamp to start from on the first run, no earlier than 30 days ago |
| `POLL_INTERVAL_MS` | `5000` | Wait between polls once caught up |

Errors go to stderr, so stdout carries only events.

## How it works

It follows the [Events API guide](https://workos.com/docs/events/data-syncing/events-api):

1. With no cursor yet, the first request passes `rangeStart`.
2. After a page is processed, the last event's ID is saved as the cursor.
3. Every later request passes that ID as `after`. Full pages are fetched back to back; once a page comes back short, it waits `POLL_INTERVAL_MS` and polls again.

The Events API [returns events up to 90 days old, and each request covers up to 30 days](https://workos.com/docs/events/data-syncing/events-api), so the first run starts at most 30 days back.

To choose which events to read, edit `EVENTS` in `src/consumer.ts`. It is typed with the SDK's `EventName`, so a misspelled type fails the build. See the [event types reference](https://workos.com/docs/events) for each payload.

To process events your own way, replace `handle()`.

## Cursor storage

The example keeps the cursor in memory, so a restart begins again from `START_FROM` and re-delivers events. In production, implement `CursorStore` against something durable:

```ts
interface CursorStore {
  load(): Promise<string | undefined>;
  save(eventId: string): Promise<void>;
}
```

Redis (`ioredis`):

```ts
import { Redis } from 'ioredis';

class RedisCursorStore implements CursorStore {
  constructor(private readonly redis: Redis, private readonly key = 'workos:events:cursor') {}

  async load(): Promise<string | undefined> {
    return (await this.redis.get(this.key)) ?? undefined;
  }

  async save(eventId: string): Promise<void> {
    await this.redis.set(this.key, eventId);
  }
}
```

Postgres (`pg`):

```sql
CREATE TABLE workos_event_cursors (
  consumer   text PRIMARY KEY,
  event_id   text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);
```

```ts
import type { Pool } from 'pg';

class PostgresCursorStore implements CursorStore {
  constructor(private readonly pool: Pool, private readonly consumer = 'default') {}

  async load(): Promise<string | undefined> {
    const { rows } = await this.pool.query<{ event_id: string }>(
      'SELECT event_id FROM workos_event_cursors WHERE consumer = $1',
      [this.consumer],
    );
    return rows[0]?.event_id;
  }

  async save(eventId: string): Promise<void> {
    await this.pool.query(
      `INSERT INTO workos_event_cursors (consumer, event_id) VALUES ($1, $2)
       ON CONFLICT (consumer) DO UPDATE SET event_id = EXCLUDED.event_id, updated_at = now()`,
      [this.consumer, eventId],
    );
  }
}
```

Then swap it in for `MemoryCursorStore` in `main()`.

## Delivery and errors

Delivery is at least once. A page goes to `handle()` before the cursor is saved, so a crash between the two re-delivers that page. Dedupe downstream on the event `id`.

On a rate limit (HTTP 429) the consumer waits for the `Retry-After` value, and other errors retry after `POLL_INTERVAL_MS`. An invalid API key stops it. All consumers using one API key share its [rate limit](https://workos.com/docs/reference/rate-limits).

## Vector

Read the consumer's stdout and fan out:

```toml
[sources.workos]
type = "stdin"
decoding.codec = "json"

[sinks.splunk]
type = "splunk_hec_logs"
inputs = ["workos"]
endpoint = "https://splunk.example.com:8088"
default_token = "${SPLUNK_HEC_TOKEN}"
encoding.codec = "json"

[sinks.s3]
type = "aws_s3"
inputs = ["workos"]
bucket = "your-bucket"
region = "us-east-1"
key_prefix = "workos/events/date=%F/"
compression = "gzip"
encoding.codec = "json"
```

### Field names

The Node SDK returns camelCase fields (`createdAt`, `organizationId`, including inside `data`), while the REST API reference and webhooks use snake_case. Write your queries against camelCase, or rename fields in Vector before the sinks:

```toml
[transforms.workos_fields]
type = "remap"
inputs = ["workos"]
source = '''
.created_at = del(.createdAt)
'''
```

### Impersonation

There is no dedicated impersonation event. `session.created` and `session.revoked` include an `impersonator` object when the session was started through impersonation, so tag on its presence:

```toml
[transforms.tag_impersonation]
type = "remap"
inputs = ["workos"]
source = '''
.impersonated = exists(.data.impersonator) && .data.impersonator != null
'''
```
