import { appendFile, mkdir, readFile, rename, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import {
  RateLimitExceededException,
  UnauthorizedException,
  WorkOS,
  type ListEventOptions,
} from '@workos-inc/node';
import {
  EventReader,
  initialState,
  type Cursor,
  type ListPage,
} from './reader.js';

type EventName = ListEventOptions['events'][number];
type WorkOSEvent = Awaited<ReturnType<WorkOS['events']['listEvents']>>['data'][number];

// Every name the SDK's typed filter accepts. Record<EventName, true> makes
// tsc fail if the SDK adds or removes a name and this list isn't updated.
const KNOWN_EVENTS: Record<EventName, true> = {
  'api_key.created': true,
  'api_key.revoked': true,
  'authentication.email_verification_succeeded': true,
  'authentication.magic_auth_failed': true,
  'authentication.magic_auth_succeeded': true,
  'authentication.mfa_succeeded': true,
  'authentication.oauth_failed': true,
  'authentication.oauth_succeeded': true,
  'authentication.passkey_failed': true,
  'authentication.passkey_succeeded': true,
  'authentication.password_failed': true,
  'authentication.password_succeeded': true,
  'authentication.radar_risk_detected': true,
  'authentication.sso_failed': true,
  'authentication.sso_succeeded': true,
  'connection.activated': true,
  'connection.deactivated': true,
  'connection.deleted': true,
  'dsync.activated': true,
  'dsync.deleted': true,
  'dsync.group.created': true,
  'dsync.group.deleted': true,
  'dsync.group.updated': true,
  'dsync.group.user_added': true,
  'dsync.group.user_removed': true,
  'dsync.user.created': true,
  'dsync.user.deleted': true,
  'dsync.user.updated': true,
  'email_verification.created': true,
  'flag.created': true,
  'flag.deleted': true,
  'flag.rule_updated': true,
  'flag.updated': true,
  'group.created': true,
  'group.deleted': true,
  'group.member_added': true,
  'group.member_removed': true,
  'group.updated': true,
  'invitation.accepted': true,
  'invitation.created': true,
  'invitation.resent': true,
  'invitation.revoked': true,
  'magic_auth.created': true,
  'organization_domain.created': true,
  'organization_domain.deleted': true,
  'organization_domain.updated': true,
  'organization_domain.verification_failed': true,
  'organization_domain.verified': true,
  'organization_membership.created': true,
  'organization_membership.deleted': true,
  'organization_membership.updated': true,
  'organization_role.created': true,
  'organization_role.deleted': true,
  'organization_role.updated': true,
  'organization.created': true,
  'organization.deleted': true,
  'organization.updated': true,
  'password_reset.created': true,
  'password_reset.succeeded': true,
  'permission.created': true,
  'permission.deleted': true,
  'permission.updated': true,
  'pipes.connected_account.connected': true,
  'pipes.connected_account.connection_failed': true,
  'pipes.connected_account.disconnected': true,
  'pipes.connected_account.reauthorization_needed': true,
  'role.created': true,
  'role.deleted': true,
  'role.updated': true,
  'session.created': true,
  'session.revoked': true,
  'user.created': true,
  'user.deleted': true,
  'user.updated': true,
  'vault.byok_key.verification_completed': true,
  'vault.data.created': true,
  'vault.data.deleted': true,
  'vault.data.read': true,
  'vault.data.updated': true,
  'vault.dek.decrypted': true,
  'vault.dek.read': true,
  'vault.kek.created': true,
  'vault.metadata.read': true,
  'vault.names.listed': true,
};

const DEFAULT_EVENTS: EventName[] = [
  'authentication.email_verification_succeeded',
  'authentication.magic_auth_failed',
  'authentication.magic_auth_succeeded',
  'authentication.mfa_succeeded',
  'authentication.oauth_failed',
  'authentication.oauth_succeeded',
  'authentication.passkey_failed',
  'authentication.passkey_succeeded',
  'authentication.password_failed',
  'authentication.password_succeeded',
  'authentication.radar_risk_detected',
  'authentication.sso_failed',
  'authentication.sso_succeeded',
  'session.created',
  'session.revoked',
  'user.created',
  'user.updated',
  'user.deleted',
  'organization.created',
  'organization.updated',
  'organization.deleted',
  'organization_membership.created',
  'organization_membership.updated',
  'organization_membership.deleted',
];

function isEventName(value: string): value is EventName {
  return Object.hasOwn(KNOWN_EVENTS, value);
}

export interface CursorStore {
  load(): Promise<Cursor | undefined>;
  save(cursor: Cursor): Promise<void>;
}

export class MemoryCursorStore implements CursorStore {
  private cursor: Cursor | undefined;

  async load(): Promise<Cursor | undefined> {
    return this.cursor;
  }

  async save(cursor: Cursor): Promise<void> {
    this.cursor = cursor;
  }
}

export class FileCursorStore implements CursorStore {
  constructor(private readonly path: string) {}

  async load(): Promise<Cursor | undefined> {
    let raw: string;
    try {
      raw = await readFile(this.path, 'utf8');
    } catch (error) {
      if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
        return undefined;
      }
      throw error;
    }
    const parsed: unknown = JSON.parse(raw);
    if (
      typeof parsed === 'object' &&
      parsed !== null &&
      'id' in parsed &&
      typeof parsed.id === 'string' &&
      'createdAt' in parsed &&
      typeof parsed.createdAt === 'string'
    ) {
      return { id: parsed.id, createdAt: parsed.createdAt };
    }
    throw new Error(`Cursor file ${this.path} is malformed`);
  }

  async save(cursor: Cursor): Promise<void> {
    await mkdir(dirname(this.path), { recursive: true });
    const tmp = `${this.path}.tmp`;
    await writeFile(tmp, JSON.stringify(cursor));
    await rename(tmp, this.path);
  }
}

export interface Sink {
  write(events: readonly WorkOSEvent[]): Promise<void>;
  close?(): Promise<void>;
}

const toNdjson = (events: readonly WorkOSEvent[]): string =>
  events.map((event) => `${JSON.stringify(event)}\n`).join('');

class StdoutSink implements Sink {
  write(events: readonly WorkOSEvent[]): Promise<void> {
    return new Promise((resolve, reject) => {
      process.stdout.write(toNdjson(events), (error) =>
        error ? reject(error) : resolve(),
      );
    });
  }
}

class VectorHttpSink implements Sink {
  constructor(
    private readonly url: string,
    private readonly authorization: string | undefined,
  ) {}

  async write(events: readonly WorkOSEvent[]): Promise<void> {
    const headers: Record<string, string> = {
      'content-type': 'application/x-ndjson',
    };
    if (this.authorization) {
      headers.authorization = this.authorization;
    }
    const response = await fetch(this.url, {
      method: 'POST',
      headers,
      body: toNdjson(events),
      signal: AbortSignal.timeout(30_000),
    });
    if (!response.ok) {
      throw new Error(`Vector responded ${response.status}`);
    }
  }
}

// Writes one file per UTC day of the event's created_at. When events for a
// later day arrive, the earlier day's file is handed to onDayComplete.
class DailyFileSink implements Sink {
  private openDay: string | undefined;

  constructor(
    private readonly dir: string,
    private readonly onDayComplete: (path: string, day: string) => Promise<void>,
  ) {}

  private pathFor(day: string): string {
    return join(this.dir, `events-${day}.ndjson`);
  }

  async write(events: readonly WorkOSEvent[]): Promise<void> {
    await mkdir(this.dir, { recursive: true });
    for (const event of events) {
      const day = new Date(event.createdAt).toISOString().slice(0, 10);
      if (this.openDay !== undefined && day > this.openDay) {
        await this.onDayComplete(this.pathFor(this.openDay), this.openDay);
      }
      if (this.openDay === undefined || day > this.openDay) {
        this.openDay = day;
      }
      await appendFile(this.pathFor(day), toNdjson([event]));
    }
  }

  async close(): Promise<void> {
    if (this.openDay !== undefined) {
      await this.onDayComplete(this.pathFor(this.openDay), this.openDay);
    }
  }
}

// Plug an S3 upload in here (for example PutObject with the AWS SDK).
async function uploadDayFile(path: string, day: string): Promise<void> {
  log(`day ${day} ready for upload: ${path}`);
}

function log(message: string): void {
  process.stderr.write(`${new Date().toISOString()} ${message}\n`);
}

function required(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

function parseEvents(raw: string | undefined): EventName[] {
  if (!raw) {
    return DEFAULT_EVENTS;
  }
  return raw
    .split(',')
    .map((name) => name.trim())
    .filter(Boolean)
    .map((name) => {
      if (!isEventName(name)) {
        throw new Error(`Unknown event type in EVENTS: ${name}`);
      }
      return name;
    });
}

function buildSink(): Sink {
  const mode = process.env.SINK ?? 'stdout';
  switch (mode) {
    case 'stdout':
      return new StdoutSink();
    case 'http':
      return new VectorHttpSink(
        required('VECTOR_HTTP_URL'),
        process.env.VECTOR_HTTP_AUTHORIZATION,
      );
    case 'daily-file':
      return new DailyFileSink(process.env.OUTPUT_DIR ?? './out', uploadDayFile);
    default:
      throw new Error(`SINK must be stdout, http or daily-file (got ${mode})`);
  }
}

const MAX_BACKOFF_MS = 60_000;

function backoffMs(attempt: number): number {
  const base = Math.min(MAX_BACKOFF_MS, 1_000 * 2 ** attempt);
  return base / 2 + Math.random() * (base / 2);
}

function buildStore(): CursorStore {
  const mode = process.env.CURSOR_STORE ?? 'memory';
  switch (mode) {
    case 'memory':
      return new MemoryCursorStore();
    case 'file':
      return new FileCursorStore(process.env.CURSOR_FILE ?? './state/cursor.json');
    default:
      throw new Error(`CURSOR_STORE must be memory or file (got ${mode})`);
  }
}

export async function run(): Promise<void> {
  const workos = new WorkOS(required('WORKOS_API_KEY'), { maxRetries: 0 });
  const pollMs = Number(process.env.POLL_INTERVAL_MS ?? 5_000);
  const limit = Number(process.env.PAGE_SIZE ?? 100);
  const runOnce = process.env.RUN_ONCE === 'true';
  const store = buildStore();
  const sink = buildSink();

  const listPage: ListPage<WorkOSEvent, EventName> = async (params) => {
    const page = await workos.events.listEvents(params);
    return page.data;
  };
  const reader = new EventReader({
    listPage,
    events: parseEvents(process.env.EVENTS),
    limit,
    organizationId: process.env.ORGANIZATION_ID,
    warn: (message) => log(`warning: ${message}`),
  });

  const abort = new AbortController();
  for (const signal of ['SIGINT', 'SIGTERM'] as const) {
    process.once(signal, () => abort.abort());
  }

  let state = initialState(await store.load(), process.env.START_FROM, Date.now());
  let failures = 0;
  log(`starting in ${state.mode} mode`);

  while (!abort.signal.aborted) {
    let waitMs = pollMs;
    let caughtUp = false;
    try {
      const result = await reader.poll(state);
      const last = result.events[result.events.length - 1];
      if (last) {
        await sink.write(result.events);
        await store.save({ id: last.id, createdAt: last.createdAt });
      }
      state = result.state;
      caughtUp = result.caughtUp;
      failures = 0;
    } catch (error) {
      if (error instanceof UnauthorizedException) {
        throw error;
      }
      waitMs =
        error instanceof RateLimitExceededException && error.retryAfter
          ? error.retryAfter * 1_000
          : backoffMs(failures);
      failures += 1;
      log(`error (attempt ${failures}), retrying in ${Math.round(waitMs)}ms: ${String(error)}`);
    }

    if (failures === 0 && !caughtUp) {
      continue;
    }
    if (runOnce && failures === 0) {
      break;
    }
    await sleep(waitMs, undefined, { signal: abort.signal }).catch(() => undefined);
  }

  await sink.close?.();
  log('stopped');
}

run().catch((error: unknown) => {
  log(`fatal: ${String(error)}`);
  process.exitCode = 1;
});
