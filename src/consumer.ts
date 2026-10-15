import { setTimeout as sleep } from 'node:timers/promises';
import {
  RateLimitExceededException,
  UnauthorizedException,
  WorkOS,
  type Event,
  type EventName,
} from '@workos-inc/node';

const workos = new WorkOS(process.env.WORKOS_API_KEY);

// The event types to consume. See https://workos.com/docs/events
const EVENTS: EventName[] = [
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

const PAGE_SIZE = 100;
const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS ?? 5_000);
const MAX_LOOKBACK_MS = 30 * 24 * 60 * 60 * 1_000;

export interface CursorStore {
  load(): Promise<string | undefined>;
  save(eventId: string): Promise<void>;
}

// Lost on restart. Use Redis or a database table in production (see README).
class MemoryCursorStore implements CursorStore {
  private eventId: string | undefined;

  async load(): Promise<string | undefined> {
    return this.eventId;
  }

  async save(eventId: string): Promise<void> {
    this.eventId = eventId;
  }
}

// Where to start when there is no cursor yet. A request covers at most 30 days.
function rangeStart(): string {
  const earliest = Date.now() - MAX_LOOKBACK_MS;
  const requested = Date.parse(process.env.START_FROM ?? '');
  return new Date(Number.isNaN(requested) ? earliest : Math.max(requested, earliest)).toISOString();
}

// Replace with your own processing. This writes one JSON line per event to
// stdout, which a Vector `stdin` source can route to Splunk, S3 or anywhere else.
function handle(events: Event[]): Promise<void> {
  const lines = events.map((event) => `${JSON.stringify(event)}\n`).join('');
  return new Promise((resolve, reject) => {
    process.stdout.write(lines, (error) => (error ? reject(error) : resolve()));
  });
}

async function main(): Promise<void> {
  const cursor: CursorStore = new MemoryCursorStore();
  let after = await cursor.load();

  while (true) {
    try {
      const { data } = await workos.events.listEvents({
        events: EVENTS,
        limit: PAGE_SIZE,
        order: 'asc',
        ...(after ? { after } : { rangeStart: rangeStart() }),
      });

      const last = data.at(-1);
      if (last) {
        await handle(data);
        await cursor.save(last.id);
        after = last.id;
      }

      if (data.length === PAGE_SIZE) {
        continue;
      }
      await sleep(POLL_INTERVAL_MS);
    } catch (error) {
      if (error instanceof UnauthorizedException) {
        throw error;
      }
      const waitMs =
        error instanceof RateLimitExceededException && error.retryAfter
          ? error.retryAfter * 1_000
          : POLL_INTERVAL_MS;
      console.error(`listEvents failed, retrying in ${waitMs}ms:`, error);
      await sleep(waitMs);
    }
  }
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
