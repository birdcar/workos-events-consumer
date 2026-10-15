export interface Cursor {
  id: string;
  createdAt: string;
}

export interface EventLike {
  id: string;
  createdAt: string;
}

export interface ListPageParams<N> {
  events: N[];
  limit: number;
  organizationId?: string;
  after?: string;
  rangeStart?: string;
  rangeEnd?: string;
}

// The only function that talks to the Events API. Results must be ascending.
export type ListPage<E extends EventLike, N> = (
  params: ListPageParams<N>,
) => Promise<E[]>;

export type ReaderState =
  | { mode: 'cursor'; cursor: Cursor }
  | {
      mode: 'timestamp';
      from: number;
      // Ids already emitted whose createdAt is exactly `from`.
      seen: ReadonlySet<string>;
      last: Cursor | undefined;
    };

export interface ReaderOptions<E extends EventLike, N> {
  listPage: ListPage<E, N>;
  events: N[];
  limit: number;
  organizationId?: string;
  now?: () => number;
  warn?: (message: string) => void;
}

export interface PollResult<E extends EventLike> {
  events: E[];
  state: ReaderState;
  caughtUp: boolean;
}

const DAY_MS = 86_400_000;
export const MAX_RANGE_AGE_MS = 90 * DAY_MS;
export const MAX_RANGE_SPAN_MS = 30 * DAY_MS;
export const CURSOR_MAX_AGE_MS = 29 * DAY_MS;
const CURSOR_LAG_WARNING_MS = 7 * DAY_MS;
const IDLE_WINDOW_MS = 10 * 60_000;

export function initialState(
  stored: Cursor | undefined,
  startFrom: string | undefined,
  now: number,
): ReaderState {
  if (stored) {
    return { mode: 'cursor', cursor: stored };
  }
  const requested = startFrom ? Date.parse(startFrom) : Number.NaN;
  const floor = now - MAX_RANGE_AGE_MS;
  const from = Number.isNaN(requested) ? floor : Math.max(requested, floor);
  return { mode: 'timestamp', from, seen: new Set(), last: undefined };
}

export class EventReader<E extends EventLike, N> {
  private readonly now: () => number;
  private readonly warn: (message: string) => void;

  constructor(private readonly options: ReaderOptions<E, N>) {
    this.now = options.now ?? Date.now;
    this.warn = options.warn ?? (() => undefined);
  }

  // Fetches the next batch without side effects. The caller writes `events`,
  // persists the last one as the cursor, then continues from `state`.
  async poll(state: ReaderState): Promise<PollResult<E>> {
    const now = this.now();
    if (state.mode === 'cursor') {
      const age = now - Date.parse(state.cursor.createdAt);
      if (age > CURSOR_MAX_AGE_MS) {
        this.warn(
          `cursor is ${Math.floor(age / DAY_MS)} days old; re-reading by timestamp`,
        );
        return this.pollTimestamp(
          {
            mode: 'timestamp',
            from: Date.parse(state.cursor.createdAt),
            seen: new Set([state.cursor.id]),
            last: undefined,
          },
          now,
        );
      }
      if (age > CURSOR_LAG_WARNING_MS) {
        this.warn(`cursor lag is ${Math.floor(age / DAY_MS)} days`);
      }
      return this.pollCursor(state.cursor);
    }
    return this.pollTimestamp(state, now);
  }

  private async pollCursor(cursor: Cursor): Promise<PollResult<E>> {
    const { limit, events, organizationId } = this.options;
    const data = await this.options.listPage({
      events,
      limit,
      organizationId,
      after: cursor.id,
    });
    const last = data[data.length - 1];
    return {
      events: data,
      state: {
        mode: 'cursor',
        cursor: last ? { id: last.id, createdAt: last.createdAt } : cursor,
      },
      caughtUp: data.length < limit,
    };
  }

  private async pollTimestamp(
    state: Extract<ReaderState, { mode: 'timestamp' }>,
    now: number,
  ): Promise<PollResult<E>> {
    const { limit, events, organizationId } = this.options;
    const from = Math.max(state.from, now - MAX_RANGE_AGE_MS);
    const seen = from === state.from ? state.seen : new Set<string>();
    const end = Math.min(from + MAX_RANGE_SPAN_MS, now);

    const data = await this.options.listPage({
      events,
      limit,
      organizationId,
      rangeStart: new Date(from).toISOString(),
      rangeEnd: new Date(end).toISOString(),
    });

    const fresh = data.filter((event) => !seen.has(event.id));
    const lastEvent = data[data.length - 1];
    const lastEmitted = fresh[fresh.length - 1];
    const windowDone = data.length < limit;
    const windowIsLast = end >= now;

    let nextFrom = lastEvent ? Date.parse(lastEvent.createdAt) : from;
    if (windowDone && !windowIsLast) {
      nextFrom = end;
    }

    const stuckOnTies =
      data.length >= limit &&
      fresh.length === 0 &&
      data.every((event) => Date.parse(event.createdAt) === from);
    if (stuckOnTies) {
      this.warn(
        `a full page of events shares timestamp ${new Date(from).toISOString()}; skipping past it, some events at this instant may be missed`,
      );
      return {
        events: [],
        state: { mode: 'timestamp', from: from + 1, seen: new Set(), last: state.last },
        caughtUp: false,
      };
    }

    const nextSeen = new Set<string>(nextFrom === from ? seen : []);
    for (const event of data) {
      if (Date.parse(event.createdAt) === nextFrom) {
        nextSeen.add(event.id);
      }
    }
    const last: Cursor | undefined = lastEmitted
      ? { id: lastEmitted.id, createdAt: lastEmitted.createdAt }
      : state.last;

    if (windowDone && windowIsLast) {
      if (last) {
        return {
          events: fresh,
          state: { mode: 'cursor', cursor: last },
          caughtUp: true,
        };
      }
      const idleFrom = Math.max(nextFrom, now - IDLE_WINDOW_MS);
      return {
        events: fresh,
        state: {
          mode: 'timestamp',
          from: idleFrom,
          seen: idleFrom === nextFrom ? nextSeen : new Set(),
          last,
        },
        caughtUp: true,
      };
    }

    return {
      events: fresh,
      state: { mode: 'timestamp', from: nextFrom, seen: nextSeen, last },
      caughtUp: false,
    };
  }
}
