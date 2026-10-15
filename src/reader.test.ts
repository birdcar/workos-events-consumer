import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  EventReader,
  initialState,
  type EventLike,
  type ListPage,
  type ReaderState,
} from './reader.js';

const DAY = 86_400_000;
const NOW = Date.parse('2026-10-15T12:00:00.000Z');

function makeFake(dataset: EventLike[], now: number): ListPage<EventLike, string> {
  const sorted = [...dataset].sort(
    (a, b) => Date.parse(a.createdAt) - Date.parse(b.createdAt) || a.id.localeCompare(b.id),
  );
  return async (p) => {
    if (p.limit < 1 || p.limit > 100) throw new Error('400 limit');
    if (p.rangeStart !== undefined && p.after !== undefined) {
      throw new Error('400 range_start and after are exclusive');
    }
    let lower = now - 30 * DAY;
    let upper = p.rangeEnd !== undefined ? Date.parse(p.rangeEnd) : now;
    let startIndex = 0;
    if (p.rangeStart !== undefined) {
      lower = Date.parse(p.rangeStart);
      const floor = Date.UTC(
        new Date(now - 91 * DAY).getUTCFullYear(),
        new Date(now - 91 * DAY).getUTCMonth(),
        new Date(now - 91 * DAY).getUTCDate(),
      );
      if (lower < floor) throw new Error('400 older than 90 days');
      if (upper - lower > 30 * DAY) throw new Error('400 longer than 30 days');
    } else if (p.after !== undefined) {
      const index = sorted.findIndex((e) => e.id === p.after);
      const cursorEvent = sorted[index];
      if (!cursorEvent) throw new Error('400 unknown cursor');
      lower = Math.max(now - 31 * DAY, Date.parse(cursorEvent.createdAt));
      startIndex = index + 1;
    }
    return sorted
      .slice(startIndex)
      .filter((e) => {
        const t = Date.parse(e.createdAt);
        return t >= lower && t <= upper && t <= now;
      })
      .slice(0, p.limit);
  };
}

function spread(count: number): EventLike[] {
  return Array.from({ length: count }, (_, i) => ({
    id: `evt_${String(i).padStart(4, '0')}`,
    createdAt: new Date(NOW - 89.5 * DAY + (i * 89 * DAY) / count).toISOString(),
  }));
}

async function drain(
  dataset: EventLike[],
  state: ReaderState,
  limit = 100,
  warnings: string[] = [],
): Promise<{ emitted: string[]; state: ReaderState }> {
  const reader = new EventReader({
    listPage: makeFake(dataset, NOW),
    events: ['user.created'],
    limit,
    now: () => NOW,
    warn: (m) => warnings.push(m),
  });
  const emitted: string[] = [];
  for (let i = 0; i < 1_000; i++) {
    const result = await reader.poll(state);
    emitted.push(...result.events.map((e) => e.id));
    state = result.state;
    if (result.caughtUp) return { emitted, state };
  }
  throw new Error('did not catch up');
}

test('first run emits 250 events across 90 days exactly once, in order, then uses cursor mode', async () => {
  const dataset = spread(250);
  const { emitted, state } = await drain(dataset, initialState(undefined, undefined, NOW));
  assert.deepEqual(emitted, dataset.map((e) => e.id));
  assert.equal(state.mode, 'cursor');
});

test('a 45-day-old cursor falls back to timestamp mode and loses nothing', async () => {
  const dataset = spread(250);
  const index = dataset.findIndex((e) => Date.parse(e.createdAt) > NOW - 45 * DAY);
  const stored = dataset[index];
  assert.ok(stored);
  const warnings: string[] = [];
  const { emitted, state } = await drain(
    dataset,
    initialState(stored, undefined, NOW),
    100,
    warnings,
  );
  assert.deepEqual(emitted, dataset.slice(index + 1).map((e) => e.id));
  assert.equal(state.mode, 'cursor');
  assert.equal(warnings.length, 1);
});

test('events sharing a timestamp across page and window boundaries are neither duplicated nor dropped', async () => {
  const windowEnd = NOW - 90 * DAY + 30 * DAY;
  const tie = new Date(NOW - 45 * DAY).toISOString();
  const atWindowEnd = new Date(windowEnd).toISOString();
  const dataset: EventLike[] = [
    { id: 'a0', createdAt: new Date(NOW - 80 * DAY).toISOString() },
    { id: 't1', createdAt: tie },
    { id: 't2', createdAt: tie },
    { id: 't3', createdAt: tie },
    { id: 'w1', createdAt: atWindowEnd },
    { id: 'w2', createdAt: atWindowEnd },
    { id: 'z0', createdAt: new Date(NOW - 10 * DAY).toISOString() },
  ];
  const { emitted } = await drain(dataset, initialState(undefined, undefined, NOW), 4);
  assert.deepEqual(emitted, ['a0', 'w1', 'w2', 't1', 't2', 't3', 'z0']);
});

test('a full page of identical timestamps warns and terminates', async () => {
  const tie = new Date(NOW - 5 * DAY).toISOString();
  const dataset = ['a', 'b', 'c', 'd', 'e'].map((id) => ({ id, createdAt: tie }));
  const warnings: string[] = [];
  await drain(dataset, initialState(undefined, undefined, NOW), 3, warnings);
  assert.equal(warnings.length, 1);
});

test('the fake rejects what the API rejects', async () => {
  const fake = makeFake([], NOW);
  const events = ['user.created'];
  await assert.rejects(fake({ events, limit: 100, rangeStart: new Date(NOW - 92 * DAY).toISOString() }));
  await assert.rejects(fake({ events, limit: 100, rangeStart: new Date(NOW - 90 * DAY).toISOString() }));
  await assert.rejects(fake({ events, limit: 100, rangeStart: new Date(NOW).toISOString(), after: 'x' }));
});
