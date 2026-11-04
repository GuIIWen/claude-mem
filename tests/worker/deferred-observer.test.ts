import { afterEach, describe, expect, it, spyOn } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DeferredObserverQueue, observerMaxWaitMs, type DeferredObserverClock, type DeferredObserverRow } from '../../src/services/worker/DeferredObserverQueue.js';
import { SessionManager } from '../../src/services/worker/SessionManager.js';
import type { DatabaseManager } from '../../src/services/worker/DatabaseManager.js';
import type { PendingMessage } from '../../src/services/worker-types.js';
import { isInternalMemoryObserverTool } from '../../src/shared/memory-retrieval-tools.js';
import { paidSendBudgetForClaimedBatch } from '../../src/services/worker/paid-send-budget.js';

const HALF_HOUR = 1_800_000;
class Clock implements DeferredObserverClock {
  time = 1_000;
  seq = 0;
  timers = new Map<number, { at: number; callback: () => void }>();
  now = () => this.time;
  setTimeout = (callback: () => void, delay: number): ReturnType<typeof setTimeout> => {
    const id = ++this.seq;
    this.timers.set(id, { at: this.time + delay, callback });
    return id as unknown as ReturnType<typeof setTimeout>;
  };
  clearTimeout = (id: ReturnType<typeof setTimeout>): void => { this.timers.delete(id as unknown as number); };
  advance(ms: number): void {
    const target = this.time + ms;
    while (true) {
      const next = [...this.timers].filter(([, timer]) => timer.at <= target).sort((a, b) => a[1].at - b[1].at)[0];
      if (!next) break;
      this.time = next[1].at;
      this.timers.delete(next[0]);
      next[1].callback();
    }
    this.time = target;
  }
}

const observation = (id: string, agentId = 'main'): PendingMessage => ({
  type: 'observation', tool_name: 'Read', tool_input: { file: id }, tool_response: { text: id },
  toolUseId: id, prompt_number: 1, cwd: '/project', agentId,
});
const dbs: Database[] = [];
const queues: DeferredObserverQueue[] = [];
const dirs: string[] = [];
afterEach(() => {
  for (const q of queues.splice(0)) q.stop();
  for (const db of dbs.splice(0)) { try { db.close(); } catch {} }
  // Bun's SQLite statement finalizers can outlive close() on Windows.
  Bun.gc(true);
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true, maxRetries: 5, retryDelay: 20 });
});
function database(path = ':memory:'): Database {
  const db = new Database(path); dbs.push(db);
  db.exec(`PRAGMA foreign_keys=ON; CREATE TABLE IF NOT EXISTS sdk_sessions(id INTEGER PRIMARY KEY);
    CREATE TABLE IF NOT EXISTS observations(id INTEGER PRIMARY KEY);
    CREATE TABLE IF NOT EXISTS session_summaries(id INTEGER PRIMARY KEY);
    INSERT OR IGNORE INTO sdk_sessions(id) VALUES (1),(2);`);
  return db;
}
function harness(maxWait = HALF_HOUR, db = database(), clock = new Clock()) {
  const calls: Array<{ rows: DeferredObserverRow[]; source: string }> = [];
  const queue = new DeferredObserverQueue(db, maxWait, (rows, source) => calls.push({ rows, source }), clock);
  queues.push(queue);
  queue.recover();
  return { queue, db, clock, calls };
}
const state = (db: Database, id: number) => db.prepare('SELECT state, reason, message_json FROM observer_deferred_outbox WHERE id=?').get(id) as { state: string; reason: string | null; message_json: string };

describe('durable deferred observer scheduling', () => {
  it('A1/A2: 32 events never trigger by count; the oldest deadline does not slide at 29:59', () => {
    const { queue, clock, calls } = harness();
    for (let i = 0; i < 32; i++) queue.enqueue(1, observation(String(i)));
    clock.advance(HALF_HOUR - 1_000);
    queue.enqueue(1, observation('late'));
    expect(calls).toHaveLength(0);
    clock.advance(1_000);
    expect(calls).toHaveLength(1);
    expect(calls[0].rows).toHaveLength(33);
    expect(queue.getWaitingCount()).toBe(0);
    clock.advance(HALF_HOUR);
    expect(calls).toHaveLength(1);
  });

  it('A3: summary snapshot is ordered and duplicate boundary/timer does not release twice', () => {
    const { queue, clock, calls } = harness();
    queue.enqueue(1, observation('one'));
    queue.enqueue(1, { type: 'summarize', prompt_number: 1, last_assistant_message: 'done' });
    expect(queue.flush(1, 'summary')).toBe(2);
    expect(queue.flush(1, 'session-end')).toBe(0);
    clock.advance(HALF_HOUR);
    expect(calls).toHaveLength(1);
    expect(calls[0].rows.map(row => JSON.parse(row.message_json).type)).toEqual(['observation', 'summarize']);
  });

  it('A4: zero waits forever without scheduling a timer', () => {
    const { queue, clock, calls } = harness(0);
    queue.enqueue(1, observation('one'));
    expect(clock.timers.size).toBe(0);
    clock.advance(48 * HALF_HOUR);
    expect(calls).toHaveLength(0);
    queue.flush(1, 'summary');
    expect(calls).toHaveLength(1);
  });

  it('A5: later arrivals and other sessions remain waiting after a released snapshot', () => {
    const { queue, clock, calls } = harness();
    queue.enqueue(1, observation('first', 'agent-one'));
    queue.enqueue(2, observation('elsewhere', 'agent-two'));
    queue.flush(1, 'summary');
    clock.advance(500);
    queue.enqueue(1, observation('next', 'agent-one'));
    expect(calls).toHaveLength(1);
    expect(calls[0].rows.map(row => JSON.parse(row.message_json).toolUseId)).toEqual(['first']);
    expect(queue.getWaitingCount(1)).toBe(1);
    expect(queue.getWaitingCount(2)).toBe(1);
  });

  it('A6: reopening SQLite retains the original deadline, confirmed dedup and quarantine', () => {
    const dir = mkdtempSync(join(tmpdir(), 'observer-deferred-')); dirs.push(dir);
    const path = join(dir, 'outbox.db');
    const clock = new Clock();
    const first = harness(HALF_HOUR, database(path), clock);
    const completed = first.queue.enqueue(1, observation('completed'));
    const uncertain = first.queue.enqueue(1, observation('uncertain'));
    const unsent = first.queue.enqueue(1, observation('released-not-yielded'));
    first.queue.flush(1, 'summary');
    first.queue.markInflight(completed); first.queue.confirm(completed);
    first.queue.markInflight(uncertain);
    first.queue.enqueue(2, observation('waiting'));
    first.queue.stop(); first.db.close();
    clock.advance(HALF_HOUR - 1_000);
    const restored = harness(HALF_HOUR, database(path), clock);
    expect(restored.calls.flatMap(call => call.rows.map(row => row.id))).toEqual([unsent]);
    expect(state(restored.db, uncertain).state).toBe('parked');
    expect(state(restored.db, uncertain).reason).toBe('restart_after_uncertain_send');
    expect(state(restored.db, completed).message_json).toBe('');
    expect(restored.queue.enqueue(1, observation('completed'))).toBe(0);
    clock.advance(1_000);
    expect(restored.calls.at(-1)?.rows.map(row => row.session_db_id)).toEqual([2]);
  });

  it('A7: stop preserves waiting and released work without flushing', () => {
    const { queue, clock, calls, db } = harness();
    const id = queue.enqueue(1, observation('one'));
    queue.stop(); clock.advance(HALF_HOUR * 2);
    expect(calls).toHaveLength(0);
    expect(state(db, id).state).toBe('waiting');
  });

  it('summary dedup follows the observation boundary, not a permanent prompt/text hash', () => {
    const { queue, calls } = harness();
    const summary: PendingMessage = { type: 'summarize', prompt_number: 1, last_assistant_message: 'done' };
    queue.enqueue(1, observation('first'));
    const first = queue.enqueue(1, summary); queue.flush(1, 'summary');
    queue.confirm(first);
    expect(queue.enqueue(1, summary)).toBe(0);
    queue.enqueue(1, observation('later'));
    expect(queue.enqueue(1, summary)).toBeGreaterThan(0);
    queue.flush(1, 'summary');
    expect(calls.at(-1)?.rows.map(row => JSON.parse(row.message_json).type)).toEqual(['observation', 'summarize']);
  });

  it('explicit cancellation cannot revive released or waiting work on restart', () => {
    const { queue, clock, calls, db } = harness();
    const released = queue.enqueue(1, observation('released'));
    queue.flush(1, 'summary');
    const waiting = queue.enqueue(1, observation('waiting'));
    queue.cancelSession(1); queue.stop();
    const restored = harness(HALF_HOUR, db, clock);
    clock.advance(HALF_HOUR * 2);
    expect(restored.calls).toHaveLength(0);
    expect(calls).toHaveLength(1);
    expect(state(db, released).reason).toBe('explicit_cancel');
    expect(state(db, waiting).state).toBe('parked');
  });

  it('recovers a persisted boundary even in infinite-wait mode', () => {
    const { queue, db, clock } = harness(0);
    queue.enqueue(1, observation('one'));
    queue.enqueue(1, { type: 'summarize', prompt_number: 1 });
    // Simulate a crash between durable boundary insertion and flush().
    queue.stop();
    const recovered = harness(0, db, clock);
    expect(recovered.calls).toHaveLength(1);
    expect(recovered.calls[0].rows.map(row => JSON.parse(row.message_json).type)).toEqual(['observation', 'summarize']);
  });

  it('deleting an existing session cascades waiting/released payloads and cannot poison recovery', () => {
    const { queue, db, clock } = harness();
    queue.enqueue(1, observation('released')); queue.flush(1, 'summary');
    queue.enqueue(1, observation('waiting'));
    db.prepare('DELETE FROM sdk_sessions WHERE id=?').run(1);
    expect(db.prepare('SELECT count(*) AS n FROM observer_deferred_outbox').get()).toEqual({ n: 0 });
    queue.stop();
    expect(harness(HALF_HOUR, db, clock).calls).toHaveLength(0);
  });

  it('parses max wait safely including infinite and invalid values', () => {
    expect(observerMaxWaitMs('0')).toBe(0);
    expect(observerMaxWaitMs('1800000')).toBe(HALF_HOUR);
    for (const input of ['-1', 'NaN', 'Infinity', '2.5', '999999999999999', undefined]) {
      expect(observerMaxWaitMs(input)).toBe(HALF_HOUR);
    }
  });
});

function manager(db: Database, clock: Clock, enabled = true): { sm: SessionManager; kicks: string[] } {
  const dbManager = {
    getSessionById: (id: number) => ({ content_session_id: `session-${id}`, project: 'project', platform_source: 'codex', user_prompt: 'work', memory_session_id: null }),
    getSessionStore: () => ({ getPromptNumberFromUserPrompts: () => 1, getLatestPromptTextFromUserPrompts: () => 'work' }),
  } as unknown as DatabaseManager;
  const sm = new SessionManager(dbManager);
  const kicks: string[] = [];
  sm.setGeneratorStarter((_id, source) => { kicks.push(source); });
  sm.configureDeferredObserver(db, {
    CLAUDE_MEM_OBSERVER_TRIGGER: enabled ? 'summary_or_session_end' : 'immediate',
    CLAUDE_MEM_OBSERVER_MAX_WAIT_MS: String(HALF_HOUR),
  }, clock);
  return { sm, kicks };
}
function enqueue(sm: SessionManager, id: string, session = 1) {
  sm.queueObservation(session, { tool_name: 'Read', tool_input: { path: id }, tool_response: 'data', toolUseId: id, prompt_number: 1, cwd: '/project' });
}

describe('SessionManager shared provider integration', () => {
  it('A1/A5: a running iterator sees only released rows, including batch extension', async () => {
    const db = database(), clock = new Clock();
    const { sm, kicks } = manager(db, clock);
    for (let i = 0; i < 16; i++) enqueue(sm, `tool-${i}`);
    expect(sm.getMessageBuffer().getPendingCount(1)).toBe(0);
    expect(sm.getTotalQueueDepth()).toBe(16);
    expect(kicks).toEqual([]);
    sm.queueSummarize(1, 'done', 1);
    await Promise.resolve(); await Promise.resolve();
    expect(sm.getMessageBuffer().getPendingCount(1)).toBe(17);
    const iterator = sm.getMessageIterator(1);
    const first = await iterator.next();
    expect(first.value?.toolUseId).toBe('tool-0');
    enqueue(sm, 'next-period');
    const batch: string[] = [];
    let next;
    while ((next = sm.claimNextObservation(1, () => true))) batch.push(next.toolUseId!);
    expect(batch).toHaveLength(15);
    expect(batch).not.toContain('next-period');
    await sm.confirmClaimedMessages(1);
    const summary = await iterator.next();
    expect(summary.value?.type).toBe('summarize');
    await sm.confirmClaimedMessages(1);
    expect(sm.getMessageBuffer().getPendingCount(1)).toBe(0);
    expect(sm.getTotalQueueDepth()).toBe(1);
    sm.getSession(1)!.abortController.abort(); await iterator.return(undefined);
    const deletion = spyOn(sm, 'deleteSession').mockImplementation(async () => {});
    await sm.shutdownAll(); deletion.mockRestore();
  });

  it('A3/A7: session_end releases observations then summary, repeated end does not duplicate', async () => {
    const db = database(), clock = new Clock();
    const { sm } = manager(db, clock);
    enqueue(sm, 'one');
    await sm.requestSessionWrapup(1);
    await sm.requestSessionWrapup(1);
    expect(sm.getMessageBuffer().peekTypes(1).map(row => row.message_type)).toEqual(['observation', 'summarize']);
    const timer = sm.getSession(1)?.telegramWrapupTimer;
    if (timer) clearTimeout(timer);
    const deletion = spyOn(sm, 'deleteSession').mockImplementation(async () => {});
    await sm.shutdownAll(); deletion.mockRestore();
  });

  it('session_end adds a summary after a timer already released unfinished observations', async () => {
    const db = database(), clock = new Clock();
    const { sm } = manager(db, clock);
    enqueue(sm, 'one'); clock.advance(HALF_HOUR);
    expect(sm.getMessageBuffer().peekTypes(1).map(row => row.message_type)).toEqual(['observation']);
    await sm.requestSessionWrapup(1);
    expect(sm.getMessageBuffer().peekTypes(1).map(row => row.message_type)).toEqual(['observation', 'summarize']);
    const timer = sm.getSession(1)?.telegramWrapupTimer;
    if (timer) clearTimeout(timer);
    const deletion = spyOn(sm, 'deleteSession').mockImplementation(async () => {});
    await sm.shutdownAll(); deletion.mockRestore();
  });

  it('session_end summarizes observations already confirmed by a timer flush', async () => {
    const db = database(), clock = new Clock();
    const { sm } = manager(db, clock);
    enqueue(sm, 'one'); clock.advance(HALF_HOUR);
    const iterator = sm.getMessageIterator(1);
    await iterator.next(); await sm.confirmClaimedMessages(1);
    await sm.requestSessionWrapup(1);
    expect(sm.getMessageBuffer().peekTypes(1).map(row => row.message_type)).toEqual(['summarize']);
    const timer = sm.getSession(1)?.telegramWrapupTimer;
    if (timer) clearTimeout(timer);
    sm.getSession(1)!.abortController.abort(); await iterator.return(undefined);
    const deletion = spyOn(sm, 'deleteSession').mockImplementation(async () => {});
    await sm.shutdownAll(); deletion.mockRestore();
  });

  it('A9: default immediate mode does not create an outbox or delay events', () => {
    const db = database(), clock = new Clock();
    const { sm } = manager(db, clock, false);
    enqueue(sm, 'now');
    expect(sm.getMessageBuffer().getPendingCount(1)).toBe(1);
    expect(db.prepare("SELECT name FROM sqlite_master WHERE name='observer_deferred_outbox'").get()).toBeNull();
  });

  it('retains the paid-send cap and persists a spent batch as parked', async () => {
    const db = database(), clock = new Clock();
    const { sm } = manager(db, clock);
    enqueue(sm, 'budget'); sm.queueSummarize(1, 'done', 1);
    const iterator = sm.getMessageIterator(1);
    await iterator.next();
    const session = sm.getSession(1)!;
    const budget = paidSendBudgetForClaimedBatch(session)!;
    budget.recordPaidSend(); budget.recordPaidSend();
    expect(sm.parkBatchOnSpentPaidSendBudget(session)).toHaveLength(1);
    const row = db.prepare("SELECT state, reason FROM observer_deferred_outbox WHERE message_type='observation'").get() as { state: string; reason: string };
    expect(row.state).toBe('parked');
    expect(row.reason).toStartWith('paid_send_budget:');
    expect(sm.claimNextObservation(1, () => true)).toBeNull();
    session.abortController.abort(); await iterator.return(undefined);
    const deletion = spyOn(sm, 'deleteSession').mockImplementation(async () => {});
    await sm.shutdownAll(); deletion.mockRestore();
  });
});

describe('A8 internal memory observer filtering', () => {
  it('matches only memory tools, including work_state, and preserves business MCP tools', () => {
    for (const name of ['mcp__mcp_search__work_state_read', 'mcp__mcp_search__work_state_write', 'mcp__claude-mem__search', 'mcp__claude-mem__timeline', 'mcp__cmem__get_observations', 'mcp__mcp_search__save_memory']) {
      expect(isInternalMemoryObserverTool(name)).toBe(true);
    }
    for (const name of ['mcp__remote_linux__exec', 'mcp__business__work_state_write', 'mcp__github__search', 'Read', 'Bash']) {
      expect(isInternalMemoryObserverTool(name)).toBe(false);
    }
  });
});
