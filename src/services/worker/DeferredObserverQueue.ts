import type { Database } from 'bun:sqlite';
import { createHash } from 'node:crypto';
import type { PendingMessage } from '../worker-types.js';
import { logger } from '../../utils/logger.js';
import { ensureObserverV2Schema, freezeSource } from './observer-batch-v2.js';

export interface DeferredObserverClock {
  now(): number;
  setTimeout(callback: () => void, delayMs: number): ReturnType<typeof setTimeout>;
  clearTimeout(timer: ReturnType<typeof setTimeout>): void;
}

export interface DeferredObserverRow {
  id: number;
  session_db_id: number;
  message_json: string;
  enqueued_at: number;
  state: string;
  source_meta_json?: string | null;
}

const systemClock: DeferredObserverClock = {
  now: () => Date.now(),
  setTimeout: (callback, delay) => setTimeout(callback, delay),
  clearTimeout: timer => clearTimeout(timer),
};

export function observerMaxWaitMs(value: string | undefined): number {
  if (value !== undefined && /^\d+$/.test(value.trim())) {
    const parsed = Number(value);
    if (Number.isSafeInteger(parsed) && parsed >= 0 && parsed <= 2_147_483_647) return parsed;
  }
  return 30 * 60 * 1000;
}

/**
 * A durable *unsent* outbox, not a replay engine for the stateful reducer.
 * Waiting rows cannot be read by a live provider iterator. A boundary takes a
 * synchronous snapshot into its existing RAM buffer. Only yielding a row marks
 * it inflight; a crash quarantines such rows rather than buying the turn again.
 */
export class DeferredObserverQueue {
  private readonly timers = new Map<number, ReturnType<typeof setTimeout>>();
  private stopped = false;

  constructor(
    private readonly db: Database,
    private readonly maxWaitMs: number,
    private readonly release: (rows: DeferredObserverRow[], source: string) => void,
    private readonly clock: DeferredObserverClock = systemClock,
  ) {
    db.exec(`CREATE TABLE IF NOT EXISTS observer_deferred_outbox (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      session_db_id INTEGER NOT NULL REFERENCES sdk_sessions(id) ON DELETE CASCADE,
      event_key TEXT,
      message_type TEXT NOT NULL,
      message_json TEXT NOT NULL,
      enqueued_at INTEGER NOT NULL,
      state TEXT NOT NULL CHECK(state IN ('waiting','released','inflight','parked','done')),
      reason TEXT,
      UNIQUE(session_db_id, event_key)
    );
    CREATE INDEX IF NOT EXISTS idx_observer_deferred_waiting
      ON observer_deferred_outbox(state, session_db_id, enqueued_at);`);
    ensureObserverV2Schema(db);
  }

  /** Explicit worker boot step: never invoked merely by constructing SessionManager in a test. */
  recover(): void {
    const interrupted = this.db.prepare(`UPDATE observer_deferred_outbox
      SET state='parked', reason='restart_after_uncertain_send' WHERE state='inflight'`).run().changes;
    if (interrupted) logger.warn('QUEUE', 'Deferred observer sends quarantined after restart; not resent', { count: interrupted });
    const released = this.db.prepare(`SELECT * FROM observer_deferred_outbox WHERE state='released' ORDER BY id`).all() as DeferredObserverRow[];
    if (released.length) this.release(released, 'deferred-recovery');
    const sessions = this.db.prepare(`SELECT DISTINCT session_db_id FROM observer_deferred_outbox WHERE state='waiting'`).all() as Array<{ session_db_id: number }>;
    for (const row of sessions) {
      const boundary = this.db.prepare(`SELECT 1 FROM observer_deferred_outbox
        WHERE session_db_id=? AND state='waiting' AND message_type='summarize' LIMIT 1`).get(row.session_db_id);
      if (boundary) this.flush(row.session_db_id, 'deferred-boundary-recovery');
      else this.arm(row.session_db_id);
    }
  }

  enqueue(sessionDbId: number, message: PendingMessage, enqueuedAt = this.clock.now(), project?: string): number {
    if (this.stopped) throw new Error('Deferred observer queue is stopped');
    const latestObservation = message.type === 'summarize'
      ? (this.db.prepare(`SELECT max(id) AS id FROM observer_deferred_outbox WHERE session_db_id=? AND message_type='observation'`).get(sessionDbId) as { id: number | null }).id
      : null;
    const eventKey = message.toolUseId ? `tool:${message.toolUseId}`
      : message.type === 'summarize' ? `summary:${latestObservation ?? 0}:${createHash('sha256').update(JSON.stringify(message)).digest('hex')}` : null;
    const id = this.db.transaction(() => {
      const result = this.db.prepare(`INSERT OR IGNORE INTO observer_deferred_outbox
        (session_db_id,event_key,message_type,message_json,enqueued_at,state) VALUES (?,?,?,?,?,'waiting')`)
        .run(sessionDbId, eventKey, message.type, JSON.stringify(message), enqueuedAt);
      if (!result.changes) return 0;
      const eventId = Number(result.lastInsertRowid);
      if (project !== undefined) this.db.prepare('UPDATE observer_deferred_outbox SET source_meta_json=? WHERE id=?')
        .run(JSON.stringify(freezeSource(eventId, sessionDbId, project, message, enqueuedAt)), eventId);
      return eventId;
    })();
    if (!id) return 0;
    this.arm(sessionDbId);
    return id;
  }

  flush(sessionDbId: number, source: string): number {
    if (this.stopped) return 0;
    // No await between the snapshot and state transition. Competing boundary
    // and timer callbacks see no waiting rows from this snapshot a second time.
    const rows = this.db.transaction(() => {
      const snapshot = this.db.prepare(`SELECT * FROM observer_deferred_outbox
        WHERE session_db_id=? AND state='waiting' ORDER BY id`).all(sessionDbId) as DeferredObserverRow[];
      this.db.prepare(`UPDATE observer_deferred_outbox SET state='released', reason=?
        WHERE session_db_id=? AND state='waiting'`).run(source, sessionDbId);
      return snapshot;
    })();
    this.cancel(sessionDbId);
    if (rows.length) this.release(rows, source);
    return rows.length;
  }

  markInflight(id: number): void {
    this.db.prepare(`UPDATE observer_deferred_outbox SET state='inflight' WHERE id=? AND state='released'`).run(id);
  }

  confirm(id: number): void {
    // Retain only the identity, so replayed toolUseIds cannot buy another turn.
    this.db.prepare(`UPDATE observer_deferred_outbox SET state='done', message_json='', reason=NULL WHERE id=?`).run(id);
  }

  park(id: number, reason: string): void {
    this.db.prepare(`UPDATE observer_deferred_outbox SET state='parked', reason=? WHERE id=? AND state!='done'`).run(reason, id);
  }

  getWaitingCount(sessionDbId?: number): number {
    const row = sessionDbId === undefined
      ? this.db.prepare(`SELECT count(*) AS count FROM observer_deferred_outbox WHERE state='waiting'`).get()
      : this.db.prepare(`SELECT count(*) AS count FROM observer_deferred_outbox WHERE state='waiting' AND session_db_id=?`).get(sessionDbId);
    return (row as { count: number }).count;
  }

  needsSummary(sessionDbId: number): boolean {
    const latest = this.db.prepare(`SELECT max(id) AS id FROM observer_deferred_outbox WHERE session_db_id=?
      AND message_type='observation' AND state!='parked'`).get(sessionDbId) as { id: number | null };
    if (latest.id === null) return false;
    // A timer may have already processed every observation, but the eventual
    // SessionEnd still owes them a summary. Conversely, a summary already
    // queued/sent for this boundary must not be queued for every repeated End.
    return !this.db.prepare(`SELECT 1 FROM observer_deferred_outbox WHERE session_db_id=?
      AND message_type='summarize' AND event_key LIKE ? LIMIT 1`).get(sessionDbId, `summary:${latest.id}:%`);
  }

  cancelSession(sessionDbId: number): void {
    this.cancel(sessionDbId);
    this.db.prepare(`UPDATE observer_deferred_outbox SET state='parked', reason='explicit_cancel'
      WHERE session_db_id=? AND state IN ('waiting','released','inflight')`).run(sessionDbId);
  }

  /** Shutdown preserves all rows and never triggers a model flush. */
  stop(): void {
    this.stopped = true;
    for (const id of this.timers.keys()) this.cancel(id);
  }

  private arm(sessionDbId: number): void {
    if (this.stopped || this.maxWaitMs === 0) return;
    // Recompute from MIN(enqueued_at), never from latest activity. Re-arming
    // also handles late spool arrivals that carry an older original timestamp.
    this.cancel(sessionDbId);
    const row = this.db.prepare(`SELECT min(enqueued_at) AS oldest FROM observer_deferred_outbox
      WHERE session_db_id=? AND state='waiting'`).get(sessionDbId) as { oldest: number | null };
    if (row.oldest === null) return;
    const timer = this.clock.setTimeout(() => {
      if (this.timers.get(sessionDbId) !== timer || this.stopped) return;
      this.timers.delete(sessionDbId);
      try { this.flush(sessionDbId, 'deferred-max-wait'); }
      catch (error) {
        logger.error('QUEUE', 'Deferred observer flush failed; durable rows retained', { sessionId: sessionDbId },
          error instanceof Error ? error : new Error(String(error)));
      }
    }, Math.max(0, row.oldest + this.maxWaitMs - this.clock.now()));
    timer.unref?.();
    this.timers.set(sessionDbId, timer);
  }

  private cancel(sessionDbId: number): void {
    const timer = this.timers.get(sessionDbId);
    if (timer !== undefined) this.clock.clearTimeout(timer);
    this.timers.delete(sessionDbId);
  }
}
