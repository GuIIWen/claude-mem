import { afterEach, describe, expect, it, jest, mock } from 'bun:test';
import { SessionMessageBuffer } from '../../../src/services/worker/SessionMessageBuffer.js';
import { LIVE_OBSERVER_MAX_AGE_MS, LIVE_OBSERVER_MAX_SESSION_MESSAGES } from '../../../src/shared/observer-work-policy.js';

const event = (id: string) => ({ type: 'observation' as const, tool_name: 'Read', toolUseId: id });
const buffers: SessionMessageBuffer[] = [];
function setup(waitMs = 100) {
  const ready = mock(() => {});
  const buffer = new SessionMessageBuffer(undefined, { observationWaitMs: () => waitMs, onReady: ready });
  buffers.push(buffer);
  return { buffer, ready };
}
afterEach(() => {
  for (const buffer of buffers.splice(0)) for (const id of [1, 2]) buffer.dispose(id);
  jest.useRealTimers();
  jest.restoreAllMocks();
});

describe('live deferred observer buffer', () => {
  it('holds a short turn until its summary releases observations in FIFO order', () => {
    const { buffer, ready } = setup();
    const ids = ['a', 'b', 'c'].map(id => buffer.enqueue(1, event(id)));
    expect(buffer.getPendingCount(1)).toBe(3);
    expect(buffer.getReadyCount(1)).toBe(0);
    expect(buffer.claimNextMatching(1, () => true)).toBeNull();
    expect(ready).not.toHaveBeenCalled();
    const summaryId = buffer.enqueue(1, { type: 'summarize', last_assistant_message: 'done' });
    expect(buffer.getReadyCount(1)).toBe(4);
    expect(ready).toHaveBeenCalledTimes(1);
    expect([0, 1, 2, 3].map(() => buffer.claimNextMatching(1, () => true)?._persistentId))
      .toEqual([...ids, summaryId]);
  });

  it('starts the wait at the oldest event, without resetting for newer intake', () => {
    jest.useFakeTimers();
    const { buffer, ready } = setup();
    buffer.enqueue(1, event('a'));
    jest.advanceTimersByTime(80);
    buffer.enqueue(1, event('b'));
    jest.advanceTimersByTime(19);
    expect(ready).not.toHaveBeenCalled();
    jest.advanceTimersByTime(1);
    expect(ready).toHaveBeenCalledWith(1, 'observer-max-wait');
    expect(buffer.getReadyCount(1)).toBe(2);
  });

  it('does not let a running drain claim a later turn before its boundary', async () => {
    const { buffer } = setup();
    const controller = new AbortController();
    const iterator = buffer.drain({ sessionDbId: 1, signal: controller.signal });
    const id = buffer.enqueue(1, event('first'));
    buffer.releaseDeferred(1, 'session-end');
    expect((await iterator.next()).value?._persistentId).toBe(id);
    buffer.confirm(id);
    buffer.enqueue(1, event('next-turn'));
    expect(buffer.getReadyCount(1)).toBe(0);
    expect(buffer.claimNextMatching(1, () => true)).toBeNull();
    buffer.enqueue(1, { type: 'summarize' });
    expect((await iterator.next()).value?.toolUseId).toBe('next-turn');
    controller.abort();
    await iterator.return();
  });

  it('never releases another session at a summary boundary', () => {
    const { buffer } = setup();
    buffer.enqueue(1, event('one'));
    buffer.enqueue(2, event('two'));
    buffer.enqueue(1, { type: 'summarize' });
    expect(buffer.getReadyCount(1)).toBe(2);
    expect(buffer.getReadyCount(2)).toBe(0);
  });

  it('cancels the timer on a quota/auth discard and retains duplicate receipts', () => {
    jest.useFakeTimers();
    const { buffer, ready } = setup();
    buffer.enqueue(1, event('discarded'));
    expect(buffer.discardKeepingDedup(1)).toBe(1);
    jest.advanceTimersByTime(LIVE_OBSERVER_MAX_AGE_MS * 10);
    expect(ready).not.toHaveBeenCalled();
    expect(buffer.getPendingCount(1)).toBe(0);
    expect(buffer.enqueue(1, event('discarded'))).toBe(0);
    // Fresh work remains usable, without reviving the discarded batch.
    buffer.enqueue(1, event('fresh'));
    buffer.enqueue(1, { type: 'summarize' });
    expect(buffer.getPendingCount(1)).toBe(2);
    expect(ready).toHaveBeenCalledTimes(1);
  });

  it('does not turn a delayed callback into replay after live inputs expire', () => {
    const { buffer, ready } = setup();
    buffer.enqueue(1, event('too-old'));
    jest.spyOn(Date, 'now').mockReturnValue(Date.now() + LIVE_OBSERVER_MAX_AGE_MS + 1);
    expect(buffer.releaseDeferred(1, 'observer-max-wait')).toBe(0);
    expect(ready).not.toHaveBeenCalled();
  });

  it('releases a full buffer even if its summary is rejected by the capacity limit', () => {
    const { buffer, ready } = setup();
    for (let i = 0; i < LIVE_OBSERVER_MAX_SESSION_MESSAGES; i++) buffer.enqueue(1, event(String(i)));
    expect(buffer.enqueue(1, { type: 'summarize' })).toBe(0);
    expect(buffer.getReadyCount(1)).toBe(LIVE_OBSERVER_MAX_SESSION_MESSAGES);
    expect(ready).toHaveBeenCalledTimes(1);
  });

  it('deduplicates a repeated Stop, but accepts a checkpoint after new source events', () => {
    const { buffer } = setup();
    buffer.enqueue(1, event('a'));
    const checkpoint = { type: 'summarize' as const, prompt_number: 1, last_assistant_message: 'done' };
    const summary = buffer.enqueue(1, checkpoint);
    expect(summary).toBeGreaterThan(0);
    expect(buffer.enqueue(1, checkpoint)).toBe(0);
    buffer.confirm(summary);
    expect(buffer.enqueue(1, checkpoint)).toBe(0);
    buffer.enqueue(1, event('b'));
    expect(buffer.enqueue(1, checkpoint)).toBeGreaterThan(0);
  });
});
