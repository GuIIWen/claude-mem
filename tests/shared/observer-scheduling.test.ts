import { describe, expect, it } from 'bun:test';
import { DEFAULT_OBSERVER_MAX_WAIT_MS, MAX_OBSERVER_WAIT_MS, OBSERVER_TRIGGERS, observerWaitMs } from '../../src/shared/observer-scheduling.js';

describe('observer scheduling configuration', () => {
  it('keeps immediate processing as the default', () => {
    expect(observerWaitMs({})).toBeUndefined();
    expect(observerWaitMs({ CLAUDE_MEM_OBSERVER_TRIGGER: OBSERVER_TRIGGERS[0] })).toBeUndefined();
  });
  it('uses a configurable wait with a live-window-safe upper bound', () => {
    const base = { CLAUDE_MEM_OBSERVER_TRIGGER: OBSERVER_TRIGGERS[1] };
    expect(observerWaitMs(base)).toBe(DEFAULT_OBSERVER_MAX_WAIT_MS);
    expect(observerWaitMs({ ...base, CLAUDE_MEM_OBSERVER_MAX_WAIT_MS: '1500' })).toBe(1500);
    expect(observerWaitMs({ ...base, CLAUDE_MEM_OBSERVER_MAX_WAIT_MS: '1800000' })).toBe(MAX_OBSERVER_WAIT_MS);
    for (const value of ['0', '-1', 'bad', '1.5', 'Infinity']) {
      expect(observerWaitMs({ ...base, CLAUDE_MEM_OBSERVER_MAX_WAIT_MS: value })).toBe(DEFAULT_OBSERVER_MAX_WAIT_MS);
    }
  });
});
