import { LIVE_OBSERVER_MAX_AGE_MS } from './observer-work-policy.js';

export const OBSERVER_TRIGGERS = ['per_observation', 'summary_or_session_end'] as const;
export type ObserverTrigger = typeof OBSERVER_TRIGGERS[number];
export const OBSERVER_TRIGGER_LABELS: Record<ObserverTrigger, string> = {
  per_observation: 'Process live events as they arrive',
  summary_or_session_end: 'Wait for a summary, session end, or the live wait limit',
};

export const DEFAULT_OBSERVER_MAX_WAIT_MS = 60_000;
// Dispatch before live inputs expire; this does not extend their lifetime.
export const MAX_OBSERVER_WAIT_MS = LIVE_OBSERVER_MAX_AGE_MS - 60_000;

export function observerWaitMs(settings: {
  CLAUDE_MEM_OBSERVER_TRIGGER?: string;
  CLAUDE_MEM_OBSERVER_MAX_WAIT_MS?: string;
}): number | undefined {
  if (settings.CLAUDE_MEM_OBSERVER_TRIGGER !== OBSERVER_TRIGGERS[1]) return undefined;
  const value = Number(settings.CLAUDE_MEM_OBSERVER_MAX_WAIT_MS);
  return Number.isSafeInteger(value) && value >= 1
    ? Math.min(value, MAX_OBSERVER_WAIT_MS)
    : DEFAULT_OBSERVER_MAX_WAIT_MS;
}
