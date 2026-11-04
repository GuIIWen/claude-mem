---
name: claude-mem-observer-ops
description: Inspect claude-mem observer health, batching, and safely recover parked history on explicit request.
---

# Claude Mem Observer Ops

Use this skill when operating the local claude-mem observer, diagnosing deferred batches, or recovering parked historical events after the user explicitly asks to process them.

The worker is a separate local process. Check `GET http://127.0.0.1:37777/api/health` before claiming it is running, and use the returned `workerPath` to identify the actual build. The patched worker must contain the deferred observer implementation (`observer_deferred_outbox`, `configureDeferredObserver`, and `summary_or_session_end`). A marketplace `plugin/scripts/worker-service.cjs` can be stale; prefer the matching installed cache build when the health response points there.

The normal runtime uses the Claude provider through the configured gateway, Haiku 5.5, `CLAUDE_MEM_OBSERVER_TRIGGER=summary_or_session_end`, a 30-minute max wait, and observation batch size 16. A batch size is an upper bound. It does not force the worker to wait for 16 events, and project/session boundaries remain hard isolation boundaries.

Always inspect SQLite before and after a recovery. In `observer_deferred_outbox`, `waiting` and `released` are live work, `inflight` is an uncertain send, `parked` is intentionally quarantined, and `done` is completed. `observer_batch_runs` is an audit ledger: historical `sent` or `attribution_error` rows can remain after all outbox rows are done.

Never replay parked rows automatically. When the user explicitly requests historical processing:

1. Verify the worker is healthy, then stop it before changing the database.
2. Verify the parked count and that every selected row has a non-empty `message_json`.
3. Create a SQLite backup and use [`scripts/replay_parked_outbox.py`](scripts/replay_parked_outbox.py) with an expected count and `--confirm`. The script changes only `parked` rows to `released` and preserves the prior reason in the new reason value.
4. Start the patched worker build, then monitor `/api/health`, outbox states, batch ledger rows, and worker logs until no selected rows remain outside `done` or a clearly recorded failure remains.
5. If a model response is quarantined for attribution, do a second explicitly authorized, separately audited replay. Do not blindly resend an uncertain paid send without that authorization.

Keep API keys, gateway URLs containing credentials, and raw message bodies out of logs, reports, and skill files. Read [`references/recovery.md`](references/recovery.md) for the state model, verification queries, and Windows start/stop examples.
