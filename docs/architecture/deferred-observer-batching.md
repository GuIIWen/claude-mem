# Deferred live observer batching

This opt-in scheduling mode uses the existing `SessionMessageBuffer`. It does
not introduce a disk inference queue or a restart recovery sweep. The observer
[lifecycle contract](observer-lifecycle.md) still applies: quota/auth discards
pending inference work, while source transcripts and already-saved memories stay.

Merge these keys into the local `~/.claude-mem/settings.json` (inside `env` if
the document uses that shape), or set the corresponding environment variables:

```json
{
  "CLAUDE_MEM_OBSERVER_TRIGGER": "summary_or_session_end",
  "CLAUDE_MEM_OBSERVER_MAX_WAIT_MS": "60000",
  "CLAUDE_MEM_CLAUDE_OBSERVATION_BATCH_SIZE": "16"
}
```

The batch size is configurable from 1 to 32; 16 is only the default. Saved
explicit values are preserved. Each Claude prompt reads the current value;
there is no hard-coded 16-event slicing path. Character limits can make a
batch smaller. Different prompts, agents, working directories and sessions
are never combined. This branch does not deploy itself or modify live settings.

## Dispatch boundaries

Tool events are held in the existing RAM buffer. A Stop summary or SessionEnd
releases this session's current held events, including a final short batch.
An existing generator cannot claim later held events while waiting for its
next prompt. A summary from the same parent turn can join the last Claude
observation prompt if the prompt fits and the summary uses the same model;
otherwise the ordinary separate summary request remains.

The fallback timer starts with the oldest held event. Its default is one minute
and its maximum is four minutes: the lifecycle contract expires unsent live
inputs after five minutes. A legacy 30-minute wait is therefore clamped rather
than extending their lifetime. The effective wait, release reason, observation
count, configured batch limit, summary inclusion and payload size are logged.

## Failure boundaries

Timers hold no payloads and belong to their buffer/session. Discard, teardown
and expiry cancel them. Quota/auth cannot turn a later timer, login, or summary
into old paid work; duplicate tool receipts remain effective. A worker restart
does not resurrect the RAM buffer or the historical parked database rows.
This deliberately revises the earlier durable/30-minute sketch to match the
current upstream lifecycle rather than reintroducing the old replay backlog.

The combined output protocol is frozen in the response context at send time,
so changing settings while a request is in flight does not drop its summary.
Only an explicitly combined request enables sibling observation/summary parsing.
Both results go through the existing transactional storage and acknowledgement
path. A content-poor summary does not throw away valid observation records.

## Behavioral evidence

`deferred-observer-buffer.test.ts` checks FIFO gating, no reset of the oldest
deadline, a running drain, session isolation, capacity pressure, expiry and
quota/auth discard. `deferred-observer-turn.test.ts` drives the real buffer,
Claude message generator and in-memory SQLite storage. It checks a three-tool
turn creates one combined prompt, stores both results and settles every claim.
The batching tests change the limit to 1, 3, 4 and 8, check the default 16 and
a short tail, and preserve context and character barriers. Existing no-backlog,
paid-send, parser and capture-disable tests remain required.

No assertion here measures real Haiku token savings: protocol tests use
synthetic model responses and make no paid model requests.
