import { afterEach, beforeEach, describe, expect, it, mock, spyOn } from 'bun:test';
import { SessionStore } from '../../src/services/sqlite/SessionStore.js';
import { SessionManager } from '../../src/services/worker/SessionManager.js';
import { ClaudeProvider } from '../../src/services/worker/ClaudeProvider.js';
import { ModeManager } from '../../src/services/domain/ModeManager.js';
import { processAgentResponse } from '../../src/services/worker/agents/ResponseProcessor.js';
import { handleGeneratorExit } from '../../src/services/worker/session/GeneratorExitHandler.js';
import { SettingsDefaultsManager } from '../../src/shared/SettingsDefaultsManager.js';
import { OBSERVER_TRIGGERS } from '../../src/shared/observer-scheduling.js';
import { snapshotResponseContext } from '../../src/services/worker/agents/ResponseProcessor.js';
import type { DatabaseManager } from '../../src/services/worker/DatabaseManager.js';

let store: SessionStore;
let db: DatabaseManager;
let manager: SessionManager;
let session: ReturnType<SessionManager['initializeSession']>;
let settings: ReturnType<typeof SettingsDefaultsManager.getAllDefaults>;
let settingsSpy: ReturnType<typeof spyOn>;
let modeSpy: ReturnType<typeof spyOn>;

beforeEach(() => {
  settings = { ...SettingsDefaultsManager.getAllDefaults(),
    CLAUDE_MEM_OBSERVER_TRIGGER: OBSERVER_TRIGGERS[1],
    CLAUDE_MEM_OBSERVER_MAX_WAIT_MS: '100',
    CLAUDE_MEM_CLAUDE_OBSERVATION_BATCH_SIZE: '3',
    CLAUDE_MEM_FOLDER_CLAUDEMD_ENABLED: 'false',
    CLAUDE_MEM_LINE_ANCHORS_ENABLED: 'false',
    CLAUDE_MEM_TELEGRAM_ENABLED: 'false',
    CLAUDE_MEM_NOTION_ENABLED: 'false',
    CLAUDE_MEM_GROK_BOT_AWARENESS_ENABLED: 'false',
    CLAUDE_MEM_GROK_BOT_INJECT_ENABLED: 'false',
  };
  settingsSpy = spyOn(SettingsDefaultsManager, 'loadFromFile').mockImplementation(() => settings);
  const mode = ModeManager.getInstance().loadMode('code');
  modeSpy = spyOn(ModeManager, 'getInstance').mockImplementation(() => ({ getActiveMode: () => mode } as ModeManager));
  store = new SessionStore(':memory:');
  db = { getSessionById: (id: number) => store.getSessionById(id), getSessionStore: () => store,
    getChromaSync: () => null, getCloudSync: () => null } as unknown as DatabaseManager;
  manager = new SessionManager(db);
  const id = store.createSDKSession('deferred-turn', 'fixture', 'Read the source');
  session = manager.initializeSession(id, undefined, 1);
  session.memorySessionId = 'deferred-memory';
  store.ensureMemorySessionIdRegistered(id, session.memorySessionId);
});

afterEach(() => {
  session.abortController.abort();
  manager.getMessageBuffer().dispose(session.sessionDbId);
  store.close();
  settingsSpy.mockRestore();
  modeSpy.mockRestore();
});

function enqueue(id: string) {
  return manager.queueObservation(session.sessionDbId, { tool_name: 'Read', tool_input: { file_path: `src/${id}.ts` },
    tool_response: `export const ${id} = 1;`, toolUseId: id, prompt_number: 1, cwd: '/fixture' });
}

describe('deferred Claude turn with the real live buffer and response storage', () => {
  it('sends one prompt after Stop and stores both the observation and summary', async () => {
    const starter = mock(() => {});
    manager.setGeneratorStarter(starter);
    for (const id of ['a', 'b', 'c']) enqueue(id);
    expect(manager.getMessageBuffer().getReadyCount(session.sessionDbId)).toBe(0);
    expect(starter).not.toHaveBeenCalled();
    manager.queueSummarize(session.sessionDbId, 'Read all three source files.', 1);
    await Promise.resolve();
    expect(starter).toHaveBeenCalledTimes(1);
    const context = { current: null };
    const provider = new ClaudeProvider(db, manager) as any;
    const requests = provider.createMessageGenerator(session, { lastCwd: undefined }, context,
      undefined, undefined, { mark: () => 0, waitForAnswer: async () => 'answered' });
    try {
      const request = await requests.next();
      expect(request.value.message.content).toContain('Both observation and summary root blocks are accepted');
      expect(manager.getClaimedMessages(session.sessionDbId)).toHaveLength(4);
      // A live settings edit must not reinterpret the in-flight response.
      settings.CLAUDE_MEM_OBSERVER_TRIGGER = OBSERVER_TRIGGERS[0];
      const response = '<observation><type>discovery</type><title>Read three source modules</title>'
        + '<facts><fact>Each source module exports a constant.</fact></facts><narrative>Reviewed the exports.</narrative>'
        + '<concepts><concept>how-it-works</concept></concepts><files_read><file>src/a.ts</file></files_read></observation>'
        + '<summary><request>Read the source</request><completed>Reviewed all three modules.</completed></summary>';
      const stored = await processAgentResponse(response, session, db, manager, undefined, 10, null,
        'SDK', undefined, undefined, context.current);
      expect(stored?.observationIds).toHaveLength(1);
      expect(stored?.summaryId).toBeGreaterThan(0);
      expect(store.getSummaryForSession('deferred-memory')?.completed).toBe('Reviewed all three modules.');
      expect(manager.getMessageBuffer().getPendingCount(session.sessionDbId)).toBe(0);
      // Re-entering the same tool IDs does not schedule another paid batch.
      expect(enqueue('a')).toBe(0);
      expect(starter).toHaveBeenCalledTimes(1);
    } finally {
      session.abortController.abort();
      await requests.return(undefined);
    }
  });

  it('keeps valid observations when the combined checkpoint has no progress content', async () => {
    enqueue('a');
    manager.queueSummarize(session.sessionDbId, 'done', 1);
    const messages = manager.getMessageIterator(session.sessionDbId);
    try {
      await messages.next();
      manager.claimNextSummary(session.sessionDbId, () => true);
      const response = '<observation><type>discovery</type><title>Source module reviewed</title>'
        + '<narrative>The module exports a constant.</narrative></observation>'
        + '<summary><request>Read the source</request><next_steps>Future task only.</next_steps></summary>';
      const stored = await processAgentResponse(response, session, db, manager, undefined, 10, null,
        'SDK', undefined, undefined, { ...snapshotResponseContext(session), allowMixed: true });
      expect(stored?.observationIds).toHaveLength(1);
      expect(stored?.summaryId).toBeNull();
      expect(manager.getTotalQueueDepth()).toBe(0);
    } finally {
      session.abortController.abort();
      await messages.return(undefined);
    }
  });

  it('cannot resume a 25-session quota/auth outage on a later boundary or timer', async () => {
    const starter = mock(() => {});
    manager.setGeneratorStarter(starter);
    for (let n = 0; n < 25; n++) {
      const id = store.createSDKSession(`outage-${n}`, 'fixture', 'live work');
      const current = manager.initializeSession(id, 'live work', 1);
      manager.queueObservation(id, { tool_name: 'Read', tool_input: {}, tool_response: {}, toolUseId: 'old' });
      await handleGeneratorExit(current, n % 2 ? 'quota:five_hour' : 'auth:observer_text', {
        sessionManager: manager, completionHandler: { finalizeSession: mock(async () => {}) } as any,
      });
      expect(manager.releaseDeferredObservations(id, 'session-end')).toBe(0);
    }
    await Bun.sleep(130);
    expect(starter).not.toHaveBeenCalled();
    expect(manager.getTotalQueueDepth()).toBe(0);
    expect(manager.getResumableSessionIds(true)).toEqual([]);
  });

  for (const reason of ['quota:five_hour', 'auth:observer_text']) {
    it(`${reason} cannot leave work for a later boundary or timer`, async () => {
      const starter = mock(() => {});
      manager.setGeneratorStarter(starter);
      enqueue('old');
      await handleGeneratorExit(session, reason, { sessionManager: manager,
        completionHandler: { finalizeSession: mock(async () => {}) } as any });
      expect(manager.releaseDeferredObservations(session.sessionDbId, 'session-end')).toBe(0);
      await Bun.sleep(130);
      expect(starter).not.toHaveBeenCalled();
      expect(manager.getTotalQueueDepth()).toBe(0);
      expect(enqueue('old')).toBe(0);
      expect(enqueue('fresh')).toBeGreaterThan(0);
      manager.queueSummarize(session.sessionDbId, 'new work', 1);
      await Promise.resolve();
      expect(starter).toHaveBeenCalledTimes(1);
    });
  }
});
