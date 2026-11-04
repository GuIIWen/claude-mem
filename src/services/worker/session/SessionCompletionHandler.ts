
import { SessionManager } from '../SessionManager.js';
import { SessionEventBroadcaster } from '../events/SessionEventBroadcaster.js';
import { DatabaseManager } from '../DatabaseManager.js';
import { logger } from '../../../utils/logger.js';

export class SessionCompletionHandler {
  constructor(
    private sessionManager: SessionManager,
    private eventBroadcaster: SessionEventBroadcaster,
    private dbManager: DatabaseManager
  ) {}

  async finalizeSession(sessionDbId: number): Promise<void> {
    const sessionStore = this.dbManager.getSessionStore();

    const row = sessionStore.getSessionById(sessionDbId);
    if (!row) {
      logger.debug('SESSION', 'finalizeSession: session not found, skipping', { sessionId: sessionDbId });
      return;
    }
    if (row.status === 'completed') {
      logger.debug('SESSION', 'finalizeSession: already completed, skipping', { sessionId: sessionDbId });
      return;
    }

    sessionStore.markSessionCompleted(sessionDbId);

    // The in-RAM message buffer is dropped when the session is removed
    // (SessionManager.removeSessionImmediate/deleteSession → buffer.dispose),
    // so there is nothing to clear here. In opt-in deferred mode SessionManager
    // retains waiting outbox rows and records released-row dispositions itself;
    // finalizing a generation must not erase a later waiting boundary.

    this.eventBroadcaster.broadcastSessionCompleted(sessionDbId);

    logger.info('SESSION', 'Session finalized', { sessionId: sessionDbId });
  }
}
