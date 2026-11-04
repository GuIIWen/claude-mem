// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'bun:test';
import { SettingsRoutes } from '../../src/services/worker/http/routes/SettingsRoutes.js';
import type { SettingsManager } from '../../src/services/worker/SettingsManager.js';

type SettingsValidator = {
  validateSettings(settings: Record<string, string>): { valid: boolean; error?: string };
};

describe('SettingsRoutes OpenRouter base URL validation', () => {
  it('rejects non-HTTP(S) custom OpenRouter base URLs', () => {
    const routes = new SettingsRoutes({} as SettingsManager) as unknown as SettingsValidator;

    expect(routes.validateSettings({
      CLAUDE_MEM_OPENROUTER_BASE_URL: 'ftp://example.com/v1',
    })).toEqual({
      valid: false,
      error: 'CLAUDE_MEM_OPENROUTER_BASE_URL must be an HTTP(S) URL',
    });
  });
});

describe('SettingsRoutes openai-compatible validation', () => {
  const routes = () => new SettingsRoutes({} as SettingsManager) as unknown as SettingsValidator;

  it('rejects a non-HTTP(S) base URL', () => {
    expect(routes().validateSettings({ CLAUDE_MEM_OPENAI_COMPAT_BASE_URL: 'file:///etc/passwd' })).toEqual({
      valid: false,
      error: 'CLAUDE_MEM_OPENAI_COMPAT_BASE_URL must be an HTTP(S) URL',
    });
    expect(routes().validateSettings({ CLAUDE_MEM_OPENAI_COMPAT_BASE_URL: 'http://localhost:11434/v1' }).valid).toBe(true);
  });

  it('never rejects a whole save over an unknown preset id (the worker reads it as custom)', () => {
    expect(routes().validateSettings({ CLAUDE_MEM_OPENAI_COMPAT_PRESET: 'nvidia-nimm' }).valid).toBe(true);
  });
});

describe('SettingsRoutes deferred observer validation', () => {
  const routes = () => new SettingsRoutes({} as SettingsManager) as unknown as SettingsValidator;

  it('validates the opt-in Claude v2 switch and bounded total input budget', () => {
    for (const enabled of ['true','false']) expect(routes().validateSettings({CLAUDE_MEM_CLAUDE_BATCH_V2_ENABLED:enabled}).valid).toBe(true);
    expect(routes().validateSettings({CLAUDE_MEM_CLAUDE_BATCH_V2_ENABLED:'yes'}).valid).toBe(false);
    for (const value of ['12000','96000']) expect(routes().validateSettings({CLAUDE_MEM_CLAUDE_BATCH_V2_MAX_CHARS:value}).valid).toBe(true);
    for (const value of ['0','11999','96001','NaN','1.5']) expect(routes().validateSettings({CLAUDE_MEM_CLAUDE_BATCH_V2_MAX_CHARS:value}).valid).toBe(false);
  });

  it('accepts both trigger modes and an infinite or thirty-minute wait', () => {
    for (const trigger of ['immediate', 'summary_or_session_end']) {
      for (const wait of ['0', '1800000']) {
        expect(routes().validateSettings({ CLAUDE_MEM_OBSERVER_TRIGGER: trigger, CLAUDE_MEM_OBSERVER_MAX_WAIT_MS: wait }).valid).toBe(true);
      }
    }
  });

  it('rejects invalid trigger, timeout and filtering settings before persisting', () => {
    expect(routes().validateSettings({ CLAUDE_MEM_OBSERVER_TRIGGER: 'on_every_16' }).valid).toBe(false);
    for (const value of ['-1', '1.5', 'Infinity', '2147483648', '']) {
      expect(routes().validateSettings({ CLAUDE_MEM_OBSERVER_MAX_WAIT_MS: value }).valid).toBe(false);
    }
    expect(routes().validateSettings({ CLAUDE_MEM_OBSERVER_SKIP_INTERNAL_TOOLS: 'maybe' }).valid).toBe(false);
    expect(routes().validateSettings({ CLAUDE_MEM_OBSERVER_SKIP_INTERNAL_TOOLS: 'true' }).valid).toBe(true);
  });
});
