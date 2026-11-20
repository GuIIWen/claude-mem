import { describe, expect, it } from 'bun:test';
import { ModeManager } from '../../src/services/domain/ModeManager.js';
import { parseAgentXml } from '../../src/sdk/parser.js';

ModeManager.getInstance().loadMode('code');
const observation = '<observation><type>discovery</type><title>Reviewed configuration</title>'
  + '<narrative>The configuration supports batching.</narrative></observation>';
const summary = '<summary><completed>Reviewed the batch configuration.</completed></summary>';

describe('explicit combined observer replies', () => {
  it('stores sibling observation and summary roots in either order only when requested', () => {
    for (const text of [observation + summary, summary + observation]) {
      const result = parseAgentXml(text, undefined, { allowMixed: true });
      expect(result.valid).toBe(true);
      if (!result.valid) throw new Error('expected valid combined reply');
      expect(result.observations).toHaveLength(1);
      expect(result.summary?.completed).toBe('Reviewed the batch configuration.');
    }
    const legacy = parseAgentXml(observation + summary);
    expect(legacy.valid && legacy.summary).toBeNull();
  });
  it('does not store nested examples as extra root records', () => {
    const nested = '<observation><type>discovery</type><title>XML example</title><narrative>'
      + '<summary><notes>This is an example in a narrative.</notes></summary>'
      + '</narrative></observation>';
    const result = parseAgentXml(nested, undefined, { allowMixed: true });
    expect(result.valid && result.observations).toHaveLength(1);
    expect(result.valid && result.summary).toBeNull();
  });
  it('retains observations when a skip precedes a genuine checkpoint', () => {
    const result = parseAgentXml('<skip_summary reason="one noisy event" />' + observation + summary,
      undefined, { allowMixed: true });
    expect(result.valid && result.observations).toHaveLength(1);
    expect(result.valid && result.summary?.completed).toBe('Reviewed the batch configuration.');
    expect(parseAgentXml('<skip_summary reason="nothing durable" />', undefined, { allowMixed: true }).valid).toBe(true);
  });
});
