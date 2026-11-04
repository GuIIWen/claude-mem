#!/usr/bin/env node
// Local observer patch build. Matches build-hooks.js's worker/SQLite options
// without rebuilding the viewer, installing runtime packages, or starting workers.
import { build } from 'esbuild';
import { createHash } from 'node:crypto';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';

const output = path.resolve(process.argv[2] || '.local-observer-build');
const { version } = JSON.parse(readFileSync('package.json', 'utf8'));
const filenameBanner = [
  'var __CM_FILENAME__ = typeof __filename !== "undefined" ? __filename : require("node:path").resolve(process.argv[1] || "");',
  'var __CM_DIRNAME__ = typeof __dirname !== "undefined" ? __dirname : require("node:path").dirname(__CM_FILENAME__);',
];
const common = {
  bundle: true, platform: 'node', target: 'node18', format: 'cjs',
  minify: true, sourcemap: true, sourcesContent: false, logLevel: 'error',
  define: {
    __DEFAULT_PACKAGE_VERSION__: JSON.stringify(version),
    __dirname: '__CM_DIRNAME__', __filename: '__CM_FILENAME__',
    'import.meta.url': '__IMPORT_META_URL__',
  },
};
const importMetaBanner = 'var __IMPORT_META_URL__ = require("node:url").pathToFileURL(__CM_FILENAME__).href;';
const files = [
  { source: 'src/services/worker-service.ts', target: 'scripts/worker-service.cjs', worker: true },
  { source: 'src/services/sqlite/SessionStore.ts', target: 'sqlite/SessionStore.js' },
  { source: 'src/services/sqlite/observations/files.ts', target: 'sqlite/observations/files.js' },
];
const checksums = [];
for (const item of files) {
  const outfile = path.join(output, item.target);
  mkdirSync(path.dirname(outfile), { recursive: true });
  await build({
    ...common, entryPoints: [item.source], outfile,
    ...(item.worker ? {
      alias: { 'zod/v4': 'zod' },
      external: ['sharp', 'bun:sqlite', 'bun:ffi', 'cohere-ai', 'ollama',
        '@chroma-core/default-embed', 'onnxruntime-node', 'better-auth',
        'better-auth/node', 'better-auth/plugins', '@better-auth/api-key', '@opentelemetry/api'],
      banner: { js: ['#!/usr/bin/env bun', ...filenameBanner, importMetaBanner].join('\n') },
    } : {
      external: ['bun:sqlite', 'zod', 'cohere-ai', 'ollama', '@chroma-core/default-embed',
        'onnxruntime-node', 'better-auth', 'better-auth/node', 'better-auth/plugins', '@better-auth/api-key'],
      banner: { js: [...filenameBanner, importMetaBanner].join('\n') },
    }),
  });
  const source = readFileSync(outfile, 'utf8');
  new vm.Script(source.replace(/^#!.*\n/, ''), { filename: outfile });
  for (const directory of ['node_modules', 'src']) {
    if (source.includes(path.join(process.cwd(), directory) + path.sep)) {
      throw new Error(`Absolute build path leaked into ${item.target}`);
    }
  }
  if (item.worker && /require\(\s*["']zod(?:\/[^"']*)?["']\s*\)/.test(source)) {
    throw new Error('Worker must bundle Zod for Codex cache installs.');
  }
  for (const filename of [outfile, `${outfile}.map`]) {
    const bytes = readFileSync(filename);
    checksums.push({ file: path.relative(output, filename).replaceAll('\\', '/'),
      bytes: bytes.length, sha256: createHash('sha256').update(bytes).digest('hex') });
  }
  console.log(`Built and syntax-checked ${item.target}`);
}
const worker = readFileSync(path.join(output, files[0].target), 'utf8');
for (const marker of ['observer_deferred_outbox', 'summary_or_session_end', 'CLAUDE_MEM_OBSERVER_MAX_WAIT_MS']) {
  if (!worker.includes(marker)) throw new Error(`Missing deferred observer code: ${marker}`);
}
writeFileSync(path.join(output, 'manifest.json'), JSON.stringify({ version, files: checksums }, null, 2) + '\n');
console.log(`PASS: ${checksums.length} artifacts, manifest at ${output}`);
