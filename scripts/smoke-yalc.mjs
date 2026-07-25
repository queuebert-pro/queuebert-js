import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const repositoryRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const smokeRoot = mkdtempSync(join(tmpdir(), 'queuebert-yalc-smoke-'));
const storeDirectory = join(smokeRoot, 'store');
const consumerDirectory = join(
  repositoryRoot,
  'tmp',
  `yalc-smoke-${Date.now()}`,
);

const esmSmoke = `
import { QueuebertModule } from '@queuebert/nest';
import { QueuebertBullMQModule } from '@queuebert/bullmq';
import { MetricsRegistry } from '@queuebert/otel';
import { QueuebertOTelModule } from '@queuebert/otel/nest';

if (![QueuebertModule, QueuebertBullMQModule, MetricsRegistry, QueuebertOTelModule].every(Boolean)) {
  throw new Error('An expected ESM export is missing');
}
`;

const commonJsSmoke = `
const { QueuebertModule } = require('@queuebert/nest');
const { QueuebertBullMQModule } = require('@queuebert/bullmq');
const { MetricsRegistry } = require('@queuebert/otel');
const { QueuebertOTelModule } = require('@queuebert/otel/nest');

if (![QueuebertModule, QueuebertBullMQModule, MetricsRegistry, QueuebertOTelModule].every(Boolean)) {
  throw new Error('An expected CommonJS export is missing');
}
`;

try {
  mkdirSync(consumerDirectory, { recursive: true });
  writeFileSync(
    join(consumerDirectory, 'package.json'),
    `${JSON.stringify({ name: 'queuebert-yalc-smoke', private: true }, null, 2)}\n`,
  );

  run(
    process.execPath,
    ['scripts/yalc-packages.mjs', 'publish', '--store-folder', storeDirectory],
    repositoryRoot,
  );

  const yalcBin = join(
    repositoryRoot,
    'node_modules',
    'yalc',
    'src',
    'yalc.js',
  );
  run(
    process.execPath,
    [
      yalcBin,
      'add',
      '@queuebert/nest',
      '@queuebert/bullmq',
      '@queuebert/otel',
      '--store-folder',
      storeDirectory,
    ],
    consumerDirectory,
  );

  run(
    process.execPath,
    ['--input-type=module', '--eval', esmSmoke],
    consumerDirectory,
  );
  run(process.execPath, ['--eval', commonJsSmoke], consumerDirectory);

  console.log('yalc ESM and CommonJS smoke tests passed');
} finally {
  rmSync(consumerDirectory, { recursive: true, force: true });
  rmSync(smokeRoot, { recursive: true, force: true });
}

function run(command, args, cwd) {
  const result = spawnSync(command, args, {
    cwd,
    stdio: 'inherit',
  });

  if (result.error) {
    throw result.error;
  }

  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}
