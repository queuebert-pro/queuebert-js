#!/usr/bin/env node

import { spawnSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const require = createRequire(import.meta.url);
const repositoryRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const yalcBin = require.resolve('yalc/src/yalc.js');
const command = process.argv[2];
const extraArgs = process.argv.slice(3);

if (command !== 'publish' && command !== 'push') {
  console.error(
    'Usage: node scripts/yalc-packages.mjs <publish|push> [yalc options]',
  );
  process.exit(1);
}

// Publish in dependency order so yalc resolves Queuebert workspace peers against
// packages that are already present in the local store.
const packages = [
  { name: '@queuebert/nest', directory: 'packages/nest' },
  { name: '@queuebert/bullmq', directory: 'packages/bullmq' },
  { name: '@queuebert/otel', directory: 'packages/otel' },
];

for (const packageConfig of packages) {
  assertPackageReady(packageConfig);
}

for (const packageConfig of packages) {
  console.log(
    `${command === 'push' ? 'Pushing' : 'Publishing'} ${packageConfig.name} with yalc`,
  );

  const result = spawnSync(process.execPath, [yalcBin, command, ...extraArgs], {
    cwd: join(repositoryRoot, packageConfig.directory),
    stdio: 'inherit',
  });

  if (result.error) {
    throw result.error;
  }

  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}

console.log(
  command === 'push'
    ? 'Pushed all Queuebert packages to linked yalc consumers.'
    : 'Published all Queuebert packages to the local yalc store.',
);

function assertPackageReady(packageConfig) {
  const packageRoot = join(repositoryRoot, packageConfig.directory);
  const manifestPath = join(packageRoot, 'package.json');
  const entryPath = join(packageRoot, 'dist', 'index.js');

  if (!existsSync(manifestPath)) {
    throw new Error(`Missing package manifest: ${manifestPath}`);
  }

  const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
  if (manifest.name !== packageConfig.name) {
    throw new Error(
      `Expected ${packageRoot} to contain ${packageConfig.name}, found ${manifest.name}`,
    );
  }

  if (manifest.private === true) {
    throw new Error(`${packageConfig.name} is marked private.`);
  }

  if (!existsSync(entryPath)) {
    throw new Error(
      `Missing ${entryPath}. Run the root yalc command so package builds complete first.`,
    );
  }
}
