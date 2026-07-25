import { readFileSync, rmSync } from 'node:fs';
import { basename, dirname, resolve } from 'node:path';

const packageRoot = process.cwd();
const packageJsonPath = resolve(packageRoot, 'package.json');
const packageJson = JSON.parse(readFileSync(packageJsonPath, 'utf8'));

if (
  !packageJson.name?.startsWith('@queuebert/') ||
  basename(dirname(packageRoot)) !== 'packages'
) {
  throw new Error(
    `Refusing to clean unexpected package directory: ${packageRoot}`,
  );
}

rmSync(resolve(packageRoot, 'dist'), { recursive: true, force: true });
