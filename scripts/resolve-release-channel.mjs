import { appendFileSync, readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const repositoryRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const packageDirectories = ['nest', 'bullmq', 'otel'];
const semverPattern =
  /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/;
const distTagPattern = /^[A-Za-z][0-9A-Za-z._-]*$/;

export function resolveReleaseChannel(tagName) {
  if (!tagName?.startsWith('v')) {
    throw new Error(
      `Release tag must start with "v": ${tagName || '(missing)'}`,
    );
  }

  const version = tagName.slice(1);
  const match = semverPattern.exec(version);
  if (!match) {
    throw new Error(`Release tag is not valid semver: ${tagName}`);
  }

  const prerelease = match[4];
  const distTag = prerelease ? prerelease.split('.')[0] : 'latest';

  if (!distTagPattern.test(distTag)) {
    throw new Error(
      `Prerelease identifier cannot be used as an npm dist-tag: ${distTag}`,
    );
  }

  if (prerelease && distTag === 'latest') {
    throw new Error(
      'Prerelease versions cannot be published to the latest channel',
    );
  }

  return {
    version,
    distTag,
    firstRelease: version === '0.0.1',
  };
}

export function assertPackageVersions(version, root = repositoryRoot) {
  const mismatches = [];

  for (const packageDirectory of packageDirectories) {
    const packagePath = join(
      root,
      'packages',
      packageDirectory,
      'package.json',
    );
    const packageJson = JSON.parse(readFileSync(packagePath, 'utf8'));

    if (packageJson.version !== version) {
      mismatches.push(`${packageJson.name}: ${packageJson.version}`);
    }
  }

  if (mismatches.length > 0) {
    throw new Error(
      `Release tag v${version} does not match package versions:\n${mismatches.join('\n')}`,
    );
  }
}

function writeOutput(name, value) {
  if (process.env.GITHUB_OUTPUT) {
    appendFileSync(process.env.GITHUB_OUTPUT, `${name}=${value}\n`);
  }
}

function main() {
  const release = resolveReleaseChannel(
    process.argv[2] || process.env.GITHUB_REF_NAME,
  );
  assertPackageVersions(release.version);

  writeOutput('version', release.version);
  writeOutput('dist_tag', release.distTag);
  writeOutput('first_release', String(release.firstRelease));

  console.log(
    `Validated v${release.version}; npm dist-tag: ${release.distTag}; first release: ${release.firstRelease}`,
  );
}

if (
  process.argv[1] &&
  resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  main();
}
