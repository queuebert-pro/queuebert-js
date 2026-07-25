import assert from 'node:assert/strict';
import { mkdirSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, describe, it } from 'node:test';
import { mkdtempSync, rmSync } from 'node:fs';

import {
  assertPackageVersions,
  resolveReleaseChannel,
} from './resolve-release-channel.mjs';

const fixtureRoot = mkdtempSync(join(tmpdir(), 'queuebert-release-channel-'));

after(() => {
  rmSync(fixtureRoot, { recursive: true, force: true });
});

describe('resolveReleaseChannel', () => {
  it('uses latest for stable versions', () => {
    assert.deepEqual(resolveReleaseChannel('v1.2.3'), {
      version: '1.2.3',
      distTag: 'latest',
      firstRelease: false,
    });
  });

  it('uses the prerelease identifier for next and other channels', () => {
    assert.equal(resolveReleaseChannel('v1.2.3-next.4').distTag, 'next');
    assert.equal(resolveReleaseChannel('v1.2.3-beta.2').distTag, 'beta');
    assert.equal(resolveReleaseChannel('v1.2.3-rc.1').distTag, 'rc');
  });

  it('recognizes the initial package release', () => {
    assert.equal(resolveReleaseChannel('v0.0.1').firstRelease, true);
  });

  it('rejects malformed and unsafe release tags', () => {
    assert.throws(() => resolveReleaseChannel('1.2.3'), /must start with "v"/);
    assert.throws(() => resolveReleaseChannel('v1.2'), /not valid semver/);
    assert.throws(
      () => resolveReleaseChannel('v1.2.3-123.0'),
      /cannot be used as an npm dist-tag/,
    );
    assert.throws(
      () => resolveReleaseChannel('v1.2.3-latest.0'),
      /cannot be published to the latest channel/,
    );
  });
});

describe('assertPackageVersions', () => {
  it('accepts packages matching the release version', () => {
    writePackageFixtures('1.2.3');
    assert.doesNotThrow(() => assertPackageVersions('1.2.3', fixtureRoot));
  });

  it('rejects packages that do not match the release tag', () => {
    writePackageFixtures('1.2.4');
    assert.throws(
      () => assertPackageVersions('1.2.3', fixtureRoot),
      /does not match package versions/,
    );
  });
});

function writePackageFixtures(version) {
  for (const packageName of ['nest', 'bullmq', 'otel']) {
    const packageDirectory = join(fixtureRoot, 'packages', packageName);
    mkdirSync(packageDirectory, { recursive: true });
    writeFileSync(
      join(packageDirectory, 'package.json'),
      JSON.stringify({ name: `@queuebert/${packageName}`, version }),
    );
  }
}
