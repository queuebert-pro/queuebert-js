# Releasing Queuebert JS

The three public packages are versioned together:

- `@queuebert/nest`
- `@queuebert/bullmq`
- `@queuebert/otel`

Nx creates one `v{version}` Git tag for the release. The publish workflow
requires that tag to point to a commit on the repository's default branch and
that every package manifest contains the tagged version.

## Distribution channels

The semver prerelease identifier selects the npm dist-tag automatically:

| Version        | npm dist-tag |
| -------------- | ------------ |
| `0.1.0`        | `latest`     |
| `0.1.0-next.0` | `next`       |
| `0.1.0-beta.0` | `beta`       |
| `0.1.0-rc.0`   | `rc`         |

Any non-numeric prerelease identifier can be used as a channel. A prerelease
identifier of `latest` is rejected.

## Prepare a stable release

Create and commit a version plan with the pull request:

```sh
npm run release:plan
```

After the changes and version plan reach `main`, prepare the release locally:

```sh
git pull --ff-only
npm ci
npm run release:check
npx nx release --skip-publish
git push origin main --follow-tags
```

Nx updates the package versions and changelogs, commits those changes, and
creates the version tag. Pushing the tag starts `publish.yml`.

For the existing initial `0.0.1` manifests, create the first tag directly after
the initial package commit:

```sh
git tag -a v0.0.1 -m "v0.0.1"
git push origin v0.0.1
```

Pull-request CI begins enforcing version plans after this initial release tag
exists.

## Prepare a prerelease

Start a prerelease using the desired semver bump and channel:

```sh
npx nx release prepatch --preid next --skip-publish
git push origin main --follow-tags
```

Use `preminor` or `premajor` when appropriate. Increment an existing channel
with:

```sh
npx nx release prerelease --preid next --skip-publish
git push origin main --follow-tags
```

Publishing a later stable semver version moves `latest`; it does not mutate
dist-tags on an existing prerelease.

## npm trusted publishing

The publish job uses npm trusted publishing through GitHub's OIDC provider.
Configure each package on npm with:

- Organization: `queuebert`
- Repository: `queuebert-js`
- Workflow filename: `publish.yml`
- Environment: `npm`
- Allowed action: `npm publish`

Protect the `npm` GitHub environment with required reviewers and protect
`v*` release tags.

Because trusted publishers are configured from an existing package's npm
settings, bootstrap the first publication with a granular `NPM_TOKEN` secret on
the `npm` environment. After all three trusted publishers are configured and a
trusted publish succeeds, remove that secret and revoke the bootstrap token.
Then set each package's publishing access to require two-factor authentication
and disallow token-based publishing.

The release job uses Node.js 24, npm 11, public package access, and provenance.
It reruns the full release gate immediately before publishing.

## Manual recovery

The workflow can be manually dispatched only while a `v`-prefixed tag is
selected. Branch-based manual runs are rejected.

Resolve and validate a tag locally without publishing:

```sh
npm run release:channel -- v0.0.1
npm run release:publish:dry-run -- --tag latest --first-release
```
