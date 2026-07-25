# Queuebert JS

Queuebert JS is the TypeScript package workspace for exposing BullMQ queue
operations, queue statistics, migration controls, and OpenTelemetry helpers to
Queuebert clients.

## Packages

| Package             | Purpose                                                                                                |
| ------------------- | ------------------------------------------------------------------------------------------------------ |
| `@queuebert/nest`   | NestJS module that exposes Queuebert queue APIs for existing BullMQ queues.                            |
| `@queuebert/bullmq` | BullMQ queue, worker, processor, and stats helpers for applications that want Queuebert-aware workers. |
| `@queuebert/otel`   | Framework-agnostic OpenTelemetry helpers plus the `@queuebert/otel/nest` NestJS module.                |

The packages are intentionally separate npm packages. Install only the pieces
your service needs.

```sh
npm install @queuebert/nest
npm install @queuebert/bullmq
npm install @queuebert/otel
```

## Package Layout

```txt
packages/
  nest/      # @queuebert/nest
  bullmq/    # @queuebert/bullmq
  otel/      # @queuebert/otel and @queuebert/otel/nest
```

`@queuebert/nest` no longer contains nested integration subpackages. Use the
top-level integration packages directly:

```ts
import { QueuebertModule } from '@queuebert/nest';
import { BaseQueueProcessor, QueuebertBullMQModule } from '@queuebert/bullmq';
import { QueuebertOTelModule } from '@queuebert/otel/nest';
```

## Development

Install dependencies from the workspace root:

```sh
npm install
```

Build packages in dependency order:

```sh
npx tsc -p packages/nest/tsconfig.lib.json
npx tsc -p packages/bullmq/tsconfig.lib.json
npx tsc -p packages/otel/tsconfig.lib.json
```

Run tests:

```sh
npx jest --config packages/nest/jest.config.cts --runInBand --no-watchman
npx jest --config packages/bullmq/jest.config.cts --runInBand --no-watchman
npx jest --config packages/otel/jest.config.cts --runInBand --no-watchman
```

With a local Redis server running, exercise the real BullMQ migration path:

```sh
npm run test:redis
```

Run lint before publishing:

```sh
npx nx run-many -t lint --all
```

Run the complete release gate with Redis available at
`QUEUEBERT_REDIS_URL` (defaults to `redis://127.0.0.1:6379`):

```sh
npm run release:check
```

## Test packages locally with yalc

Build all three packages and publish them to the local yalc store:

```sh
npm run yalc:publish
```

Then add them to a local consumer:

```sh
yalc add @queuebert/nest @queuebert/bullmq @queuebert/otel
```

After making library changes, rebuild and push the updated packages to every
linked consumer:

```sh
npm run yalc:push
```

The packages are published in dependency order: `@queuebert/nest`,
`@queuebert/bullmq`, then `@queuebert/otel`.

Run the automated yalc smoke test to publish into an isolated local store and
verify both ESM and CommonJS consumers:

```sh
npm run test:yalc
```

Check package contents before publishing:

```sh
npm pack --dry-run --workspace @queuebert/nest
npm pack --dry-run --workspace @queuebert/bullmq
npm pack --dry-run --workspace @queuebert/otel
```

## Publishing Notes

Each package cleans and rebuilds its own `dist` directory during `prepack`, then
publishes only `dist`, `README.md`, `LICENSE`, `CHANGELOG.md`, and
`package.json`. Publish from a clean, reviewed commit after CI and the production
dependency audit pass.
