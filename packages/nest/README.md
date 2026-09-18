# @queuebert/nest

NestJS module for exposing Queuebert-compatible queue monitoring and queue
management APIs for BullMQ queues.

Use this package when your NestJS app already owns BullMQ queues and you want a
Queuebert client to read stats, inspect queue health, pause/resume queues, clean
old jobs, drain waiting jobs, or run queue/cache migrations.

## Installation

```sh
npm install @queuebert/nest @nestjs/bullmq @nestjs/common @nestjs/core bullmq
```

Your NestJS application should already install and initialize Nest's standard
`reflect-metadata` and `rxjs` runtime dependencies.

Add `ioredis` when you use explicit Redis connection objects or multi-Redis
migration configuration:

```sh
npm install ioredis
```

## Basic Usage

```ts
import { BullModule } from '@nestjs/bullmq';
import { Module } from '@nestjs/common';
import { QueuebertModule } from '@queuebert/nest';

import { EmailProcessor } from './email.processor';

@Module({
  imports: [
    BullModule.forRoot({
      connection: {
        host: 'localhost',
        port: 6379,
      },
    }),
    BullModule.registerQueue({
      name: 'emails',
    }),
    QueuebertModule.forRoot({
      path: 'admin/queue',
      queues: [
        {
          name: 'emails',
          processor: EmailProcessor,
        },
      ],
    }),
  ],
  providers: [EmailProcessor],
})
export class AppModule {}
```

The default API path above exposes endpoints under `/admin/queue`.

## Processor Stats

Processors can expose Queuebert metrics by implementing `QueuebertProcessor`.
The `@queuebert/bullmq` package includes `BaseQueueProcessor`, which implements
this interface for common NestJS BullMQ processors.

```ts
import type {
  QueuebertProcessor,
  QueuebertProcessorStats,
} from '@queuebert/nest';

export class EmailProcessor implements QueuebertProcessor {
  getProcessorStats(): QueuebertProcessorStats {
    return {
      duration: {
        avgMs: 120,
        minMs: 30,
        maxMs: 500,
        p50Ms: 100,
        p95Ms: 280,
        p99Ms: 450,
        recentAvgMs: 110,
        sampleCount: 42,
      },
      jobs: {
        processed: 1000,
        completed: 990,
        failed: 10,
        failureRate: 0.01,
        successRate: 0.99,
        lastJobTime: new Date().toISOString(),
      },
      throughput: {
        jobsPerMinute: 24,
        windowStartTime: new Date(Date.now() - 60000).toISOString(),
        jobsInWindow: 24,
      },
    };
  }
}
```

## Endpoint Controls

`stats` is always enabled. By default, only read-only `metrics` is enabled.
Queue mutation endpoints must be opted in with the `endpoints` option:

```ts
QueuebertModule.forRoot({
  queues: [{ name: 'emails', processor: EmailProcessor }],
  endpoints: ['metrics', 'pause', 'resume'],
});
```

Supported optional endpoints are:

- `metrics`
- `pause`
- `resume`
- `clean`
- `drain`
- `jobs`
- `migrations`

`jobs` is read-only but is still off by default: unlike `metrics`, which
exposes aggregates, it returns per-job identifiers, failure reasons, and stack
traces.

## Job Inspection

Enable the `jobs` endpoint to read individual jobs, which is the fastest way to
work out why a job failed:

```ts
QueuebertModule.forRoot({
  queues: [{ name: 'emails', processor: EmailProcessor }],
  endpoints: ['metrics', 'jobs'],
});
```

| Route                         | Returns                                           |
| ----------------------------- | ------------------------------------------------- |
| `GET /:queueName/jobs`        | A page of jobs plus the total for that state      |
| `GET /:queueName/jobs/:jobId` | One job, with its state resolved via `getState()` |

Query parameters on the list route:

- `state` — one of `waiting`, `waiting-children`, `active`, `delayed`,
  `prioritized`, `completed`, `failed`. Defaults to `failed`.
- `start` / `end` — zero-based inclusive window. Defaults to the first 50 jobs
  and may span at most 100.
- `jobType` — narrow to jobs with a given name.

Two behaviours worth knowing:

- **Ordering is BullMQ's and varies by state.** `completed` and `failed` come
  back newest-first; every other state comes back oldest-first.
- **`jobType` filters within the page, not across the state.** The filter is
  applied after the `start`/`end` window is read from Redis, so `jobs` can be
  shorter than the requested page without that page being the last one. The
  response sets `jobTypeFilter` whenever this applies, and `total` always
  counts the whole state.

## Auth

Queuebert does not authenticate requests for you. Never expose these routes to
an untrusted network without application-level authentication and
authorization. This is especially important for `pause`, `resume`, `clean`,
`drain`, and `migrations`, which can change or delete production queue data.

Apply Nest guards or class decorators to every generated Queuebert endpoint:

```ts
QueuebertModule.forRoot({
  queues: [{ name: 'emails', processor: EmailProcessor }],
  auth: {
    guards: [AdminGuard],
    decorators: [Roles('admin')],
  },
});
```

For async registration, route metadata must be supplied outside the async
factory because NestJS controller paths are resolved before dependency injection:

```ts
QueuebertModule.forRootAsync({
  path: 'admin/queue',
  auth: {
    guards: [AdminGuard],
  },
  imports: [ConfigModule],
  useFactory: (config: ConfigService) => ({
    queues: buildQueueConfig(config),
    endpoints: ['metrics'],
  }),
  inject: [ConfigService],
});
```

## Multi-Redis and Migrations

Register multiple Redis instances when you need to monitor or migrate queues
across Redis deployments.

```ts
import { Queue } from 'bullmq';

const oldEmailQueue = new Queue('emails', {
  connection: {
    host: 'old-redis.internal',
    port: 6379,
  },
});

QueuebertModule.forRoot({
  redis: [
    { id: 'current', label: 'Current Redis' },
    {
      id: 'old',
      label: 'Old Redis',
      connection: {
        host: 'old-redis.internal',
        port: 6379,
      },
    },
  ],
  queues: [
    {
      name: 'emails',
      processor: EmailProcessor,
      redis: 'current',
    },
    {
      name: 'emails',
      queue: oldEmailQueue,
      statsKey: 'emails-old',
      redis: 'old',
    },
  ],
  auth: {
    guards: [AdminGuard],
  },
  endpoints: ['metrics', 'migrations'],
});
```

Queue migration endpoints can preview, start, pause, resume, cancel, and inspect
migrations for waiting, delayed, and failed jobs. Waiting and delayed are the
safe defaults. Failed jobs are opt-in and become new waiting jobs at the target;
BullMQ does not provide a supported way to recreate their failed state.

Migrations take a distributed source-queue lease, pause both queues, reject
active source jobs, preserve job IDs/options/timestamps, and restore each
queue's prior pause state after a clean exit. Flow and repeatable jobs are
rejected. A process crash intentionally leaves the queues paused rather than
risk concurrent processing; after inspecting both Redis instances, an operator
must resume queues explicitly. Background progress is process-local and is
available for one hour, so use a single long-lived Queuebert instance for a
migration and use conservative limits and batches.

Job inspection responses and migration preview sample jobs withhold raw BullMQ
job payloads by default because job data often contains PII, tokens, or other
application-specific secrets. `includeJobData` governs both surfaces and covers
`data` and `returnvalue`; opt in only when the consuming client needs to
display payloads:

```ts
QueuebertModule.forRoot({
  queues: [{ name: 'emails', processor: EmailProcessor }],
  includeJobData: true,
});
```

`includeJobDataInMigrationPreview` is retained as a deprecated alias.
`includeJobData` wins when both are set.

For applications with their own scrubber, `jobRedaction` runs over every job
that leaves either surface, after the `includeJobData` tier has been applied:

```ts
QueuebertModule.forRoot({
  queues: [{ name: 'emails', processor: EmailProcessor }],
  jobRedaction: (job) => ({
    ...job,
    failedReason: scrub(job.failedReason),
  }),
});
```

The hook must be synchronous. If it throws, the job is reduced to its `id`,
`name`, and `state` rather than being returned unscrubbed.

## Cache Management

Processors can expose managed caches with `getCacheConfigs()`. Queuebert uses
those configs to report cache stats and to support cache migration workflows.

```ts
import type { QueuebertCacheConfig } from '@queuebert/nest';

export class EmailProcessor {
  getCacheConfigs(): QueuebertCacheConfig[] {
    return [
      {
        id: 'domains',
        label: 'Domain Cache',
        keyPrefix: 'cache:emails:domains',
        cache: this.domainCache,
      },
    ];
  }
}
```

## Integration Packages

The BullMQ and OpenTelemetry integrations are separate packages:

```ts
import { BaseQueueProcessor, QueuebertBullMQModule } from '@queuebert/bullmq';
import { QueuebertOTelModule } from '@queuebert/otel/nest';
```

There are no `@queuebert/nest/bullmq` or `@queuebert/nest/otel` subpackages.
Use the standalone package imports above.
