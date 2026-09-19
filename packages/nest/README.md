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

### Worker presence and stop reasons

Workers built on `@queuebert/bullmq` (`BaseQueueProcessor` or
`QueuebertWorker`) announce themselves under the queue's own key prefix and
record why they stopped. `/stats` reports both per queue:

```json
"workers": {
  "count": 2,
  "lastStop": {
    "workerId": "api-7f9c:4132:k3x9q1",
    "host": "api-7f9c",
    "reason": "lost_connection",
    "description": "Lost connection to Redis",
    "at": "2026-09-19T04:12:30.000Z",
    "jobsProcessed": 1284,
    "lastJobAt": "2026-09-19T04:11:58.000Z"
  }
}
```

`reason` is one of `WorkerStopReason`: `shutdown`, `closed`,
`lost_connection`, `recovery_failed`, `error` or `unknown`. `unknown` is
what a reader records for a worker whose heartbeat stopped without a stop
being written, such as a killed process. The field is omitted entirely for a
queue no Queuebert-aware worker has ever served, so "no workers" and "not
reported" stay distinguishable.

Presence costs one small hash write per worker every 15 seconds and two
reads per queue on `/stats`. `readWorkerPresence` and `WorkerPresence` are
exported for other integrations.

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
- `retry`
- `remove`
- `migrations`

`jobs` is read-only but is still off by default: unlike `metrics`, which
exposes aggregates, it returns per-job identifiers, failure reasons, and stack
traces.

### Pause notes

`POST /pause` and `POST /:queueName/pause` accept an optional JSON body
saying why, and for how long:

```json
{ "reason": "Deploying api v2.3", "until": "2026-09-19T05:30:00.000Z" }
```

`reason` is trimmed and limited to 200 characters; longer is a 400 rather
than a silent cut. `until` must be in the future and at most 7 days out.
Posting a pause to a queue that is already paused only updates the note and
keeps the original `pausedAt`, which is how a client attaches a reason after
the pause itself. Resume clears the note.

While a queue is paused, stats carry the note beside `paused`:

```json
"paused": true,
"pause": {
  "reason": "Deploying api v2.3",
  "pausedAt": "2026-09-19T04:55:12.000Z",
  "until": "2026-09-19T05:30:00.000Z",
  "source": "api"
}
```

`source` is `api` for the endpoints and `migration` for the pauses a
migration performs. A queue paused outside Queuebert reports `paused: true`
with no note. Capabilities report `canPauseWithReason` when the endpoints
take a body, and `canPauseUntil` when the module honours `until` by resuming
the queue itself: it checks every 30 seconds when the `pause` endpoint is
enabled.

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

Enabling `jobs` also adds `lastFailure` to each queue's stats, so a dashboard
can show what failed most recently without a second request:

```json
{
  "jobId": "12345",
  "name": "send-email",
  "failedReason": "SMTP timeout",
  "finishedOn": 1758158460000
}
```

It is `null` when a queue has no failures, and omitted entirely when `jobs` is
disabled — `stats` cannot be turned off, and a failure reason is the same risk
class as the inspection endpoint. It costs one Redis round-trip per queue, and
nothing at all for a queue whose failed count is already zero.

Two behaviours worth knowing:

- **Ordering is BullMQ's and varies by state.** `completed` and `failed` come
  back newest-first; every other state comes back oldest-first.
- **`jobType` filters within the page, not across the state.** The filter is
  applied after the `start`/`end` window is read from Redis, so `jobs` can be
  shorter than the requested page without that page being the last one. The
  response sets `jobTypeFilter` whenever this applies, and `total` always
  counts the whole state.

### Failure retention

Job inspection can only show you what BullMQ still has. `removeOnFail` decides
that, and its two forms behave very differently when something goes wrong:

```ts
// Volume-based: keep the most recent 1000 failures
await queue.add('send-email', data, { removeOnFail: { count: 1000 } });

// Time-based: keep failures for 7 days (age is in SECONDS)
await queue.add('send-email', data, {
  removeOnFail: { age: 7 * 24 * 60 * 60 },
});
```

Prefer `age` for anything you intend to debug later. With `count`, retention
depends on failure volume, so a burst of 1001 failures evicts everything from
before the burst — which is exactly the window you want during an incident,
and exactly when the burst happens. A Sentry event, a `lastFailure` you saw a
moment ago, or a job id from a log can all point at a job that has already
been evicted, and `GET /:queueName/jobs/:jobId` then answers 404. With `age`,
an incident cannot push earlier failures out of the window.

Two caveats worth knowing:

- **Eviction is best-effort, not scheduled.** BullMQ evaluates it when a job
  transitions into the failed set; there is no background timer. On a quiet
  queue, failures older than `age` stay until something else fails. That
  favours debugging, but it means `age` is not a retention guarantee, so do
  not rely on it to satisfy a data-retention policy.
- **`age` and `count` together are an AND.** Jobs are kept only if they
  satisfy both, so `count` acts as a hard ceiling. Combine them when you want
  a memory bound, and set `count` high enough that it is not the binding
  constraint in normal operation.

If `removeOnFail` is unset, failed jobs are kept indefinitely — nothing is
evicted, and the risk is unbounded growth rather than missing jobs.

## Job Controls

`retry` and `remove` are mutating and gated separately from `jobs`, so
read-only inspection can be enabled without handing out the controls:

```ts
QueuebertModule.forRoot({
  queues: [{ name: 'emails', processor: EmailProcessor }],
  endpoints: ['metrics', 'jobs', 'retry', 'remove'],
});
```

| Route                                | Effect                                       |
| ------------------------------------ | -------------------------------------------- |
| `POST /:queueName/jobs/:jobId/retry` | Move one finished job back to wait           |
| `POST /:queueName/jobs/retry`        | Move a bounded page of finished jobs to wait |
| `DELETE /:queueName/jobs/:jobId`     | Remove one job                               |

Single retry accepts `state` (`failed` by default, or `completed`) and
`resetAttempts`. Without `resetAttempts`, the attempt counters are preserved,
so a job that exhausted its attempts gets one further run rather than a fresh
budget. A job in any other state returns `409`, naming the state it is
actually in.

Bulk retry accepts `state`, `jobType`, and `limit` (default 50, max 1000). It
reports `retried`, `failed`, and details for the first few failures, so one
locked job does not hide the rest of the work.

Two things to know about bulk retry:

- **`limit` is a real cap.** It is not BullMQ's `retryJobs({ count })`, where
  `count` is a per-iteration batch size and the call drains the entire state.
  Queuebert retries a bounded page job-by-job instead, which is what makes
  `limit` and `jobType` possible at all.
- **`jobType` filters within the page**, the same way it does when listing, and
  the response sets `jobTypeFilter` whenever it applies.

Removal returns `409` when BullMQ refuses, most commonly because a worker holds
a lock on the job while processing it. An active job is not rejected up front,
because BullMQ can remove an active job that is not locked.

Note that `POST /:queueName/jobs/retry` shadows a job whose id is literally
`retry`, in the same way the migration routes shadow `preview` and `start`.

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
