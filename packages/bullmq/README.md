# @queuebert/bullmq

BullMQ helpers for Queuebert-aware NestJS applications.

This package provides wrappers and base classes for collecting Queuebert stats
from BullMQ queues and workers without putting the integration inside
`@queuebert/nest`.

## Installation

```sh
npm install @queuebert/bullmq @queuebert/nest @nestjs/bullmq @nestjs/common @nestjs/core bullmq
```

## Exports

- `QueuebertBullMQModule`
- `QueuebertBullMQService`
- `QueuebertQueue`
- `QueuebertWorker`
- `BaseQueueProcessor`
- `DurationStatsCollector`
- `GlobalStatsCollector`
- `QueuebertProcessorAdapter`
- Queue, worker, lifecycle, dispatch, and stats types

## NestJS Module

Use `QueuebertBullMQModule` when you want to create Queuebert-wrapped queues and
workers from a service.

```ts
import { Module } from '@nestjs/common';
import { QueuebertBullMQModule } from '@queuebert/bullmq';

@Module({
  imports: [
    QueuebertBullMQModule.forRoot({
      connection: {
        host: 'localhost',
        port: 6379,
      },
      defaultQueueOptions: {
        defaultJobOptions: {
          attempts: 3,
          removeOnComplete: true,
        },
      },
      statsConfig: {
        windowMs: 60000,
        maxSamples: 1000,
      },
    }),
  ],
})
export class AppModule {}
```

Async configuration is also supported:

```ts
QueuebertBullMQModule.forRootAsync({
  imports: [ConfigModule],
  useFactory: (config: ConfigService) => ({
    connection: {
      host: config.getOrThrow('REDIS_HOST'),
      port: config.getOrThrow('REDIS_PORT'),
    },
  }),
  inject: [ConfigService],
});
```

## BaseQueueProcessor

`BaseQueueProcessor` extends Nest's `WorkerHost` and implements the
`QueuebertProcessor` interface from `@queuebert/nest`. It tracks job duration,
success/failure counts, throughput, Redis connection status, and optional cache
stats.

```ts
import { Processor } from '@nestjs/bullmq';
import { Injectable } from '@nestjs/common';
import { BaseQueueProcessor } from '@queuebert/bullmq';
import type { Job } from 'bullmq';

@Injectable()
@Processor('emails', { concurrency: 10 })
export class EmailProcessor extends BaseQueueProcessor {
  constructor(private readonly emailService: EmailService) {
    super({ queueName: 'emails' });
  }

  async processJob(job: Job<{ to: string; template: string }>) {
    await this.emailService.send(job.data);
    return true;
  }

  protected getCustomStats() {
    return {
      provider: this.emailService.providerName,
    };
  }
}
```

### Per-job hooks

`onJobFailed` fires for every failed attempt, before the error is rethrown to
BullMQ. It exists so consumers do not have to re-derive "will BullMQ retry
this?" in each processor's catch block:

```ts
@Injectable()
@Processor('emails')
export class EmailProcessor extends BaseQueueProcessor {
  protected override async onJobFailed(
    job: Job,
    error: unknown,
    ctx: JobFailureContext,
  ) {
    if (!ctx.isFinalAttempt) return;

    Sentry.captureException(error, {
      tags: { queue: 'emails', jobName: job.name },
      extra: { jobId: job.id, attempt: ctx.attempt, max: ctx.maxAttempts },
    });
  }
}
```

`ctx.isFinalAttempt` mirrors BullMQ's own retry decision and covers all three
reasons it declines a retry: the attempt ceiling is reached, the handler called
`job.discard()`, or the error is an `UnrecoverableError`. `ctx.discarded` and
`ctx.unrecoverable` report those last two individually.

One caveat: a custom `backoffStrategy` returning `-1` also stops retries, and
that cannot be known without running the strategy. If you use one, treat
`isFinalAttempt` as a lower bound.

`ctx.attempt` is 1-based and equals `attemptsMade + 1`, which is what BullMQ
compares against `opts.attempts` — `attemptsMade` has not been incremented yet
when your handler throws.

The hook is awaited, so an async reporter can flush before the job is moved to
failed. A hook that throws is logged and swallowed; the original job error is
always the one rethrown. `onJobCompleted(job, result, ctx)` is available for
symmetry.

### Lifecycle events

`QueuebertWorker` emits `job:failed` for every failed attempt and
`job:retrying` additionally when BullMQ will try the job again. Both carry
`attempt`, `maxAttempts` and `isFinalAttempt`, derived from the same logic as
`onJobFailed`, so a listener never has to repeat BullMQ's retry arithmetic:

```ts
worker.on('job:failed', ({ jobId, error, isFinalAttempt }) => {
  if (isFinalAttempt) Sentry.captureException(error, { extra: { jobId } });
});
```

Note that `job:failed` fires on every attempt, so filter on `isFinalAttempt`
rather than assuming it means the job is finished.

Register that processor with `@queuebert/nest`:

```ts
import { QueuebertModule } from '@queuebert/nest';

QueuebertModule.forRoot({
  queues: [
    {
      name: 'emails',
      processor: EmailProcessor,
    },
  ],
});
```

## QueuebertBullMQService

Create Queuebert-wrapped queues and workers programmatically:

```ts
import { Injectable, OnModuleInit } from '@nestjs/common';
import { QueuebertBullMQService } from '@queuebert/bullmq';

@Injectable()
export class WorkerBootstrap implements OnModuleInit {
  constructor(private readonly queuebertBullMQ: QueuebertBullMQService) {}

  async onModuleInit() {
    const queue = this.queuebertBullMQ.createQueue<{ email: string }>('emails');

    this.queuebertBullMQ.createWorker('emails', async (job) => {
      await sendEmail(job.data.email);
    });

    await queue.add('welcome', {
      email: 'person@example.com',
    });
  }
}
```

## Direct Queue and Worker Wrappers

Use `QueuebertQueue` and `QueuebertWorker` directly outside NestJS when you want
typed dispatch results and local stats collection.

```ts
import { QueuebertQueue, QueuebertWorker } from '@queuebert/bullmq';

const queue = new QueuebertQueue<{ email: string }>('emails', {
  connection: {
    host: 'localhost',
    port: 6379,
  },
});

const worker = new QueuebertWorker(
  'emails',
  async (job) => {
    await sendEmail(job.data.email);
  },
  {
    connection: {
      host: 'localhost',
      port: 6379,
    },
  },
);

await queue.add('welcome', {
  email: 'person@example.com',
});

console.log(worker.getStats());
```

## Import Path

Import this integration directly from `@queuebert/bullmq`.

```ts
import { BaseQueueProcessor } from '@queuebert/bullmq';
```

The old nested import shape is not part of the package contract.
