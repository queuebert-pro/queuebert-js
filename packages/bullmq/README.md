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
