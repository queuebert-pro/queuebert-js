# @queuebert/otel

OpenTelemetry metrics and Queuebert snapshot helpers for BullMQ applications.

The package records counters, duration histograms, and observable queue gauges
through an application-owned OpenTelemetry `Meter`. It does not start an SDK,
choose an exporter, open a Prometheus endpoint, or install third-party BullMQ
auto-instrumentation.

## Installation

```sh
npm install @queuebert/otel @opentelemetry/api
```

Install the Nest peers when using the Nest integration:

```sh
npm install @queuebert/otel @queuebert/nest @nestjs/bullmq @nestjs/common @nestjs/core bullmq @opentelemetry/api
```

Configure your OpenTelemetry SDK before importing application code, then pass
its meter to Queuebert. The global API meter is also used automatically when a
meter is not supplied.

## Framework-agnostic usage

```ts
import { metrics } from '@opentelemetry/api';
import { createResourceAttributes, MetricsRegistry } from '@queuebert/otel';

const registry = new MetricsRegistry(
  {
    durationBuckets: [10, 50, 100, 500, 1000, 5000],
    perJobNameMetrics: true,
    maxJobNames: 100,
  },
  metrics.getMeter('email-worker.queuebert', '1.0.0'),
  createResourceAttributes({
    serviceName: 'email-worker',
    deploymentEnvironment: 'production',
  }),
);

registry.recordJobCompleted('emails', 'welcome', 82);
registry.recordJobFailed('emails', 'digest', 410);
registry.updateQueueCounts('emails', {
  waiting: 12,
  active: 2,
  completed: 240,
  failed: 4,
  delayed: 1,
});

const snapshot = registry.getSnapshot();
const queuebertStats = registry.transformToQueuebertStats('emails');
```

Call `registry.destroy()` during shutdown to unregister observable callbacks.

## NestJS module

```ts
import { Module } from '@nestjs/common';
import { QueuebertOTelModule } from '@queuebert/otel/nest';

@Module({
  imports: [
    QueuebertOTelModule.forRoot({
      instrumentationName: 'email-worker.queuebert',
      instrumentationVersion: '1.0.0',
      resource: {
        serviceName: 'email-worker',
        deploymentEnvironment: 'production',
      },
    }),
  ],
})
export class AppModule {}
```

`QueuebertOTelService` exposes `recordJobCompleted`, `recordJobFailed`,
`updateQueueCounts`, Queuebert-shaped snapshots, and the configured `Meter` and
`Tracer`. The same instances are injectable with `QUEUEBERT_OTEL_METER` and
`QUEUEBERT_OTEL_TRACER`.

Async module configuration is supported through
`QueuebertOTelModule.forRootAsync()`.

## Metrics

The stable metric names are exported as `METRIC_NAMES`:

- `queuebert.job.completed`
- `queuebert.job.failed`
- `queuebert.job.duration`
- `queuebert.job.waiting`
- `queuebert.job.active`
- `queuebert.job.delayed`
- `queuebert.queue.throughput`

Job-name cardinality is bounded by `maxJobNames`; overflow names are aggregated
under `__other__`. Failure and success rates are fractions from `0` to `1`.
