import {
  Inject,
  Injectable,
  OnModuleDestroy,
  OnModuleInit,
  Optional,
} from '@nestjs/common';
import type { Meter, Tracer } from '@opentelemetry/api';

import {
  QUEUEBERT_INTEGRATION_REGISTRY,
  QueuebertIntegrationRegistry,
} from '@queuebert/nest';
import { createResourceAttributes } from '../../lib/instrumentation';
import { MetricsRegistry } from '../../lib/metrics-collector';
import { DEFAULT_OTEL_CONFIG } from '../../lib/types';
import type {
  OTelMetricsSnapshot,
  TransformedQueueStats,
} from '../../lib/types';

import type { QueuebertOTelModuleOptions } from './types';
import {
  QUEUEBERT_OTEL_METER,
  QUEUEBERT_OTEL_OPTIONS,
  QUEUEBERT_OTEL_TRACER,
} from './types';

const NEST_QUEUEBERT_OTEL_VERSION = (
  require('../../../package.json') as { version: string }
).version;

/**
 * Records Queuebert metrics through the application-owned OpenTelemetry Meter
 * and keeps an in-process snapshot for the Queuebert Nest integration.
 */
@Injectable()
export class QueuebertOTelService implements OnModuleInit, OnModuleDestroy {
  private readonly metricsRegistry: MetricsRegistry;
  private readonly config: QueuebertOTelModuleOptions;

  constructor(
    @Inject(QUEUEBERT_OTEL_OPTIONS) options: QueuebertOTelModuleOptions,
    @Inject(QUEUEBERT_OTEL_METER) private readonly meter: Meter,
    @Inject(QUEUEBERT_OTEL_TRACER) private readonly tracer: Tracer,
    @Optional()
    @Inject(QUEUEBERT_INTEGRATION_REGISTRY)
    private readonly integrationRegistry?: QueuebertIntegrationRegistry,
  ) {
    this.config = {
      ...DEFAULT_OTEL_CONFIG,
      ...options,
      metrics: {
        ...DEFAULT_OTEL_CONFIG.metrics,
        ...options.metrics,
      },
      resource: {
        ...DEFAULT_OTEL_CONFIG.resource,
        ...options.resource,
        custom: {
          ...DEFAULT_OTEL_CONFIG.resource?.custom,
          ...options.resource?.custom,
        },
      },
    };
    this.metricsRegistry = new MetricsRegistry(
      this.config.metrics,
      this.meter,
      createResourceAttributes(this.config.resource),
    );
  }

  onModuleInit(): void {
    this.integrationRegistry?.register({
      name: '@queuebert/otel/nest',
      version: NEST_QUEUEBERT_OTEL_VERSION,
      description: 'OpenTelemetry metrics integration for Queuebert',
    });
  }

  onModuleDestroy(): void {
    this.metricsRegistry.destroy();
  }

  getMeter(): Meter {
    return this.meter;
  }

  getTracer(): Tracer {
    return this.tracer;
  }

  getResourceAttributes(): Record<string, string | number | boolean> {
    return createResourceAttributes(this.config.resource);
  }

  recordJobCompleted(
    queueName: string,
    jobName: string,
    durationMs: number,
  ): void {
    this.metricsRegistry.recordJobCompleted(queueName, jobName, durationMs);
  }

  recordJobFailed(
    queueName: string,
    jobName: string,
    durationMs: number,
  ): void {
    this.metricsRegistry.recordJobFailed(queueName, jobName, durationMs);
  }

  updateQueueCounts(
    queueName: string,
    counts: {
      waiting: number;
      active: number;
      completed: number;
      failed: number;
      delayed: number;
    },
  ): void {
    this.metricsRegistry.updateQueueCounts(queueName, counts);
  }

  getMetricsSnapshot(): OTelMetricsSnapshot {
    return this.metricsRegistry.getSnapshot();
  }

  getQueuebertStats(): Record<string, TransformedQueueStats> {
    return this.metricsRegistry.transformAllToQueuebertStats();
  }

  getQueueStats(queueName: string): TransformedQueueStats | null {
    return this.metricsRegistry.transformToQueuebertStats(queueName);
  }

  getQueueNames(): string[] {
    return this.metricsRegistry.getQueueNames();
  }

  clearMetrics(): void {
    this.metricsRegistry.clear();
  }

  getConfig(): QueuebertOTelModuleOptions {
    return {
      ...this.config,
      metrics: { ...this.config.metrics },
      resource: {
        ...this.config.resource,
        custom: { ...this.config.resource?.custom },
      },
    };
  }
}
