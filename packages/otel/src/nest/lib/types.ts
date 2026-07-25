import type {
  InjectionToken,
  ModuleMetadata,
  OptionalFactoryDependency,
} from '@nestjs/common';
import type { QueuebertOTelConfig } from '../../lib/types';

// Re-export all core types
export type {
  MetricsConfig,
  ResourceAttributes,
  QueuebertOTelConfig,
  OTelMetricsSnapshot,
  OTelQueueMetrics,
  TransformedQueueStats,
} from '../../lib/types';

export { DEFAULT_OTEL_CONFIG } from '../../lib/types';

/**
 * NestJS module configuration options
 * Extends the core config with NestJS-specific options
 */
export type QueuebertOTelModuleOptions = QueuebertOTelConfig;

/**
 * Async module configuration
 */
export interface QueuebertOTelModuleAsyncOptions {
  imports?: ModuleMetadata['imports'];
  useFactory: (
    ...args: unknown[]
  ) => Promise<QueuebertOTelModuleOptions> | QueuebertOTelModuleOptions;
  inject?: Array<InjectionToken | OptionalFactoryDependency>;
}

/**
 * NestJS injection tokens
 */
export const QUEUEBERT_OTEL_OPTIONS = 'QUEUEBERT_OTEL_OPTIONS';
export const QUEUEBERT_OTEL_METER = 'QUEUEBERT_OTEL_METER';
export const QUEUEBERT_OTEL_TRACER = 'QUEUEBERT_OTEL_TRACER';
