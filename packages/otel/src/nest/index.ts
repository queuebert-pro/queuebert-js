// Re-export everything from the core queuebert-otel package
export * from '../index';

// NestJS-specific types
export type {
  QueuebertOTelModuleOptions,
  QueuebertOTelModuleAsyncOptions,
} from './lib/types';

// NestJS injection tokens
export {
  QUEUEBERT_OTEL_OPTIONS,
  QUEUEBERT_OTEL_METER,
  QUEUEBERT_OTEL_TRACER,
} from './lib/types';

// NestJS module and service
export { QueuebertOTelModule } from './lib/queuebert-otel.module';
export { QueuebertOTelService } from './lib/queuebert-otel.service';
