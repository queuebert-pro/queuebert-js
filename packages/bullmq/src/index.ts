// Types
export type {
  JobLifecycleEvent,
  JobLifecycleEventData,
  JobLifecycleListener,
  JobDurationRecord,
  RollingWindowConfig,
  QueuebertWorkerStats,
  QueuebertWorkerOptions,
  QueuebertQueueOptions,
  JobDispatchResult,
  BulkDispatchResult,
  JobDefinition,
  StatsSnapshot,
  StatsCollector,
  QueuebertWorkerInterface,
  QueuebertQueueInterface,
  QueuebertBullMQModuleOptions,
  QueuebertBullMQModuleAsyncOptions,
  WorkerStopListener,
} from './lib/types';

// Worker stop reasons, shared with @queuebert/nest which reports them
export {
  WorkerStopReason,
  describeWorkerStopReason,
  classifyWorkerStop,
} from '@queuebert/nest';
export type { WorkerStopRecord, QueueWorkersStats } from '@queuebert/nest';

// Injection tokens
export {
  QUEUEBERT_BULLMQ_OPTIONS,
  QUEUEBERT_STATS_COLLECTOR,
} from './lib/types';

// Core classes
export { QueuebertQueue } from './lib/queuebert-queue';
export { QueuebertWorker } from './lib/queuebert-worker';
export {
  DurationStatsCollector,
  GlobalStatsCollector,
} from './lib/stats-collector';

// Retry lifecycle helpers
export {
  isUnrecoverableError,
  resolveRetryOutcome,
  trackDiscard,
} from './lib/retry-outcome';
export type { RetryOutcome } from './lib/retry-outcome';

// Base processor class
export { BaseQueueProcessor } from './lib/base-queue-processor';
export type {
  BaseQueueProcessorOptions,
  JobFailureContext,
  JobCompletionContext,
} from './lib/base-queue-processor';

// Adapter for @queuebert/nest integration
export type { QueueContext } from './lib/queuebert-processor-adapter';
export {
  QueuebertProcessorAdapter,
  createProcessorAdapter,
} from './lib/queuebert-processor-adapter';

// NestJS module and service
export { QueuebertBullMQModule } from './lib/queuebert-bullmq.module';
export { QueuebertBullMQService } from './lib/queuebert-bullmq.service';

// Error types
export {
  QueuebertBullMQError,
  QueueAlreadyExistsError,
  WorkerAlreadyExistsError,
  QueueNotFoundError,
  WorkerNotFoundError,
  InvalidOptionsError,
} from './lib/errors';
