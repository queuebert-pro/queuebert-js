import type { ResourceAttributes } from './types';

/**
 * Create resource attributes for an application-owned OpenTelemetry provider.
 * Queuebert deliberately does not create or start an SDK on import.
 */
export function createResourceAttributes(
  options: ResourceAttributes = {},
): Record<string, string | number | boolean> {
  const attrs: Record<string, string | number | boolean> = {};

  if (options.serviceName) attrs['service.name'] = options.serviceName;
  if (options.serviceVersion) attrs['service.version'] = options.serviceVersion;
  if (options.serviceNamespace) {
    attrs['service.namespace'] = options.serviceNamespace;
  }
  if (options.deploymentEnvironment) {
    attrs['deployment.environment'] = options.deploymentEnvironment;
  }

  Object.assign(attrs, options.custom);
  return attrs;
}

/** Metric names emitted through the configured OpenTelemetry Meter. */
export const METRIC_NAMES = {
  JOB_DURATION: 'queuebert.job.duration',
  JOB_COMPLETED: 'queuebert.job.completed',
  JOB_FAILED: 'queuebert.job.failed',
  JOB_WAITING: 'queuebert.job.waiting',
  JOB_ACTIVE: 'queuebert.job.active',
  JOB_DELAYED: 'queuebert.job.delayed',
  QUEUE_THROUGHPUT: 'queuebert.queue.throughput',
} as const;

/** Stable span names for applications that add Queuebert-compatible spans. */
export const SPAN_NAMES = {
  QUEUE_ADD: 'bullmq.queue.add',
  QUEUE_ADD_BULK: 'bullmq.queue.addBulk',
  WORKER_PROCESS: 'bullmq.worker.process',
  JOB_COMPLETED: 'bullmq.job.completed',
  JOB_FAILED: 'bullmq.job.failed',
} as const;

/** Stable attribute names for Queuebert-compatible metrics and spans. */
export const SPAN_ATTRIBUTES = {
  QUEUE_NAME: 'bullmq.queue.name',
  JOB_ID: 'bullmq.job.id',
  JOB_NAME: 'bullmq.job.name',
  JOB_ATTEMPTS: 'bullmq.job.attempts',
  JOB_DELAY: 'bullmq.job.delay',
  JOB_PRIORITY: 'bullmq.job.priority',
  ERROR_TYPE: 'error.type',
  ERROR_MESSAGE: 'error.message',
} as const;
