import type { Meter, Tracer } from '@opentelemetry/api';

/**
 * Metrics collection configuration
 */
export interface MetricsConfig {
  /** Histogram buckets for duration metrics */
  durationBuckets?: number[];
  /** Whether to collect per-job-name metrics (default: true) */
  perJobNameMetrics?: boolean;
  /** Maximum unique job names to track (default: 100) */
  maxJobNames?: number;
  /** Rolling throughput window in milliseconds (default: 60000) */
  throughputWindowMs?: number;
}

/**
 * Resource attributes for OTel
 */
export interface ResourceAttributes {
  /** Service name (default: 'queuebert') */
  serviceName?: string;
  /** Service version */
  serviceVersion?: string;
  /** Service namespace */
  serviceNamespace?: string;
  /** Deployment environment (e.g., 'production', 'staging') */
  deploymentEnvironment?: string;
  /** Additional custom attributes */
  custom?: Record<string, string | number | boolean>;
}

/**
 * Core configuration options for Queuebert OTel (framework-agnostic)
 */
export interface QueuebertOTelConfig {
  /**
   * Metrics collection configuration
   */
  metrics?: MetricsConfig;

  /**
   * Resource attributes for identifying this service
   */
  resource?: ResourceAttributes;

  /** Existing OTel meter. Defaults to the global meter provider. */
  meter?: Meter;
  /** Existing OTel tracer. Defaults to the global tracer provider. */
  tracer?: Tracer;
  /** Instrumentation scope name (default: '@queuebert/otel'). */
  instrumentationName?: string;
  /** Instrumentation scope version. */
  instrumentationVersion?: string;
}

/**
 * Metrics snapshot from OTel collector
 */
export interface OTelMetricsSnapshot {
  /** Queue-level metrics */
  queues: Record<string, OTelQueueMetrics>;
  /** Timestamp of collection */
  timestamp: string;
  /** Resource attributes */
  resource: Record<string, string | number | boolean>;
}

/**
 * Queue metrics from OTel
 */
export interface OTelQueueMetrics {
  /** Queue name */
  name: string;
  /** Job counts by state */
  counts: {
    waiting: number;
    active: number;
    completed: number;
    failed: number;
    delayed: number;
  };
  /** Duration histogram data */
  duration: {
    sum: number;
    count: number;
    min: number | undefined;
    max: number | undefined;
    recentSum: number;
    recentCount: number;
    buckets: { le: number; count: number }[];
  };
  /** Throughput (jobs/minute) */
  throughput: number;
  throughputWindow: {
    startTime: string;
    jobs: number;
  };
  lastJobTime: string | null;
  /** Error rate (0-1) */
  errorRate: number;
  /** Per-job-name breakdown */
  byJobName?: Record<
    string,
    {
      count: number;
      duration: { sum: number; count: number };
      errors: number;
    }
  >;
}

/**
 * Transformed stats in Queuebert format (for @queuebert/nest integration)
 */
export interface TransformedQueueStats {
  name: string;
  paused: boolean;
  counts: {
    waiting: number;
    active: number;
    completed: number;
    failed: number;
    delayed: number;
    total: number;
  };
  jobMetrics: {
    duration: {
      avgMs: number | undefined;
      minMs: number | undefined;
      maxMs: number | undefined;
      p50Ms: number | undefined;
      p95Ms: number | undefined;
      p99Ms: number | undefined;
      recentAvgMs: number | undefined;
    };
    failureRate: number;
    successRate: number;
    processed: number;
    completed: number;
    failed: number;
    byType?: Record<
      string,
      { processed: number; completed: number; failed: number }
    >;
    lastJobTime: string | null;
    sampleCount: number;
    performance: {
      score: number;
      trend: 'catching_up' | 'falling_behind' | 'stable' | 'idle' | 'paused';
      throughputPerMin: number;
      addedPerMin: number;
      deltaPerMin: number;
      estimatedClearTimeMs: number | null;
      health: 'excellent' | 'good' | 'fair' | 'poor' | 'critical';
    };
  };
  throughput: {
    jobsPerMinute: number;
    windowStartTime: string;
    jobsInWindow: number;
  };
}

/**
 * Default configuration values
 */
export const DEFAULT_OTEL_CONFIG: QueuebertOTelConfig = {
  metrics: {
    durationBuckets: [5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000],
    perJobNameMetrics: true,
    maxJobNames: 100,
    throughputWindowMs: 60000,
  },
  resource: {
    serviceName: 'queuebert',
  },
  instrumentationName: '@queuebert/otel',
};
