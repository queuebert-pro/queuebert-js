import type { InjectionToken, OptionalFactoryDependency } from '@nestjs/common';
import type {
  Job,
  Queue,
  JobsOptions,
  QueueOptions,
  WorkerOptions,
} from 'bullmq';

/**
 * Job lifecycle events emitted by QueuebertWorker.
 *
 * Note that 'job:failed' fires for every failed attempt, and 'job:retrying'
 * fires additionally when BullMQ will try the job again. A listener that only
 * wants genuine failures should filter on `isFinalAttempt` rather than
 * assuming 'job:failed' means the job is finished.
 */
export type JobLifecycleEvent =
  | 'job:started'
  | 'job:completed'
  | 'job:failed'
  | 'job:progress'
  | 'job:stalled'
  | 'job:retrying';

/**
 * Job lifecycle event data
 */
export interface JobLifecycleEventData<T = unknown, R = unknown> {
  event: JobLifecycleEvent;
  jobId: string;
  jobName: string;
  queueName: string;
  timestamp: Date;
  data?: T;
  result?: R;
  error?: Error;
  progress?: string | boolean | number | object;
  attemptsMade?: number;
  delay?: number;
  duration?: number;
  /**
   * 1-based number of the attempt this event concerns. Present on
   * 'job:failed' and 'job:retrying'.
   *
   * BullMQ does not increment `attemptsMade` until a job is moved to failed,
   * so this is `attemptsMade + 1` and matches what BullMQ compares against
   * `opts.attempts`.
   */
  attempt?: number;
  /** Configured attempt ceiling (`opts.attempts`), defaulting to 1 */
  maxAttempts?: number;
  /**
   * Whether BullMQ will decline to retry the job. Present on 'job:failed' and
   * always false on 'job:retrying'.
   *
   * Covers the attempt ceiling, `job.discard()`, and `UnrecoverableError`. A
   * custom `backoffStrategy` returning -1 also stops retries and cannot be
   * detected here, so treat this as a lower bound if you use one.
   */
  isFinalAttempt?: boolean;
}

/**
 * Job lifecycle listener function
 */
export type JobLifecycleListener<T = unknown, R = unknown> = (
  event: JobLifecycleEventData<T, R>,
) => void | Promise<void>;

/**
 * Duration tracking for individual jobs
 */
export interface JobDurationRecord {
  jobId: string;
  jobName: string;
  startedAt: number;
  completedAt?: number;
  duration?: number;
  success: boolean;
  error?: string;
}

/**
 * Rolling window statistics configuration
 */
export interface RollingWindowConfig {
  /** Window size in milliseconds (default: 60000 = 1 minute) */
  windowMs?: number;
  /** Maximum samples to keep in memory (default: 1000) */
  maxSamples?: number;
  /** How often to prune old samples in ms (default: 10000) */
  pruneIntervalMs?: number;
}

/**
 * Statistics collected by QueuebertWorker
 */
export interface QueuebertWorkerStats {
  /** Total jobs processed since worker started */
  totalProcessed: number;
  /** Total jobs completed successfully */
  totalCompleted: number;
  /** Total jobs failed */
  totalFailed: number;
  /** Jobs currently being processed */
  activeJobs: number;
  /** Duration statistics */
  duration: {
    avgMs: number | undefined;
    minMs: number | undefined;
    maxMs: number | undefined;
    p50Ms: number | undefined;
    p95Ms: number | undefined;
    p99Ms: number | undefined;
    recentAvgMs: number | undefined;
    sampleCount: number;
  };
  /** Throughput in the rolling window */
  throughput: {
    jobsPerMinute: number;
    windowStartTime: string;
    jobsInWindow: number;
  };
  /** Stats broken down by job name/type */
  byJobName: Record<
    string,
    {
      processed: number;
      completed: number;
      failed: number;
      avgDurationMs: number | undefined;
    }
  >;
  /** When the worker was started */
  startedAt: string;
  /** Last job completion time */
  lastJobTime: string | null;
  /** Whether the worker is paused */
  isPaused: boolean;
  /** Whether the worker is running */
  isRunning: boolean;
}

/**
 * Configuration options for QueuebertWorker
 */
export interface QueuebertWorkerOptions extends WorkerOptions {
  /**
   * Rolling window configuration for statistics
   */
  statsWindow?: RollingWindowConfig;

  /**
   * Whether to track stats by job name (default: true)
   */
  trackByJobName?: boolean;

  /**
   * Maximum job name entries to track (default: 100)
   * Prevents memory issues with dynamic job names. Additional names are
   * aggregated under `__other__`.
   */
  maxJobNameEntries?: number;

  /**
   * Custom tags/labels for this worker (for identification)
   */
  tags?: Record<string, string>;
}

/**
 * Configuration options for QueuebertQueue
 */
export interface QueuebertQueueOptions extends QueueOptions {
  /**
   * Default job options to apply to all jobs added through this queue
   */
  defaultJobOptions?: JobsOptions;

  /**
   * Custom tags/labels for this queue (for identification)
   */
  tags?: Record<string, string>;
}

/**
 * Job dispatch result from QueuebertQueue
 */
export interface JobDispatchResult<T = unknown> {
  jobId: string;
  jobName: string;
  queueName: string;
  dispatchedAt: Date;
  options?: JobsOptions;
  data: T;
}

/**
 * Bulk dispatch result
 */
export interface BulkDispatchResult<T = unknown> {
  jobs: JobDispatchResult<T>[];
  totalDispatched: number;
  queueName: string;
  dispatchedAt: Date;
}

/**
 * Job definition for bulk operations
 */
export interface JobDefinition<T = unknown> {
  name: string;
  data: T;
  opts?: JobsOptions;
}

/**
 * Stats snapshot for external reporting
 */
export interface StatsSnapshot {
  queueName: string;
  workerStats: QueuebertWorkerStats | null;
  queueStats: {
    waiting: number;
    active: number;
    completed: number;
    failed: number;
    delayed: number;
    paused: boolean;
  };
  timestamp: string;
}

/**
 * Stats collector interface for aggregating stats from multiple workers
 */
export interface StatsCollector {
  /**
   * Register a worker to collect stats from
   */
  registerWorker(queueName: string, worker: QueuebertWorkerInterface): void;

  /**
   * Unregister a worker
   */
  unregisterWorker(queueName: string): void;

  /**
   * Get aggregated stats for all registered workers
   */
  getAggregatedStats(): Promise<Record<string, StatsSnapshot>>;

  /**
   * Get stats for a specific queue
   */
  getQueueStats(queueName: string): Promise<StatsSnapshot | null>;
}

/**
 * Interface that QueuebertWorker implements
 */
export interface QueuebertWorkerInterface {
  readonly queueName: string;
  readonly isRunning: boolean;
  readonly isPaused: boolean;

  getStats(): QueuebertWorkerStats;
  on(event: JobLifecycleEvent, listener: JobLifecycleListener): void;
  off(event: JobLifecycleEvent, listener: JobLifecycleListener): void;
  pause(doNotWaitActive?: boolean): Promise<void>;
  resume(): Promise<void>;
  close(force?: boolean): Promise<void>;
}

/**
 * Interface that QueuebertQueue implements
 */
export interface QueuebertQueueInterface<T = unknown> {
  readonly name: string;
  readonly queue: Queue;

  add(
    jobName: string,
    data: T,
    opts?: JobsOptions,
  ): Promise<JobDispatchResult<T>>;
  addBulk(jobs: JobDefinition<T>[]): Promise<BulkDispatchResult<T>>;
  getJob(jobId: string): Promise<Job<T> | undefined>;
  pause(): Promise<void>;
  resume(): Promise<void>;
  close(): Promise<void>;
}

/**
 * Module configuration for QueuebertBullMQ
 */
export interface QueuebertBullMQModuleOptions {
  /**
   * Redis connection options (same as BullMQ)
   */
  connection: {
    host?: string;
    port?: number;
    username?: string;
    password?: string;
    db?: number;
    /** Or use a Redis URL */
    url?: string;
  };

  /**
   * Default queue options applied to all queues
   */
  defaultQueueOptions?: Partial<QueuebertQueueOptions>;

  /**
   * Default worker options applied to all workers
   */
  defaultWorkerOptions?: Partial<QueuebertWorkerOptions>;

  /**
   * Global stats collection configuration
   */
  statsConfig?: RollingWindowConfig;
}

/**
 * Async module configuration
 */
export interface QueuebertBullMQModuleAsyncOptions {
  imports?: unknown[];
  useFactory: (
    ...args: unknown[]
  ) => Promise<QueuebertBullMQModuleOptions> | QueuebertBullMQModuleOptions;
  inject?: (InjectionToken | OptionalFactoryDependency)[];
}

/**
 * Injection tokens
 */
export const QUEUEBERT_BULLMQ_OPTIONS = 'QUEUEBERT_BULLMQ_OPTIONS';
export const QUEUEBERT_STATS_COLLECTOR = 'QUEUEBERT_STATS_COLLECTOR';
