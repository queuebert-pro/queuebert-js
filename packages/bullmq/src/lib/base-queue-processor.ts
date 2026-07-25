import { WorkerHost } from '@nestjs/bullmq';
import { Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Job, Worker } from 'bullmq';

import type {
  QueuebertProcessor,
  QueuebertProcessorStats,
  QueuebertCacheConfig,
} from '@queuebert/nest';

import { DurationStatsCollector } from './stats-collector';

const STATS_LOG_INTERVAL = 30000; // Log stats every 30 seconds
const WORKER_RESTART_DELAY = 5000; // Wait 5 seconds before attempting worker restart
const MAX_WORKER_RESTART_ATTEMPTS = 10; // Maximum restart attempts before giving up

/**
 * Configuration options for BaseQueueProcessor
 */
export interface BaseQueueProcessorOptions {
  /**
   * The queue name used for log prefixes and Redis key lookups.
   * Must match the name passed to @Processor().
   */
  queueName: string;

  /**
   * Maximum number of worker restart attempts before giving up.
   * @default 10
   */
  maxWorkerRestartAttempts?: number;

  /**
   * Base delay in ms before the first worker restart attempt (doubles each retry).
   * @default 5000
   */
  workerRestartDelay?: number;

  /**
   * Interval in ms between heartbeat/stats log messages.
   * @default 30000
   */
  statsLogInterval?: number;
}

/**
 * Abstract base class for BullMQ queue processors that integrates with Queuebert.
 *
 * Provides out-of-the-box:
 * - Duration tracking via DurationStatsCollector (avg, min, max, p50, p95, p99)
 * - Redis connection monitoring and status tracking
 * - Worker event listeners with automatic recovery on unexpected close
 * - Periodic heartbeat logging with queue depth and alert detection
 * - QueuebertProcessor interface implementation (getProcessorStats)
 * - Job timing wrapper in process() that tracks success/failure counts
 *
 * Subclasses must:
 * 1. Apply @Processor('queue-name', { ... }) and @Injectable() decorators
 * 2. Implement processJob(job) with their domain-specific logic
 *
 * Subclasses may override:
 * - onProcessorInit()    — run custom init logic (cache setup, queue resume, etc.)
 * - onProcessorDestroy() — run custom cleanup (cache teardown, etc.)
 * - getCustomStats()     — add domain-specific metrics to getProcessorStats()
 * - getCustomLogLines()  — append custom lines to the periodic heartbeat log
 * - getCacheConfigs()    — expose caches to Queuebert for management
 * - getRedisClient()     — provide the Redis client for connection monitoring
 *                          (defaults to this.worker.client)
 *
 * @example
 * ```typescript
 * @Injectable()
 * @Processor('my-queue', { concurrency: 10 })
 * export class MyProcessor extends BaseQueueProcessor {
 *   constructor(private readonly db: PrismaService) {
 *     super({ queueName: 'my-queue' })
 *   }
 *
 *   async processJob(job: Job) {
 *     await this.db.record.create({ data: job.data })
 *     return true
 *   }
 * }
 * ```
 */
export abstract class BaseQueueProcessor
  extends WorkerHost
  implements OnModuleInit, OnModuleDestroy, QueuebertProcessor
{
  protected readonly logger: Logger;
  protected readonly queueName: string;

  // Duration stats collector (replaces hand-rolled circular buffers)
  private readonly durationStats: DurationStatsCollector;

  // Worker recovery tracking
  private workerRestartAttempts = 0;
  private readonly maxRestartAttempts: number;
  private readonly restartDelay: number;
  private isShuttingDown = false;
  private lastWorkerCloseTime: Date | null = null;
  protected redisConnectionStatus:
    | 'connected'
    | 'disconnected'
    | 'reconnecting' = 'connected';

  // Job stats
  private jobsProcessed = 0;
  private jobsCompleted = 0;
  private jobsFailed = 0;
  private lastJobTime: Date | null = null;

  // Throughput window
  private statsWindow = {
    windowStart: Date.now(),
    jobsCompletedInWindow: 0,
  };

  // Heartbeat
  private heartbeatCount = 0;
  private statsLogInterval: NodeJS.Timeout | null = null;
  private workerRecoveryTimer: NodeJS.Timeout | null = null;
  private statsLogInProgress = false;
  private readonly statsLogIntervalMs: number;

  constructor(options: BaseQueueProcessorOptions) {
    super();
    this.queueName = options.queueName;
    this.logger = new Logger(this.constructor.name);
    this.maxRestartAttempts =
      options.maxWorkerRestartAttempts ?? MAX_WORKER_RESTART_ATTEMPTS;
    this.restartDelay = options.workerRestartDelay ?? WORKER_RESTART_DELAY;
    this.statsLogIntervalMs = options.statsLogInterval ?? STATS_LOG_INTERVAL;

    this.durationStats = new DurationStatsCollector({
      windowMs: 60000,
      maxSamples: 1000,
    });
  }

  // ---------------------------------------------------------------------------
  // NestJS lifecycle
  // ---------------------------------------------------------------------------

  async onModuleInit() {
    this.logger.log('Processor initialized, starting stats logging');

    // Let subclass run custom init (cache setup, queue resume, etc.)
    await this.onProcessorInit();

    // Set up Redis and worker monitoring
    await this.setupRedisConnectionListeners();
    await this.setupWorkerEventListeners();

    // Start periodic heartbeat
    this.statsLogInterval = setInterval(() => {
      void this.logStatsSafely();
    }, this.statsLogIntervalMs);
    this.statsLogInterval.unref();
  }

  async onModuleDestroy(): Promise<void> {
    this.isShuttingDown = true;

    if (this.statsLogInterval) {
      clearInterval(this.statsLogInterval);
      this.statsLogInterval = null;
    }

    if (this.workerRecoveryTimer) {
      clearTimeout(this.workerRecoveryTimer);
      this.workerRecoveryTimer = null;
    }

    // Let subclass clean up (cache teardown, etc.)
    await this.onProcessorDestroy();

    // Destroy the stats collector (stops internal pruning timer)
    this.durationStats.destroy();

    // Log final stats
    await this.logStatsSafely();
    this.logger.log('Processor shutting down');
  }

  // ---------------------------------------------------------------------------
  // Subclass hooks
  // ---------------------------------------------------------------------------

  /**
   * Override to run custom initialization logic after base setup.
   * Called during onModuleInit before Redis/worker listeners are set up.
   */
  protected async onProcessorInit(): Promise<void> {
    // no-op by default
  }

  /**
   * Override to run custom cleanup logic during shutdown.
   * Called during onModuleDestroy before stats collector is destroyed.
   */
  protected onProcessorDestroy(): void | Promise<void> {
    // no-op by default
  }

  /**
   * Override to add domain-specific metrics to getProcessorStats().
   * Returned object is spread into the `custom` field of QueuebertProcessorStats.
   */
  protected getCustomStats(): Record<string, unknown> {
    return {};
  }

  /**
   * Override to append custom lines to the periodic heartbeat log.
   * Each string is logged as a separate line.
   */
  protected getCustomLogLines(): string[] {
    return [];
  }

  /**
   * Override to expose caches to Queuebert for management and monitoring.
   */
  getCacheConfigs(): QueuebertCacheConfig[] {
    return [];
  }

  /**
   * Override to provide cache stats for getProcessorStats().
   * Keys are cache names, values are Queuebert CacheStats objects.
   */
  protected getCacheStats():
    | Record<
        string,
        { size: number; hitRate: string; hits: number; misses: number }
      >
    | undefined {
    return undefined;
  }

  private getWorkerOrNull(): Worker | null {
    try {
      return this.worker;
    } catch {
      return null;
    }
  }

  /**
   * Override to provide a custom Redis client for connection monitoring.
   * Defaults to `this.worker.client`.
   */
  protected async getRedisClient(): Promise<Awaited<Worker['client']> | null> {
    const worker = this.getWorkerOrNull();
    if (!worker) return null;
    return worker.client;
  }

  /**
   * Called when a Redis connection error occurs. Override to add custom
   * error reporting (e.g., Sentry).
   */
  protected onRedisError(err: Error): void {
    this.logger.error(`Redis client error: ${err.message}`);
  }

  /**
   * Called when a worker error occurs. Override to add custom
   * error reporting (e.g., Sentry).
   */
  protected onWorkerError(err: Error): void {
    this.logger.error(`Worker error: ${err.message}`);
  }

  /**
   * Called when worker recovery exhausts all restart attempts.
   * Override to add alerting (e.g., Sentry fatal).
   */
  protected onWorkerRecoveryExhausted(): void {
    this.logger.error(
      `Worker recovery failed after ${this.maxRestartAttempts} attempts. Manual intervention required.`,
    );
  }

  // ---------------------------------------------------------------------------
  // Abstract: subclasses implement their job handling here
  // ---------------------------------------------------------------------------

  /**
   * Process a single job. Subclasses implement their domain-specific logic here.
   * The base class handles timing, stats tracking, and error counting.
   */
  protected abstract processJob(job: Job): Promise<unknown>;

  // ---------------------------------------------------------------------------
  // WorkerHost process() — wraps processJob with timing and stats
  // ---------------------------------------------------------------------------

  async process(job: Job): Promise<unknown> {
    const startTime = Date.now();
    this.jobsProcessed++;
    this.lastJobTime = new Date();

    try {
      const result = await this.processJob(job);

      const duration = Date.now() - startTime;
      this.recordDuration(job, duration, true);

      this.jobsCompleted++;
      this.statsWindow.jobsCompletedInWindow++;

      return result;
    } catch (error) {
      const duration = Date.now() - startTime;
      this.recordDuration(job, duration, false, error as Error);

      this.jobsFailed++;

      throw error;
    }
  }

  // ---------------------------------------------------------------------------
  // Duration recording
  // ---------------------------------------------------------------------------

  private recordDuration(
    job: Job,
    duration: number,
    success: boolean,
    error?: Error,
  ): void {
    this.durationStats.record({
      jobId: job.id ?? 'unknown',
      jobName: job.name,
      startedAt: Date.now() - duration,
      completedAt: Date.now(),
      duration,
      success,
      error: error?.message,
    });
  }

  // ---------------------------------------------------------------------------
  // QueuebertProcessor interface
  // ---------------------------------------------------------------------------

  getProcessorStats(): QueuebertProcessorStats {
    const durationMetrics = this.durationStats.calculateStats();
    const throughput = this.durationStats.calculateThroughput();

    const totalFinished = this.jobsCompleted + this.jobsFailed;
    const failureRate =
      totalFinished > 0
        ? parseFloat((this.jobsFailed / totalFinished).toFixed(4))
        : 0;
    const successRate =
      totalFinished > 0
        ? parseFloat((this.jobsCompleted / totalFinished).toFixed(4))
        : 0;

    const worker = this.getWorkerOrNull();

    const stats: QueuebertProcessorStats = {
      duration: durationMetrics,
      jobs: {
        processed: this.jobsProcessed,
        completed: this.jobsCompleted,
        failed: this.jobsFailed,
        failureRate,
        successRate,
        lastJobTime: this.lastJobTime?.toISOString() || null,
      },
      throughput: {
        jobsPerMinute: throughput.jobsPerMinute,
        windowStartTime: throughput.windowStartTime,
        jobsInWindow: throughput.jobsInWindow,
      },
      jobsByType: this.durationStats.getStatsByJobName(),
      custom: {
        ...this.getCustomStats(),
        worker: {
          state: worker?.isRunning() ? 'running' : 'stopped',
          redisStatus: this.redisConnectionStatus,
          restartAttempts: this.workerRestartAttempts,
          lastCloseTime: this.lastWorkerCloseTime?.toISOString() || null,
        },
      },
    };

    const cacheStats = this.getCacheStats();
    if (cacheStats) {
      stats.cache = cacheStats;
    }

    return stats;
  }

  // ---------------------------------------------------------------------------
  // Redis connection monitoring
  // ---------------------------------------------------------------------------

  private async setupRedisConnectionListeners(): Promise<void> {
    try {
      const client = await this.getRedisClient();
      if (!client) {
        this.logger.warn(
          'Redis client not available — connection monitoring disabled',
        );
        return;
      }

      client.on('connect', () => {
        this.logger.log('Redis client connecting...');
        this.redisConnectionStatus = 'reconnecting';
      });

      client.on('ready', () => {
        this.logger.log('Redis client ready - connection established');
        this.redisConnectionStatus = 'connected';
        if (this.workerRestartAttempts > 0) {
          this.logger.log(
            `Worker recovery successful after ${this.workerRestartAttempts} restart attempts`,
          );
          this.workerRestartAttempts = 0;
        }
      });

      client.on('error', (err: Error) => {
        this.onRedisError(err);
      });

      client.on('close', () => {
        this.logger.warn('Redis client connection closed');
        this.redisConnectionStatus = 'disconnected';
      });

      client.on('reconnecting', () => {
        this.logger.log('Redis client reconnecting...');
        this.redisConnectionStatus = 'reconnecting';
      });

      client.on('end', () => {
        this.logger.warn('Redis client connection ended');
        this.redisConnectionStatus = 'disconnected';
      });

      this.logger.log('Redis connection event listeners configured');
    } catch (err) {
      this.logger.error(`Failed to setup Redis connection listeners: ${err}`);
    }
  }

  // ---------------------------------------------------------------------------
  // Worker event monitoring + recovery
  // ---------------------------------------------------------------------------

  private async setupWorkerEventListeners(): Promise<void> {
    const worker = this.getWorkerOrNull();
    if (!worker) {
      this.logger.warn('Worker not available - jobs will not be processed');
      return;
    }

    worker.on('error', (err: Error) => {
      this.onWorkerError(err);
    });

    worker.on('failed', (job: Job | undefined, err: Error) => {
      this.logger.error(`Job ${job?.id} failed: ${err.message}`);
    });

    worker.on('ready', () => {
      this.logger.log('Worker ready - connected to Redis and processing jobs');
      this.redisConnectionStatus = 'connected';
    });

    worker.on('stalled', (jobId: string) => {
      this.logger.warn(`Job ${jobId} stalled - will be retried`);
    });

    worker.on('closing', (msg: string) => {
      this.logger.warn(`Worker closing: ${msg}`);
    });

    worker.on('closed', () => {
      this.lastWorkerCloseTime = new Date();
      this.logger.error('Worker closed - no longer processing jobs');

      if (!this.isShuttingDown) {
        this.attemptWorkerRecovery();
      }
    });

    worker.on('drained', () => {
      this.logger.log('Worker drained - queue is empty');
    });

    this.logger.log(
      `Worker state: ${worker.isRunning() ? 'running' : 'stopped'}`,
    );
  }

  private async attemptWorkerRecovery(): Promise<void> {
    if (this.isShuttingDown) {
      this.logger.log('Shutdown in progress, skipping worker recovery');
      return;
    }

    this.workerRestartAttempts++;

    if (this.workerRestartAttempts > this.maxRestartAttempts) {
      this.onWorkerRecoveryExhausted();
      return;
    }

    const delay =
      this.restartDelay * Math.pow(2, this.workerRestartAttempts - 1);
    const cappedDelay = Math.min(delay, 60000);

    this.logger.warn(
      `Attempting worker recovery (attempt ${this.workerRestartAttempts}/${this.maxRestartAttempts}) in ${cappedDelay}ms...`,
    );

    if (this.workerRecoveryTimer) {
      clearTimeout(this.workerRecoveryTimer);
    }

    this.workerRecoveryTimer = setTimeout(async () => {
      this.workerRecoveryTimer = null;
      if (this.isShuttingDown) return;

      try {
        const worker = this.getWorkerOrNull();
        if (!worker) {
          this.logger.error('Worker instance not available for recovery');
          return;
        }

        if (worker.isRunning()) {
          this.logger.log('Worker already running - recovery not needed');
          this.workerRestartAttempts = 0;
          return;
        }

        this.logger.log('Attempting to restart worker...');
        await worker.run();
        this.logger.log('Worker restarted successfully');
        this.workerRestartAttempts = 0;
      } catch (err) {
        this.logger.error(`Worker restart failed: ${err}`);
        this.onWorkerError(err as Error);
        await this.attemptWorkerRecovery();
      }
    }, cappedDelay);
    this.workerRecoveryTimer.unref();
  }

  // ---------------------------------------------------------------------------
  // Heartbeat logging
  // ---------------------------------------------------------------------------

  private async logStats(): Promise<void> {
    const durationMetrics = this.durationStats.calculateStats();

    // Calculate throughput for the stats window
    const windowDurationMs = Date.now() - this.statsWindow.windowStart;
    const windowDurationMin = windowDurationMs / 60000;
    const completedPerMin =
      windowDurationMin > 0
        ? Math.round(this.statsWindow.jobsCompletedInWindow / windowDurationMin)
        : 0;

    // Get queue depth
    let queueInfo = '';
    let waitCount = 0;
    let activeCount = 0;
    let workerState = 'unknown';
    const worker = this.getWorkerOrNull();
    if (worker) {
      workerState = worker.isRunning() ? 'running' : 'stopped';
      try {
        const client = await worker.client;
        const queueKey = `${worker.opts?.prefix ?? 'bull'}:${this.queueName}`;
        waitCount = await client.llen(`${queueKey}:wait`);
        activeCount = await client.llen(`${queueKey}:active`);
        queueInfo = ` | Queue: ${waitCount} waiting, ${activeCount} active`;
      } catch (err) {
        this.logger.warn(`Failed to get queue stats: ${err}`);
        queueInfo = ' | Queue: stats unavailable';
      }
    } else {
      workerState = 'unavailable';
    }

    const timeSinceLastJob = this.lastJobTime
      ? Math.round((Date.now() - this.lastJobTime.getTime()) / 1000)
      : null;
    const lastJobInfo =
      timeSinceLastJob !== null ? `${timeSinceLastJob}s ago` : 'never';

    this.heartbeatCount++;

    this.logger.log(
      `[Heartbeat #${this.heartbeatCount}] Worker: ${workerState} | Redis: ${this.redisConnectionStatus} | ` +
        `Jobs: ${this.jobsProcessed} (${this.jobsCompleted} ok, ${this.jobsFailed} failed) | ` +
        `Last job: ${lastJobInfo}${queueInfo}`,
    );

    // Alerts
    if (waitCount > 0 && timeSinceLastJob !== null && timeSinceLastJob > 120) {
      this.logger.warn(
        `[Alert] ${waitCount} jobs waiting but no jobs processed in ${timeSinceLastJob}s - possible stall`,
      );
    }

    if (workerState !== 'running' && waitCount > 0) {
      this.logger.error(
        `[Alert] Worker is ${workerState} but ${waitCount} jobs are waiting!`,
      );
    }

    if (this.redisConnectionStatus !== 'connected') {
      this.logger.warn(
        `[Alert] Redis connection status: ${this.redisConnectionStatus}. ` +
          `Worker restart attempts: ${this.workerRestartAttempts}/${this.maxRestartAttempts}`,
      );
    }

    // Duration metrics
    this.logger.log(
      `[Duration] Avg: ${durationMetrics.avgMs}ms | Recent avg: ${durationMetrics.recentAvgMs}ms | ` +
        `Min: ${durationMetrics.minMs}ms | Max: ${durationMetrics.maxMs}ms | ` +
        `Throughput: ${completedPerMin}/min | Samples: ${durationMetrics.sampleCount}`,
    );

    // Custom log lines from subclass
    for (const line of this.getCustomLogLines()) {
      this.logger.log(line);
    }

    // Reset throughput window
    this.statsWindow = {
      windowStart: Date.now(),
      jobsCompletedInWindow: 0,
    };
  }

  private async logStatsSafely(): Promise<void> {
    if (this.statsLogInProgress) return;
    this.statsLogInProgress = true;
    try {
      await this.logStats();
    } catch (error) {
      this.logger.warn(`Failed to log processor stats: ${error}`);
    } finally {
      this.statsLogInProgress = false;
    }
  }
}
