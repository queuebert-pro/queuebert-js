import { Logger } from '@nestjs/common';
import { Worker, Job, Processor } from 'bullmq';

import { resolveRetryOutcome, trackDiscard } from './retry-outcome';
import { DurationStatsCollector } from './stats-collector';
import type {
  QueuebertWorkerOptions,
  QueuebertWorkerInterface,
  QueuebertWorkerStats,
  JobLifecycleEvent,
  JobLifecycleListener,
  JobLifecycleEventData,
  JobDurationRecord,
} from './types';

const OTHER_JOB_NAME = '__other__';

/**
 * Enhanced BullMQ Worker wrapper that provides:
 * - Automatic job duration tracking
 * - Lifecycle event emission
 * - Rolling window statistics
 * - Stats by job name/type
 */
export class QueuebertWorker<
  T = unknown,
  R = unknown,
> implements QueuebertWorkerInterface {
  private readonly logger: Logger;
  private readonly _worker: Worker<T, R>;
  private readonly _queueName: string;
  private readonly statsCollector: DurationStatsCollector;
  private readonly tags: Record<string, string>;
  private readonly trackByJobName: boolean;
  private readonly maxJobNameEntries: number;
  private readonly startedAt: Date;

  private _isRunning = false;
  private _isPaused = false;
  private activeJobCount = 0;
  private lastJobTime: Date | null = null;
  private jobNameSet = new Set<string>();

  // Event listeners
  private listeners: Map<JobLifecycleEvent, Set<JobLifecycleListener<T, R>>> =
    new Map();

  // Track job start times for duration calculation
  private jobStartTimes: Map<string, number> = new Map();

  constructor(
    queueName: string,
    processor: Processor<T, R>,
    options: QueuebertWorkerOptions,
  ) {
    this._queueName = queueName;
    this.logger = new Logger(`QueuebertWorker[${queueName}]`);
    this.trackByJobName = options.trackByJobName ?? true;
    this.maxJobNameEntries = options.maxJobNameEntries ?? 100;
    this.tags = options.tags ?? {};
    this.startedAt = new Date();

    // Initialize stats collector
    this.statsCollector = new DurationStatsCollector(options.statsWindow);

    // Omit Queuebert-specific options before forwarding to BullMQ.
    const workerOptions = { ...options };
    delete workerOptions.statsWindow;
    delete workerOptions.trackByJobName;
    delete workerOptions.maxJobNameEntries;
    delete workerOptions.tags;

    // Wrap the processor to track jobs
    const wrappedProcessor = this.wrapProcessor(processor);

    // Create the underlying worker
    this._worker = new Worker<T, R>(queueName, wrappedProcessor, workerOptions);

    // Set up event listeners on the underlying worker
    this.setupWorkerEvents();

    this._isRunning = true;
  }

  /**
   * The queue name this worker processes
   */
  get queueName(): string {
    return this._queueName;
  }

  /**
   * Whether the worker is currently running
   */
  get isRunning(): boolean {
    return this._isRunning;
  }

  /**
   * Whether the worker is paused
   */
  get isPaused(): boolean {
    return this._isPaused;
  }

  /**
   * The underlying BullMQ Worker instance
   */
  get worker(): Worker<T, R> {
    return this._worker;
  }

  /**
   * Get current worker statistics
   */
  getStats(): QueuebertWorkerStats {
    const durationStats = this.statsCollector.calculateStats();
    const throughput = this.statsCollector.calculateThroughput();
    const byJobName = this.trackByJobName
      ? this.statsCollector.getStatsByJobName()
      : {};

    const totals = this.statsCollector.getTotals();

    return {
      totalProcessed: totals.processed,
      totalCompleted: totals.completed,
      totalFailed: totals.failed,
      activeJobs: this.activeJobCount,
      duration: durationStats,
      throughput,
      byJobName,
      startedAt: this.startedAt.toISOString(),
      lastJobTime: this.lastJobTime?.toISOString() ?? null,
      isPaused: this._isPaused,
      isRunning: this._isRunning,
    };
  }

  /**
   * Subscribe to job lifecycle events
   */
  on(event: JobLifecycleEvent, listener: JobLifecycleListener<T, R>): void {
    let eventListeners = this.listeners.get(event);
    if (!eventListeners) {
      eventListeners = new Set();
      this.listeners.set(event, eventListeners);
    }
    eventListeners.add(listener);
  }

  /**
   * Unsubscribe from job lifecycle events
   */
  off(event: JobLifecycleEvent, listener: JobLifecycleListener<T, R>): void {
    const eventListeners = this.listeners.get(event);
    if (eventListeners) {
      eventListeners.delete(listener);
    }
  }

  /**
   * Pause the worker
   */
  async pause(doNotWaitActive?: boolean): Promise<void> {
    await this._worker.pause(doNotWaitActive);
    this._isPaused = true;
  }

  /**
   * Resume the worker
   */
  async resume(): Promise<void> {
    await this._worker.resume();
    this._isPaused = false;
  }

  /**
   * Close the worker
   */
  async close(force?: boolean): Promise<void> {
    this._isRunning = false;
    this.statsCollector.destroy();
    await this._worker.close(force);
  }

  /**
   * Get the tags associated with this worker
   */
  getTags(): Record<string, string> {
    return { ...this.tags };
  }

  /**
   * Wrap the processor to track job lifecycle
   */
  private wrapProcessor(processor: Processor<T, R>): Processor<T, R> {
    return async (job: Job<T, R>, token?: string): Promise<R> => {
      const jobId = job.id ?? 'unknown';
      const jobName = job.name;
      const startTime = Date.now();

      const statsJobName = this.getStatsJobName(jobName);

      // Record start
      this.jobStartTimes.set(jobId, startTime);
      this.activeJobCount++;

      // BullMQ keeps the discard flag protected, so observe the call instead.
      const discardTracker = trackDiscard(job);

      // Emit started event
      await this.emit('job:started', {
        event: 'job:started',
        jobId,
        jobName,
        queueName: this._queueName,
        timestamp: new Date(),
        data: job.data,
        attemptsMade: job.attemptsMade,
      });

      try {
        // Execute the actual processor
        const result = await processor(job, token);

        // Record completion
        const endTime = Date.now();
        const duration = endTime - startTime;
        this.jobStartTimes.delete(jobId);
        this.activeJobCount--;
        this.lastJobTime = new Date();

        // Record the duration sample
        const record: JobDurationRecord = {
          jobId,
          jobName: statsJobName,
          startedAt: startTime,
          completedAt: endTime,
          duration,
          success: true,
        };
        this.statsCollector.record(record);

        // Emit completed event
        await this.emit('job:completed', {
          event: 'job:completed',
          jobId,
          jobName,
          queueName: this._queueName,
          timestamp: new Date(),
          data: job.data,
          result,
          duration,
          attemptsMade: job.attemptsMade,
        });

        return result;
      } catch (error) {
        // Record failure
        const endTime = Date.now();
        const duration = endTime - startTime;
        this.jobStartTimes.delete(jobId);
        this.activeJobCount--;
        this.lastJobTime = new Date();

        // Record the duration sample
        const record: JobDurationRecord = {
          jobId,
          jobName: statsJobName,
          startedAt: startTime,
          completedAt: endTime,
          duration,
          success: false,
          error: error instanceof Error ? error.message : String(error),
        };
        this.statsCollector.record(record);

        const outcome = resolveRetryOutcome(
          job,
          error,
          discardTracker.wasDiscarded(),
        );
        const normalizedError =
          error instanceof Error ? error : new Error(String(error));

        // Emit failed event for every failed attempt, carrying where the
        // attempt sits in the retry lifecycle.
        await this.emit('job:failed', {
          event: 'job:failed',
          jobId,
          jobName,
          queueName: this._queueName,
          timestamp: new Date(),
          data: job.data,
          error: normalizedError,
          duration,
          attemptsMade: job.attemptsMade,
          attempt: outcome.attempt,
          maxAttempts: outcome.maxAttempts,
          isFinalAttempt: outcome.isFinalAttempt,
        });

        // And a distinct event when another attempt is coming, so listeners
        // can tell a transient failure from a terminal one without repeating
        // BullMQ's retry arithmetic.
        if (!outcome.isFinalAttempt) {
          await this.emit('job:retrying', {
            event: 'job:retrying',
            jobId,
            jobName,
            queueName: this._queueName,
            timestamp: new Date(),
            data: job.data,
            error: normalizedError,
            duration,
            attemptsMade: job.attemptsMade,
            attempt: outcome.attempt,
            maxAttempts: outcome.maxAttempts,
            isFinalAttempt: false,
          });
        }

        throw error;
      } finally {
        discardTracker.restore();
      }
    };
  }

  private getStatsJobName(jobName: string): string {
    if (!this.trackByJobName) return OTHER_JOB_NAME;
    if (this.jobNameSet.has(jobName)) return jobName;
    if (this.jobNameSet.size < this.maxJobNameEntries) {
      this.jobNameSet.add(jobName);
      return jobName;
    }
    return OTHER_JOB_NAME;
  }

  /**
   * Set up event listeners on the underlying worker
   */
  private setupWorkerEvents(): void {
    // Listen for progress updates
    this._worker.on('progress', async (job, progress) => {
      await this.emit('job:progress', {
        event: 'job:progress',
        jobId: job.id ?? 'unknown',
        jobName: job.name,
        queueName: this._queueName,
        timestamp: new Date(),
        data: job.data,
        progress,
      });
    });

    // Listen for stalled jobs
    this._worker.on('stalled', async (jobId: string) => {
      await this.emit('job:stalled', {
        event: 'job:stalled',
        jobId,
        jobName: 'unknown', // BullMQ doesn't provide job name in stalled event
        queueName: this._queueName,
        timestamp: new Date(),
      });
    });

    // Listen for errors
    this._worker.on('error', (error: Error) => {
      // Log error but don't crash
      this.logger.error('Worker error', error.stack);
    });
  }

  /**
   * Emit an event to all registered listeners
   */
  private async emit(
    event: JobLifecycleEvent,
    data: JobLifecycleEventData<T, R>,
  ): Promise<void> {
    const eventListeners = this.listeners.get(event);
    if (!eventListeners || eventListeners.size === 0) return;

    const promises: Promise<void>[] = [];
    for (const listener of eventListeners) {
      try {
        const result = listener(data);
        if (result instanceof Promise) {
          promises.push(result);
        }
      } catch (error) {
        this.logger.error(
          `Listener error for ${event}`,
          error instanceof Error ? error.stack : String(error),
        );
      }
    }

    // Wait for all async listeners
    if (promises.length > 0) {
      await Promise.allSettled(promises);
    }
  }
}
