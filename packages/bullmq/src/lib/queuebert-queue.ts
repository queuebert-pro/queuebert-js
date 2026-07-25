import { Queue, Job, JobsOptions } from 'bullmq';

import type {
  QueuebertQueueOptions,
  QueuebertQueueInterface,
  JobDispatchResult,
  BulkDispatchResult,
  JobDefinition,
} from './types';

/**
 * Enhanced BullMQ Queue wrapper that provides:
 * - Typed job dispatching with result tracking
 * - Default job options support
 * - Bulk operation support with results
 * - Queue lifecycle management
 */
export class QueuebertQueue<T = unknown> implements QueuebertQueueInterface<T> {
  private readonly _queue: Queue<T>;
  private readonly _name: string;
  private readonly defaultJobOptions?: JobsOptions;
  private readonly tags: Record<string, string>;

  constructor(name: string, options: QueuebertQueueOptions) {
    this._name = name;
    this.defaultJobOptions = options.defaultJobOptions;
    this.tags = options.tags ?? {};

    // Extract queue-specific options (omit defaultJobOptions and tags from BullMQ options)
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const { defaultJobOptions: _, tags: __, ...queueOptions } = options;

    this._queue = new Queue<T>(name, queueOptions);
  }

  /**
   * The queue name
   */
  get name(): string {
    return this._name;
  }

  /**
   * The underlying BullMQ Queue instance
   */
  get queue(): Queue<T> {
    return this._queue;
  }

  /**
   * Add a job to the queue with tracking
   */
  async add(
    jobName: string,
    data: T,
    opts?: JobsOptions,
  ): Promise<JobDispatchResult<T>> {
    const mergedOpts = { ...this.defaultJobOptions, ...opts };
    const dispatchedAt = new Date();

    // Cast to any to work around BullMQ's strict type inference
    const job = await (this._queue as Queue).add(jobName, data, mergedOpts);

    return {
      jobId: job.id ?? '',
      jobName,
      queueName: this._name,
      dispatchedAt,
      options: mergedOpts,
      data,
    };
  }

  /**
   * Add multiple jobs to the queue in bulk
   */
  async addBulk(jobs: JobDefinition<T>[]): Promise<BulkDispatchResult<T>> {
    const dispatchedAt = new Date();

    const bullJobs = jobs.map((job) => ({
      name: job.name,
      data: job.data,
      opts: { ...this.defaultJobOptions, ...job.opts },
    }));

    // Cast to any to work around BullMQ's strict type inference
    const addedJobs = await (this._queue as Queue).addBulk(bullJobs);

    const results: JobDispatchResult<T>[] = addedJobs.map((job, index) => ({
      jobId: job.id ?? '',
      jobName: jobs[index].name,
      queueName: this._name,
      dispatchedAt,
      options: bullJobs[index].opts,
      data: jobs[index].data,
    }));

    return {
      jobs: results,
      totalDispatched: results.length,
      queueName: this._name,
      dispatchedAt,
    };
  }

  /**
   * Get a job by ID
   */
  async getJob(jobId: string): Promise<Job<T> | undefined> {
    return this._queue.getJob(jobId);
  }

  /**
   * Get multiple jobs by IDs
   */
  async getJobs(jobIds: string[]): Promise<(Job<T> | undefined)[]> {
    return Promise.all(jobIds.map((id) => this._queue.getJob(id)));
  }

  /**
   * Get job counts for the queue
   */
  async getJobCounts(): Promise<{
    waiting: number;
    active: number;
    completed: number;
    failed: number;
    delayed: number;
    paused: number;
    prioritized: number;
  }> {
    const counts = await this._queue.getJobCounts();
    return counts as {
      waiting: number;
      active: number;
      completed: number;
      failed: number;
      delayed: number;
      paused: number;
      prioritized: number;
    };
  }

  /**
   * Get waiting jobs
   */
  async getWaiting(start = 0, end = -1): Promise<Job<T>[]> {
    return this._queue.getWaiting(start, end);
  }

  /**
   * Get active jobs
   */
  async getActive(start = 0, end = -1): Promise<Job<T>[]> {
    return this._queue.getActive(start, end);
  }

  /**
   * Get completed jobs
   */
  async getCompleted(start = 0, end = -1): Promise<Job<T>[]> {
    return this._queue.getCompleted(start, end);
  }

  /**
   * Get failed jobs
   */
  async getFailed(start = 0, end = -1): Promise<Job<T>[]> {
    return this._queue.getFailed(start, end);
  }

  /**
   * Get delayed jobs
   */
  async getDelayed(start = 0, end = -1): Promise<Job<T>[]> {
    return this._queue.getDelayed(start, end);
  }

  /**
   * Remove completed jobs older than the specified age
   */
  async clean(
    grace: number,
    limit: number,
    type:
      | 'completed'
      | 'wait'
      | 'active'
      | 'paused'
      | 'delayed'
      | 'failed' = 'completed',
  ): Promise<string[]> {
    return this._queue.clean(grace, limit, type);
  }

  /**
   * Drain the queue (remove all waiting jobs)
   */
  async drain(delayed = false): Promise<void> {
    return this._queue.drain(delayed);
  }

  /**
   * Pause the queue
   */
  async pause(): Promise<void> {
    return this._queue.pause();
  }

  /**
   * Resume the queue
   */
  async resume(): Promise<void> {
    return this._queue.resume();
  }

  /**
   * Check if the queue is paused
   */
  async isPaused(): Promise<boolean> {
    return this._queue.isPaused();
  }

  /**
   * Obliterate the queue (remove all data)
   * Use with caution!
   */
  async obliterate(opts?: { force?: boolean }): Promise<void> {
    return this._queue.obliterate(opts);
  }

  /**
   * Close the queue connection
   */
  async close(): Promise<void> {
    return this._queue.close();
  }

  /**
   * Get the tags associated with this queue
   */
  getTags(): Record<string, string> {
    return { ...this.tags };
  }

  /**
   * Get the default job options
   */
  getDefaultJobOptions(): JobsOptions | undefined {
    return this.defaultJobOptions ? { ...this.defaultJobOptions } : undefined;
  }
}
