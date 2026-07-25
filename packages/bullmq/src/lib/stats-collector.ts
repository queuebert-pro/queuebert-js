import type {
  RollingWindowConfig,
  JobDurationRecord,
  QueuebertWorkerInterface,
  StatsSnapshot,
  StatsCollector,
} from './types';

/**
 * Default rolling window configuration
 */
const DEFAULT_WINDOW_CONFIG: Required<RollingWindowConfig> = {
  windowMs: 60000, // 1 minute
  maxSamples: 1000,
  pruneIntervalMs: 10000, // 10 seconds
};

/**
 * Collects and calculates duration statistics with rolling window support
 */
export class DurationStatsCollector {
  private samples: JobDurationRecord[] = [];
  private totals = { processed: 0, completed: 0, failed: 0 };
  private totalsByJobName: Record<
    string,
    {
      processed: number;
      completed: number;
      failed: number;
      durationTotalMs: number;
      durationCount: number;
    }
  > = {};
  private config: Required<RollingWindowConfig>;
  private pruneTimer: ReturnType<typeof setInterval> | null = null;

  constructor(config: RollingWindowConfig = {}) {
    this.config = { ...DEFAULT_WINDOW_CONFIG, ...config };
    this.startPruning();
  }

  /**
   * Record a job duration sample
   */
  record(record: JobDurationRecord): void {
    this.samples.push(record);
    this.totals.processed++;
    if (record.success) {
      this.totals.completed++;
    } else {
      this.totals.failed++;
    }

    const byName = (this.totalsByJobName[record.jobName] ??= {
      processed: 0,
      completed: 0,
      failed: 0,
      durationTotalMs: 0,
      durationCount: 0,
    });
    byName.processed++;
    if (record.success) {
      byName.completed++;
    } else {
      byName.failed++;
    }
    if (record.duration !== undefined) {
      byName.durationTotalMs += record.duration;
      byName.durationCount++;
    }

    // Enforce max samples
    if (this.samples.length > this.config.maxSamples) {
      this.samples = this.samples.slice(-this.config.maxSamples);
    }
  }

  /**
   * Get samples within the rolling window
   */
  getWindowSamples(): JobDurationRecord[] {
    const cutoff = Date.now() - this.config.windowMs;
    return this.samples.filter(
      (s) => s.completedAt !== undefined && s.completedAt >= cutoff,
    );
  }

  /**
   * Get all samples (for full duration stats)
   */
  getAllSamples(): JobDurationRecord[] {
    return [...this.samples];
  }

  /**
   * Get lifetime counters. These are intentionally independent from the
   * bounded rolling sample buffer used for duration percentiles.
   */
  getTotals(): Readonly<{
    processed: number;
    completed: number;
    failed: number;
  }> {
    return { ...this.totals };
  }

  /**
   * Calculate duration statistics
   */
  calculateStats(): {
    avgMs: number | undefined;
    minMs: number | undefined;
    maxMs: number | undefined;
    p50Ms: number | undefined;
    p95Ms: number | undefined;
    p99Ms: number | undefined;
    recentAvgMs: number | undefined;
    sampleCount: number;
  } {
    const allDurations = this.samples
      .filter((s) => s.duration !== undefined)
      .map((s) => s.duration as number);

    const windowDurations = this.getWindowSamples()
      .filter((s) => s.duration !== undefined)
      .map((s) => s.duration as number);

    return {
      avgMs: this.calculateAverage(allDurations),
      minMs: allDurations.length > 0 ? Math.min(...allDurations) : undefined,
      maxMs: allDurations.length > 0 ? Math.max(...allDurations) : undefined,
      p50Ms: this.calculatePercentile(allDurations, 50),
      p95Ms: this.calculatePercentile(allDurations, 95),
      p99Ms: this.calculatePercentile(allDurations, 99),
      recentAvgMs: this.calculateAverage(windowDurations),
      sampleCount: allDurations.length,
    };
  }

  /**
   * Calculate throughput within the window
   */
  calculateThroughput(): {
    jobsPerMinute: number;
    windowStartTime: string;
    jobsInWindow: number;
  } {
    const windowSamples = this.getWindowSamples();
    const windowMs = this.config.windowMs;
    const jobsInWindow = windowSamples.length;
    const windowStartTime = new Date(Date.now() - windowMs).toISOString();

    // Jobs per minute based on window
    const minutesFraction = windowMs / 60000;
    const jobsPerMinute =
      minutesFraction > 0 ? jobsInWindow / minutesFraction : 0;

    return {
      jobsPerMinute: Math.round(jobsPerMinute * 100) / 100,
      windowStartTime,
      jobsInWindow,
    };
  }

  /**
   * Get stats grouped by job name
   */
  getStatsByJobName(): Record<
    string,
    {
      processed: number;
      completed: number;
      failed: number;
      avgDurationMs: number | undefined;
    }
  > {
    const result: Record<
      string,
      {
        processed: number;
        completed: number;
        failed: number;
        avgDurationMs: number | undefined;
      }
    > = {};

    for (const [name, stats] of Object.entries(this.totalsByJobName)) {
      result[name] = {
        processed: stats.processed,
        completed: stats.completed,
        failed: stats.failed,
        avgDurationMs:
          stats.durationCount > 0
            ? Math.round((stats.durationTotalMs / stats.durationCount) * 100) /
              100
            : undefined,
      };
    }

    return result;
  }

  /**
   * Clear all samples
   */
  clear(): void {
    this.samples = [];
    this.totals = { processed: 0, completed: 0, failed: 0 };
    this.totalsByJobName = {};
  }

  /**
   * Stop the prune timer
   */
  destroy(): void {
    if (this.pruneTimer) {
      clearInterval(this.pruneTimer);
      this.pruneTimer = null;
    }
  }

  private startPruning(): void {
    this.pruneTimer = setInterval(() => {
      this.prune();
    }, this.config.pruneIntervalMs);
    this.pruneTimer.unref?.();
  }

  private prune(): void {
    // Remove samples older than 2x the window (keep some history)
    const cutoff = Date.now() - this.config.windowMs * 2;
    this.samples = this.samples.filter(
      (s) => s.completedAt === undefined || s.completedAt >= cutoff,
    );
  }

  private calculateAverage(values: number[]): number | undefined {
    if (values.length === 0) return undefined;
    const sum = values.reduce((a, b) => a + b, 0);
    return Math.round((sum / values.length) * 100) / 100;
  }

  private calculatePercentile(
    values: number[],
    percentile: number,
  ): number | undefined {
    if (values.length === 0) return undefined;

    const sorted = [...values].sort((a, b) => a - b);
    const index = Math.ceil((percentile / 100) * sorted.length) - 1;
    return sorted[Math.max(0, index)];
  }
}

/**
 * Global stats collector that aggregates stats from multiple workers
 */
export class GlobalStatsCollector implements StatsCollector {
  private workers: Map<string, QueuebertWorkerInterface> = new Map();

  /**
   * Register a worker to collect stats from
   */
  registerWorker(queueName: string, worker: QueuebertWorkerInterface): void {
    this.workers.set(queueName, worker);
  }

  /**
   * Unregister a worker
   */
  unregisterWorker(queueName: string): void {
    this.workers.delete(queueName);
  }

  /**
   * Get aggregated stats for all registered workers
   */
  async getAggregatedStats(): Promise<Record<string, StatsSnapshot>> {
    const results: Record<string, StatsSnapshot> = {};

    for (const queueName of this.workers.keys()) {
      const stats = await this.getQueueStats(queueName);
      if (stats) {
        results[queueName] = stats;
      }
    }

    return results;
  }

  /**
   * Get stats for a specific queue
   */
  async getQueueStats(queueName: string): Promise<StatsSnapshot | null> {
    const worker = this.workers.get(queueName);
    if (!worker) return null;

    const workerStats = worker.getStats();

    return {
      queueName,
      workerStats,
      queueStats: {
        waiting: 0, // These would need to come from the queue itself
        active: workerStats.activeJobs,
        completed: workerStats.totalCompleted,
        failed: workerStats.totalFailed,
        delayed: 0,
        paused: workerStats.isPaused,
      },
      timestamp: new Date().toISOString(),
    };
  }

  /**
   * Get list of registered queue names
   */
  getRegisteredQueues(): string[] {
    return Array.from(this.workers.keys());
  }

  /**
   * Check if a queue is registered
   */
  hasQueue(queueName: string): boolean {
    return this.workers.has(queueName);
  }
}
