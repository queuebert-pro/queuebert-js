import type {
  QueuebertProcessor,
  QueuebertProcessorStats,
  PerformanceMetrics,
  PerformanceTrend,
  HealthStatus,
} from '@queuebert/nest';

import type { QueuebertWorkerInterface, QueuebertWorkerStats } from './types';

/**
 * Queue context for performance calculation
 */
export interface QueueContext {
  /** Number of jobs waiting in queue */
  waiting: number;
  /** Number of jobs currently active */
  active: number;
  /** Number of delayed jobs */
  delayed: number;
  /** Jobs added per minute (if tracked) */
  addedPerMin?: number;
}

/**
 * Adapter that wraps a QueuebertWorker to implement the QueuebertProcessor interface
 * This allows QueuebertWorker to be used with @queuebert/nest for
 * unified stats reporting.
 *
 * @example
 * ```typescript
 * // In your processor service
 * @Injectable()
 * export class EmailProcessor implements QueuebertProcessor {
 *   private worker: QueuebertWorker<EmailJob, void>
 *   private adapter: QueuebertProcessorAdapter
 *
 *   constructor(private queuebertBullMQ: QueuebertBullMQService) {
 *     this.worker = queuebertBullMQ.createWorker('email', this.process.bind(this))
 *     this.adapter = new QueuebertProcessorAdapter(this.worker)
 *   }
 *
 *   getProcessorStats(): QueuebertProcessorStats {
 *     return this.adapter.getProcessorStats()
 *   }
 *
 *   private async process(job: Job<EmailJob>): Promise<void> {
 *     // Process the job...
 *   }
 * }
 * ```
 */
export class QueuebertProcessorAdapter implements QueuebertProcessor {
  private queueContext: QueueContext | null = null;

  constructor(private readonly worker: QueuebertWorkerInterface) {}

  /**
   * Update queue context for performance calculations
   * Call this periodically with queue.getJobCounts() data
   */
  updateQueueContext(context: QueueContext): void {
    this.queueContext = context;
  }

  /**
   * Get processor stats in the format expected by the main Queuebert module
   */
  getProcessorStats(): QueuebertProcessorStats {
    const workerStats = this.worker.getStats();
    const failureRate = this.calculateRate(
      workerStats.totalFailed,
      workerStats.totalProcessed,
    );

    // Calculate performance if we have queue context
    const performance = this.calculatePerformance(workerStats, failureRate);

    return {
      duration: {
        avgMs: workerStats.duration.avgMs,
        minMs: workerStats.duration.minMs,
        maxMs: workerStats.duration.maxMs,
        p50Ms: workerStats.duration.p50Ms,
        p95Ms: workerStats.duration.p95Ms,
        p99Ms: workerStats.duration.p99Ms,
        recentAvgMs: workerStats.duration.recentAvgMs,
        sampleCount: workerStats.duration.sampleCount,
      },
      jobs: {
        processed: workerStats.totalProcessed,
        completed: workerStats.totalCompleted,
        failed: workerStats.totalFailed,
        failureRate,
        successRate: this.calculateRate(
          workerStats.totalCompleted,
          workerStats.totalProcessed,
        ),
        lastJobTime: workerStats.lastJobTime,
      },
      throughput: {
        jobsPerMinute: workerStats.throughput.jobsPerMinute,
        windowStartTime: workerStats.throughput.windowStartTime,
        jobsInWindow: workerStats.throughput.jobsInWindow,
      },
      jobsByType: this.convertJobsByType(workerStats.byJobName),
      custom: {
        activeJobs: workerStats.activeJobs,
        isPaused: workerStats.isPaused,
        isRunning: workerStats.isRunning,
        startedAt: workerStats.startedAt,
        // Include performance in custom if calculated
        ...(performance && { performance }),
      },
    };
  }

  /**
   * Get performance metrics directly (for use outside @queuebert/nest)
   */
  getPerformance(): PerformanceMetrics | null {
    const workerStats = this.worker.getStats();
    const failureRate = this.calculateRate(
      workerStats.totalFailed,
      workerStats.totalProcessed,
    );
    return this.calculatePerformance(workerStats, failureRate);
  }

  /**
   * Calculate performance metrics based on worker stats and queue context
   */
  private calculatePerformance(
    workerStats: QueuebertWorkerStats,
    failureRate: number,
  ): PerformanceMetrics | null {
    const throughputPerMin = workerStats.throughput.jobsPerMinute;
    const avgDurationMs = workerStats.duration.avgMs;
    const isPaused = workerStats.isPaused;

    // If paused, return paused state
    if (isPaused) {
      return {
        score: 0,
        trend: 'paused' as PerformanceTrend,
        throughputPerMin: 0,
        addedPerMin: 0,
        deltaPerMin: 0,
        estimatedClearTimeMs: null,
        health: 'poor' as HealthStatus,
      };
    }

    // Get backlog from queue context if available
    const backlog = this.queueContext
      ? this.queueContext.waiting + this.queueContext.active
      : 0;
    const addedPerMin = this.queueContext?.addedPerMin ?? 0;
    const deltaPerMin = throughputPerMin - addedPerMin;

    // Idle state - no work being done, no backlog
    if (throughputPerMin === 0 && backlog === 0) {
      return {
        score: 100,
        trend: 'idle' as PerformanceTrend,
        throughputPerMin: 0,
        addedPerMin,
        deltaPerMin: -addedPerMin,
        estimatedClearTimeMs: null,
        health: 'excellent' as HealthStatus,
      };
    }

    // Calculate estimated clear time
    let estimatedClearTimeMs: number | null = null;
    if (backlog > 0) {
      if (deltaPerMin > 0) {
        estimatedClearTimeMs = Math.round((backlog / deltaPerMin) * 60000);
      } else if (throughputPerMin > 0) {
        estimatedClearTimeMs = Math.round((backlog / throughputPerMin) * 60000);
      }
    }

    // Determine trend
    let trend: PerformanceTrend;
    if (backlog === 0 && addedPerMin === 0) {
      trend = 'idle';
    } else if (deltaPerMin > 50) {
      trend = 'catching_up';
    } else if (deltaPerMin < -50) {
      trend = 'falling_behind';
    } else if (deltaPerMin >= -10 && deltaPerMin <= 10) {
      trend = 'stable';
    } else if (deltaPerMin > 0) {
      const clearTimeMin = estimatedClearTimeMs
        ? estimatedClearTimeMs / 60000
        : Infinity;
      trend = clearTimeMin < 10 ? 'catching_up' : 'stable';
    } else {
      trend = backlog > 1000 ? 'falling_behind' : 'stable';
    }

    // Calculate performance score
    let score = 100;

    // Penalize negative delta (falling behind)
    if (deltaPerMin < 0) {
      const deltaPenalty = Math.min(40, Math.floor(Math.abs(deltaPerMin) / 10));
      score -= deltaPenalty;
    } else if (deltaPerMin > 100) {
      score = Math.min(100, score + 5);
    }

    // Penalize large backlog
    const backlogPenalty = Math.min(20, Math.floor(backlog / 200));
    score -= backlogPenalty;

    // Penalize high failure rate
    const failurePenalty = Math.min(20, Math.floor(failureRate * 100 * 2));
    score -= failurePenalty;

    // Penalize slow processing
    if (avgDurationMs !== undefined && avgDurationMs > 500) {
      const durationPenalty = Math.min(
        10,
        Math.floor((avgDurationMs - 500) / 100),
      );
      score -= durationPenalty;
    }

    score = Math.max(0, Math.min(100, score));

    // Determine health
    let health: HealthStatus;
    if (score >= 90) {
      health = 'excellent';
    } else if (score >= 70) {
      health = 'good';
    } else if (score >= 50) {
      health = 'fair';
    } else if (score >= 25) {
      health = 'poor';
    } else {
      health = 'critical';
    }

    return {
      score,
      trend,
      throughputPerMin,
      addedPerMin,
      deltaPerMin,
      estimatedClearTimeMs,
      health,
    };
  }

  /**
   * Calculate a rate as a decimal (0-1)
   */
  private calculateRate(numerator: number, denominator: number): number {
    if (denominator === 0) return 0;
    return Math.round((numerator / denominator) * 10000) / 10000;
  }

  /**
   * Convert job-by-name stats to the expected format
   */
  private convertJobsByType(
    byJobName: QueuebertWorkerStats['byJobName'],
  ): Record<string, { processed: number; completed: number; failed: number }> {
    const result: Record<
      string,
      { processed: number; completed: number; failed: number }
    > = {};

    for (const [name, stats] of Object.entries(byJobName)) {
      result[name] = {
        processed: stats.processed,
        completed: stats.completed,
        failed: stats.failed,
      };
    }

    return result;
  }
}

/**
 * Factory function to create an adapter from a worker
 */
export function createProcessorAdapter(
  worker: QueuebertWorkerInterface,
): QueuebertProcessor {
  return new QueuebertProcessorAdapter(worker);
}
