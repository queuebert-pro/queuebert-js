import {
  createProcessorAdapter,
  QueuebertProcessorAdapter,
} from './queuebert-processor-adapter';
import type { QueuebertWorkerInterface, QueuebertWorkerStats } from './types';

describe('QueuebertProcessorAdapter', () => {
  function createWorker(
    stats: Partial<QueuebertWorkerStats> = {},
  ): QueuebertWorkerInterface {
    const baseStats: QueuebertWorkerStats = {
      totalProcessed: 10,
      totalCompleted: 8,
      totalFailed: 2,
      activeJobs: 1,
      duration: {
        avgMs: 120,
        minMs: 10,
        maxMs: 500,
        p50Ms: 100,
        p95Ms: 450,
        p99Ms: 500,
        recentAvgMs: 90,
        sampleCount: 10,
      },
      throughput: {
        jobsPerMinute: 12,
        windowStartTime: '2026-01-01T00:00:00.000Z',
        jobsInWindow: 12,
      },
      byJobName: {
        welcome: {
          processed: 6,
          completed: 5,
          failed: 1,
          avgDurationMs: 100,
        },
        digest: {
          processed: 4,
          completed: 3,
          failed: 1,
          avgDurationMs: 140,
        },
      },
      startedAt: '2026-01-01T00:00:00.000Z',
      lastJobTime: '2026-01-01T00:01:00.000Z',
      isPaused: false,
      isRunning: true,
      ...stats,
    };

    return {
      queueName: 'emails',
      isRunning: baseStats.isRunning,
      isPaused: baseStats.isPaused,
      getStats: jest.fn().mockReturnValue(baseStats),
      on: jest.fn(),
      off: jest.fn(),
      pause: jest.fn(),
      resume: jest.fn(),
      close: jest.fn(),
    };
  }

  it('adapts worker stats to QueuebertProcessor stats', () => {
    const adapter = new QueuebertProcessorAdapter(createWorker());
    adapter.updateQueueContext({
      waiting: 20,
      active: 5,
      delayed: 3,
      addedPerMin: 4,
    });

    const stats = adapter.getProcessorStats();

    expect(stats.jobs).toMatchObject({
      processed: 10,
      completed: 8,
      failed: 2,
      failureRate: 0.2,
      successRate: 0.8,
    });
    expect(stats.jobsByType).toEqual({
      welcome: { processed: 6, completed: 5, failed: 1 },
      digest: { processed: 4, completed: 3, failed: 1 },
    });
    expect(stats.custom?.['performance']).toMatchObject({
      trend: 'stable',
      throughputPerMin: 12,
      addedPerMin: 4,
    });
  });

  it('returns paused performance for paused workers', () => {
    const adapter = new QueuebertProcessorAdapter(
      createWorker({ isPaused: true }),
    );

    expect(adapter.getPerformance()).toMatchObject({
      score: 0,
      trend: 'paused',
      health: 'poor',
    });
  });

  it('returns zero rates when no jobs have been processed', () => {
    const adapter = new QueuebertProcessorAdapter(
      createWorker({
        totalProcessed: 0,
        totalCompleted: 0,
        totalFailed: 0,
        throughput: {
          jobsPerMinute: 0,
          windowStartTime: '2026-01-01T00:00:00.000Z',
          jobsInWindow: 0,
        },
        byJobName: {},
      }),
    );

    const stats = adapter.getProcessorStats();

    expect(stats.jobs.failureRate).toBe(0);
    expect(stats.jobs.successRate).toBe(0);
    expect(stats.custom?.['performance']).toMatchObject({
      trend: 'idle',
      health: 'excellent',
    });
  });

  it('creates adapters via factory function', () => {
    const adapter = createProcessorAdapter(createWorker());

    expect(adapter).toBeInstanceOf(QueuebertProcessorAdapter);
  });
});
