import {
  DurationStatsCollector,
  GlobalStatsCollector,
} from './stats-collector';
import type { JobDurationRecord, QueuebertWorkerInterface } from './types';

// Helper to create a job record with defaults
function createRecord(
  overrides: Partial<JobDurationRecord> &
    Pick<JobDurationRecord, 'jobId' | 'jobName' | 'success'>,
): JobDurationRecord {
  const now = Date.now();
  return {
    startedAt: now - 100, // Default: started 100ms ago
    completedAt: now,
    ...overrides,
  };
}

describe('DurationStatsCollector', () => {
  let collector: DurationStatsCollector;

  beforeEach(() => {
    collector = new DurationStatsCollector({
      windowMs: 60000,
      maxSamples: 100,
      pruneIntervalMs: 100000, // Long interval to avoid pruning during tests
    });
  });

  afterEach(() => {
    collector.destroy();
  });

  describe('record', () => {
    it('should record a job duration sample', () => {
      const record = createRecord({
        jobId: 'job-1',
        jobName: 'test-job',
        duration: 100,
        success: true,
      });

      collector.record(record);

      expect(collector.getAllSamples()).toHaveLength(1);
      expect(collector.getAllSamples()[0]).toEqual(record);
    });

    it('should enforce max samples limit', () => {
      const collector = new DurationStatsCollector({
        windowMs: 60000,
        maxSamples: 5,
        pruneIntervalMs: 100000,
      });

      for (let i = 0; i < 10; i++) {
        collector.record(
          createRecord({
            jobId: `job-${i}`,
            jobName: 'test-job',
            duration: 100,
            success: true,
          }),
        );
      }

      expect(collector.getAllSamples()).toHaveLength(5);
      // Should keep the most recent samples
      expect(collector.getAllSamples()[0].jobId).toBe('job-5');
      expect(collector.getTotals()).toEqual({
        processed: 10,
        completed: 10,
        failed: 0,
      });

      collector.destroy();
    });
  });

  describe('getWindowSamples', () => {
    it('should return samples within the rolling window', () => {
      const now = Date.now();

      // Recent sample (within window)
      collector.record(
        createRecord({
          jobId: 'job-recent',
          jobName: 'test-job',
          duration: 100,
          success: true,
          startedAt: now - 30100,
          completedAt: now - 30000, // 30 seconds ago
        }),
      );

      // Old sample (outside window)
      collector.record(
        createRecord({
          jobId: 'job-old',
          jobName: 'test-job',
          duration: 100,
          success: true,
          startedAt: now - 120100,
          completedAt: now - 120000, // 2 minutes ago
        }),
      );

      const windowSamples = collector.getWindowSamples();

      expect(windowSamples).toHaveLength(1);
      expect(windowSamples[0].jobId).toBe('job-recent');
    });

    it('should exclude samples without completedAt', () => {
      collector.record({
        jobId: 'job-1',
        jobName: 'test-job',
        startedAt: Date.now(),
        duration: 100,
        success: true,
        // No completedAt
      });

      expect(collector.getWindowSamples()).toHaveLength(0);
    });
  });

  describe('calculateStats', () => {
    it('should calculate correct average', () => {
      collector.record(
        createRecord({
          jobId: 'j1',
          jobName: 'test',
          duration: 100,
          success: true,
        }),
      );
      collector.record(
        createRecord({
          jobId: 'j2',
          jobName: 'test',
          duration: 200,
          success: true,
        }),
      );
      collector.record(
        createRecord({
          jobId: 'j3',
          jobName: 'test',
          duration: 300,
          success: true,
        }),
      );

      const stats = collector.calculateStats();

      expect(stats.avgMs).toBe(200);
      expect(stats.sampleCount).toBe(3);
    });

    it('should calculate min and max', () => {
      collector.record(
        createRecord({
          jobId: 'j1',
          jobName: 'test',
          duration: 50,
          success: true,
        }),
      );
      collector.record(
        createRecord({
          jobId: 'j2',
          jobName: 'test',
          duration: 200,
          success: true,
        }),
      );
      collector.record(
        createRecord({
          jobId: 'j3',
          jobName: 'test',
          duration: 500,
          success: true,
        }),
      );

      const stats = collector.calculateStats();

      expect(stats.minMs).toBe(50);
      expect(stats.maxMs).toBe(500);
    });

    it('should calculate percentiles correctly', () => {
      // Add 100 samples with durations 1-100
      for (let i = 1; i <= 100; i++) {
        collector.record(
          createRecord({
            jobId: `j${i}`,
            jobName: 'test',
            duration: i,
            success: true,
          }),
        );
      }

      const stats = collector.calculateStats();

      expect(stats.p50Ms).toBe(50);
      expect(stats.p95Ms).toBe(95);
      expect(stats.p99Ms).toBe(99);
    });

    it('should return undefined for empty samples', () => {
      const stats = collector.calculateStats();

      expect(stats.avgMs).toBeUndefined();
      expect(stats.minMs).toBeUndefined();
      expect(stats.maxMs).toBeUndefined();
      expect(stats.p50Ms).toBeUndefined();
      expect(stats.p95Ms).toBeUndefined();
      expect(stats.p99Ms).toBeUndefined();
      expect(stats.sampleCount).toBe(0);
    });

    it('should filter out samples without duration', () => {
      collector.record(
        createRecord({ jobId: 'j1', jobName: 'test', success: true }),
      );
      collector.record(
        createRecord({
          jobId: 'j2',
          jobName: 'test',
          duration: 100,
          success: true,
        }),
      );

      const stats = collector.calculateStats();

      expect(stats.sampleCount).toBe(1);
      expect(stats.avgMs).toBe(100);
    });
  });

  describe('calculateThroughput', () => {
    it('should calculate jobs per minute', () => {
      const now = Date.now();

      // Add 30 jobs in the last 30 seconds (half the window)
      for (let i = 0; i < 30; i++) {
        const completedAt = now - (30000 - i * 1000);
        collector.record(
          createRecord({
            jobId: `j${i}`,
            jobName: 'test',
            duration: 100,
            success: true,
            startedAt: completedAt - 100,
            completedAt,
          }),
        );
      }

      const throughput = collector.calculateThroughput();

      expect(throughput.jobsInWindow).toBe(30);
      // 30 jobs in 60 second window = 30 jobs/min
      expect(throughput.jobsPerMinute).toBe(30);
    });

    it('should return zero for empty window', () => {
      const throughput = collector.calculateThroughput();

      expect(throughput.jobsInWindow).toBe(0);
      expect(throughput.jobsPerMinute).toBe(0);
    });
  });

  describe('getStatsByJobName', () => {
    it('should group stats by job name', () => {
      collector.record(
        createRecord({
          jobId: 'j1',
          jobName: 'email',
          duration: 100,
          success: true,
        }),
      );
      collector.record(
        createRecord({
          jobId: 'j2',
          jobName: 'email',
          duration: 150,
          success: true,
        }),
      );
      collector.record(
        createRecord({
          jobId: 'j3',
          jobName: 'sms',
          duration: 50,
          success: true,
        }),
      );
      collector.record(
        createRecord({
          jobId: 'j4',
          jobName: 'sms',
          duration: 60,
          success: false,
        }),
      );

      const statsByName = collector.getStatsByJobName();

      expect(statsByName['email']).toEqual({
        processed: 2,
        completed: 2,
        failed: 0,
        avgDurationMs: 125,
      });

      expect(statsByName['sms']).toEqual({
        processed: 2,
        completed: 1,
        failed: 1,
        avgDurationMs: 55,
      });
    });

    it('should handle job types with no duration', () => {
      collector.record(
        createRecord({ jobId: 'j1', jobName: 'quick', success: true }),
      );
      collector.record(
        createRecord({ jobId: 'j2', jobName: 'quick', success: false }),
      );

      const statsByName = collector.getStatsByJobName();

      expect(statsByName['quick']).toEqual({
        processed: 2,
        completed: 1,
        failed: 1,
        avgDurationMs: undefined,
      });
    });
  });

  describe('clear', () => {
    it('should clear all samples', () => {
      collector.record(
        createRecord({
          jobId: 'j1',
          jobName: 'test',
          duration: 100,
          success: true,
        }),
      );
      collector.record(
        createRecord({
          jobId: 'j2',
          jobName: 'test',
          duration: 200,
          success: true,
        }),
      );

      expect(collector.getAllSamples()).toHaveLength(2);

      collector.clear();

      expect(collector.getAllSamples()).toHaveLength(0);
      expect(collector.getTotals()).toEqual({
        processed: 0,
        completed: 0,
        failed: 0,
      });
    });
  });
});

describe('GlobalStatsCollector', () => {
  let collector: GlobalStatsCollector;

  beforeEach(() => {
    collector = new GlobalStatsCollector();
  });

  function createMockWorker(
    stats: Partial<ReturnType<QueuebertWorkerInterface['getStats']>> = {},
  ): QueuebertWorkerInterface {
    return {
      getStats: jest.fn().mockReturnValue({
        totalCompleted: 100,
        totalFailed: 5,
        activeJobs: 3,
        isPaused: false,
        duration: {
          avgMs: 150,
          minMs: 50,
          maxMs: 500,
          p50Ms: 100,
          p95Ms: 400,
          p99Ms: 480,
          recentAvgMs: 120,
          sampleCount: 100,
        },
        throughput: {
          jobsPerMinute: 60,
          windowStartTime: new Date().toISOString(),
          jobsInWindow: 60,
        },
        jobs: {
          processed: 105,
          completed: 100,
          failed: 5,
          failureRate: 4.76,
          successRate: 95.24,
          lastJobTime: new Date().toISOString(),
        },
        jobsByType: {
          email: { processed: 50, completed: 48, failed: 2 },
          sms: { processed: 55, completed: 52, failed: 3 },
        },
        custom: {},
        ...stats,
      }),
      pause: jest.fn(),
      resume: jest.fn(),
      close: jest.fn(),
    } as unknown as QueuebertWorkerInterface;
  }

  describe('registerWorker', () => {
    it('should register a worker', () => {
      const worker = createMockWorker();

      collector.registerWorker('test-queue', worker);

      expect(collector.hasQueue('test-queue')).toBe(true);
      expect(collector.getRegisteredQueues()).toContain('test-queue');
    });
  });

  describe('unregisterWorker', () => {
    it('should unregister a worker', () => {
      const worker = createMockWorker();

      collector.registerWorker('test-queue', worker);
      collector.unregisterWorker('test-queue');

      expect(collector.hasQueue('test-queue')).toBe(false);
    });
  });

  describe('getQueueStats', () => {
    it('should return stats for a registered queue', async () => {
      const worker = createMockWorker({ totalCompleted: 200, totalFailed: 10 });

      collector.registerWorker('email-queue', worker);

      const stats = await collector.getQueueStats('email-queue');

      expect(stats).not.toBeNull();
      expect(stats!.queueName).toBe('email-queue');
      expect(stats!.queueStats.completed).toBe(200);
      expect(stats!.queueStats.failed).toBe(10);
    });

    it('should return null for unregistered queue', async () => {
      const stats = await collector.getQueueStats('unknown-queue');

      expect(stats).toBeNull();
    });
  });

  describe('getAggregatedStats', () => {
    it('should return stats for all registered queues', async () => {
      collector.registerWorker(
        'queue-a',
        createMockWorker({ totalCompleted: 100 }),
      );
      collector.registerWorker(
        'queue-b',
        createMockWorker({ totalCompleted: 200 }),
      );

      const aggregated = await collector.getAggregatedStats();

      expect(Object.keys(aggregated)).toHaveLength(2);
      expect(aggregated['queue-a']).toBeDefined();
      expect(aggregated['queue-b']).toBeDefined();
    });

    it('should return empty object when no workers registered', async () => {
      const aggregated = await collector.getAggregatedStats();

      expect(aggregated).toEqual({});
    });
  });

  describe('getRegisteredQueues', () => {
    it('should return list of registered queue names', () => {
      collector.registerWorker('queue-1', createMockWorker());
      collector.registerWorker('queue-2', createMockWorker());
      collector.registerWorker('queue-3', createMockWorker());

      const queues = collector.getRegisteredQueues();

      expect(queues).toHaveLength(3);
      expect(queues).toContain('queue-1');
      expect(queues).toContain('queue-2');
      expect(queues).toContain('queue-3');
    });
  });
});
