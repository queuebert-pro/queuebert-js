import { MetricsRegistry } from './metrics-collector';
import type { Meter, ObservableCallback } from '@opentelemetry/api';

describe('MetricsRegistry', () => {
  let registry: MetricsRegistry;

  beforeEach(() => {
    registry = new MetricsRegistry({
      durationBuckets: [10, 50, 100, 500, 1000],
      perJobNameMetrics: true,
      maxJobNames: 10,
    });
  });

  describe('recordJobCompleted', () => {
    it('should record a completed job', () => {
      registry.recordJobCompleted('test-queue', 'send-email', 100);

      const metrics = registry.getQueueMetrics('test-queue');

      expect(metrics).not.toBeNull();
      expect(metrics!.counts.completed).toBe(1);
      expect(metrics!.counts.failed).toBe(0);
      expect(metrics!.duration.count).toBe(1);
      expect(metrics!.duration.sum).toBe(100);
    });

    it('should accumulate multiple completed jobs', () => {
      registry.recordJobCompleted('test-queue', 'job-a', 100);
      registry.recordJobCompleted('test-queue', 'job-b', 200);
      registry.recordJobCompleted('test-queue', 'job-a', 150);

      const metrics = registry.getQueueMetrics('test-queue');

      expect(metrics!.counts.completed).toBe(3);
      expect(metrics!.duration.sum).toBe(450); // 100 + 200 + 150
      expect(metrics!.duration.count).toBe(3);
    });

    it('should track per-job-name metrics', () => {
      registry.recordJobCompleted('test-queue', 'email', 100);
      registry.recordJobCompleted('test-queue', 'email', 150);
      registry.recordJobCompleted('test-queue', 'sms', 50);

      const metrics = registry.getQueueMetrics('test-queue');

      expect(metrics!.byJobName).toBeDefined();
      expect(metrics!.byJobName!['email'].count).toBe(2);
      expect(metrics!.byJobName!['email'].duration.sum).toBe(250);
      expect(metrics!.byJobName!['sms'].count).toBe(1);
      expect(metrics!.byJobName!['sms'].duration.sum).toBe(50);
    });
  });

  describe('recordJobFailed', () => {
    it('should record a failed job', () => {
      registry.recordJobFailed('test-queue', 'send-email', 50);

      const metrics = registry.getQueueMetrics('test-queue');

      expect(metrics!.counts.failed).toBe(1);
      expect(metrics!.counts.completed).toBe(0);
    });

    it('should track errors per job name', () => {
      registry.recordJobFailed('test-queue', 'risky-job', 100);
      registry.recordJobFailed('test-queue', 'risky-job', 120);
      registry.recordJobCompleted('test-queue', 'risky-job', 80);

      const metrics = registry.getQueueMetrics('test-queue');

      expect(metrics!.byJobName!['risky-job'].count).toBe(3);
      expect(metrics!.byJobName!['risky-job'].errors).toBe(2);
    });
  });

  describe('updateQueueCounts', () => {
    it('should update queue counts', () => {
      registry.updateQueueCounts('test-queue', {
        waiting: 10,
        active: 3,
        completed: 100,
        failed: 5,
        delayed: 2,
      });

      const metrics = registry.getQueueMetrics('test-queue');

      expect(metrics!.counts.waiting).toBe(10);
      expect(metrics!.counts.active).toBe(3);
      expect(metrics!.counts.delayed).toBe(2);
    });

    it('does not treat absolute queue totals as duration observations', () => {
      registry.recordJobCompleted('test-queue', 'job', 100);
      registry.updateQueueCounts('test-queue', {
        waiting: 0,
        active: 0,
        completed: 100,
        failed: 5,
        delayed: 0,
      });

      const metrics = registry.getQueueMetrics('test-queue');

      expect(metrics!.duration.count).toBe(1);
      expect(metrics!.duration.sum).toBe(100);
      expect(metrics!.errorRate).toBeCloseTo(5 / 105);
    });
  });

  describe('OpenTelemetry instruments', () => {
    it('records counters, histograms, gauges, and removes callbacks', () => {
      const completedAdd = jest.fn();
      const failedAdd = jest.fn();
      const histogramRecord = jest.fn();
      const gaugeCallbacks: ObservableCallback[] = [];
      const removeCallback = jest.fn();
      const meter = {
        createCounter: jest
          .fn()
          .mockReturnValueOnce({ add: completedAdd })
          .mockReturnValueOnce({ add: failedAdd }),
        createHistogram: jest.fn().mockReturnValue({
          record: histogramRecord,
        }),
        createObservableGauge: jest.fn().mockImplementation(() => ({
          addCallback: (callback: ObservableCallback) => {
            gaugeCallbacks.push(callback);
          },
          removeCallback,
        })),
      } as unknown as Meter;
      const registry = new MetricsRegistry(
        { perJobNameMetrics: true, maxJobNames: 1 },
        meter,
      );

      registry.recordJobCompleted('emails', 'welcome', 25);
      registry.recordJobFailed('emails', 'overflow-name', 50);
      registry.updateQueueCounts('emails', {
        waiting: 3,
        active: 1,
        completed: 10,
        failed: 2,
        delayed: 4,
      });

      expect(completedAdd).toHaveBeenCalledWith(1, {
        'queue.name': 'emails',
        'job.name': 'welcome',
      });
      expect(failedAdd).toHaveBeenCalledWith(1, {
        'queue.name': 'emails',
        'job.name': '__other__',
      });
      expect(histogramRecord).toHaveBeenLastCalledWith(50, {
        'queue.name': 'emails',
        'job.name': '__other__',
        'job.success': false,
      });

      const observe = jest.fn();
      for (const callback of gaugeCallbacks) {
        callback({ observe } as never);
      }
      expect(observe).toHaveBeenCalledWith(3, { 'queue.name': 'emails' });
      expect(observe).toHaveBeenCalledWith(1, { 'queue.name': 'emails' });
      expect(observe).toHaveBeenCalledWith(4, { 'queue.name': 'emails' });

      registry.destroy();
      expect(removeCallback).toHaveBeenCalledTimes(4);
    });
  });

  describe('getSnapshot', () => {
    it('should return snapshot of all queue metrics', () => {
      registry.recordJobCompleted('queue-a', 'job', 100);
      registry.recordJobCompleted('queue-b', 'job', 200);
      registry.recordJobFailed('queue-a', 'job', 50);

      const snapshot = registry.getSnapshot();

      expect(snapshot.queues['queue-a']).toBeDefined();
      expect(snapshot.queues['queue-b']).toBeDefined();
      expect(snapshot.queues['queue-a'].counts.completed).toBe(1);
      expect(snapshot.queues['queue-a'].counts.failed).toBe(1);
      expect(snapshot.queues['queue-b'].counts.completed).toBe(1);
      expect(snapshot.timestamp).toBeDefined();
    });

    it('should return empty queues when nothing recorded', () => {
      const snapshot = registry.getSnapshot();

      expect(Object.keys(snapshot.queues)).toHaveLength(0);
    });
  });

  describe('getQueueNames', () => {
    it('should return list of registered queue names', () => {
      registry.recordJobCompleted('alpha', 'job', 100);
      registry.recordJobCompleted('beta', 'job', 100);
      registry.recordJobCompleted('gamma', 'job', 100);

      const names = registry.getQueueNames();

      expect(names).toHaveLength(3);
      expect(names).toContain('alpha');
      expect(names).toContain('beta');
      expect(names).toContain('gamma');
    });
  });

  describe('clear', () => {
    it('should clear all metrics', () => {
      registry.recordJobCompleted('test-queue', 'job', 100);

      expect(registry.getQueueNames()).toHaveLength(1);

      registry.clear();

      expect(registry.getQueueNames()).toHaveLength(0);
      expect(registry.getQueueMetrics('test-queue')).toBeNull();
    });
  });

  describe('duration histogram buckets', () => {
    it('should categorize durations into correct buckets', () => {
      // Record jobs with different durations
      registry.recordJobCompleted('test-queue', 'job', 5); // <= 10
      registry.recordJobCompleted('test-queue', 'job', 10); // <= 10
      registry.recordJobCompleted('test-queue', 'job', 25); // <= 50
      registry.recordJobCompleted('test-queue', 'job', 75); // <= 100
      registry.recordJobCompleted('test-queue', 'job', 250); // <= 500
      registry.recordJobCompleted('test-queue', 'job', 750); // <= 1000

      const metrics = registry.getQueueMetrics('test-queue');
      const buckets = metrics!.duration.buckets;

      // Find specific bucket counts
      const bucket10 = buckets.find((b) => b.le === 10);
      const bucket50 = buckets.find((b) => b.le === 50);
      const bucket100 = buckets.find((b) => b.le === 100);
      const bucket500 = buckets.find((b) => b.le === 500);
      const bucket1000 = buckets.find((b) => b.le === 1000);

      // Buckets are cumulative in histogram format
      expect(bucket10?.count).toBe(2); // 5, 10
      expect(bucket50?.count).toBe(3); // 5, 10, 25
      expect(bucket100?.count).toBe(4); // 5, 10, 25, 75
      expect(bucket500?.count).toBe(5); // 5, 10, 25, 75, 250
      expect(bucket1000?.count).toBe(6); // all
    });
  });

  describe('error rate calculation', () => {
    it('should calculate error rate correctly', () => {
      registry.recordJobCompleted('test-queue', 'job', 100);
      registry.recordJobCompleted('test-queue', 'job', 100);
      registry.recordJobCompleted('test-queue', 'job', 100);
      registry.recordJobFailed('test-queue', 'job', 50);

      const metrics = registry.getQueueMetrics('test-queue');

      // 1 failed out of 4 total = 25%
      expect(metrics!.errorRate).toBe(0.25);
    });

    it('should return 0 error rate when no jobs', () => {
      registry.updateQueueCounts('test-queue', {
        waiting: 5,
        active: 0,
        completed: 0,
        failed: 0,
        delayed: 0,
      });

      const metrics = registry.getQueueMetrics('test-queue');

      expect(metrics!.errorRate).toBe(0);
    });
  });

  describe('throughput calculation', () => {
    it('should calculate throughput (jobs per minute)', () => {
      // Record jobs
      for (let i = 0; i < 30; i++) {
        registry.recordJobCompleted('test-queue', 'job', 100);
      }

      const metrics = registry.getQueueMetrics('test-queue');

      // Throughput depends on time elapsed, just verify it's calculated
      expect(metrics!.throughput).toBeGreaterThanOrEqual(0);
    });
  });

  describe('max job names limit', () => {
    it('should respect max job names limit', () => {
      const registry = new MetricsRegistry({
        perJobNameMetrics: true,
        maxJobNames: 3,
      });

      // Record jobs with different names
      registry.recordJobCompleted('test-queue', 'job-1', 100);
      registry.recordJobCompleted('test-queue', 'job-2', 100);
      registry.recordJobCompleted('test-queue', 'job-3', 100);
      registry.recordJobCompleted('test-queue', 'job-4', 100); // Should be ignored
      registry.recordJobCompleted('test-queue', 'job-5', 100); // Should be ignored
      // But existing ones should still be tracked
      registry.recordJobCompleted('test-queue', 'job-1', 100);

      const metrics = registry.getQueueMetrics('test-queue');

      expect(Object.keys(metrics!.byJobName!)).toHaveLength(4);
      expect(metrics!.byJobName!['job-1'].count).toBe(2);
      expect(metrics!.byJobName!['job-4']).toBeUndefined();
      expect(metrics!.byJobName!['__other__'].count).toBe(2);
    });
  });

  describe('transformToQueuebertStats', () => {
    it('should transform metrics to Queuebert format', () => {
      registry.recordJobCompleted('test-queue', 'email', 100);
      registry.recordJobCompleted('test-queue', 'email', 200);
      registry.recordJobFailed('test-queue', 'email', 50);
      registry.updateQueueCounts('test-queue', {
        waiting: 5,
        active: 2,
        completed: 100,
        failed: 10,
        delayed: 3,
      });

      const stats = registry.transformToQueuebertStats('test-queue');

      expect(stats).not.toBeNull();
      expect(stats!.name).toBe('test-queue');
      expect(stats!.counts.waiting).toBe(5);
      expect(stats!.counts.active).toBe(2);
      expect(stats!.jobMetrics).toBeDefined();
      expect(stats!.jobMetrics.failureRate).toBeGreaterThan(0);
    });

    it('should return null for unknown queue', () => {
      const stats = registry.transformToQueuebertStats('unknown-queue');

      expect(stats).toBeNull();
    });
  });

  describe('transformAllToQueuebertStats', () => {
    it('should transform all queue metrics', () => {
      registry.recordJobCompleted('queue-a', 'job', 100);
      registry.recordJobCompleted('queue-b', 'job', 200);

      const allStats = registry.transformAllToQueuebertStats();

      expect(Object.keys(allStats)).toHaveLength(2);
      expect(allStats['queue-a']).toBeDefined();
      expect(allStats['queue-b']).toBeDefined();
    });
  });
});
