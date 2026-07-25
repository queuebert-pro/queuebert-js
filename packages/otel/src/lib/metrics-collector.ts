import type {
  Counter,
  Histogram,
  Meter,
  ObservableCallback,
  ObservableGauge,
} from '@opentelemetry/api';

import { METRIC_NAMES } from './instrumentation';
import type {
  MetricsConfig,
  OTelMetricsSnapshot,
  OTelQueueMetrics,
  TransformedQueueStats,
} from './types';

const DEFAULT_BUCKETS = [5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000];
const OTHER_JOB_NAME = '__other__';

/**
 * Collects Queuebert snapshots and emits the same events through a real
 * OpenTelemetry Meter when one is supplied.
 */
export class MetricsRegistry {
  private readonly queues = new Map<string, QueueMetricsData>();
  private readonly config: Required<MetricsConfig>;
  private readonly resource: Record<string, string | number | boolean>;
  private readonly completedCounter?: Counter;
  private readonly failedCounter?: Counter;
  private readonly durationHistogram?: Histogram;
  private readonly observableCallbacks: Array<{
    instrument: ObservableGauge;
    callback: ObservableCallback;
  }> = [];

  constructor(
    config: MetricsConfig = {},
    meter?: Meter,
    resource: Record<string, string | number | boolean> = {},
  ) {
    this.config = {
      durationBuckets: normalizeBuckets(config.durationBuckets),
      perJobNameMetrics: config.perJobNameMetrics ?? true,
      maxJobNames: normalizePositiveInteger(
        config.maxJobNames ?? 100,
        'maxJobNames',
      ),
      throughputWindowMs: normalizePositiveInteger(
        config.throughputWindowMs ?? 60000,
        'throughputWindowMs',
      ),
    };
    this.resource = { ...resource };

    if (meter) {
      this.completedCounter = meter.createCounter(METRIC_NAMES.JOB_COMPLETED, {
        description: 'Jobs completed successfully',
        unit: '{job}',
      });
      this.failedCounter = meter.createCounter(METRIC_NAMES.JOB_FAILED, {
        description: 'Jobs that failed',
        unit: '{job}',
      });
      this.durationHistogram = meter.createHistogram(
        METRIC_NAMES.JOB_DURATION,
        {
          description: 'Job processing duration',
          unit: 'ms',
        },
      );
      this.addGauge(
        meter,
        METRIC_NAMES.JOB_WAITING,
        'Waiting jobs',
        (queue) => queue.waiting,
      );
      this.addGauge(
        meter,
        METRIC_NAMES.JOB_ACTIVE,
        'Active jobs',
        (queue) => queue.active,
      );
      this.addGauge(
        meter,
        METRIC_NAMES.JOB_DELAYED,
        'Delayed jobs',
        (queue) => queue.delayed,
      );
      this.addGauge(
        meter,
        METRIC_NAMES.QUEUE_THROUGHPUT,
        'Jobs completed or failed per minute',
        (queue) => queue.getThroughput(this.config.throughputWindowMs).rate,
        '{job}/min',
      );
    }
  }

  recordJobCompleted(
    queueName: string,
    jobName: string,
    durationMs: number,
  ): void {
    this.record(queueName, jobName, durationMs, true);
  }

  recordJobFailed(
    queueName: string,
    jobName: string,
    durationMs: number,
  ): void {
    this.record(queueName, jobName, durationMs, false);
  }

  updateQueueCounts(
    queueName: string,
    counts: {
      waiting: number;
      active: number;
      completed: number;
      failed: number;
      delayed: number;
    },
  ): void {
    validateName(queueName, 'queueName');
    for (const [name, value] of Object.entries(counts)) {
      if (!Number.isSafeInteger(value) || value < 0) {
        throw new TypeError(`${name} must be a non-negative safe integer`);
      }
    }

    const queue = this.getOrCreateQueue(queueName);
    Object.assign(queue, counts);
  }

  getSnapshot(): OTelMetricsSnapshot {
    const queues: Record<string, OTelQueueMetrics> = {};
    for (const [name, data] of this.queues) {
      queues[name] = data.toOTelMetrics(
        this.config.durationBuckets,
        this.config.throughputWindowMs,
      );
    }
    return {
      queues,
      timestamp: new Date().toISOString(),
      resource: { ...this.resource },
    };
  }

  getQueueMetrics(queueName: string): OTelQueueMetrics | null {
    const queue = this.queues.get(queueName);
    return (
      queue?.toOTelMetrics(
        this.config.durationBuckets,
        this.config.throughputWindowMs,
      ) ?? null
    );
  }

  transformToQueuebertStats(queueName: string): TransformedQueueStats | null {
    const metrics = this.getQueueMetrics(queueName);
    return metrics ? transformOTelToQueuebert(metrics) : null;
  }

  transformAllToQueuebertStats(): Record<string, TransformedQueueStats> {
    const result: Record<string, TransformedQueueStats> = {};
    for (const name of this.queues.keys()) {
      const stats = this.transformToQueuebertStats(name);
      if (stats) result[name] = stats;
    }
    return result;
  }

  clear(): void {
    this.queues.clear();
  }

  destroy(): void {
    for (const { instrument, callback } of this.observableCallbacks) {
      instrument.removeCallback(callback);
    }
    this.observableCallbacks.length = 0;
    this.clear();
  }

  getQueueNames(): string[] {
    return Array.from(this.queues.keys());
  }

  private record(
    queueName: string,
    jobName: string,
    durationMs: number,
    success: boolean,
  ): void {
    validateName(queueName, 'queueName');
    validateName(jobName, 'jobName');
    if (!Number.isFinite(durationMs) || durationMs < 0) {
      throw new TypeError('durationMs must be a non-negative finite number');
    }

    const queue = this.getOrCreateQueue(queueName);
    const metricJobName = this.config.perJobNameMetrics
      ? queue.resolveJobName(jobName, this.config.maxJobNames)
      : undefined;
    queue.record(
      durationMs,
      success,
      metricJobName,
      this.config.throughputWindowMs,
      this.config.durationBuckets,
    );

    const attributes = {
      'queue.name': queueName,
      ...(metricJobName ? { 'job.name': metricJobName } : {}),
    };
    if (success) this.completedCounter?.add(1, attributes);
    else this.failedCounter?.add(1, attributes);
    this.durationHistogram?.record(durationMs, {
      ...attributes,
      'job.success': success,
    });
  }

  private getOrCreateQueue(queueName: string): QueueMetricsData {
    let queue = this.queues.get(queueName);
    if (!queue) {
      queue = new QueueMetricsData(queueName);
      this.queues.set(queueName, queue);
    }
    return queue;
  }

  private addGauge(
    meter: Meter,
    name: string,
    description: string,
    read: (queue: QueueMetricsData) => number,
    unit = '{job}',
  ): void {
    const instrument = meter.createObservableGauge(name, { description, unit });
    const callback: ObservableCallback = (result) => {
      for (const queue of this.queues.values()) {
        result.observe(read(queue), { 'queue.name': queue.name });
      }
    };
    instrument.addCallback(callback);
    this.observableCallbacks.push({ instrument, callback });
  }
}

class QueueMetricsData {
  completed = 0;
  failed = 0;
  totalDuration = 0;
  durationCount = 0;
  waiting = 0;
  active = 0;
  delayed = 0;
  minDuration: number | undefined;
  maxDuration: number | undefined;
  lastJobTime: string | null = null;

  private readonly durationBucketCounts = new Map<number, number>();
  private readonly byJobName = new Map<string, JobNameMetrics>();
  private readonly trackedJobNames = new Set<string>();
  private readonly recentBuckets = new Map<
    number,
    { count: number; duration: number }
  >();

  constructor(readonly name: string) {}

  resolveJobName(jobName: string, maxEntries: number): string {
    if (this.trackedJobNames.has(jobName)) return jobName;
    if (this.trackedJobNames.size < maxEntries) {
      this.trackedJobNames.add(jobName);
      return jobName;
    }
    return OTHER_JOB_NAME;
  }

  record(
    durationMs: number,
    success: boolean,
    jobName: string | undefined,
    windowMs: number,
    durationBuckets?: number[],
  ): void {
    if (success) this.completed++;
    else this.failed++;
    this.totalDuration += durationMs;
    this.durationCount++;
    this.minDuration = Math.min(this.minDuration ?? durationMs, durationMs);
    this.maxDuration = Math.max(this.maxDuration ?? durationMs, durationMs);
    for (const bucket of durationBuckets ?? []) {
      if (durationMs <= bucket) {
        this.durationBucketCounts.set(
          bucket,
          (this.durationBucketCounts.get(bucket) ?? 0) + 1,
        );
      }
    }
    this.lastJobTime = new Date().toISOString();
    const timeBucket = Math.floor(Date.now() / 1000) * 1000;
    const recent = this.recentBuckets.get(timeBucket) ?? {
      count: 0,
      duration: 0,
    };
    recent.count++;
    recent.duration += durationMs;
    this.recentBuckets.set(timeBucket, recent);
    this.pruneRecent(windowMs);

    if (jobName) {
      const metrics = this.byJobName.get(jobName) ?? {
        count: 0,
        totalDuration: 0,
        errors: 0,
      };
      metrics.count++;
      metrics.totalDuration += durationMs;
      if (!success) metrics.errors++;
      this.byJobName.set(jobName, metrics);
    }
  }

  getThroughput(windowMs: number): {
    rate: number;
    jobs: number;
    start: number;
  } {
    this.pruneRecent(windowMs);
    const jobs = Array.from(this.recentBuckets.values()).reduce(
      (sum, bucket) => sum + bucket.count,
      0,
    );
    return {
      rate: Math.round((jobs / (windowMs / 60000)) * 100) / 100,
      jobs,
      start: Date.now() - windowMs,
    };
  }

  toOTelMetrics(buckets: number[], windowMs: number): OTelQueueMetrics {
    const total = this.completed + this.failed;
    const throughput = this.getThroughput(windowMs);
    const recentSum = Array.from(this.recentBuckets.values()).reduce(
      (sum, bucket) => sum + bucket.duration,
      0,
    );
    const byJobName: NonNullable<OTelQueueMetrics['byJobName']> = {};
    for (const [name, metrics] of this.byJobName) {
      byJobName[name] = {
        count: metrics.count,
        duration: { sum: metrics.totalDuration, count: metrics.count },
        errors: metrics.errors,
      };
    }

    return {
      name: this.name,
      counts: {
        waiting: this.waiting,
        active: this.active,
        completed: this.completed,
        failed: this.failed,
        delayed: this.delayed,
      },
      duration: {
        sum: this.totalDuration,
        count: this.durationCount,
        min: this.minDuration,
        max: this.maxDuration,
        recentSum,
        recentCount: throughput.jobs,
        buckets: buckets.map((le) => ({
          le,
          count: this.durationBucketCounts.get(le) ?? 0,
        })),
      },
      throughput: throughput.rate,
      throughputWindow: {
        startTime: new Date(throughput.start).toISOString(),
        jobs: throughput.jobs,
      },
      lastJobTime: this.lastJobTime,
      errorRate: total > 0 ? this.failed / total : 0,
      byJobName: Object.keys(byJobName).length > 0 ? byJobName : undefined,
    };
  }

  private pruneRecent(windowMs: number): void {
    const cutoff = Date.now() - windowMs;
    for (const timestamp of this.recentBuckets.keys()) {
      if (timestamp < cutoff) this.recentBuckets.delete(timestamp);
    }
  }
}

interface JobNameMetrics {
  count: number;
  totalDuration: number;
  errors: number;
}

export function transformOTelToQueuebert(
  metrics: OTelQueueMetrics,
): TransformedQueueStats {
  const processed = metrics.counts.completed + metrics.counts.failed;
  const avgMs =
    metrics.duration.count > 0
      ? metrics.duration.sum / metrics.duration.count
      : undefined;
  const recentAvgMs =
    metrics.duration.recentCount > 0
      ? metrics.duration.recentSum / metrics.duration.recentCount
      : undefined;
  const percentile = (value: number) =>
    calculatePercentileFromBuckets(
      metrics.duration.buckets,
      value,
      metrics.duration.count,
      metrics.duration.max,
    );

  const byType: Record<
    string,
    { processed: number; completed: number; failed: number }
  > = {};
  for (const [name, data] of Object.entries(metrics.byJobName ?? {})) {
    byType[name] = {
      processed: data.count,
      completed: data.count - data.errors,
      failed: data.errors,
    };
  }

  const health = calculateHealth(metrics.errorRate, metrics.counts.waiting);
  const trend = calculateTrend(metrics.throughput, metrics.counts.waiting);

  return {
    name: metrics.name,
    paused: false,
    counts: {
      ...metrics.counts,
      total:
        metrics.counts.waiting +
        metrics.counts.active +
        metrics.counts.completed +
        metrics.counts.failed +
        metrics.counts.delayed,
    },
    jobMetrics: {
      duration: {
        avgMs,
        minMs: metrics.duration.min,
        maxMs: metrics.duration.max,
        p50Ms: percentile(50),
        p95Ms: percentile(95),
        p99Ms: percentile(99),
        recentAvgMs,
      },
      failureRate: metrics.errorRate,
      successRate: processed > 0 ? 1 - metrics.errorRate : 0,
      processed,
      completed: metrics.counts.completed,
      failed: metrics.counts.failed,
      byType: Object.keys(byType).length > 0 ? byType : undefined,
      lastJobTime: metrics.lastJobTime,
      sampleCount: metrics.duration.count,
      performance: {
        score: calculatePerformanceScore(metrics),
        trend,
        throughputPerMin: metrics.throughput,
        addedPerMin: 0,
        deltaPerMin: 0,
        estimatedClearTimeMs: calculateEstimatedClearTime(metrics),
        health,
      },
    },
    throughput: {
      jobsPerMinute: metrics.throughput,
      windowStartTime: metrics.throughputWindow.startTime,
      jobsInWindow: metrics.throughputWindow.jobs,
    },
  };
}

function calculatePercentileFromBuckets(
  buckets: { le: number; count: number }[],
  percentile: number,
  total: number,
  max: number | undefined,
): number | undefined {
  if (total === 0) return undefined;
  const targetCount = (percentile / 100) * total;
  for (const bucket of buckets) {
    if (bucket.count >= targetCount) return bucket.le;
  }
  return max;
}

function calculateHealth(
  errorRate: number,
  waiting: number,
): 'excellent' | 'good' | 'fair' | 'poor' | 'critical' {
  if (errorRate > 0.5) return 'critical';
  if (errorRate > 0.2) return 'poor';
  if (errorRate > 0.1 || waiting > 1000) return 'fair';
  if (errorRate > 0.05 || waiting > 100) return 'good';
  return 'excellent';
}

function calculateTrend(
  throughput: number,
  waiting: number,
): 'catching_up' | 'falling_behind' | 'stable' | 'idle' | 'paused' {
  if (throughput === 0 && waiting === 0) return 'idle';
  if (throughput === 0 && waiting > 0) return 'falling_behind';
  if (waiting > throughput * 10) return 'falling_behind';
  if (waiting < throughput) return 'catching_up';
  return 'stable';
}

function calculatePerformanceScore(metrics: OTelQueueMetrics): number {
  let score = 100 - metrics.errorRate * 50;
  if (metrics.counts.waiting > 1000) score -= 20;
  else if (metrics.counts.waiting > 100) score -= 10;
  else if (metrics.counts.waiting > 10) score -= 5;
  if (metrics.throughput > 100) score += 5;
  else if (metrics.throughput < 1) score -= 10;
  return Math.max(0, Math.min(100, Math.round(score)));
}

function calculateEstimatedClearTime(metrics: OTelQueueMetrics): number | null {
  if (metrics.counts.waiting === 0) return 0;
  if (metrics.throughput === 0) return null;
  return Math.round((metrics.counts.waiting / metrics.throughput) * 60000);
}

function normalizeBuckets(values: number[] | undefined): number[] {
  const buckets = values ?? DEFAULT_BUCKETS;
  if (buckets.some((value) => !Number.isFinite(value) || value <= 0)) {
    throw new TypeError('durationBuckets must contain positive finite numbers');
  }
  return Array.from(new Set(buckets)).sort((a, b) => a - b);
}

function normalizePositiveInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive safe integer`);
  }
  return value;
}

function validateName(value: string, name: string): void {
  if (value.trim().length === 0) {
    throw new TypeError(`${name} must not be empty`);
  }
}
