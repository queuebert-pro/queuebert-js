import { Injectable, Inject, Logger, OnModuleDestroy } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import type { Queue } from 'bullmq';

import type { QueuebertIntegrationRegistry } from './integration-registry';
import {
  QUEUEBERT_OPTIONS,
  QUEUEBERT_INTEGRATION_REGISTRY,
  READONLY_OPTIONAL_ENDPOINTS,
  DEFAULT_REDIS_INSTANCE_ID,
  DEFAULT_REDIS_INSTANCE_LABEL,
} from './types';
import type {
  QueuebertModuleOptions,
  QueuebertProcessor,
  RateSample,
  PerformanceMetrics,
  QueueCounts,
  SingleQueueStats,
  MultiQueueStats,
  CleanResult,
  DrainResult,
  StatusResult,
  RedisMemoryStats,
  RedisInstanceStats,
  QueuebertEndpoint,
  QueuebertCapabilities,
  JobTypesDiscovery,
  QueueJobTypeStats,
  MigrationParams,
  MigrationPreview,
  MigrationResult,
  MigrationJobInfo,
  MigrationJobState,
  MigrationStatus,
  MigrationStartResponse,
  MigrationCancelResponse,
  MigrationPauseResumeResponse,
  CacheInstanceStats,
  QueuebertCacheConfig,
  CacheMigrationParams,
  CacheMigrationPreview,
  CacheMigrationStatus,
} from './types';

/**
 * Internal state for tracking active migrations
 */
interface ActiveMigration {
  status: MigrationStatus;
  abortController: AbortController;
  sourceQueue: Queue;
  targetQueue: Queue;
  /** Flag to pause the migration */
  isPaused: boolean;
  /** Promise resolver to resume from pause */
  resumeResolver?: () => void;
  cleanupTimer?: ReturnType<typeof setTimeout>;
  runPromise?: Promise<void>;
}

interface MigrationLease {
  refresh(): Promise<void>;
  release(): Promise<void>;
}

interface MigrationQueueGuard {
  restore(leavePaused: boolean): Promise<void>;
}

class MigrationRollbackError extends Error {}

const MIGRATION_LOCK_TTL_MS = 5 * 60 * 1000;
const MAX_MIGRATION_ERRORS = 100;

@Injectable()
export class QueuebertService implements OnModuleDestroy {
  private readonly logger = new Logger(QueuebertService.name);
  private readonly enabledEndpoints: Set<QueuebertEndpoint>;

  /** Active migrations store (in-memory) */
  private readonly activeMigrations: Map<string, ActiveMigration> = new Map();
  /** Counter for generating unique migration IDs */
  private migrationCounter = 0;
  private readonly rateSamples = new Map<string, RateSample>();
  private isShuttingDown = false;

  constructor(
    @Inject(QUEUEBERT_OPTIONS) private readonly options: QueuebertModuleOptions,
    @Inject(QUEUEBERT_INTEGRATION_REGISTRY)
    private readonly integrationRegistry: QueuebertIntegrationRegistry,
  ) {
    // Build set of enabled endpoints (stats is always enabled).
    // Default to read-only behavior; destructive controls must be opted in.
    const optionalEndpoints = options.endpoints ?? READONLY_OPTIONAL_ENDPOINTS;
    this.enabledEndpoints = new Set<QueuebertEndpoint>([
      'stats',
      ...optionalEndpoints,
    ]);

    for (const integration of options.integrations ?? []) {
      this.integrationRegistry.register(integration);
    }
  }

  async onModuleDestroy(): Promise<void> {
    this.isShuttingDown = true;
    const runPromises: Promise<void>[] = [];

    for (const migration of this.activeMigrations.values()) {
      migration.abortController.abort();
      migration.resumeResolver?.();
      if (migration.cleanupTimer) clearTimeout(migration.cleanupTimer);
      if (migration.runPromise) runPromises.push(migration.runPromise);
    }
    for (const migration of this.activeCacheMigrations.values()) {
      migration.abortController.abort();
      migration.resumeResolver?.();
      if (migration.cleanupTimer) clearTimeout(migration.cleanupTimer);
      if (migration.runPromise) runPromises.push(migration.runPromise);
    }

    await Promise.allSettled(runPromises);
    for (const migration of this.activeMigrations.values()) {
      if (migration.cleanupTimer) clearTimeout(migration.cleanupTimer);
    }
    for (const migration of this.activeCacheMigrations.values()) {
      if (migration.cleanupTimer) clearTimeout(migration.cleanupTimer);
    }
    this.activeMigrations.clear();
    this.activeCacheMigrations.clear();
    this.rateSamples.clear();
  }

  /**
   * Check if an endpoint is enabled
   */
  isEndpointEnabled(endpoint: QueuebertEndpoint): boolean {
    return this.enabledEndpoints.has(endpoint);
  }

  /**
   * Get the list of enabled endpoints
   */
  getEnabledEndpoints(): QueuebertEndpoint[] {
    return Array.from(this.enabledEndpoints);
  }

  /**
   * Get capabilities object for stats response
   */
  getCapabilities(): QueuebertCapabilities {
    // Migrations require multiple Redis instances to be useful
    const hasMultipleRedis = (this.options.redis?.length ?? 1) > 1;
    const migrationsEnabled =
      this.enabledEndpoints.has('migrations') && hasMultipleRedis;
    return {
      endpoints: this.getEnabledEndpoints(),
      canPause:
        this.enabledEndpoints.has('pause') &&
        this.enabledEndpoints.has('resume'),
      canClean: this.enabledEndpoints.has('clean'),
      canDrain: this.enabledEndpoints.has('drain'),
      canMigrate: migrationsEnabled,
      canMigrateCache: migrationsEnabled,
    };
  }

  /**
   * Get the Redis key for storing pause reason
   */
  private getPauseReasonKey(queueName: string): string {
    return `bull:${queueName}:pause-reason`;
  }

  /**
   * Scan Redis keys incrementally. Avoid KEYS because it can block Redis on
   * large keyspaces and create an availability risk for consuming apps.
   */
  private async *scanKeyBatches(
    client: RedisClientForMigration,
    pattern: string,
    batchSize = 1000,
    limit?: number,
  ): AsyncGenerator<string[]> {
    let yielded = 0;
    let cursor = '0';

    do {
      const [nextCursor, batch] = await client.scan(
        cursor,
        'MATCH',
        pattern,
        'COUNT',
        batchSize,
      );
      cursor = nextCursor;

      for (let offset = 0; offset < batch.length; offset += batchSize) {
        const remaining = limit === undefined ? batchSize : limit - yielded;
        const nextBatch = batch.slice(
          offset,
          offset + Math.max(0, Math.min(batchSize, remaining)),
        );
        if (nextBatch.length > 0) {
          yielded += nextBatch.length;
          yield nextBatch;
        }
        if (limit !== undefined && yielded >= limit) return;
      }
    } while (cursor !== '0');
  }

  /**
   * Parse Redis INFO memory output into structured data
   */
  parseRedisInfo(info: string): Record<string, string> {
    const result: Record<string, string> = {};
    const lines = info.split('\r\n');
    for (const line of lines) {
      if (line && !line.startsWith('#')) {
        const [key, value] = line.split(':');
        if (key && value) {
          result[key] = value;
        }
      }
    }
    return result;
  }

  /**
   * Get Redis memory statistics
   */
  async getRedisMemoryStats(
    queue: Queue,
  ): Promise<{ memory: RedisMemoryStats; usagePercent: string }> {
    const redis = await queue.client;
    const memoryInfo = await redis.info('memory');
    const memory = this.parseRedisInfo(memoryInfo);

    return {
      memory: {
        used: parseInt(memory['used_memory'] || '0', 10),
        usedHuman: memory['used_memory_human'] || 'unknown',
        peak: parseInt(memory['used_memory_peak'] || '0', 10),
        peakHuman: memory['used_memory_peak_human'] || 'unknown',
        rss: parseInt(memory['used_memory_rss'] || '0', 10),
        rssHuman: memory['used_memory_rss_human'] || 'unknown',
        maxmemory: parseInt(memory['maxmemory'] || '0', 10),
        maxmemoryHuman: memory['maxmemory_human'] || 'unlimited',
        maxmemoryPolicy: memory['maxmemory_policy'] || 'unknown',
        fragmentationRatio: parseFloat(
          memory['mem_fragmentation_ratio'] || '0',
        ),
      },
      usagePercent:
        parseInt(memory['maxmemory'] || '0', 10) > 0
          ? (
              (parseInt(memory['used_memory'] || '0', 10) /
                parseInt(memory['maxmemory'] || '1', 10)) *
              100
            ).toFixed(2) + '%'
          : 'unlimited',
    };
  }

  /**
   * Calculate rates from counter-based samples stored in Redis
   */
  calculateRatesFromCounters(
    current: RateSample,
    previous: RateSample | null,
  ): { throughputPerMin: number; addedPerMin: number } {
    if (!previous) {
      return { throughputPerMin: 0, addedPerMin: 0 };
    }

    const timeDeltaMs = current.timestamp - previous.timestamp;
    if (timeDeltaMs <= 0) {
      return { throughputPerMin: 0, addedPerMin: 0 };
    }

    const completedDelta = current.completed - previous.completed;
    const throughputPerMin = Math.round((completedDelta / timeDeltaMs) * 60000);

    const backlogDelta = current.backlog - previous.backlog;
    const addedDelta = completedDelta + backlogDelta;
    const addedPerMin = Math.round((addedDelta / timeDeltaMs) * 60000);

    return {
      throughputPerMin: Math.max(0, throughputPerMin),
      addedPerMin: Math.max(0, addedPerMin),
    };
  }

  /**
   * Calculate performance metrics based on counter-based rate data
   */
  calculatePerformanceMetrics(
    throughputPerMin: number,
    addedPerMin: number,
    backlog: number,
    avgDurationMs: number | undefined,
    failureRate: number,
    isPaused: boolean,
  ): PerformanceMetrics {
    if (isPaused) {
      return {
        score: 0,
        trend: 'paused',
        throughputPerMin: 0,
        addedPerMin: 0,
        deltaPerMin: 0,
        estimatedClearTimeMs: null,
        health: 'poor',
      };
    }

    const deltaPerMin = throughputPerMin - addedPerMin;

    if (throughputPerMin === 0 && backlog === 0) {
      return {
        score: 100,
        trend: 'idle',
        throughputPerMin: 0,
        addedPerMin,
        deltaPerMin: -addedPerMin,
        estimatedClearTimeMs: null,
        health: 'excellent',
      };
    }

    let estimatedClearTimeMs: number | null = null;
    if (backlog > 0) {
      if (deltaPerMin > 0) {
        estimatedClearTimeMs = Math.round((backlog / deltaPerMin) * 60000);
      } else if (throughputPerMin > 0) {
        estimatedClearTimeMs = Math.round((backlog / throughputPerMin) * 60000);
      }
    }

    let trend: PerformanceMetrics['trend'];
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

    let score = 100;

    if (deltaPerMin < 0) {
      const deltaPenalty = Math.min(40, Math.floor(Math.abs(deltaPerMin) / 10));
      score -= deltaPenalty;
    } else if (deltaPerMin > 100) {
      score = Math.min(100, score + 5);
    }

    const backlogPenalty = Math.min(20, Math.floor(backlog / 200));
    score -= backlogPenalty;

    const failurePenalty = Math.min(20, Math.floor(failureRate * 100 * 2));
    score -= failurePenalty;

    if (avgDurationMs !== undefined && avgDurationMs > 500) {
      const durationPenalty = Math.min(
        10,
        Math.floor((avgDurationMs - 500) / 100),
      );
      score -= durationPenalty;
    }

    score = Math.max(0, Math.min(100, score));

    let health: PerformanceMetrics['health'];
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
   * Validate that an object is a valid PerformanceMetrics
   * Used to check if a processor provides pre-calculated performance
   */
  private isValidPerformanceMetrics(obj: unknown): obj is PerformanceMetrics {
    if (!obj || typeof obj !== 'object') return false;

    const metrics = obj as Record<string, unknown>;

    // Check required fields exist and have correct types
    return (
      typeof metrics['score'] === 'number' &&
      typeof metrics['trend'] === 'string' &&
      typeof metrics['throughputPerMin'] === 'number' &&
      typeof metrics['addedPerMin'] === 'number' &&
      typeof metrics['deltaPerMin'] === 'number' &&
      (metrics['estimatedClearTimeMs'] === null ||
        typeof metrics['estimatedClearTimeMs'] === 'number') &&
      typeof metrics['health'] === 'string' &&
      // Validate trend is a valid value
      ['catching_up', 'falling_behind', 'stable', 'idle', 'paused'].includes(
        metrics['trend'] as string,
      ) &&
      // Validate health is a valid value
      ['excellent', 'good', 'fair', 'poor', 'critical'].includes(
        metrics['health'] as string,
      )
    );
  }

  /**
   * Get queue counts
   */
  async getQueueCounts(queue: Queue): Promise<QueueCounts> {
    const [waiting, active, completed, failed, delayed] = await Promise.all([
      queue.getWaitingCount(),
      queue.getActiveCount(),
      queue.getCompletedCount(),
      queue.getFailedCount(),
      queue.getDelayedCount(),
    ]);

    return {
      waiting,
      active,
      completed,
      failed,
      delayed,
      total: waiting + active + completed + failed + delayed,
    };
  }

  /**
   * Discover job types from a queue by sampling jobs in different states.
   * Merges queue-based discovery with processor-tracked stats for completed/failed counts.
   */
  async discoverJobTypes(
    queue: Queue,
    counts: QueueCounts,
    processorJobsByType?: Record<
      string,
      { processed: number; completed: number; failed: number }
    >,
  ): Promise<JobTypesDiscovery | undefined> {
    const config = this.options.jobTypeDiscovery;
    if (config?.enabled === false) {
      return undefined;
    }

    const sampleSize = config?.sampleSize ?? 100;
    const threshold = config?.threshold ?? 1000;

    const types: Record<string, QueueJobTypeStats> = {};
    let sampled = false;

    try {
      // Determine if we need to sample or can get all jobs
      const totalPending = counts.waiting + counts.active + counts.delayed;

      if (totalPending > threshold) {
        // Use sampling for large queues
        sampled = true;
        const [waitingJobs, activeJobs, delayedJobs] = await Promise.all([
          queue.getJobs(['waiting'], 0, sampleSize - 1),
          queue.getJobs(['active'], 0, sampleSize - 1),
          queue.getJobs(['delayed'], 0, sampleSize - 1),
        ]);

        // Count job types from samples
        for (const job of waitingJobs) {
          const name = job.name || '__default__';
          if (!types[name]) {
            types[name] = { waiting: 0, active: 0, delayed: 0 };
          }
          types[name].waiting++;
        }

        for (const job of activeJobs) {
          const name = job.name || '__default__';
          if (!types[name]) {
            types[name] = { waiting: 0, active: 0, delayed: 0 };
          }
          types[name].active++;
        }

        for (const job of delayedJobs) {
          const name = job.name || '__default__';
          if (!types[name]) {
            types[name] = { waiting: 0, active: 0, delayed: 0 };
          }
          types[name].delayed++;
        }
      } else if (totalPending > 0) {
        // Get all pending jobs for accurate counts
        const [waitingJobs, activeJobs, delayedJobs] = await Promise.all([
          queue.getJobs(['waiting'], 0, counts.waiting),
          queue.getJobs(['active'], 0, counts.active),
          queue.getJobs(['delayed'], 0, counts.delayed),
        ]);

        for (const job of waitingJobs) {
          const name = job.name || '__default__';
          if (!types[name]) {
            types[name] = { waiting: 0, active: 0, delayed: 0 };
          }
          types[name].waiting++;
        }

        for (const job of activeJobs) {
          const name = job.name || '__default__';
          if (!types[name]) {
            types[name] = { waiting: 0, active: 0, delayed: 0 };
          }
          types[name].active++;
        }

        for (const job of delayedJobs) {
          const name = job.name || '__default__';
          if (!types[name]) {
            types[name] = { waiting: 0, active: 0, delayed: 0 };
          }
          types[name].delayed++;
        }
      }

      // Merge with processor-tracked stats for completed/failed counts
      if (processorJobsByType) {
        for (const [name, stats] of Object.entries(processorJobsByType)) {
          if (!types[name]) {
            types[name] = { waiting: 0, active: 0, delayed: 0 };
          }
          types[name].completed = stats.completed;
          types[name].failed = stats.failed;
        }
      }

      // Return undefined if no job types discovered
      if (Object.keys(types).length === 0) {
        return undefined;
      }

      return {
        types,
        sampled,
        ...(sampled ? { sampleSize } : {}),
      };
    } catch (error) {
      this.logger.warn(
        `Failed to discover job types for queue ${queue.name}: ${error}`,
      );
      return undefined;
    }
  }

  /**
   * Get stats for a single queue
   * Note: Returns stats without 'redis' field; getAllQueueStats adds that field
   * Processor is optional - monitoring-only queues won't have processor metrics
   * @param queue The BullMQ queue
   * @param processor Optional processor for additional metrics
   * @param statsKey Optional statsKey to use as the queue name (defaults to queue.name)
   */
  async getSingleQueueStats(
    queue: Queue,
    processor?: QueuebertProcessor,
    statsKey?: string,
  ): Promise<Omit<SingleQueueStats, 'redis'>> {
    const [counts, paused] = await Promise.all([
      this.getQueueCounts(queue),
      queue.isPaused(),
    ]);

    const processorStats = processor?.getProcessorStats();

    // Counter-based rate calculation is kept in process so a read-only stats
    // request never writes operational metadata into the application's Redis.
    const backlog = counts.waiting + counts.active;
    const now = Date.now();
    const currentSample: RateSample = {
      completed: processorStats?.jobs.completed ?? counts.completed,
      failed: counts.failed,
      backlog,
      timestamp: now,
    };

    const rateSampleKey = statsKey ?? queue.name;
    const previousSample = this.rateSamples.get(rateSampleKey) ?? null;

    const rates = this.calculateRatesFromCounters(
      currentSample,
      previousSample,
    );
    this.rateSamples.set(rateSampleKey, currentSample);

    // Check if processor provides pre-calculated performance (from queuebert-bullmq or queuebert-otel)
    // This avoids duplicate calculations and uses the more accurate data from the integration package
    const providedPerformance = processorStats?.custom?.['performance'] as
      | PerformanceMetrics
      | undefined;

    let performance: PerformanceMetrics;
    if (
      providedPerformance &&
      this.isValidPerformanceMetrics(providedPerformance)
    ) {
      // Use pre-calculated performance from integration package
      performance = providedPerformance;
    } else {
      // Fall back to internal calculation for basic processors
      const effectiveThroughputPerMin =
        rates.throughputPerMin > 0
          ? rates.throughputPerMin
          : (processorStats?.throughput.jobsPerMinute ?? 0);

      performance = this.calculatePerformanceMetrics(
        effectiveThroughputPerMin,
        rates.addedPerMin,
        backlog,
        processorStats?.duration.avgMs,
        processorStats?.jobs.failureRate ?? 0,
        paused,
      );
    }

    // Discover job types from queue (merges with processor stats)
    const jobTypes = await this.discoverJobTypes(
      queue,
      counts,
      processorStats?.jobsByType,
    );

    return {
      name: statsKey ?? queue.name,
      paused,
      counts,
      jobMetrics: {
        duration: {
          avgMs: processorStats?.duration.avgMs,
          minMs: processorStats?.duration.minMs,
          maxMs: processorStats?.duration.maxMs,
          p50Ms: processorStats?.duration.p50Ms,
          p95Ms: processorStats?.duration.p95Ms,
          p99Ms: processorStats?.duration.p99Ms,
          recentAvgMs: processorStats?.duration.recentAvgMs,
        },
        failureRate: processorStats?.jobs.failureRate ?? 0,
        successRate: processorStats?.jobs.successRate ?? 0,
        processed: processorStats?.jobs.processed ?? 0,
        completed: processorStats?.jobs.completed ?? 0,
        failed: processorStats?.jobs.failed ?? 0,
        byType: processorStats?.jobsByType,
        lastJobTime: processorStats?.jobs.lastJobTime ?? null,
        sampleCount: processorStats?.duration.sampleCount ?? 0,
        performance,
      },
      throughput: {
        jobsPerMinute: processorStats?.throughput.jobsPerMinute ?? 0,
        windowStartTime:
          processorStats?.throughput.windowStartTime ??
          new Date().toISOString(),
        jobsInWindow: processorStats?.throughput.jobsInWindow ?? 0,
      },
      jobTypes,
      custom: processorStats?.custom,
    };
  }

  /**
   * Get the Redis instance ID for a queue based on configuration
   */
  private getRedisInstanceId(queueConfig: {
    name: string;
    redis?: string;
  }): string {
    // If redis is explicitly set, use it
    if (queueConfig.redis) {
      return queueConfig.redis;
    }
    // If multi-redis is configured, default to first instance
    if (this.options.redis && this.options.redis.length > 0) {
      return this.options.redis[0].id;
    }
    // Default single-redis case
    return DEFAULT_REDIS_INSTANCE_ID;
  }

  /**
   * Get the Redis instance label
   */
  private getRedisInstanceLabel(instanceId: string): string {
    if (!this.options.redis) {
      return DEFAULT_REDIS_INSTANCE_LABEL;
    }
    const config = this.options.redis.find((r) => r.id === instanceId);
    return config?.label ?? instanceId;
  }

  /**
   * Build queue to Redis instance mapping from config
   */
  private buildQueueRedisMapping(): Map<string, string> {
    const mapping = new Map<string, string>();
    for (const queueConfig of this.options.queues) {
      const statsKey = queueConfig.statsKey ?? queueConfig.name;
      const redisId = this.getRedisInstanceId(queueConfig);
      mapping.set(statsKey, redisId);
    }
    return mapping;
  }

  /**
   * Get stats for all configured queues
   */
  async getAllQueueStats(
    queues: Map<string, { queue: Queue; processor?: QueuebertProcessor }>,
  ): Promise<MultiQueueStats> {
    const queueStats: Record<string, SingleQueueStats> = {};
    const queueRedisMapping = this.buildQueueRedisMapping();

    // Get stats for all queues in parallel
    const statsPromises = Array.from(queues.entries()).map(
      async ([statsKey, { queue, processor }]) => {
        const stats = await this.getSingleQueueStats(
          queue,
          processor,
          statsKey,
        );
        return { name: statsKey, stats, queue };
      },
    );

    const settledResults = await Promise.allSettled(statsPromises);
    const results: Array<{
      name: string;
      stats: Omit<SingleQueueStats, 'redis'>;
      queue: Queue;
    }> = [];
    const collectionErrors: Record<string, string> = {};

    settledResults.forEach((settled, index) => {
      if (settled.status === 'fulfilled') {
        results.push(settled.value);
        return;
      }

      const name = Array.from(queues.keys())[index] ?? `queue-${index}`;
      const message =
        settled.reason instanceof Error
          ? settled.reason.message
          : String(settled.reason);
      collectionErrors[name] = message;
      this.logger.warn(`Failed to collect stats for ${name}: ${message}`);
    });

    for (const { name, stats } of results) {
      // Add redis instance ID to each queue's stats
      const redisId = queueRedisMapping.get(name) ?? DEFAULT_REDIS_INSTANCE_ID;
      queueStats[name] = { ...stats, redis: redisId };
    }

    // Build Redis instance stats
    let redisStats: Record<string, RedisInstanceStats> | undefined;

    if (this.options.includeRedisStats !== false) {
      redisStats = {};

      // Group queues by Redis instance
      const queuesByRedis = new Map<
        string,
        { queue: Queue; statsKey: string }[]
      >();
      for (const { name, queue } of results) {
        const redisId =
          queueRedisMapping.get(name) ?? DEFAULT_REDIS_INSTANCE_ID;
        const redisQueues = queuesByRedis.get(redisId);
        if (redisQueues) {
          redisQueues.push({ queue, statsKey: name });
        } else {
          queuesByRedis.set(redisId, [{ queue, statsKey: name }]);
        }
      }

      // Get Redis stats for each instance
      const redisPromises = Array.from(queuesByRedis.entries()).map(
        async ([redisId, redisQueues]) => {
          // Use the first queue in this Redis instance to get memory stats
          const firstQueue = redisQueues[0];
          if (!firstQueue) return null;

          try {
            const memoryStats = await this.getRedisMemoryStats(
              firstQueue.queue,
            );
            const instanceStats: RedisInstanceStats = {
              id: redisId,
              label: this.getRedisInstanceLabel(redisId),
              memory: memoryStats.memory,
              usagePercent: memoryStats.usagePercent,
            };
            return { redisId, instanceStats };
          } catch (error) {
            this.logger.warn(
              `Failed to get Redis stats for instance ${redisId}: ${error}`,
            );
            return null;
          }
        },
      );

      const redisResults = await Promise.all(redisPromises);
      for (const result of redisResults) {
        if (result) {
          redisStats[result.redisId] = result.instanceStats;
        }
      }
    }

    // Collect cache stats from processors that implement getCacheConfigs()
    const cacheStats = this.collectCacheStats(queues, queueRedisMapping);

    const result: MultiQueueStats = {
      queues: queueStats,
      capabilities: this.getCapabilities(),
      timestamp: new Date().toISOString(),
    };

    // Only include redis stats if available and enabled
    if (redisStats && Object.keys(redisStats).length > 0) {
      result.redis = redisStats;
    }

    // Only include cache stats if any processors have caches
    if (Object.keys(cacheStats).length > 0) {
      result.caches = cacheStats;
    }

    if (Object.keys(collectionErrors).length > 0) {
      result.errors = collectionErrors;
    }

    // Include integrations from registry (auto-discovered from loaded modules)
    const registeredIntegrations = this.integrationRegistry.getAll();
    if (registeredIntegrations.length > 0) {
      result.integrations = registeredIntegrations;
    }

    return result;
  }

  /**
   * Collect cache statistics from all processors that implement getCacheConfigs()
   */
  private collectCacheStats(
    queues: Map<string, { queue: Queue; processor?: QueuebertProcessor }>,
    queueRedisMapping: Map<string, string>,
  ): Record<string, CacheInstanceStats> {
    const cacheStats: Record<string, CacheInstanceStats> = {};

    for (const [queueName, { processor }] of queues.entries()) {
      if (!processor || typeof processor.getCacheConfigs !== 'function') {
        continue;
      }

      const redisId =
        queueRedisMapping.get(queueName) ?? DEFAULT_REDIS_INSTANCE_ID;

      try {
        const cacheConfigs = processor.getCacheConfigs();
        for (const config of cacheConfigs) {
          const stats = config.cache.getStats();

          // Calculate combined hit rate (L1 hits + L2 hits vs L2 misses)
          const totalHits = stats.l1.hits + stats.l2.hits;
          const totalRequests = totalHits + stats.l2.misses;
          const combinedHitRate =
            totalRequests > 0
              ? `${((totalHits / totalRequests) * 100).toFixed(1)}%`
              : '0.0%';

          cacheStats[config.id] = {
            id: config.id,
            label: config.label ?? config.id,
            keyPrefix: config.keyPrefix,
            redis: redisId,
            processor: queueName,
            l1: {
              size: stats.l1.size,
              maxSize: stats.l1.maxSize,
              utilizationPercent: stats.l1.utilizationPercent,
              hitRate: stats.l1.hitRate,
              hits: stats.l1.hits,
              misses: stats.l1.misses,
              evictions: stats.l1.evictions,
            },
            l2: {
              hitRate: stats.l2.hitRate,
              hits: stats.l2.hits,
              misses: stats.l2.misses,
              errors: stats.l2.errors,
            },
            combinedHitRate,
          };
        }
      } catch (error) {
        this.logger.warn(
          `Failed to get cache configs from processor ${queueName}: ${error}`,
        );
      }
    }

    return cacheStats;
  }

  /**
   * Clean completed/failed jobs from a queue
   * @param queue The BullMQ queue to clean
   * @param processor Optional processor for stats
   * @param graceMs Grace period in milliseconds
   * @param maxJobs Maximum number of jobs to clean
   * @param statsKey Optional statsKey to use in logging (defaults to queue.name)
   */
  async cleanQueue(
    queue: Queue,
    processor?: QueuebertProcessor,
    graceMs = 300000,
    maxJobs = 10000,
    statsKey?: string,
  ): Promise<CleanResult> {
    const beforeStats = await this.getSingleQueueStats(
      queue,
      processor,
      statsKey,
    );

    const cleanedCompleted = await queue.clean(graceMs, maxJobs, 'completed');
    const cleanedFailed = await queue.clean(graceMs, maxJobs, 'failed');

    const afterStats = await this.getSingleQueueStats(
      queue,
      processor,
      statsKey,
    );

    const displayName = statsKey ?? queue.name;
    this.logger.log(
      `Queue ${displayName} cleaned: ${cleanedCompleted.length} completed, ${cleanedFailed.length} failed jobs removed`,
    );

    return {
      cleaned: {
        completed: cleanedCompleted.length,
        failed: cleanedFailed.length,
        total: cleanedCompleted.length + cleanedFailed.length,
      },
      before: beforeStats.counts,
      after: afterStats.counts,
      options: {
        graceMs,
        maxJobs,
      },
    };
  }

  /**
   * Drain all waiting jobs from a queue
   * @param queue The BullMQ queue to drain
   * @param processor Optional processor for stats
   * @param statsKey Optional statsKey to use in logging (defaults to queue.name)
   */
  async drainQueue(
    queue: Queue,
    processor?: QueuebertProcessor,
    statsKey?: string,
  ): Promise<DrainResult> {
    const beforeStats = await this.getSingleQueueStats(
      queue,
      processor,
      statsKey,
    );

    await queue.drain();

    const afterStats = await this.getSingleQueueStats(
      queue,
      processor,
      statsKey,
    );

    const displayName = statsKey ?? queue.name;
    this.logger.warn(
      `Queue ${displayName} drained: ${beforeStats.counts.waiting} waiting jobs removed`,
    );

    return {
      drained: beforeStats.counts.waiting,
      before: beforeStats.counts,
      after: afterStats.counts,
    };
  }

  /**
   * Pause a queue
   * @param queue The BullMQ queue to pause
   * @param reason Optional pause reason
   * @param statsKey Optional statsKey to use in response (defaults to queue.name)
   */
  async pauseQueue(
    queue: Queue,
    reason = 'manual-pause',
    statsKey?: string,
  ): Promise<StatusResult> {
    await queue.pause();

    const client = await queue.client;
    await client.set(this.getPauseReasonKey(queue.name), reason);

    const displayName = statsKey ?? queue.name;
    this.logger.warn(`Queue ${displayName} paused (reason: ${reason})`);

    return {
      status: 'paused',
      reason,
      queues: [displayName],
      timestamp: new Date().toISOString(),
    };
  }

  /**
   * Resume a queue
   * @param queue The BullMQ queue to resume
   * @param statsKey Optional statsKey to use in response (defaults to queue.name)
   */
  async resumeQueue(queue: Queue, statsKey?: string): Promise<StatusResult> {
    await queue.resume();

    const client = await queue.client;
    await client.del(this.getPauseReasonKey(queue.name));

    const displayName = statsKey ?? queue.name;
    this.logger.log(`Queue ${displayName} resumed (pause reason cleared)`);

    return {
      status: 'resumed',
      queues: [displayName],
      timestamp: new Date().toISOString(),
    };
  }

  /**
   * Pause multiple queues
   * @param queuesWithKeys Array of queue objects with their statsKeys
   * @param reason Optional pause reason
   */
  async pauseQueues(
    queuesWithKeys: Array<{ queue: Queue; statsKey: string }>,
    reason = 'manual-pause',
  ): Promise<StatusResult> {
    await Promise.all(queuesWithKeys.map(({ queue }) => queue.pause()));

    await Promise.all(
      queuesWithKeys.map(async ({ queue }) => {
        const client = await queue.client;
        return client.set(this.getPauseReasonKey(queue.name), reason);
      }),
    );

    const queueNames = queuesWithKeys.map(({ statsKey }) => statsKey);
    this.logger.warn(
      `Queues paused (reason: ${reason}): ${queueNames.join(', ')}`,
    );

    return {
      status: 'paused',
      reason,
      queues: queueNames,
      timestamp: new Date().toISOString(),
    };
  }

  /**
   * Resume multiple queues
   * @param queuesWithKeys Array of queue objects with their statsKeys
   */
  async resumeQueues(
    queuesWithKeys: Array<{ queue: Queue; statsKey: string }>,
  ): Promise<StatusResult> {
    await Promise.all(queuesWithKeys.map(({ queue }) => queue.resume()));

    await Promise.all(
      queuesWithKeys.map(async ({ queue }) => {
        const client = await queue.client;
        return client.del(this.getPauseReasonKey(queue.name));
      }),
    );

    const queueNames = queuesWithKeys.map(({ statsKey }) => statsKey);
    this.logger.log(
      `Queues resumed (pause reasons cleared): ${queueNames.join(', ')}`,
    );

    return {
      status: 'resumed',
      queues: queueNames,
      timestamp: new Date().toISOString(),
    };
  }

  /**
   * Get jobs from a queue by state for migration
   */
  private async getJobsByState(
    queue: Queue,
    state: MigrationJobState,
    jobType?: string,
    limit?: number,
  ): Promise<MigrationJobInfo[]> {
    let jobs: Awaited<ReturnType<Queue['getWaiting']>>;

    switch (state) {
      case 'waiting':
        jobs = await queue.getWaiting(0, limit ? limit - 1 : undefined);
        break;
      case 'delayed':
        jobs = await queue.getDelayed(0, limit ? limit - 1 : undefined);
        break;
      case 'failed':
        jobs = await queue.getFailed(0, limit ? limit - 1 : undefined);
        break;
      default:
        jobs = [];
    }

    // Filter by job type if specified
    const filteredJobs = jobType
      ? jobs.filter((job) => job.name === jobType)
      : jobs;

    return filteredJobs.map((job) => {
      const info: MigrationJobInfo = {
        id: job.id ?? 'unknown',
        name: job.name,
        data: job.data,
        state,
      };

      if (state === 'delayed' && job.delay) {
        const processedOn = job.processedOn || Date.now();
        info.remainingDelayMs = Math.max(
          0,
          job.delay - (Date.now() - processedOn),
        );
      }

      if (state === 'failed') {
        info.attemptsMade = job.attemptsMade;
        info.failedReason = job.failedReason ?? undefined;
      }

      return info;
    });
  }

  /**
   * Preview a migration without executing it
   */
  async previewMigration(
    sourceQueue: Queue,
    targetQueue: Queue,
    params: MigrationParams,
    sourceRedisId: string,
    targetRedisId: string,
  ): Promise<MigrationPreview> {
    this.assertDistinctMigrationTargets(
      sourceQueue,
      targetQueue,
      sourceRedisId,
      targetRedisId,
    );
    const states: MigrationJobState[] = params.states ?? ['waiting', 'delayed'];
    const isCrossRedis = sourceRedisId !== targetRedisId;

    const jobs: MigrationJobInfo[] = [];
    const byState: Record<MigrationJobState, number> = {
      waiting: 0,
      delayed: 0,
      failed: 0,
    };
    const byJobType: Record<string, number> = {};

    for (const state of states) {
      const remainingLimit =
        params.limit !== undefined ? params.limit - jobs.length : undefined;
      if (remainingLimit !== undefined && remainingLimit <= 0) break;
      const stateJobs = await this.getJobsByState(
        sourceQueue,
        state,
        params.jobType,
        remainingLimit,
      );
      jobs.push(...stateJobs);
      byState[state] = stateJobs.length;

      // Count by job type
      for (const job of stateJobs) {
        const jobName = job.name || 'unknown';
        byJobType[jobName] = (byJobType[jobName] || 0) + 1;
      }
    }

    const jobsToMigrate = jobs.length;

    // Apply total limit for sample jobs (limit to 50 for preview)
    const sampleLimit = Math.min(params.limit || 50, 50);
    const sampleJobs = jobs.slice(0, sampleLimit).map((job) => {
      if (this.options.includeJobDataInMigrationPreview) {
        return job;
      }

      const redactedJob = { ...job };
      delete redactedJob.data;
      return redactedJob;
    });

    // Calculate estimated duration based on batch size and delay
    const batchSize = params.batchSize || 100;
    const delayMs = params.delayBetweenBatchesMs || 50;
    const batches = Math.ceil(jobsToMigrate / batchSize);
    const estimatedDurationMs =
      batches > 0 ? (batches - 1) * delayMs + batches * 10 : 0;

    return {
      params: {
        sourceQueue: params.sourceQueue,
        sourceRedis: sourceRedisId,
        targetQueue: params.targetQueue,
        targetRedis: targetRedisId,
        jobType: params.jobType,
        states,
        batchSize: params.batchSize,
        delayBetweenBatchesMs: params.delayBetweenBatchesMs,
      },
      jobsToMigrate,
      byState,
      byJobType,
      sampleJobs,
      isCrossRedis,
      estimatedDurationMs:
        estimatedDurationMs > 0 ? estimatedDurationMs : undefined,
      timestamp: new Date().toISOString(),
    };
  }

  /**
   * Helper to sleep for a given duration
   */
  private sleep(ms: number, signal?: AbortSignal): Promise<void> {
    if (ms <= 0) return Promise.resolve();

    return new Promise((resolve, reject) => {
      const timer = setTimeout(resolve, ms);
      timer.unref?.();

      if (!signal) return;
      const onAbort = () => {
        clearTimeout(timer);
        reject(new Error('Migration cancelled'));
      };
      if (signal.aborted) {
        onAbort();
        return;
      }
      signal.addEventListener('abort', onAbort, { once: true });
    });
  }

  private assertDistinctMigrationTargets(
    sourceQueue: Queue,
    targetQueue: Queue,
    sourceRedisId: string,
    targetRedisId: string,
  ): void {
    if (
      sourceQueue === targetQueue ||
      (sourceQueue.name === targetQueue.name && sourceRedisId === targetRedisId)
    ) {
      throw new Error('Migration source and target must be different queues');
    }
  }

  private async acquireMigrationLease(
    sourceQueue: Queue,
    sourceRedisId: string,
  ): Promise<MigrationLease> {
    const client = await sourceQueue.client;
    const key = `queuebert:migration-lock:${sourceRedisId}:${sourceQueue.name}`;
    const token = randomUUID();
    const acquired = await client.set(
      key,
      token,
      'PX',
      MIGRATION_LOCK_TTL_MS,
      'NX',
    );
    if (acquired !== 'OK') {
      throw new Error(
        `Another migration is already using source queue '${sourceQueue.name}'`,
      );
    }

    const refresh = async () => {
      const result = await client.eval(
        "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end",
        1,
        key,
        token,
        String(MIGRATION_LOCK_TTL_MS),
      );
      if (result !== 1) {
        throw new Error('Migration lease was lost; queues remain paused');
      }
    };

    const release = async () => {
      await client.eval(
        "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
        1,
        key,
        token,
      );
    };

    return { refresh, release };
  }

  private async pauseQueuesForMigration(
    sourceQueue: Queue,
    targetQueue: Queue,
  ): Promise<MigrationQueueGuard> {
    const [sourceWasPaused, targetWasPaused] = await Promise.all([
      sourceQueue.isPaused(),
      targetQueue.isPaused(),
    ]);

    await Promise.all([
      sourceWasPaused ? Promise.resolve() : sourceQueue.pause(),
      targetWasPaused ? Promise.resolve() : targetQueue.pause(),
    ]);

    const activeCount = await sourceQueue.getActiveCount();
    if (activeCount > 0) {
      if (!targetWasPaused) await targetQueue.resume();
      if (!sourceWasPaused) await sourceQueue.resume();
      throw new Error(
        `Source queue '${sourceQueue.name}' still has ${activeCount} active job(s); retry after they finish`,
      );
    }

    return {
      restore: async (leavePaused: boolean) => {
        if (leavePaused) return;
        if (!targetWasPaused) await targetQueue.resume();
        if (!sourceWasPaused) await sourceQueue.resume();
      },
    };
  }

  private async migrateJob(
    sourceQueue: Queue,
    targetQueue: Queue,
    jobInfo: MigrationJobInfo,
    state: MigrationJobState,
  ): Promise<void> {
    const job = await sourceQueue.getJob(jobInfo.id);
    if (!job) {
      throw new Error('Job no longer exists in the source queue');
    }

    if (job.parentKey || job.repeatJobKey || job.opts.repeat) {
      throw new Error(
        'Flow and repeatable jobs are not supported by safe migration',
      );
    }

    const existingTargetJob = await targetQueue.getJob(jobInfo.id);
    if (existingTargetJob) {
      throw new Error(`Target queue already contains job id '${jobInfo.id}'`);
    }

    const jobOpts: Record<string, unknown> = {
      ...job.opts,
      jobId: jobInfo.id,
      timestamp: job.timestamp,
    };
    if (state === 'delayed') {
      jobOpts['delay'] = jobInfo.remainingDelayMs ?? 0;
    } else {
      delete jobOpts['delay'];
    }

    const addedJob = await targetQueue.add(job.name, job.data, jobOpts);
    try {
      await job.remove();
    } catch (sourceError) {
      try {
        await addedJob.remove();
      } catch (rollbackError) {
        throw new MigrationRollbackError(
          `Source removal failed (${String(sourceError)}) and target rollback failed (${String(rollbackError)})`,
        );
      }
      throw sourceError;
    }
  }

  /**
   * Wait while migration is paused
   */
  private async waitWhilePaused(migration: ActiveMigration): Promise<void> {
    if (!migration.isPaused) return;

    return new Promise((resolve) => {
      migration.resumeResolver = resolve;
    });
  }

  /**
   * Execute a migration between queues (potentially across Redis instances)
   * Supports rate limiting via batchSize and delayBetweenBatchesMs
   */
  async executeMigration(
    sourceQueue: Queue,
    targetQueue: Queue,
    params: MigrationParams,
    sourceRedisId: string,
    targetRedisId: string,
  ): Promise<MigrationResult> {
    this.assertDistinctMigrationTargets(
      sourceQueue,
      targetQueue,
      sourceRedisId,
      targetRedisId,
    );
    const startTime = Date.now();
    const states: MigrationJobState[] = params.states ?? ['waiting', 'delayed'];
    const isCrossRedis = sourceRedisId !== targetRedisId;
    const batchSize = params.batchSize;
    const delayBetweenBatchesMs = params.delayBetweenBatchesMs ?? 0;

    const migrated = { waiting: 0, delayed: 0, failed: 0, total: 0 };
    const errors: Array<{ jobId: string; error: string }> = [];
    let batchesProcessed = 0;
    let requiresIntervention = false;
    const lease = await this.acquireMigrationLease(sourceQueue, sourceRedisId);
    let queueGuard: MigrationQueueGuard | undefined;

    this.logger.log(
      `Starting migration: ${params.sourceQueue} (${sourceRedisId}) -> ${params.targetQueue} (${targetRedisId})` +
        (params.jobType ? ` [jobType=${params.jobType}]` : '') +
        (isCrossRedis ? ' [CROSS-REDIS]' : '') +
        (batchSize
          ? ` [batchSize=${batchSize}, delay=${delayBetweenBatchesMs}ms]`
          : ''),
    );

    let totalProcessed = 0;
    let currentBatchCount = 0;

    try {
      queueGuard = await this.pauseQueuesForMigration(sourceQueue, targetQueue);

      for (const state of states) {
        const remainingLimit =
          params.limit !== undefined
            ? params.limit - totalProcessed
            : undefined;
        if (remainingLimit !== undefined && remainingLimit <= 0) break;

        const jobs = await this.getJobsByState(
          sourceQueue,
          state,
          params.jobType,
          remainingLimit,
        );

        for (const jobInfo of jobs) {
          if (params.limit && totalProcessed >= params.limit) break;

          if (batchSize && currentBatchCount >= batchSize) {
            batchesProcessed++;
            if (delayBetweenBatchesMs > 0) {
              this.logger.debug(
                `Batch ${batchesProcessed} complete (${currentBatchCount} jobs), waiting ${delayBetweenBatchesMs}ms...`,
              );
              await this.sleep(delayBetweenBatchesMs);
            }
            currentBatchCount = 0;
          }

          try {
            try {
              await lease.refresh();
            } catch (error) {
              requiresIntervention = true;
              throw error;
            }
            await this.migrateJob(sourceQueue, targetQueue, jobInfo, state);

            migrated[state]++;
            migrated.total++;
            totalProcessed++;
            currentBatchCount++;

            this.logger.debug(`Migrated job ${jobInfo.id} (${state})`);
          } catch (error) {
            const errorMsg =
              error instanceof Error ? error.message : String(error);
            if (error instanceof MigrationRollbackError) {
              requiresIntervention = true;
            }
            if (
              error instanceof Error &&
              error.message.includes('Migration lease was lost')
            ) {
              throw error;
            }
            if (errors.length < MAX_MIGRATION_ERRORS) {
              errors.push({ jobId: jobInfo.id, error: errorMsg });
            }
            this.logger.warn(
              `Failed to migrate job ${jobInfo.id}: ${errorMsg}`,
            );
          }
        }
      }
    } finally {
      await queueGuard?.restore(requiresIntervention);
      await lease.release();
    }

    // Count the final partial batch if any jobs were processed
    if (currentBatchCount > 0 && batchSize) {
      batchesProcessed++;
    }

    const durationMs = Date.now() - startTime;

    this.logger.log(
      `Migration complete: ${migrated.total} jobs migrated in ${durationMs}ms` +
        (batchSize ? ` (${batchesProcessed} batches)` : '') +
        (errors.length > 0 ? `, ${errors.length} errors` : ''),
    );

    const result: MigrationResult = {
      sourceQueue: params.sourceQueue,
      sourceRedis: sourceRedisId,
      targetQueue: params.targetQueue,
      targetRedis: targetRedisId,
      jobType: params.jobType,
      migrated,
      errors,
      isCrossRedis,
      durationMs,
      timestamp: new Date().toISOString(),
    };

    // Include rate limit info if batching was used
    if (batchSize) {
      result.rateLimit = {
        batchSize,
        delayBetweenBatchesMs,
        batchesProcessed,
      };
    }

    return result;
  }

  /**
   * Get available queues for migration with full info for the mobile app
   */
  async getAvailableQueuesForMigration(
    queues: Map<string, { queue: Queue; processor?: QueuebertProcessor }>,
  ): Promise<{
    queues: Array<{
      name: string;
      displayName: string;
      redisId: string;
      redisLabel: string;
      counts: QueueCounts;
    }>;
    redisInstances: Array<{
      id: string;
      label: string;
      memory?: {
        used: number;
        usedHuman: string;
        peak: number;
        peakHuman: string;
      };
      usagePercent?: string;
      caches?: Array<{
        id: string;
        label: string;
        l1Size: number;
        l2Hits: number;
        l2Misses: number;
      }>;
    }>;
  }> {
    const queueList: Array<{
      name: string;
      displayName: string;
      redisId: string;
      redisLabel: string;
      counts: QueueCounts;
    }> = [];
    // Track which queues belong to which Redis instance for memory lookups
    const queuesByRedis = new Map<string, Queue>();

    // Check if we have multiple Redis instances
    const hasMultipleRedis =
      this.options.redis && this.options.redis.length > 1;

    // Build queue list with full info
    for (const queueConfig of this.options.queues) {
      const statsKey = queueConfig.statsKey ?? queueConfig.name;
      const queueEntry = queues.get(statsKey);
      if (queueEntry) {
        const redisId = this.getRedisInstanceId(queueConfig);
        const redisLabel = this.getRedisInstanceLabel(redisId);

        // Track first queue per Redis instance for memory lookups
        if (!queuesByRedis.has(redisId)) {
          queuesByRedis.set(redisId, queueEntry.queue);
        }

        // Get queue counts
        let counts: QueueCounts = {
          waiting: 0,
          active: 0,
          completed: 0,
          failed: 0,
          delayed: 0,
          total: 0,
        };

        try {
          const jobCounts = await queueEntry.queue.getJobCounts();
          counts = {
            waiting: jobCounts['waiting'] ?? 0,
            active: jobCounts['active'] ?? 0,
            completed: jobCounts['completed'] ?? 0,
            failed: jobCounts['failed'] ?? 0,
            delayed: jobCounts['delayed'] ?? 0,
            total:
              (jobCounts['waiting'] ?? 0) +
              (jobCounts['active'] ?? 0) +
              (jobCounts['completed'] ?? 0) +
              (jobCounts['failed'] ?? 0) +
              (jobCounts['delayed'] ?? 0),
          };
        } catch (error) {
          this.logger.warn(
            `Failed to get job counts for queue ${statsKey}: ${error}`,
          );
        }

        // Include Redis label in display name when multi-Redis
        const displayName = hasMultipleRedis
          ? `${statsKey} (${redisLabel})`
          : statsKey;

        queueList.push({
          name: statsKey,
          displayName,
          redisId,
          redisLabel,
          counts,
        });
      }
    }

    // Collect cache stats from all processors
    const queueRedisMapping = this.buildQueueRedisMapping();
    const allCacheStats = this.collectCacheStats(queues, queueRedisMapping);

    // Group cache stats by Redis instance
    const cachesByRedis = new Map<
      string,
      Array<{
        id: string;
        label: string;
        l1Size: number;
        l2Hits: number;
        l2Misses: number;
      }>
    >();
    for (const cacheInfo of Object.values(allCacheStats)) {
      const redisId = cacheInfo.redis;
      const redisCaches = cachesByRedis.get(redisId);
      const cacheStats = {
        id: cacheInfo.id,
        label: cacheInfo.label,
        l1Size: cacheInfo.l1.size,
        l2Hits: cacheInfo.l2.hits,
        l2Misses: cacheInfo.l2.misses,
      };
      if (redisCaches) {
        redisCaches.push(cacheStats);
      } else {
        cachesByRedis.set(redisId, [cacheStats]);
      }
    }

    // Build Redis instances list with memory stats and cache info
    const redisInstances: Array<{
      id: string;
      label: string;
      memory?: {
        used: number;
        usedHuman: string;
        peak: number;
        peakHuman: string;
      };
      usagePercent?: string;
      caches?: Array<{
        id: string;
        label: string;
        l1Size: number;
        l2Hits: number;
        l2Misses: number;
      }>;
    }> = [];

    if (this.options.redis && this.options.redis.length > 0) {
      for (const redis of this.options.redis) {
        const instanceInfo: {
          id: string;
          label: string;
          memory?: {
            used: number;
            usedHuman: string;
            peak: number;
            peakHuman: string;
          };
          usagePercent?: string;
          caches?: Array<{
            id: string;
            label: string;
            l1Size: number;
            l2Hits: number;
            l2Misses: number;
          }>;
        } = {
          id: redis.id,
          label: redis.label ?? redis.id,
        };

        // Get memory stats using a queue from this Redis instance
        const queue = queuesByRedis.get(redis.id);
        if (queue) {
          try {
            const memoryStats = await this.getRedisMemoryStats(queue);
            instanceInfo.memory = memoryStats.memory;
            instanceInfo.usagePercent = memoryStats.usagePercent;
          } catch (error) {
            this.logger.warn(
              `Failed to get memory stats for Redis ${redis.id}: ${error}`,
            );
          }
        }

        // Add cache stats for this Redis instance
        const caches = cachesByRedis.get(redis.id);
        if (caches && caches.length > 0) {
          instanceInfo.caches = caches;
        }

        redisInstances.push(instanceInfo);
      }
    } else {
      // Single Redis instance - get memory from first available queue
      const firstQueue =
        queuesByRedis.get(DEFAULT_REDIS_INSTANCE_ID) ??
        queues.values().next().value?.queue;
      const instanceInfo: {
        id: string;
        label: string;
        memory?: {
          used: number;
          usedHuman: string;
          peak: number;
          peakHuman: string;
        };
        usagePercent?: string;
        caches?: Array<{
          id: string;
          label: string;
          l1Size: number;
          l2Hits: number;
          l2Misses: number;
        }>;
      } = {
        id: DEFAULT_REDIS_INSTANCE_ID,
        label: DEFAULT_REDIS_INSTANCE_LABEL,
      };

      if (firstQueue) {
        try {
          const memoryStats = await this.getRedisMemoryStats(firstQueue);
          instanceInfo.memory = memoryStats.memory;
          instanceInfo.usagePercent = memoryStats.usagePercent;
        } catch (error) {
          this.logger.warn(
            `Failed to get memory stats for default Redis: ${error}`,
          );
        }
      }

      // Add cache stats for default Redis
      const caches = cachesByRedis.get(DEFAULT_REDIS_INSTANCE_ID);
      if (caches && caches.length > 0) {
        instanceInfo.caches = caches;
      }

      redisInstances.push(instanceInfo);
    }

    return { queues: queueList, redisInstances };
  }

  /**
   * Generate a unique migration ID
   */
  private generateMigrationId(): string {
    this.migrationCounter++;
    const timestamp = Date.now().toString(36);
    const counter = this.migrationCounter.toString(36).padStart(4, '0');
    return `mig_${timestamp}${counter}`;
  }

  /**
   * Start a migration in the background
   * Returns immediately with a migration ID for tracking
   */
  async startMigration(
    sourceQueue: Queue,
    targetQueue: Queue,
    params: MigrationParams,
    sourceRedisId: string,
    targetRedisId: string,
    estimatedTotal: number,
  ): Promise<MigrationStartResponse> {
    this.assertDistinctMigrationTargets(
      sourceQueue,
      targetQueue,
      sourceRedisId,
      targetRedisId,
    );
    const migrationId = this.generateMigrationId();
    const isCrossRedis = sourceRedisId !== targetRedisId;
    const now = new Date().toISOString();

    // Create initial status
    const status: MigrationStatus = {
      migrationId,
      status: 'pending',
      params,
      progress: {
        total: estimatedTotal,
        processed: 0,
        percent: 0,
        byState: { waiting: 0, delayed: 0, failed: 0 },
      },
      errors: [],
      isCrossRedis,
      startedAt: now,
      updatedAt: now,
    };

    // Add rate limit info if present
    if (params.batchSize) {
      status.rateLimit = {
        batchSize: params.batchSize,
        delayBetweenBatchesMs: params.delayBetweenBatchesMs ?? 0,
      };
      // Estimate total batches
      if (status.progress) {
        status.progress.totalBatches = Math.ceil(
          estimatedTotal / params.batchSize,
        );
      }
    }

    // Create abort controller for cancellation
    const abortController = new AbortController();

    // Store active migration
    const activeMigration: ActiveMigration = {
      status,
      abortController,
      sourceQueue,
      targetQueue,
      isPaused: false,
    };
    this.activeMigrations.set(migrationId, activeMigration);

    // Start migration in background (don't await)
    activeMigration.runPromise = this.runMigrationInBackground(
      migrationId,
      sourceRedisId,
      targetRedisId,
    );

    this.logger.log(`Migration ${migrationId} started in background`);

    return {
      migrationId,
      status: 'pending',
      message: 'Migration started. Poll status endpoint for progress.',
      timestamp: now,
    };
  }

  /**
   * Run migration in background with progress tracking
   */
  private async runMigrationInBackground(
    migrationId: string,
    sourceRedisId: string,
    targetRedisId: string,
  ): Promise<void> {
    const migration = this.activeMigrations.get(migrationId);
    if (!migration) {
      this.logger.error(
        `Migration ${migrationId} not found in active migrations`,
      );
      return;
    }

    const { status, abortController, sourceQueue, targetQueue } = migration;
    const progress = status.progress;
    if (!progress) {
      status.status = 'failed';
      status.error = 'Migration progress was not initialized';
      status.completedAt = new Date().toISOString();
      status.updatedAt = status.completedAt;
      return;
    }

    const params = status.params;
    const states: MigrationJobState[] = params.states ?? ['waiting', 'delayed'];
    const batchSize = params.batchSize;
    const delayBetweenBatchesMs = params.delayBetweenBatchesMs ?? 0;

    // Update status to running
    status.status = 'running';
    status.updatedAt = new Date().toISOString();

    const startTime = Date.now();
    let totalProcessed = 0;
    let currentBatchCount = 0;
    let batchesProcessed = 0;
    let requiresIntervention = false;
    let queueGuard: MigrationQueueGuard | undefined;
    let lease: MigrationLease | undefined;

    this.logger.log(
      `Migration ${migrationId}: Starting execution ` +
        `${params.sourceQueue} (${sourceRedisId}) -> ${params.targetQueue} (${targetRedisId})` +
        (params.jobType ? ` [jobType=${params.jobType}]` : '') +
        (status.isCrossRedis ? ' [CROSS-REDIS]' : '') +
        (batchSize
          ? ` [batchSize=${batchSize}, delay=${delayBetweenBatchesMs}ms]`
          : ''),
    );

    try {
      lease = await this.acquireMigrationLease(sourceQueue, sourceRedisId);
      queueGuard = await this.pauseQueuesForMigration(sourceQueue, targetQueue);

      for (const state of states) {
        // Check for cancellation
        if (abortController.signal.aborted) {
          throw new Error('Migration cancelled');
        }

        const remainingLimit = params.limit
          ? params.limit - totalProcessed
          : undefined;
        if (remainingLimit !== undefined && remainingLimit <= 0) break;

        const jobs = await this.getJobsByState(
          sourceQueue,
          state,
          params.jobType,
          remainingLimit,
        );

        for (const jobInfo of jobs) {
          // Check for cancellation
          if (abortController.signal.aborted) {
            throw new Error('Migration cancelled');
          }

          // Check for pause
          if (migration.isPaused) {
            this.logger.log(
              `Migration ${migrationId}: Paused at ${totalProcessed} jobs`,
            );
            await this.waitWhilePaused(migration);
            // Check for cancellation after resume
            if (abortController.signal.aborted) {
              throw new Error('Migration cancelled');
            }
            this.logger.log(`Migration ${migrationId}: Resumed`);
          }

          if (params.limit && totalProcessed >= params.limit) break;

          // Check if we need to pause between batches
          if (batchSize && currentBatchCount >= batchSize) {
            batchesProcessed++;
            progress.currentBatch = batchesProcessed;

            if (delayBetweenBatchesMs > 0) {
              this.logger.debug(
                `Migration ${migrationId}: Batch ${batchesProcessed} complete, waiting ${delayBetweenBatchesMs}ms...`,
              );
              await this.sleep(delayBetweenBatchesMs, abortController.signal);
            }
            currentBatchCount = 0;
          }

          try {
            await lease.refresh();
            await this.migrateJob(sourceQueue, targetQueue, jobInfo, state);

            // Update progress
            totalProcessed++;
            currentBatchCount++;
            progress.byState[state]++;
            progress.processed = totalProcessed;
            progress.percent =
              progress.total > 0
                ? Math.round((totalProcessed / progress.total) * 100)
                : 100;
            status.updatedAt = new Date().toISOString();

            this.logger.debug(
              `Migration ${migrationId}: Migrated job ${jobInfo.id} (${state})`,
            );
          } catch (error) {
            const errorMsg =
              error instanceof Error ? error.message : String(error);
            if (error instanceof MigrationRollbackError) {
              requiresIntervention = true;
            }
            if (
              error instanceof Error &&
              error.message.includes('Migration lease was lost')
            ) {
              requiresIntervention = true;
              throw error;
            }
            if (status.errors.length < MAX_MIGRATION_ERRORS) {
              status.errors.push({ jobId: jobInfo.id, error: errorMsg });
            }
            this.logger.warn(
              `Migration ${migrationId}: Failed to migrate job ${jobInfo.id}: ${errorMsg}`,
            );
          }
        }
      }

      // Count the final partial batch
      if (currentBatchCount > 0 && batchSize) {
        batchesProcessed++;
      }

      const durationMs = Date.now() - startTime;

      // Build final result
      const result: MigrationResult = {
        sourceQueue: params.sourceQueue,
        sourceRedis: sourceRedisId,
        targetQueue: params.targetQueue,
        targetRedis: targetRedisId,
        jobType: params.jobType,
        migrated: {
          waiting: progress.byState.waiting,
          delayed: progress.byState.delayed,
          failed: progress.byState.failed,
          total: totalProcessed,
        },
        errors: status.errors,
        isCrossRedis: status.isCrossRedis,
        durationMs,
        timestamp: new Date().toISOString(),
      };

      if (batchSize) {
        result.rateLimit = {
          batchSize,
          delayBetweenBatchesMs,
          batchesProcessed,
        };
      }

      // Update status to completed
      status.status = 'completed';
      status.result = result;
      status.completedAt = new Date().toISOString();
      status.updatedAt = status.completedAt;
      progress.percent = 100;

      this.logger.log(
        `Migration ${migrationId}: Completed - ${totalProcessed} jobs migrated in ${durationMs}ms` +
          (status.errors.length > 0 ? `, ${status.errors.length} errors` : ''),
      );
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : String(error);

      if (errorMsg === 'Migration cancelled') {
        status.status = 'cancelled';
        this.logger.log(
          `Migration ${migrationId}: Cancelled after ${totalProcessed} jobs`,
        );
      } else {
        status.status = 'failed';
        status.error = errorMsg;
        this.logger.error(`Migration ${migrationId}: Failed - ${errorMsg}`);
      }

      status.completedAt = new Date().toISOString();
      status.updatedAt = status.completedAt;
    } finally {
      const leavePaused =
        requiresIntervention ||
        (status.status === 'failed' && queueGuard !== undefined);
      await queueGuard?.restore(leavePaused);
      await lease?.release();
    }

    // Clean up old completed migrations after 1 hour
    migration.cleanupTimer = setTimeout(
      () => {
        this.activeMigrations.delete(migrationId);
        this.logger.debug(`Migration ${migrationId}: Cleaned up from memory`);
      },
      60 * 60 * 1000,
    );
    migration.cleanupTimer.unref?.();
  }

  /**
   * Get the status of a migration
   */
  getMigrationStatus(migrationId: string): MigrationStatus | undefined {
    return this.activeMigrations.get(migrationId)?.status;
  }

  /**
   * Cancel a running migration
   */
  cancelMigration(migrationId: string): MigrationCancelResponse {
    const migration = this.activeMigrations.get(migrationId);

    if (!migration) {
      return {
        migrationId,
        success: false,
        message: 'Migration not found',
        timestamp: new Date().toISOString(),
      };
    }

    if (
      migration.status.status !== 'running' &&
      migration.status.status !== 'pending' &&
      migration.status.status !== 'paused'
    ) {
      return {
        migrationId,
        success: false,
        message: `Migration cannot be cancelled (status: ${migration.status.status})`,
        timestamp: new Date().toISOString(),
      };
    }

    // Signal cancellation
    migration.abortController.abort();
    migration.resumeResolver?.();

    return {
      migrationId,
      success: true,
      message: 'Cancellation requested',
      timestamp: new Date().toISOString(),
    };
  }

  /**
   * Pause a running migration
   */
  pauseMigration(migrationId: string): MigrationPauseResumeResponse {
    const migration = this.activeMigrations.get(migrationId);

    if (!migration) {
      return {
        migrationId,
        success: false,
        status: 'failed',
        message: 'Migration not found',
        timestamp: new Date().toISOString(),
      };
    }

    if (migration.status.status !== 'running') {
      return {
        migrationId,
        success: false,
        status: migration.status.status,
        message: `Migration cannot be paused (status: ${migration.status.status})`,
        timestamp: new Date().toISOString(),
      };
    }

    // Set pause flag
    migration.isPaused = true;
    migration.status.status = 'paused';
    migration.status.updatedAt = new Date().toISOString();

    this.logger.log(`Migration ${migrationId}: Pause requested`);

    return {
      migrationId,
      success: true,
      status: 'paused',
      message: 'Migration paused',
      timestamp: new Date().toISOString(),
    };
  }

  /**
   * Resume a paused migration
   */
  resumeMigration(migrationId: string): MigrationPauseResumeResponse {
    const migration = this.activeMigrations.get(migrationId);

    if (!migration) {
      return {
        migrationId,
        success: false,
        status: 'failed',
        message: 'Migration not found',
        timestamp: new Date().toISOString(),
      };
    }

    if (migration.status.status !== 'paused') {
      return {
        migrationId,
        success: false,
        status: migration.status.status,
        message: `Migration cannot be resumed (status: ${migration.status.status})`,
        timestamp: new Date().toISOString(),
      };
    }

    // Clear pause flag and resolve the waiting promise
    migration.isPaused = false;
    migration.status.status = 'running';
    migration.status.updatedAt = new Date().toISOString();

    if (migration.resumeResolver) {
      migration.resumeResolver();
      migration.resumeResolver = undefined;
    }

    this.logger.log(`Migration ${migrationId}: Resumed`);

    return {
      migrationId,
      success: true,
      status: 'running',
      message: 'Migration resumed',
      timestamp: new Date().toISOString(),
    };
  }

  /**
   * List all active migrations
   */
  listMigrations(): MigrationStatus[] {
    return Array.from(this.activeMigrations.values()).map((m) => m.status);
  }

  // ============================================
  // Cache Migration Methods
  // ============================================

  /** Active cache migrations store (in-memory) */
  private readonly activeCacheMigrations: Map<string, ActiveCacheMigration> =
    new Map();
  /** Counter for generating unique cache migration IDs */
  private cacheMigrationCounter = 0;

  /**
   * Find a cache configuration by ID across all processors
   */
  findCacheConfig(
    cacheId: string,
    queues: Map<string, { queue: Queue; processor?: QueuebertProcessor }>,
  ):
    | {
        config: QueuebertCacheConfig;
        queueName: string;
        processor: QueuebertProcessor;
      }
    | undefined {
    for (const [queueName, { processor }] of queues.entries()) {
      if (!processor || typeof processor.getCacheConfigs !== 'function') {
        continue;
      }

      try {
        const configs = processor.getCacheConfigs();
        const config = configs.find((c) => c.id === cacheId);
        if (config) {
          return { config, queueName, processor };
        }
      } catch {
        // Skip processors that fail to return configs
      }
    }
    return undefined;
  }

  /**
   * Get all cache configurations across all processors
   */
  getAllCacheConfigs(
    queues: Map<string, { queue: Queue; processor?: QueuebertProcessor }>,
  ): Array<{ config: QueuebertCacheConfig; queueName: string }> {
    const result: Array<{ config: QueuebertCacheConfig; queueName: string }> =
      [];

    for (const [queueName, { processor }] of queues.entries()) {
      if (!processor || typeof processor.getCacheConfigs !== 'function') {
        continue;
      }

      try {
        const configs = processor.getCacheConfigs();
        for (const config of configs) {
          result.push({ config, queueName });
        }
      } catch {
        // Skip processors that fail to return configs
      }
    }
    return result;
  }

  /**
   * Preview a cache migration - count keys that would be migrated
   */
  async previewCacheMigration(
    params: CacheMigrationParams,
    queues: Map<string, { queue: Queue; processor?: QueuebertProcessor }>,
    sourceRedisClient: RedisClientForMigration,
    targetRedisId: string,
  ): Promise<CacheMigrationPreview> {
    if (params.sourceRedis === targetRedisId) {
      throw new Error('Cache migration source and target Redis must differ');
    }
    const cacheInfo = this.findCacheConfig(params.sourceCache, queues);
    if (!cacheInfo) {
      throw new Error(`Cache '${params.sourceCache}' not found`);
    }

    const { config } = cacheInfo;
    const keyPattern = params.keyPattern
      ? `${config.keyPrefix}:${params.keyPattern}`
      : `${config.keyPrefix}:*`;

    // Count incrementally so a preview cannot materialize an entire keyspace.
    let keyCount = 0;
    const sampleKeys: string[] = [];
    for await (const batch of this.scanKeyBatches(
      sourceRedisClient,
      keyPattern,
      1000,
      params.limit,
    )) {
      keyCount += batch.length;
      for (const key of batch) {
        if (sampleKeys.length >= 10) break;
        sampleKeys.push(key.replace(`${config.keyPrefix}:`, ''));
      }
    }

    // Calculate estimated duration
    const batchSize = params.batchSize ?? 1000;
    const delayMs = params.delayBetweenBatchesMs ?? 100;
    const batches = Math.ceil(keyCount / batchSize);
    const estimatedDurationMs = batches * delayMs + keyCount * 2; // ~2ms per key for get+set

    return {
      sourceCache: params.sourceCache,
      sourceRedis: params.sourceRedis,
      targetRedis: targetRedisId,
      keyPrefix: config.keyPrefix,
      keyCount,
      sampleKeys,
      estimatedDurationMs,
      isCrossRedis: params.sourceRedis !== targetRedisId,
      timestamp: new Date().toISOString(),
    };
  }

  /**
   * Start a cache migration in the background
   */
  async startCacheMigration(
    params: CacheMigrationParams,
    queues: Map<string, { queue: Queue; processor?: QueuebertProcessor }>,
    sourceRedisClient: RedisClientForMigration,
    targetRedisClient: RedisClientForMigration,
    estimatedKeyCount: number,
  ): Promise<MigrationStartResponse> {
    if (params.sourceRedis === params.targetRedis) {
      throw new Error('Cache migration source and target Redis must differ');
    }
    const migrationId = `cache-${++this.cacheMigrationCounter}-${Date.now()}`;
    const cacheInfo = this.findCacheConfig(params.sourceCache, queues);

    if (!cacheInfo) {
      throw new Error(`Cache '${params.sourceCache}' not found`);
    }

    const now = new Date().toISOString();
    const batchSize = params.batchSize ?? 1000;
    const delayMs = params.delayBetweenBatchesMs ?? 100;

    const status: CacheMigrationStatus = {
      migrationId,
      status: 'pending',
      params,
      keyPrefix: cacheInfo.config.keyPrefix,
      progress: {
        total: estimatedKeyCount,
        processed: 0,
        migrated: 0,
        failed: 0,
        percent: 0,
        totalBatches: Math.ceil(estimatedKeyCount / batchSize),
      },
      errors: [],
      isCrossRedis: params.sourceRedis !== params.targetRedis,
      rateLimit: { batchSize, delayBetweenBatchesMs: delayMs },
      startedAt: now,
      updatedAt: now,
    };

    const abortController = new AbortController();

    const activeMigration: ActiveCacheMigration = {
      status,
      abortController,
      sourceRedis: sourceRedisClient,
      targetRedis: targetRedisClient,
      keyPrefix: cacheInfo.config.keyPrefix,
      isPaused: false,
    };
    this.activeCacheMigrations.set(migrationId, activeMigration);

    // Start migration in background
    activeMigration.runPromise = this.runCacheMigrationInBackground(
      migrationId,
      params,
    );

    this.logger.log(`Cache migration ${migrationId} started in background`);

    return {
      migrationId,
      status: 'pending',
      message: 'Cache migration started. Poll status endpoint for progress.',
      timestamp: now,
    };
  }

  /**
   * Run cache migration in background with progress tracking
   */
  private async runCacheMigrationInBackground(
    migrationId: string,
    params: CacheMigrationParams,
  ): Promise<void> {
    const migration = this.activeCacheMigrations.get(migrationId);
    if (!migration) {
      this.logger.error(`Cache migration ${migrationId} not found`);
      return;
    }

    const { status, abortController, sourceRedis, targetRedis, keyPrefix } =
      migration;
    const progress = status.progress;
    if (!progress) {
      status.status = 'failed';
      status.error = 'Cache migration progress was not initialized';
      status.completedAt = new Date().toISOString();
      status.updatedAt = status.completedAt;
      return;
    }

    const batchSize = params.batchSize ?? 1000;
    const delayMs = params.delayBetweenBatchesMs ?? 100;

    status.status = 'running';
    status.updatedAt = new Date().toISOString();

    const startTime = Date.now();
    let totalProcessed = 0;
    let totalMigrated = 0;
    let totalFailed = 0;
    let batchesProcessed = 0;

    this.logger.log(
      `Cache migration ${migrationId}: Starting ${keyPrefix}:* ` +
        `from ${params.sourceRedis} to ${params.targetRedis}` +
        ` [batchSize=${batchSize}, delay=${delayMs}ms]`,
    );

    try {
      const keyPattern = params.keyPattern
        ? `${keyPrefix}:${params.keyPattern}`
        : `${keyPrefix}:*`;

      for await (const batch of this.scanKeyBatches(
        sourceRedis,
        keyPattern,
        batchSize,
        params.limit,
      )) {
        // Check for cancellation
        if (abortController.signal.aborted) {
          throw new Error('Migration cancelled');
        }

        // Check for pause
        if (migration.isPaused) {
          this.logger.log(
            `Cache migration ${migrationId}: Paused at ${totalProcessed} keys`,
          );
          await this.waitWhileCacheMigrationPaused(migration);
          if (abortController.signal.aborted) {
            throw new Error('Migration cancelled');
          }
          this.logger.log(`Cache migration ${migrationId}: Resumed`);
        }

        const batchStartTime = Date.now();

        const [values, ttls] = await Promise.all([
          sourceRedis.mget(...batch),
          Promise.all(batch.map((key) => sourceRedis.pttl(key))),
        ]);

        // Set values in target with TTL
        for (let j = 0; j < batch.length; j++) {
          const key = batch[j];
          const value = values[j];
          const ttlMs = ttls[j];

          if (value !== null && value !== undefined && ttlMs !== -2) {
            try {
              if (ttlMs === -1) {
                await targetRedis.set(key, value);
              } else {
                await targetRedis.psetex(key, Math.max(1, ttlMs), value);
              }
              totalMigrated++;
            } catch (error) {
              totalFailed++;
              const errorMsg =
                error instanceof Error ? error.message : String(error);
              if (status.errors.length < 100) {
                // Limit stored errors
                status.errors.push({
                  key: key.replace(`${keyPrefix}:`, ''),
                  error: errorMsg,
                });
              }
            }
          }
          totalProcessed++;
        }

        batchesProcessed++;
        progress.processed = totalProcessed;
        progress.migrated = totalMigrated;
        progress.failed = totalFailed;
        progress.percent =
          progress.total > 0
            ? Math.min(100, Math.round((totalProcessed / progress.total) * 100))
            : 100;
        progress.currentBatch = batchesProcessed;
        status.updatedAt = new Date().toISOString();

        this.logger.debug(
          `Cache migration ${migrationId}: Batch ${batchesProcessed} complete ` +
            `(${totalProcessed}/${progress.total} keys, ${Date.now() - batchStartTime}ms)`,
        );

        // Delay between batches
        if (delayMs > 0) {
          await this.sleep(delayMs, abortController.signal);
        }
      }

      // Migration completed successfully
      const durationMs = Date.now() - startTime;
      status.status = 'completed';
      status.completedAt = new Date().toISOString();
      status.updatedAt = status.completedAt;

      this.logger.log(
        `Cache migration ${migrationId}: Completed - ` +
          `${totalMigrated} keys migrated, ${totalFailed} failed ` +
          `in ${durationMs}ms (${batchesProcessed} batches)`,
      );
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : String(error);

      if (errorMsg === 'Migration cancelled') {
        status.status = 'cancelled';
        this.logger.log(
          `Cache migration ${migrationId}: Cancelled after ${totalProcessed} keys`,
        );
      } else {
        status.status = 'failed';
        status.error = errorMsg;
        this.logger.error(
          `Cache migration ${migrationId}: Failed - ${errorMsg}`,
        );
      }

      status.completedAt = new Date().toISOString();
      status.updatedAt = status.completedAt;
    }

    // Clean up after 1 hour
    migration.cleanupTimer = setTimeout(
      () => {
        this.activeCacheMigrations.delete(migrationId);
        this.logger.debug(
          `Cache migration ${migrationId}: Cleaned up from memory`,
        );
      },
      60 * 60 * 1000,
    );
    migration.cleanupTimer.unref?.();
  }

  /**
   * Wait while cache migration is paused
   */
  private async waitWhileCacheMigrationPaused(
    migration: ActiveCacheMigration,
  ): Promise<void> {
    return new Promise<void>((resolve) => {
      migration.resumeResolver = resolve;
    });
  }

  /**
   * Get the status of a cache migration
   */
  getCacheMigrationStatus(
    migrationId: string,
  ): CacheMigrationStatus | undefined {
    return this.activeCacheMigrations.get(migrationId)?.status;
  }

  /**
   * Cancel a running cache migration
   */
  cancelCacheMigration(migrationId: string): MigrationCancelResponse {
    const migration = this.activeCacheMigrations.get(migrationId);

    if (!migration) {
      return {
        migrationId,
        success: false,
        message: 'Cache migration not found',
        timestamp: new Date().toISOString(),
      };
    }

    if (
      migration.status.status !== 'running' &&
      migration.status.status !== 'pending' &&
      migration.status.status !== 'paused'
    ) {
      return {
        migrationId,
        success: false,
        message: `Cache migration cannot be cancelled (status: ${migration.status.status})`,
        timestamp: new Date().toISOString(),
      };
    }

    migration.abortController.abort();
    if (migration.resumeResolver) {
      migration.resumeResolver();
    }

    return {
      migrationId,
      success: true,
      message: 'Cancellation requested',
      timestamp: new Date().toISOString(),
    };
  }

  /**
   * Pause a running cache migration
   */
  pauseCacheMigration(migrationId: string): MigrationPauseResumeResponse {
    const migration = this.activeCacheMigrations.get(migrationId);

    if (!migration) {
      return {
        migrationId,
        success: false,
        status: 'failed',
        message: 'Cache migration not found',
        timestamp: new Date().toISOString(),
      };
    }

    if (migration.status.status !== 'running') {
      return {
        migrationId,
        success: false,
        status: migration.status.status,
        message: `Cache migration cannot be paused (status: ${migration.status.status})`,
        timestamp: new Date().toISOString(),
      };
    }

    migration.isPaused = true;
    migration.status.status = 'paused';
    migration.status.updatedAt = new Date().toISOString();

    this.logger.log(`Cache migration ${migrationId}: Pause requested`);

    return {
      migrationId,
      success: true,
      status: 'paused',
      message: 'Cache migration paused',
      timestamp: new Date().toISOString(),
    };
  }

  /**
   * Resume a paused cache migration
   */
  resumeCacheMigration(migrationId: string): MigrationPauseResumeResponse {
    const migration = this.activeCacheMigrations.get(migrationId);

    if (!migration) {
      return {
        migrationId,
        success: false,
        status: 'failed',
        message: 'Cache migration not found',
        timestamp: new Date().toISOString(),
      };
    }

    if (migration.status.status !== 'paused') {
      return {
        migrationId,
        success: false,
        status: migration.status.status,
        message: `Cache migration cannot be resumed (status: ${migration.status.status})`,
        timestamp: new Date().toISOString(),
      };
    }

    migration.isPaused = false;
    migration.status.status = 'running';
    migration.status.updatedAt = new Date().toISOString();

    if (migration.resumeResolver) {
      migration.resumeResolver();
      migration.resumeResolver = undefined;
    }

    this.logger.log(`Cache migration ${migrationId}: Resumed`);

    return {
      migrationId,
      success: true,
      status: 'running',
      message: 'Cache migration resumed',
      timestamp: new Date().toISOString(),
    };
  }

  /**
   * List all active cache migrations
   */
  listCacheMigrations(): CacheMigrationStatus[] {
    return Array.from(this.activeCacheMigrations.values()).map((m) => m.status);
  }
}

/**
 * Internal state for tracking active cache migrations
 */
interface ActiveCacheMigration {
  status: CacheMigrationStatus;
  abortController: AbortController;
  sourceRedis: RedisClientForMigration;
  targetRedis: RedisClientForMigration;
  keyPrefix: string;
  isPaused: boolean;
  resumeResolver?: () => void;
  cleanupTimer?: ReturnType<typeof setTimeout>;
  runPromise?: Promise<void>;
}

/**
 * Minimal Redis client interface for cache migration
 */
interface RedisClientForMigration {
  scan: (
    cursor: string,
    matchKeyword: 'MATCH',
    pattern: string,
    countKeyword: 'COUNT',
    count: number,
  ) => Promise<[string, string[]]>;
  mget: (...keys: string[]) => Promise<(string | null)[]>;
  pttl: (key: string) => Promise<number>;
  set: (key: string, value: string) => Promise<unknown>;
  psetex: (
    key: string,
    milliseconds: number,
    value: string,
  ) => Promise<unknown>;
}
