import {
  Controller,
  Get,
  Post,
  Query,
  Param,
  Inject,
  HttpException,
  HttpStatus,
} from '@nestjs/common';
import type { Type } from '@nestjs/common';
import * as BullMQ from 'bullmq';

import { QueuebertService } from './queuebert.service';
import {
  QUEUEBERT_OPTIONS,
  QUEUEBERT_QUEUES,
  DEFAULT_REDIS_INSTANCE_ID,
} from './types';
import type {
  QueuebertModuleOptions,
  QueuebertProcessor,
  QueuebertEndpoint,
  MigrationParams,
  MigrationJobState,
  CacheMigrationParams,
} from './types';

const VALID_MIGRATION_STATES: MigrationJobState[] = [
  'waiting',
  'delayed',
  'failed',
];
const MAX_MIGRATION_LIMIT = 100_000;
const MAX_MIGRATION_BATCH_SIZE = 5_000;
const MAX_MIGRATION_DELAY_MS = 60_000;

function parsePositiveInteger(
  value: string | undefined,
  name: string,
  max?: number,
): number | undefined {
  if (value === undefined || value === '') return undefined;

  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new HttpException(
      `${name} must be a positive integer`,
      HttpStatus.BAD_REQUEST,
    );
  }
  if (max !== undefined && parsed > max) {
    throw new HttpException(
      `${name} must be at most ${max}`,
      HttpStatus.BAD_REQUEST,
    );
  }
  return parsed;
}

function parseNonNegativeInteger(
  value: string | undefined,
  name: string,
  max?: number,
): number | undefined {
  if (value === undefined || value === '') return undefined;

  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new HttpException(
      `${name} must be a non-negative integer`,
      HttpStatus.BAD_REQUEST,
    );
  }
  if (max !== undefined && parsed > max) {
    throw new HttpException(
      `${name} must be at most ${max}`,
      HttpStatus.BAD_REQUEST,
    );
  }
  return parsed;
}

function parseMigrationStates(
  states: string | undefined,
): MigrationJobState[] | undefined {
  if (states === undefined || states.trim() === '') return undefined;

  const parsedStates = states.split(',').map((state) => state.trim());
  const invalidStates = parsedStates.filter(
    (state) => !VALID_MIGRATION_STATES.includes(state as MigrationJobState),
  );

  if (invalidStates.length > 0 || parsedStates.length === 0) {
    throw new HttpException(
      `states must contain only: ${VALID_MIGRATION_STATES.join(', ')}`,
      HttpStatus.BAD_REQUEST,
    );
  }

  return parsedStates as MigrationJobState[];
}

/**
 * Interface for the public methods of QueuebertController
 * This allows us to define an explicit return type for the factory function
 */
export interface IQueuebertController {
  getAllStats(): Promise<unknown>;
  pauseAll(): Promise<unknown>;
  resumeAll(): Promise<unknown>;
  getQueueStats(queueName: string): Promise<unknown>;
  pauseQueue(queueName: string): Promise<unknown>;
  resumeQueue(queueName: string): Promise<unknown>;
  cleanQueue(
    queueName: string,
    grace?: string,
    limit?: string,
  ): Promise<unknown>;
  drainQueue(queueName: string): Promise<unknown>;
  getQueueMetrics(queueName: string): Promise<unknown>;
  getMigrationQueues(): Promise<unknown>;
  listMigrations(): Promise<unknown>;
  previewMigration(
    sourceQueue: string,
    targetQueue: string,
    sourceRedis?: string,
    targetRedis?: string,
    jobType?: string,
    states?: string,
    limit?: string,
  ): Promise<unknown>;
  startMigration(
    sourceQueue: string,
    targetQueue: string,
    sourceRedis?: string,
    targetRedis?: string,
    jobType?: string,
    states?: string,
    limit?: string,
    batchSize?: string,
    delayMs?: string,
  ): Promise<unknown>;
  getMigrationStatus(migrationId: string): Promise<unknown>;
  pauseMigration(migrationId: string): Promise<unknown>;
  resumeMigration(migrationId: string): Promise<unknown>;
  cancelMigration(migrationId: string): Promise<unknown>;
  executeMigration(
    sourceQueue: string,
    targetQueue: string,
    sourceRedis?: string,
    targetRedis?: string,
    jobType?: string,
    states?: string,
    limit?: string,
    batchSize?: string,
    delayMs?: string,
  ): Promise<unknown>;
}

/**
 * Factory function to create a Queuebert controller with a configurable path
 */
export function createQueuebertController(
  basePath = 'admin/queue',
): Type<IQueuebertController> {
  @Controller(basePath)
  class QueuebertController implements IQueuebertController {
    private readonly _queuebertService: QueuebertService;
    private readonly _options: QueuebertModuleOptions;
    private readonly _getQueues: () => Map<
      string,
      { queue: BullMQ.Queue; processor?: QueuebertProcessor }
    >;

    constructor(
      queuebertService: QueuebertService,
      @Inject(QUEUEBERT_OPTIONS) options: QueuebertModuleOptions,
      @Inject(QUEUEBERT_QUEUES)
      getQueues: () => Map<
        string,
        { queue: BullMQ.Queue; processor?: QueuebertProcessor }
      >,
    ) {
      this._queuebertService = queuebertService;
      this._options = options;
      this._getQueues = getQueues;
    }

    /**
     * Get the queues map (lazy loaded after module initialization)
     */
    private get queues(): Map<
      string,
      { queue: BullMQ.Queue; processor?: QueuebertProcessor }
    > {
      return this._getQueues();
    }

    private getRedisIdForStatsKey(statsKey: string): string {
      const queueConfig = this._options.queues.find(
        (queue) => (queue.statsKey ?? queue.name) === statsKey,
      );
      return (
        queueConfig?.redis ??
        this._options.redis?.[0]?.id ??
        DEFAULT_REDIS_INSTANCE_ID
      );
    }

    /**
     * Get the queue and processor by name, throwing if not found
     */
    private getQueueByName(name: string): {
      queue: BullMQ.Queue;
      processor?: QueuebertProcessor;
    } {
      const queueConfig = this.queues.get(name);
      if (!queueConfig) {
        throw new HttpException(
          `Queue '${name}' not found. Available queues: ${Array.from(this.queues.keys()).join(', ')}`,
          HttpStatus.NOT_FOUND,
        );
      }
      return queueConfig;
    }

    /**
     * Check if an endpoint is enabled, throwing 404 if not
     */
    private requireEndpoint(endpoint: QueuebertEndpoint): void {
      if (!this._queuebertService.isEndpointEnabled(endpoint)) {
        throw new HttpException(
          `Endpoint '${endpoint}' is not enabled on this Queuebert instance`,
          HttpStatus.NOT_FOUND,
        );
      }
    }

    /**
     * GET /stats
     * Get stats for all configured queues
     */
    @Get('stats')
    async getAllStats() {
      return this._queuebertService.getAllQueueStats(this.queues);
    }

    /**
     * POST /pause
     * Pause all configured queues
     */
    @Post('pause')
    async pauseAll() {
      this.requireEndpoint('pause');
      const queuesWithKeys = Array.from(this.queues.entries()).map(
        ([statsKey, { queue }]) => ({
          queue,
          statsKey,
        }),
      );
      return this._queuebertService.pauseQueues(queuesWithKeys);
    }

    /**
     * POST /resume
     * Resume all configured queues
     */
    @Post('resume')
    async resumeAll() {
      this.requireEndpoint('resume');
      const queuesWithKeys = Array.from(this.queues.entries()).map(
        ([statsKey, { queue }]) => ({
          queue,
          statsKey,
        }),
      );
      return this._queuebertService.resumeQueues(queuesWithKeys);
    }

    /**
     * GET /:queueName/stats
     * Get stats for a specific queue
     */
    @Get(':queueName/stats')
    async getQueueStats(@Param('queueName') queueName: string) {
      const { queue, processor } = this.getQueueByName(queueName);
      const stats = await this._queuebertService.getSingleQueueStats(
        queue,
        processor,
        queueName,
      );

      // Include Redis stats if enabled
      if (this._options.includeRedisStats !== false) {
        const redisStats =
          await this._queuebertService.getRedisMemoryStats(queue);
        return {
          ...stats,
          redis: redisStats,
          timestamp: new Date().toISOString(),
        };
      }

      return {
        ...stats,
        timestamp: new Date().toISOString(),
      };
    }

    /**
     * POST /:queueName/pause
     * Pause a specific queue
     */
    @Post(':queueName/pause')
    async pauseQueue(@Param('queueName') queueName: string) {
      this.requireEndpoint('pause');
      const { queue } = this.getQueueByName(queueName);
      return this._queuebertService.pauseQueue(
        queue,
        'manual-pause',
        queueName,
      );
    }

    /**
     * POST /:queueName/resume
     * Resume a specific queue
     */
    @Post(':queueName/resume')
    async resumeQueue(@Param('queueName') queueName: string) {
      this.requireEndpoint('resume');
      const { queue } = this.getQueueByName(queueName);
      return this._queuebertService.resumeQueue(queue, queueName);
    }

    /**
     * POST /:queueName/clean
     * Clean completed/failed jobs from a specific queue
     */
    @Post(':queueName/clean')
    async cleanQueue(
      @Param('queueName') queueName: string,
      @Query('grace') grace?: string,
      @Query('limit') limit?: string,
    ) {
      this.requireEndpoint('clean');
      const { queue, processor } = this.getQueueByName(queueName);
      const graceMs = parseNonNegativeInteger(grace, 'grace') ?? 300000;
      const maxJobs =
        parsePositiveInteger(limit, 'limit', MAX_MIGRATION_LIMIT) ?? 10000;
      return this._queuebertService.cleanQueue(
        queue,
        processor,
        graceMs,
        maxJobs,
        queueName,
      );
    }

    /**
     * POST /:queueName/drain
     * Remove all waiting jobs from a specific queue
     */
    @Post(':queueName/drain')
    async drainQueue(@Param('queueName') queueName: string) {
      this.requireEndpoint('drain');
      const { queue, processor } = this.getQueueByName(queueName);
      return this._queuebertService.drainQueue(queue, processor, queueName);
    }

    /**
     * GET /:queueName/metrics
     * Get detailed job metrics for a specific queue
     */
    @Get(':queueName/metrics')
    async getQueueMetrics(@Param('queueName') queueName: string) {
      this.requireEndpoint('metrics');
      const { queue, processor } = this.getQueueByName(queueName);
      const processorStats = processor?.getProcessorStats();

      const [waiting, active, completed, failed] = await Promise.all([
        queue.getWaitingCount(),
        queue.getActiveCount(),
        queue.getCompletedCount(),
        queue.getFailedCount(),
      ]);

      // Return queue stats with processor metrics if available
      return {
        duration: processorStats
          ? {
              avgMs: processorStats.duration.avgMs,
              minMs: processorStats.duration.minMs,
              maxMs: processorStats.duration.maxMs,
              p50Ms: processorStats.duration.p50Ms,
              p95Ms: processorStats.duration.p95Ms,
              p99Ms: processorStats.duration.p99Ms,
              recentAvgMs: processorStats.duration.recentAvgMs,
              sampleCount: processorStats.duration.sampleCount,
            }
          : undefined,
        jobs: processorStats
          ? {
              processed: processorStats.jobs.processed,
              completed: processorStats.jobs.completed,
              failed: processorStats.jobs.failed,
              failureRate: processorStats.jobs.failureRate,
              successRate: processorStats.jobs.successRate,
              lastJobTime: processorStats.jobs.lastJobTime,
            }
          : undefined,
        queue: {
          waiting,
          active,
          completed,
          failed,
          backlog: waiting + active,
        },
        throughput: processorStats
          ? {
              jobsPerMinute: processorStats.throughput.jobsPerMinute,
              windowStartTime: processorStats.throughput.windowStartTime,
              jobsInWindow: processorStats.throughput.jobsInWindow,
            }
          : undefined,
        cache: processorStats?.cache,
        custom: processorStats?.custom,
        hasProcessor: !!processor,
        timestamp: new Date().toISOString(),
      };
    }

    /**
     * GET /migrations/queues
     * Get available queues for migration with their Redis instances
     */
    @Get('migrations/queues')
    async getMigrationQueues() {
      this.requireEndpoint('migrations');
      const { queues, redisInstances } =
        await this._queuebertService.getAvailableQueuesForMigration(
          this.queues,
        );
      return {
        queues,
        redisInstances,
        timestamp: new Date().toISOString(),
      };
    }

    /**
     * GET /migrations
     * List all active and recent migrations
     */
    @Get('migrations')
    async listMigrations() {
      this.requireEndpoint('migrations');
      return {
        migrations: this._queuebertService.listMigrations(),
        timestamp: new Date().toISOString(),
      };
    }

    /**
     * Find or create a queue for a specific Redis instance.
     * First tries to find a registered queue matching the name and Redis instance.
     * If not found, creates a temporary Queue using the Redis connection config.
     */
    private async getQueueForRedisInstance(
      queueName: string,
      redisId: string,
    ): Promise<{ queue: BullMQ.Queue; isTemporary: boolean }> {
      // First, look for an existing registered queue on the requested Redis instance
      for (const [statsKey, qConfig] of this.queues.entries()) {
        const queueConfig = this._options.queues.find(
          (q) => (q.statsKey ?? q.name) === statsKey,
        );
        const queueRedisId = queueConfig?.redis || DEFAULT_REDIS_INSTANCE_ID;

        // Match by actual queue name (not statsKey) AND Redis instance
        if (qConfig.queue.name === queueName && queueRedisId === redisId) {
          return { queue: qConfig.queue, isTemporary: false };
        }
      }

      // No registered queue found - try to create a temporary one using Redis connection config
      const redisConfig = this._options.redis?.find((r) => r.id === redisId);
      if (!redisConfig?.connection) {
        throw new HttpException(
          `No queue '${queueName}' found on Redis instance '${redisId}' and no connection config available to create one. ` +
            `Either register a queue for this Redis instance or provide connection config in the redis[] configuration.`,
          HttpStatus.NOT_FOUND,
        );
      }

      // Create a temporary Queue instance for scanning
      const tempQueue = new BullMQ.Queue(queueName, {
        connection: redisConfig.connection,
      });
      return { queue: tempQueue, isTemporary: true };
    }

    /**
     * GET /migrations/preview
     * Preview a migration without executing it
     * NOTE: This route MUST be declared before /migrations/:migrationId
     * to prevent NestJS from matching 'preview' as a migrationId parameter
     */
    @Get('migrations/preview')
    async previewMigration(
      @Query('sourceQueue') sourceQueue: string,
      @Query('targetQueue') targetQueue: string,
      @Query('sourceRedis') sourceRedis?: string,
      @Query('targetRedis') targetRedis?: string,
      @Query('jobType') jobType?: string,
      @Query('states') states?: string,
      @Query('limit') limit?: string,
    ) {
      this.requireEndpoint('migrations');

      if (!sourceQueue || !targetQueue) {
        throw new HttpException(
          'sourceQueue and targetQueue are required query parameters',
          HttpStatus.BAD_REQUEST,
        );
      }

      const sourceRedisId = sourceRedis || DEFAULT_REDIS_INSTANCE_ID;
      const targetRedisId = targetRedis || DEFAULT_REDIS_INSTANCE_ID;
      const parsedStates = parseMigrationStates(states);
      const parsedLimit = parsePositiveInteger(
        limit,
        'limit',
        MAX_MIGRATION_LIMIT,
      );

      // Get source queue for the specified Redis instance
      const { queue: sourceQueueInstance, isTemporary: sourceIsTemp } =
        await this.getQueueForRedisInstance(sourceQueue, sourceRedisId);

      // Get target queue for the specified Redis instance
      let targetQueueInstance: BullMQ.Queue;
      let targetIsTemp = false;
      try {
        const result = await this.getQueueForRedisInstance(
          targetQueue,
          targetRedisId,
        );
        targetQueueInstance = result.queue;
        targetIsTemp = result.isTemporary;
      } catch {
        // Clean up source if it was temporary
        if (sourceIsTemp) {
          await sourceQueueInstance.close();
        }
        throw new HttpException(
          `Target queue '${targetQueue}' not found on Redis instance '${targetRedisId}'.`,
          HttpStatus.NOT_FOUND,
        );
      }

      const params: MigrationParams = {
        sourceQueue,
        targetQueue,
        sourceRedis: sourceRedisId,
        targetRedis: targetRedisId,
        jobType: jobType || undefined,
        states: parsedStates,
        limit: parsedLimit,
      };

      try {
        return await this._queuebertService.previewMigration(
          sourceQueueInstance,
          targetQueueInstance,
          params,
          sourceRedisId,
          targetRedisId,
        );
      } finally {
        // Clean up temporary queues
        if (sourceIsTemp) {
          await sourceQueueInstance.close();
        }
        if (targetIsTemp) {
          await targetQueueInstance.close();
        }
      }
    }

    /**
     * GET /migrations/:migrationId
     * Get the status of a specific migration
     */
    @Get('migrations/:migrationId')
    async getMigrationStatus(@Param('migrationId') migrationId: string) {
      this.requireEndpoint('migrations');

      const status = this._queuebertService.getMigrationStatus(migrationId);
      if (!status) {
        throw new HttpException(
          `Migration '${migrationId}' not found`,
          HttpStatus.NOT_FOUND,
        );
      }

      return status;
    }

    /**
     * POST /migrations/:migrationId/pause
     * Pause a running migration
     */
    @Post('migrations/:migrationId/pause')
    async pauseMigration(@Param('migrationId') migrationId: string) {
      this.requireEndpoint('migrations');
      return this._queuebertService.pauseMigration(migrationId);
    }

    /**
     * POST /migrations/:migrationId/resume
     * Resume a paused migration
     */
    @Post('migrations/:migrationId/resume')
    async resumeMigration(@Param('migrationId') migrationId: string) {
      this.requireEndpoint('migrations');
      return this._queuebertService.resumeMigration(migrationId);
    }

    /**
     * POST /migrations/:migrationId/cancel
     * Cancel a running migration
     */
    @Post('migrations/:migrationId/cancel')
    async cancelMigration(@Param('migrationId') migrationId: string) {
      this.requireEndpoint('migrations');
      return this._queuebertService.cancelMigration(migrationId);
    }

    /**
     * POST /migrations/start
     * Start a migration in the background
     * Returns immediately with a migration ID for tracking progress
     *
     * Rate limiting parameters:
     * - batchSize: Number of jobs to process per batch
     * - delayMs: Delay in milliseconds between batches
     */
    @Post('migrations/start')
    async startMigration(
      @Query('sourceQueue') sourceQueue: string,
      @Query('targetQueue') targetQueue: string,
      @Query('sourceRedis') sourceRedis?: string,
      @Query('targetRedis') targetRedis?: string,
      @Query('jobType') jobType?: string,
      @Query('states') states?: string,
      @Query('limit') limit?: string,
      @Query('batchSize') batchSize?: string,
      @Query('delayMs') delayMs?: string,
    ) {
      this.requireEndpoint('migrations');

      if (!sourceQueue || !targetQueue) {
        throw new HttpException(
          'sourceQueue and targetQueue are required query parameters',
          HttpStatus.BAD_REQUEST,
        );
      }

      const sourceRedisId = sourceRedis || DEFAULT_REDIS_INSTANCE_ID;
      const targetRedisId = targetRedis || DEFAULT_REDIS_INSTANCE_ID;
      const parsedStates = parseMigrationStates(states);
      const parsedBatchSize = parsePositiveInteger(
        batchSize,
        'batchSize',
        MAX_MIGRATION_BATCH_SIZE,
      );
      const parsedDelayMs = parseNonNegativeInteger(
        delayMs,
        'delayMs',
        MAX_MIGRATION_DELAY_MS,
      );
      const parsedLimit = parsePositiveInteger(
        limit,
        'limit',
        MAX_MIGRATION_LIMIT,
      );

      // Get source queue for the specified Redis instance
      // Note: For background migrations, we don't support temporary queues - queue must be registered
      const { queue: sourceQueueInstance, isTemporary: sourceIsTemp } =
        await this.getQueueForRedisInstance(sourceQueue, sourceRedisId);

      if (sourceIsTemp) {
        await sourceQueueInstance.close();
        throw new HttpException(
          `Background migrations require the source queue to be registered in Queuebert configuration. ` +
            `Use GET /migrations/preview to preview jobs, or register the queue with statsKey for migration.`,
          HttpStatus.BAD_REQUEST,
        );
      }

      // Get target queue for the specified Redis instance
      const { queue: targetQueueInstance, isTemporary: targetIsTemp } =
        await this.getQueueForRedisInstance(targetQueue, targetRedisId);

      if (targetIsTemp) {
        await targetQueueInstance.close();
        throw new HttpException(
          `Background migrations require the target queue to be registered in Queuebert configuration. ` +
            `Register the queue with the appropriate Redis instance.`,
          HttpStatus.BAD_REQUEST,
        );
      }

      const params: MigrationParams = {
        sourceQueue,
        targetQueue,
        sourceRedis: sourceRedisId,
        targetRedis: targetRedisId,
        jobType: jobType || undefined,
        states: parsedStates,
        limit: parsedLimit,
        batchSize: parsedBatchSize,
        delayBetweenBatchesMs: parsedDelayMs,
      };

      // First get preview to estimate total jobs
      const preview = await this._queuebertService.previewMigration(
        sourceQueueInstance,
        targetQueueInstance,
        params,
        sourceRedisId,
        targetRedisId,
      );

      // Start migration in background
      return this._queuebertService.startMigration(
        sourceQueueInstance,
        targetQueueInstance,
        params,
        sourceRedisId,
        targetRedisId,
        preview.jobsToMigrate,
      );
    }

    /**
     * POST /migrations/execute
     * Execute a migration between queues (synchronous - waits for completion)
     * For long migrations, use POST /migrations/start instead
     *
     * Rate limiting parameters:
     * - batchSize: Number of jobs to process per batch
     * - delayMs: Delay in milliseconds between batches
     */
    @Post('migrations/execute')
    async executeMigration(
      @Query('sourceQueue') sourceQueue: string,
      @Query('targetQueue') targetQueue: string,
      @Query('sourceRedis') sourceRedis?: string,
      @Query('targetRedis') targetRedis?: string,
      @Query('jobType') jobType?: string,
      @Query('states') states?: string,
      @Query('limit') limit?: string,
      @Query('batchSize') batchSize?: string,
      @Query('delayMs') delayMs?: string,
    ) {
      this.requireEndpoint('migrations');

      if (!sourceQueue || !targetQueue) {
        throw new HttpException(
          'sourceQueue and targetQueue are required query parameters',
          HttpStatus.BAD_REQUEST,
        );
      }

      const sourceRedisId = sourceRedis || DEFAULT_REDIS_INSTANCE_ID;
      const targetRedisId = targetRedis || DEFAULT_REDIS_INSTANCE_ID;
      const parsedStates = parseMigrationStates(states);
      const parsedBatchSize = parsePositiveInteger(
        batchSize,
        'batchSize',
        MAX_MIGRATION_BATCH_SIZE,
      );
      const parsedDelayMs = parseNonNegativeInteger(
        delayMs,
        'delayMs',
        MAX_MIGRATION_DELAY_MS,
      );
      const parsedLimit = parsePositiveInteger(
        limit,
        'limit',
        MAX_MIGRATION_LIMIT,
      );

      // Get source queue for the specified Redis instance
      const { queue: sourceQueueInstance, isTemporary: sourceIsTemp } =
        await this.getQueueForRedisInstance(sourceQueue, sourceRedisId);

      // Get target queue for the specified Redis instance
      let targetQueueInstance: BullMQ.Queue;
      let targetIsTemp = false;
      try {
        const result = await this.getQueueForRedisInstance(
          targetQueue,
          targetRedisId,
        );
        targetQueueInstance = result.queue;
        targetIsTemp = result.isTemporary;
      } catch {
        // Clean up source if it was temporary
        if (sourceIsTemp) {
          await sourceQueueInstance.close();
        }
        throw new HttpException(
          `Target queue '${targetQueue}' not found on Redis instance '${targetRedisId}'.`,
          HttpStatus.NOT_FOUND,
        );
      }

      const params: MigrationParams = {
        sourceQueue,
        targetQueue,
        sourceRedis: sourceRedisId,
        targetRedis: targetRedisId,
        jobType: jobType || undefined,
        states: parsedStates,
        limit: parsedLimit,
        batchSize: parsedBatchSize,
        delayBetweenBatchesMs: parsedDelayMs,
      };

      try {
        return await this._queuebertService.executeMigration(
          sourceQueueInstance,
          targetQueueInstance,
          params,
          sourceRedisId,
          targetRedisId,
        );
      } finally {
        // Clean up temporary queues
        if (sourceIsTemp) {
          await sourceQueueInstance.close();
        }
        if (targetIsTemp) {
          await targetQueueInstance.close();
        }
      }
    }

    // ============================================
    // Cache Migration Endpoints
    // ============================================

    /**
     * GET /cache/list
     * List all available caches from processors
     */
    @Get('cache/list')
    async listCaches() {
      this.requireEndpoint('migrations');
      const cacheConfigs = this._queuebertService.getAllCacheConfigs(
        this.queues,
      );
      return {
        caches: cacheConfigs.map(({ config, queueName }) => ({
          id: config.id,
          label: config.label ?? config.id,
          keyPrefix: config.keyPrefix,
          processor: queueName,
        })),
        timestamp: new Date().toISOString(),
      };
    }

    /**
     * GET /cache/migrations
     * List all active cache migrations
     */
    @Get('cache/migrations')
    listCacheMigrations() {
      this.requireEndpoint('migrations');
      return this._queuebertService.listCacheMigrations();
    }

    /**
     * GET /cache/migrations/preview
     * Preview a cache migration without executing it
     * NOTE: This route MUST be declared before /cache/migrations/:migrationId
     * to prevent NestJS from matching 'preview' as a migrationId parameter
     */
    @Get('cache/migrations/preview')
    async previewCacheMigration(
      @Query('sourceCache') sourceCache: string,
      @Query('sourceRedis') sourceRedis?: string,
      @Query('targetRedis') targetRedis?: string,
      @Query('keyPattern') keyPattern?: string,
      @Query('limit') limit?: string,
    ) {
      this.requireEndpoint('migrations');

      if (!sourceCache) {
        throw new HttpException(
          'sourceCache is a required query parameter',
          HttpStatus.BAD_REQUEST,
        );
      }

      const sourceRedisId = sourceRedis || DEFAULT_REDIS_INSTANCE_ID;
      const targetRedisId = targetRedis || DEFAULT_REDIS_INSTANCE_ID;

      // Find the cache config (to get the key prefix)
      const cacheInfo = this._queuebertService.findCacheConfig(
        sourceCache,
        this.queues,
      );
      if (!cacheInfo) {
        throw new HttpException(
          `Cache '${sourceCache}' not found`,
          HttpStatus.NOT_FOUND,
        );
      }

      // Find a Redis client for the source Redis instance
      // We need to find any queue configured for the sourceRedisId
      const queueConfig = this.queues.get(cacheInfo.queueName);
      let sourceRedisClient: Awaited<BullMQ.Queue['client']> | null = null;

      // Check if the cache's processor queue is on the requested Redis instance
      const cacheQueueRedis = this.getRedisIdForStatsKey(cacheInfo.queueName);
      if (cacheQueueRedis === sourceRedisId && queueConfig) {
        sourceRedisClient = await queueConfig.queue.client;
      } else {
        // Find any queue on the requested source Redis instance
        for (const [statsKey, qConfig] of this.queues.entries()) {
          const queueRedis = this.getRedisIdForStatsKey(statsKey);
          if (queueRedis === sourceRedisId) {
            sourceRedisClient = await qConfig.queue.client;
            break;
          }
        }
      }

      if (!sourceRedisClient) {
        throw new HttpException(
          `No queue found configured for Redis instance '${sourceRedisId}'. Available Redis instances can be found via the queue configuration.`,
          HttpStatus.NOT_FOUND,
        );
      }

      const params: CacheMigrationParams = {
        sourceCache,
        sourceRedis: sourceRedisId,
        targetRedis: targetRedisId,
        keyPattern: keyPattern || undefined,
        limit: parsePositiveInteger(limit, 'limit', MAX_MIGRATION_LIMIT),
      };

      return this._queuebertService.previewCacheMigration(
        params,
        this.queues,
        sourceRedisClient,
        targetRedisId,
      );
    }

    /**
     * GET /cache/migrations/:migrationId
     * Get the status of a cache migration
     */
    @Get('cache/migrations/:migrationId')
    getCacheMigrationStatus(@Param('migrationId') migrationId: string) {
      this.requireEndpoint('migrations');
      const status =
        this._queuebertService.getCacheMigrationStatus(migrationId);
      if (!status) {
        throw new HttpException(
          'Cache migration not found',
          HttpStatus.NOT_FOUND,
        );
      }
      return status;
    }

    /**
     * POST /cache/migrations/start
     * Start a cache migration in the background
     */
    @Post('cache/migrations/start')
    async startCacheMigration(
      @Query('sourceCache') sourceCache: string,
      @Query('sourceRedis') sourceRedis?: string,
      @Query('targetRedis') targetRedis?: string,
      @Query('keyPattern') keyPattern?: string,
      @Query('limit') limit?: string,
      @Query('batchSize') batchSize?: string,
      @Query('delayMs') delayMs?: string,
    ) {
      this.requireEndpoint('migrations');

      if (!sourceCache) {
        throw new HttpException(
          'sourceCache is a required query parameter',
          HttpStatus.BAD_REQUEST,
        );
      }

      const sourceRedisId = sourceRedis || DEFAULT_REDIS_INSTANCE_ID;
      const targetRedisId = targetRedis || DEFAULT_REDIS_INSTANCE_ID;

      // Find the cache config (to get the key prefix)
      const cacheInfo = this._queuebertService.findCacheConfig(
        sourceCache,
        this.queues,
      );
      if (!cacheInfo) {
        throw new HttpException(
          `Cache '${sourceCache}' not found`,
          HttpStatus.NOT_FOUND,
        );
      }

      // Find Redis clients for source and target instances
      let sourceRedisClient: Awaited<BullMQ.Queue['client']> | null = null;
      let targetRedisClient: Awaited<BullMQ.Queue['client']> | null = null;

      // Find clients for both source and target Redis instances
      for (const [statsKey, qConfig] of this.queues.entries()) {
        const queueRedis = this.getRedisIdForStatsKey(statsKey);

        if (queueRedis === sourceRedisId && !sourceRedisClient) {
          sourceRedisClient = await qConfig.queue.client;
        }
        if (queueRedis === targetRedisId && !targetRedisClient) {
          targetRedisClient = await qConfig.queue.client;
        }

        // Stop early if we found both
        if (sourceRedisClient && targetRedisClient) {
          break;
        }
      }

      if (!sourceRedisClient) {
        throw new HttpException(
          `No queue found configured for source Redis instance '${sourceRedisId}'.`,
          HttpStatus.NOT_FOUND,
        );
      }

      // Default target to source if same Redis instance
      if (!targetRedisClient) {
        if (targetRedisId === sourceRedisId) {
          targetRedisClient = sourceRedisClient;
        } else {
          throw new HttpException(
            `No queue found configured for target Redis instance '${targetRedisId}'.`,
            HttpStatus.NOT_FOUND,
          );
        }
      }

      // Parse rate limiting parameters
      const parsedBatchSize = parsePositiveInteger(
        batchSize,
        'batchSize',
        MAX_MIGRATION_BATCH_SIZE,
      );
      const parsedDelayMs = parseNonNegativeInteger(
        delayMs,
        'delayMs',
        MAX_MIGRATION_DELAY_MS,
      );

      const params: CacheMigrationParams = {
        sourceCache,
        sourceRedis: sourceRedisId,
        targetRedis: targetRedisId,
        keyPattern: keyPattern || undefined,
        limit: parsePositiveInteger(limit, 'limit', MAX_MIGRATION_LIMIT),
        batchSize: parsedBatchSize,
        delayBetweenBatchesMs: parsedDelayMs,
      };

      // Get preview to estimate key count
      const preview = await this._queuebertService.previewCacheMigration(
        params,
        this.queues,
        sourceRedisClient,
        targetRedisId,
      );

      return this._queuebertService.startCacheMigration(
        params,
        this.queues,
        sourceRedisClient,
        targetRedisClient,
        preview.keyCount,
      );
    }

    /**
     * POST /cache/migrations/:migrationId/pause
     * Pause a running cache migration
     */
    @Post('cache/migrations/:migrationId/pause')
    pauseCacheMigration(@Param('migrationId') migrationId: string) {
      this.requireEndpoint('migrations');
      return this._queuebertService.pauseCacheMigration(migrationId);
    }

    /**
     * POST /cache/migrations/:migrationId/resume
     * Resume a paused cache migration
     */
    @Post('cache/migrations/:migrationId/resume')
    resumeCacheMigration(@Param('migrationId') migrationId: string) {
      this.requireEndpoint('migrations');
      return this._queuebertService.resumeCacheMigration(migrationId);
    }

    /**
     * POST /cache/migrations/:migrationId/cancel
     * Cancel a running cache migration
     */
    @Post('cache/migrations/:migrationId/cancel')
    cancelCacheMigration(@Param('migrationId') migrationId: string) {
      this.requireEndpoint('migrations');
      return this._queuebertService.cancelCacheMigration(migrationId);
    }
  }

  return QueuebertController;
}

/**
 * Default Queuebert controller with 'admin/queue' path
 */
export const QueuebertController = createQueuebertController('admin/queue');
