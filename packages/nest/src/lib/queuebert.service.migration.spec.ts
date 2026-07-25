import { QueuebertIntegrationRegistry } from './integration-registry';
import { QueuebertService } from './queuebert.service';
import type {
  MigrationStatus,
  QueuebertCacheConfig,
  QueuebertModuleOptions,
  QueuebertProcessor,
} from './types';

describe('QueuebertService migration and cache behavior', () => {
  afterEach(() => {
    jest.useRealTimers();
    jest.restoreAllMocks();
  });

  function createService(options: Partial<QueuebertModuleOptions> = {}) {
    return new QueuebertService(
      {
        queues: [{ name: 'emails' }],
        ...options,
      },
      new QueuebertIntegrationRegistry(),
    );
  }

  function createJob(
    id: string,
    name = 'send-email',
    data: unknown = { to: 'person@example.com' },
  ) {
    return {
      id,
      name,
      data,
      delay: 0,
      processedOn: Date.now(),
      timestamp: Date.now(),
      attemptsMade: 1,
      failedReason: 'boom',
      opts: {
        attempts: 3,
        backoff: { type: 'exponential', delay: 1000 },
        removeOnComplete: true,
        removeOnFail: false,
      },
      remove: jest.fn().mockResolvedValue(undefined),
    };
  }

  function createQueue(
    name = 'emails',
    overrides: Record<string, unknown> = {},
  ) {
    let paused = false;
    const addedJob = { remove: jest.fn().mockResolvedValue(undefined) };
    const redis = {
      info: jest
        .fn()
        .mockResolvedValue(
          [
            '# Memory',
            'used_memory:100',
            'used_memory_human:100B',
            'used_memory_peak:200',
            'used_memory_peak_human:200B',
            'used_memory_rss:150',
            'used_memory_rss_human:150B',
            'maxmemory:1000',
            'maxmemory_human:1KB',
            'maxmemory_policy:noeviction',
            'mem_fragmentation_ratio:1.5',
          ].join('\r\n'),
        ),
      get: jest.fn().mockResolvedValue(null),
      set: jest.fn().mockResolvedValue('OK'),
      del: jest.fn().mockResolvedValue(1),
      eval: jest.fn().mockResolvedValue(1),
    };

    return {
      name,
      client: Promise.resolve(redis),
      getWaitingCount: jest.fn().mockResolvedValue(2),
      getActiveCount: jest
        .fn()
        .mockImplementation(() => Promise.resolve(paused ? 0 : 1)),
      getCompletedCount: jest.fn().mockResolvedValue(10),
      getFailedCount: jest.fn().mockResolvedValue(1),
      getDelayedCount: jest.fn().mockResolvedValue(1),
      isPaused: jest.fn().mockImplementation(() => Promise.resolve(paused)),
      getJobs: jest.fn().mockImplementation((states: string[]) => {
        const state = states[0];
        if (state === 'waiting')
          return Promise.resolve([
            createJob('w1', 'welcome'),
            createJob('w2', 'digest'),
          ]);
        if (state === 'active')
          return Promise.resolve([createJob('a1', 'welcome')]);
        if (state === 'delayed')
          return Promise.resolve([createJob('d1', 'retry')]);
        return Promise.resolve([]);
      }),
      getWaiting: jest
        .fn()
        .mockResolvedValue([createJob('w1', 'welcome', { secret: 'token' })]),
      getDelayed: jest.fn().mockResolvedValue([]),
      getFailed: jest.fn().mockResolvedValue([]),
      getJob: jest.fn().mockResolvedValue(null),
      add: jest.fn().mockResolvedValue(addedJob),
      clean: jest
        .fn()
        .mockResolvedValueOnce(['completed-1'])
        .mockResolvedValueOnce(['failed-1', 'failed-2']),
      drain: jest.fn().mockResolvedValue(undefined),
      pause: jest.fn().mockImplementation(() => {
        paused = true;
        return Promise.resolve();
      }),
      resume: jest.fn().mockImplementation(() => {
        paused = false;
        return Promise.resolve();
      }),
      getJobCounts: jest.fn().mockResolvedValue({
        waiting: 2,
        active: 1,
        completed: 10,
        failed: 1,
        delayed: 1,
      }),
      ...overrides,
      __redis: redis,
    } as any;
  }

  function createProcessor(
    cacheConfigs: QueuebertCacheConfig[] = [],
  ): QueuebertProcessor {
    return {
      getProcessorStats: jest.fn().mockReturnValue({
        duration: {
          avgMs: 50,
          minMs: 25,
          maxMs: 100,
          p50Ms: 50,
          p95Ms: 95,
          p99Ms: 99,
          recentAvgMs: 45,
          sampleCount: 10,
        },
        jobs: {
          processed: 11,
          completed: 10,
          failed: 1,
          failureRate: 0.09,
          successRate: 0.91,
          lastJobTime: '2026-01-01T00:00:00.000Z',
        },
        throughput: {
          jobsPerMinute: 12,
          windowStartTime: '2026-01-01T00:00:00.000Z',
          jobsInWindow: 12,
        },
        jobsByType: {
          welcome: { processed: 8, completed: 7, failed: 1 },
        },
      }),
      getCacheConfigs: jest.fn().mockReturnValue(cacheConfigs),
    };
  }

  function createCacheConfig(id = 'domains'): QueuebertCacheConfig {
    return {
      id,
      label: 'Domain Cache',
      keyPrefix: 'cache:domains',
      cache: {
        size: 3,
        getKeyPrefix: () => 'cache:domains',
        getL2TtlSeconds: () => 300,
        getStats: () => ({
          l1: {
            hits: 6,
            misses: 2,
            size: 3,
            hitRate: '75.0%',
            evictions: 1,
            maxSize: 10,
            utilizationPercent: '30.0%',
          },
          l2: {
            hits: 4,
            misses: 1,
            errors: 0,
            hitRate: '80.0%',
          },
        }),
      },
    };
  }

  it('gets single queue stats with counts, Redis samples, and job type discovery', async () => {
    const service = createService();
    const queue = createQueue();
    const processor = createProcessor();

    const stats = await service.getSingleQueueStats(
      queue,
      processor,
      'primary-emails',
    );

    expect(stats.name).toBe('primary-emails');
    expect(stats.counts.total).toBe(15);
    expect(stats.jobMetrics.processed).toBe(11);
    expect(stats.jobTypes?.types['welcome']).toMatchObject({
      waiting: 1,
      active: 1,
      completed: 7,
      failed: 1,
    });
    expect(queue.__redis.set).not.toHaveBeenCalledWith(
      expect.stringContaining('rate-sample'),
      expect.anything(),
      expect.anything(),
      expect.anything(),
    );
  });

  it('samples large queues for job type discovery', async () => {
    const service = createService({
      jobTypeDiscovery: { sampleSize: 2, threshold: 1 },
    });
    const queue = createQueue();

    const result = await service.discoverJobTypes(queue, {
      waiting: 2,
      active: 1,
      delayed: 1,
      completed: 0,
      failed: 0,
      total: 4,
    });

    expect(result).toMatchObject({
      sampled: true,
      sampleSize: 2,
    });
    expect(result?.types['welcome']).toMatchObject({ waiting: 1, active: 1 });
    expect(result?.types['digest']).toMatchObject({ waiting: 1 });
    expect(result?.types['retry']).toMatchObject({ delayed: 1 });
    expect(queue.getJobs).toHaveBeenCalledWith(['waiting'], 0, 1);
  });

  it('can disable job type discovery and tolerates queue discovery errors', async () => {
    const disabled = createService({ jobTypeDiscovery: { enabled: false } });
    await expect(
      disabled.discoverJobTypes(createQueue(), {
        waiting: 1,
        active: 0,
        delayed: 0,
        completed: 0,
        failed: 0,
        total: 1,
      }),
    ).resolves.toBeUndefined();

    const service = createService();
    const queue = createQueue('emails', {
      getJobs: jest.fn().mockRejectedValue(new Error('scan failed')),
    });

    await expect(
      service.discoverJobTypes(queue, {
        waiting: 1,
        active: 0,
        delayed: 0,
        completed: 0,
        failed: 0,
        total: 1,
      }),
    ).resolves.toBeUndefined();
  });

  it('returns Redis memory stats and calculates usage percentage', async () => {
    const service = createService();
    const queue = createQueue();

    const stats = await service.getRedisMemoryStats(queue);

    expect(stats.memory.used).toBe(100);
    expect(stats.memory.fragmentationRatio).toBe(1.5);
    expect(stats.usagePercent).toBe('10.00%');
  });

  it('aggregates all queue stats with Redis, cache, and integration metadata', async () => {
    const registry = new QueuebertIntegrationRegistry();
    registry.register({ name: '@queuebert/test', version: '1.0.0' });
    const service = new QueuebertService(
      {
        redis: [{ id: 'primary', label: 'Primary Redis' }],
        queues: [
          { name: 'emails', statsKey: 'primary-emails', redis: 'primary' },
        ],
      },
      registry,
    );
    const queue = createQueue();
    const processor = createProcessor([createCacheConfig()]);

    const result = await service.getAllQueueStats(
      new Map([['primary-emails', { queue, processor }]]),
    );

    expect(result.queues['primary-emails'].redis).toBe('primary');
    expect(result.redis?.['primary']).toMatchObject({
      id: 'primary',
      label: 'Primary Redis',
    });
    expect(result.caches?.['domains']).toMatchObject({
      id: 'domains',
      redis: 'primary',
      combinedHitRate: '90.9%',
    });
    expect(result.integrations).toEqual([
      { name: '@queuebert/test', version: '1.0.0' },
    ]);
  });

  it('cleans, drains, pauses, and resumes queues with pause reason keys', async () => {
    const service = createService();
    const queue = createQueue();

    await expect(
      service.cleanQueue(queue, undefined, 1000, 5, 'primary'),
    ).resolves.toMatchObject({
      cleaned: { completed: 1, failed: 2, total: 3 },
      options: { graceMs: 1000, maxJobs: 5 },
    });
    expect(queue.clean).toHaveBeenCalledWith(1000, 5, 'completed');
    expect(queue.clean).toHaveBeenCalledWith(1000, 5, 'failed');

    await expect(
      service.drainQueue(queue, undefined, 'primary'),
    ).resolves.toMatchObject({
      drained: 2,
    });
    expect(queue.drain).toHaveBeenCalled();

    await expect(
      service.pauseQueue(queue, 'maintenance', 'primary'),
    ).resolves.toMatchObject({
      status: 'paused',
      reason: 'maintenance',
      queues: ['primary'],
    });
    expect(queue.__redis.set).toHaveBeenCalledWith(
      'bull:emails:pause-reason',
      'maintenance',
    );

    await expect(service.resumeQueue(queue, 'primary')).resolves.toMatchObject({
      status: 'resumed',
      queues: ['primary'],
    });
    expect(queue.__redis.del).toHaveBeenCalledWith('bull:emails:pause-reason');
  });

  it('pauses and resumes multiple queues with per-queue pause reason storage', async () => {
    const service = createService();
    const emails = createQueue('emails');
    const reports = createQueue('reports');
    const queues = [
      { queue: emails, statsKey: 'primary-emails' },
      { queue: reports, statsKey: 'reporting' },
    ];

    await expect(service.pauseQueues(queues, 'deploy')).resolves.toMatchObject({
      status: 'paused',
      reason: 'deploy',
      queues: ['primary-emails', 'reporting'],
    });
    expect(emails.pause).toHaveBeenCalled();
    expect(reports.pause).toHaveBeenCalled();
    expect(emails.__redis.set).toHaveBeenCalledWith(
      'bull:emails:pause-reason',
      'deploy',
    );
    expect(reports.__redis.set).toHaveBeenCalledWith(
      'bull:reports:pause-reason',
      'deploy',
    );

    await expect(service.resumeQueues(queues)).resolves.toMatchObject({
      status: 'resumed',
      queues: ['primary-emails', 'reporting'],
    });
    expect(emails.resume).toHaveBeenCalled();
    expect(reports.resume).toHaveBeenCalled();
    expect(emails.__redis.del).toHaveBeenCalledWith('bull:emails:pause-reason');
    expect(reports.__redis.del).toHaveBeenCalledWith(
      'bull:reports:pause-reason',
    );
  });

  it('redacts migration preview job data by default', async () => {
    const service = createService();
    const sourceQueue = createQueue();
    const targetQueue = createQueue();

    const preview = await service.previewMigration(
      sourceQueue,
      targetQueue,
      { sourceQueue: 'emails', targetQueue: 'emails-new', states: ['waiting'] },
      'default',
      'target',
    );

    expect(preview.jobsToMigrate).toBe(1);
    expect(preview.sampleJobs[0]).toMatchObject({
      id: 'w1',
      name: 'welcome',
      state: 'waiting',
    });
    expect(preview.sampleJobs[0]).not.toHaveProperty('data');
  });

  it('can include migration preview job data when explicitly configured', async () => {
    const service = createService({ includeJobDataInMigrationPreview: true });
    const sourceQueue = createQueue();
    const targetQueue = createQueue();

    const preview = await service.previewMigration(
      sourceQueue,
      targetQueue,
      { sourceQueue: 'emails', targetQueue: 'emails-new', states: ['waiting'] },
      'default',
      'target',
    );

    expect(preview.sampleJobs[0].data).toEqual({ secret: 'token' });
  });

  it('previews delayed and failed migration jobs with filtering and limits', async () => {
    const service = createService({ includeJobDataInMigrationPreview: true });
    const delayed = createJob('d1', 'retry', { retry: true });
    delayed.delay = 10000;
    delayed.processedOn = Date.now();
    const failed = createJob('f1', 'retry', { failed: true });
    const sourceQueue = createQueue('emails', {
      getDelayed: jest
        .fn()
        .mockResolvedValue([delayed, createJob('d2', 'other')]),
      getFailed: jest.fn().mockResolvedValue([failed]),
    });
    const targetQueue = createQueue('emails-new');

    const preview = await service.previewMigration(
      sourceQueue,
      targetQueue,
      {
        sourceQueue: 'emails',
        targetQueue: 'emails-new',
        states: ['delayed', 'failed'],
        jobType: 'retry',
        limit: 2,
        batchSize: 1,
        delayBetweenBatchesMs: 25,
      },
      'default',
      'target',
    );

    expect(preview.jobsToMigrate).toBe(2);
    expect(preview.byState).toMatchObject({ delayed: 1, failed: 1 });
    expect(preview.byJobType).toEqual({ retry: 2 });
    expect(preview.sampleJobs).toEqual([
      expect.objectContaining({
        id: 'd1',
        state: 'delayed',
        remainingDelayMs: expect.any(Number),
      }),
      expect.objectContaining({
        id: 'f1',
        state: 'failed',
        failedReason: 'boom',
      }),
    ]);
    expect(preview.estimatedDurationMs).toBeGreaterThan(0);
  });

  it('executes migrations and preserves job options', async () => {
    const service = createService();
    const job = createJob('w1', 'welcome', { secret: 'token' });
    const sourceQueue = createQueue('emails', {
      getWaiting: jest.fn().mockResolvedValue([job]),
      getJob: jest.fn().mockResolvedValue(job),
    });
    const targetQueue = createQueue('emails-new');

    const result = await service.executeMigration(
      sourceQueue,
      targetQueue,
      { sourceQueue: 'emails', targetQueue: 'emails-new', states: ['waiting'] },
      'default',
      'target',
    );

    expect(targetQueue.add).toHaveBeenCalledWith(
      'welcome',
      { secret: 'token' },
      expect.objectContaining({
        jobId: 'w1',
        attempts: 3,
        backoff: { type: 'exponential', delay: 1000 },
        removeOnComplete: true,
        removeOnFail: false,
      }),
    );
    expect(job.remove).toHaveBeenCalled();
    expect(result.migrated).toEqual({
      waiting: 1,
      delayed: 0,
      failed: 0,
      total: 1,
    });
  });

  it('executes delayed migrations with batch metadata and records per-job errors', async () => {
    const service = createService();
    const success = createJob('d1', 'retry', { id: 1 });
    success.delay = 10000;
    success.processedOn = Date.now();
    const rejected = createJob('d2', 'retry', { id: 2 });
    rejected.delay = 5000;
    rejected.processedOn = Date.now();
    const sourceQueue = createQueue('emails', {
      getDelayed: jest.fn().mockResolvedValue([success, rejected]),
      getJob: jest.fn().mockImplementation((id: string) => {
        if (id === 'd1') return Promise.resolve(success);
        if (id === 'd2') return Promise.resolve(rejected);
        return Promise.resolve(null);
      }),
    });
    const targetQueue = createQueue('emails-new', {
      add: jest
        .fn()
        .mockResolvedValueOnce(undefined)
        .mockRejectedValueOnce(new Error('target write failed')),
    });

    const result = await service.executeMigration(
      sourceQueue,
      targetQueue,
      {
        sourceQueue: 'emails',
        targetQueue: 'emails-new',
        states: ['delayed'],
        jobType: 'retry',
        batchSize: 1,
        delayBetweenBatchesMs: 0,
      },
      'default',
      'target',
    );

    expect(targetQueue.add).toHaveBeenCalledWith(
      'retry',
      { id: 1 },
      expect.objectContaining({ delay: expect.any(Number) }),
    );
    expect(success.remove).toHaveBeenCalled();
    expect(rejected.remove).not.toHaveBeenCalled();
    expect(result.migrated).toEqual({
      waiting: 0,
      delayed: 1,
      failed: 0,
      total: 1,
    });
    expect(result.errors).toEqual([
      { jobId: 'd2', error: 'target write failed' },
    ]);
    expect(result.rateLimit).toMatchObject({
      batchSize: 1,
      batchesProcessed: 1,
    });
  });

  it('lists migration queue metadata with Redis memory and cache summaries', async () => {
    const service = createService({
      redis: [
        { id: 'primary', label: 'Primary Redis' },
        { id: 'secondary', label: 'Secondary Redis' },
      ],
      queues: [
        { name: 'emails', statsKey: 'primary-emails', redis: 'primary' },
      ],
    });
    const queue = createQueue();
    const processor = createProcessor([createCacheConfig()]);

    const result = await service.getAvailableQueuesForMigration(
      new Map([['primary-emails', { queue, processor }]]),
    );

    expect(result.queues).toEqual([
      expect.objectContaining({
        name: 'primary-emails',
        displayName: 'primary-emails (Primary Redis)',
        redisId: 'primary',
        redisLabel: 'Primary Redis',
        counts: expect.objectContaining({ total: 15 }),
      }),
    ]);
    expect(result.redisInstances).toEqual([
      expect.objectContaining({
        id: 'primary',
        label: 'Primary Redis',
        memory: expect.objectContaining({ used: 100, peak: 200 }),
        usagePercent: '10.00%',
        caches: [
          expect.objectContaining({
            id: 'domains',
            l1Size: 3,
            l2Hits: 4,
            l2Misses: 1,
          }),
        ],
      }),
      expect.objectContaining({
        id: 'secondary',
        label: 'Secondary Redis',
      }),
    ]);
  });

  it('lists migration queue metadata for single Redis setups', async () => {
    const service = createService();
    const queue = createQueue();
    const processor = createProcessor([createCacheConfig()]);

    const result = await service.getAvailableQueuesForMigration(
      new Map([['emails', { queue, processor }]]),
    );

    expect(result.queues[0]).toMatchObject({
      name: 'emails',
      displayName: 'emails',
      redisId: 'default',
      redisLabel: 'Default',
    });
    expect(result.redisInstances).toEqual([
      expect.objectContaining({
        id: 'default',
        label: 'Default',
        memory: expect.objectContaining({ used: 100 }),
        caches: [expect.objectContaining({ id: 'domains' })],
      }),
    ]);
  });

  it('starts background migrations with initial status and rate limit metadata', async () => {
    const service = createService();
    const run = jest
      .spyOn(service as any, 'runMigrationInBackground')
      .mockImplementation(() => undefined);

    const response = await service.startMigration(
      createQueue(),
      createQueue('emails-new'),
      {
        sourceQueue: 'emails',
        targetQueue: 'emails-new',
        batchSize: 5,
        delayBetweenBatchesMs: 20,
      },
      'default',
      'target',
      12,
    );

    expect(response).toMatchObject({
      status: 'pending',
      message: 'Migration started. Poll status endpoint for progress.',
    });
    expect(run).toHaveBeenCalledWith(response.migrationId, 'default', 'target');
    expect(service.getMigrationStatus(response.migrationId)).toMatchObject({
      status: 'pending',
      isCrossRedis: true,
      rateLimit: { batchSize: 5, delayBetweenBatchesMs: 20 },
      progress: { total: 12, totalBatches: 3 },
    });
  });

  it('tracks migration pause, resume, cancel, and list operations', () => {
    const service = createService();
    const migrationStatus: MigrationStatus = {
      migrationId: 'mig_1',
      status: 'running',
      params: { sourceQueue: 'emails', targetQueue: 'emails-new' },
      progress: {
        total: 1,
        processed: 0,
        percent: 0,
        byState: { waiting: 0, delayed: 0, failed: 0 },
      },
      errors: [],
      isCrossRedis: true,
      startedAt: '2026-01-01T00:00:00.000Z',
      updatedAt: '2026-01-01T00:00:00.000Z',
    };
    const resumeResolver = jest.fn();
    const migration = {
      status: migrationStatus,
      abortController: new AbortController(),
      sourceQueue: createQueue(),
      targetQueue: createQueue(),
      isPaused: false,
      resumeResolver,
    };
    (service as any).activeMigrations.set('mig_1', migration);

    expect(service.listMigrations()).toEqual([migrationStatus]);
    expect(service.pauseMigration('mig_1')).toMatchObject({
      success: true,
      status: 'paused',
    });
    expect(service.resumeMigration('mig_1')).toMatchObject({
      success: true,
      status: 'running',
    });
    expect(resumeResolver).toHaveBeenCalled();
    expect(service.cancelMigration('mig_1')).toMatchObject({ success: true });
    expect(migration.abortController.signal.aborted).toBe(true);
    expect(service.cancelMigration('missing')).toMatchObject({
      success: false,
    });

    migration.status.status = 'completed';
    expect(service.cancelMigration('mig_1')).toMatchObject({
      success: false,
      message: 'Migration cannot be cancelled (status: completed)',
    });
    expect(service.pauseMigration('missing')).toMatchObject({ success: false });
    expect(service.resumeMigration('missing')).toMatchObject({
      success: false,
    });
    expect(service.pauseMigration('mig_1')).toMatchObject({
      success: false,
      status: 'completed',
    });
    expect(service.resumeMigration('mig_1')).toMatchObject({
      success: false,
      status: 'completed',
    });
  });

  it('finds and lists cache configurations while tolerating processor errors', () => {
    const service = createService();
    const cacheConfig = createCacheConfig();
    const processor = createProcessor([cacheConfig]);
    const badProcessor = {
      getProcessorStats: jest.fn(),
      getCacheConfigs: jest.fn(() => {
        throw new Error('broken');
      }),
    } as unknown as QueuebertProcessor;
    const queues = new Map([
      ['emails', { queue: createQueue(), processor }],
      ['broken', { queue: createQueue('broken'), processor: badProcessor }],
    ]);

    expect(service.findCacheConfig('domains', queues)).toMatchObject({
      config: cacheConfig,
      queueName: 'emails',
    });
    expect(service.findCacheConfig('missing', queues)).toBeUndefined();
    expect(service.getAllCacheConfigs(queues)).toEqual([
      { config: cacheConfig, queueName: 'emails' },
    ]);
  });

  it('previews cache migrations using SCAN instead of blocking KEYS', async () => {
    const service = createService();
    const cacheConfig = createCacheConfig();
    const queues = new Map([
      [
        'emails',
        { queue: createQueue(), processor: createProcessor([cacheConfig]) },
      ],
    ]);
    const redis = {
      scan: jest
        .fn()
        .mockResolvedValueOnce(['2', ['cache:domains:a', 'cache:domains:b']])
        .mockResolvedValueOnce(['0', ['cache:domains:c']]),
      mget: jest.fn(),
      pttl: jest.fn(),
      set: jest.fn(),
      psetex: jest.fn(),
      keys: jest.fn(),
    };

    const preview = await service.previewCacheMigration(
      {
        sourceCache: 'domains',
        sourceRedis: 'default',
        targetRedis: 'target',
        limit: 2,
      },
      queues,
      redis,
      'target',
    );

    expect(redis.scan).toHaveBeenCalledWith(
      '0',
      'MATCH',
      'cache:domains:*',
      'COUNT',
      1000,
    );
    expect(redis.scan).toHaveBeenCalledTimes(1);
    expect(redis.keys).not.toHaveBeenCalled();
    expect(preview.keyCount).toBe(2);
    expect(preview.sampleKeys).toEqual(['a', 'b']);
  });

  it('starts and completes cache migrations with per-key error tracking', async () => {
    jest.useFakeTimers();
    const service = createService();
    const cacheConfig = createCacheConfig();
    const queues = new Map([
      [
        'emails',
        { queue: createQueue(), processor: createProcessor([cacheConfig]) },
      ],
    ]);
    const sourceRedis = {
      scan: jest
        .fn()
        .mockResolvedValue([
          '0',
          ['cache:domains:a', 'cache:domains:b', 'cache:domains:c'],
        ]),
      mget: jest.fn().mockResolvedValue(['one', 'two', 'three']),
      pttl: jest.fn().mockResolvedValue(300_000),
      set: jest.fn(),
      psetex: jest.fn(),
    };
    const targetRedis = {
      scan: jest.fn(),
      mget: jest.fn(),
      pttl: jest.fn(),
      set: jest.fn(),
      psetex: jest
        .fn()
        .mockResolvedValueOnce('OK')
        .mockRejectedValueOnce(new Error('write failed'))
        .mockResolvedValueOnce('OK'),
    };
    const run = jest
      .spyOn(service as any, 'runCacheMigrationInBackground')
      .mockImplementation(() => undefined);

    const response = await service.startCacheMigration(
      {
        sourceCache: 'domains',
        sourceRedis: 'default',
        targetRedis: 'target',
        batchSize: 2,
        delayBetweenBatchesMs: 0,
      },
      queues,
      sourceRedis as any,
      targetRedis as any,
      3,
    );

    expect(response.status).toBe('pending');
    expect(run).toHaveBeenCalledWith(response.migrationId, {
      sourceCache: 'domains',
      sourceRedis: 'default',
      targetRedis: 'target',
      batchSize: 2,
      delayBetweenBatchesMs: 0,
    });
    run.mockRestore();

    await (service as any).runCacheMigrationInBackground(response.migrationId, {
      sourceCache: 'domains',
      sourceRedis: 'default',
      targetRedis: 'target',
      batchSize: 2,
      delayBetweenBatchesMs: 0,
    });

    expect(sourceRedis.scan).toHaveBeenCalledWith(
      '0',
      'MATCH',
      'cache:domains:*',
      'COUNT',
      2,
    );
    expect(sourceRedis.mget).toHaveBeenCalledWith(
      'cache:domains:a',
      'cache:domains:b',
    );
    expect(targetRedis.psetex).toHaveBeenCalledWith(
      'cache:domains:a',
      300_000,
      'one',
    );
    expect(service.getCacheMigrationStatus(response.migrationId)).toMatchObject(
      {
        status: 'completed',
        progress: {
          total: 3,
          processed: 3,
          migrated: 2,
          failed: 1,
          percent: 100,
          currentBatch: 2,
        },
        errors: [{ key: 'b', error: 'write failed' }],
      },
    );
    jest.clearAllTimers();
  });

  it('tracks cache migration pause, resume, cancel, and list operations', () => {
    const service = createService();
    const resumeResolver = jest.fn();
    const migration = {
      status: {
        migrationId: 'cache_1',
        status: 'running',
        params: {
          sourceCache: 'domains',
          sourceRedis: 'default',
          targetRedis: 'target',
        },
        keyPrefix: 'cache:domains',
        progress: {
          total: 1,
          processed: 0,
          migrated: 0,
          failed: 0,
          percent: 0,
        },
        errors: [],
        isCrossRedis: true,
        rateLimit: { batchSize: 1, delayBetweenBatchesMs: 0 },
        startedAt: '2026-01-01T00:00:00.000Z',
        updatedAt: '2026-01-01T00:00:00.000Z',
      },
      abortController: new AbortController(),
      sourceRedis: {
        scan: jest.fn(),
        mget: jest.fn(),
        pttl: jest.fn(),
        set: jest.fn(),
        psetex: jest.fn(),
      },
      targetRedis: {
        scan: jest.fn(),
        mget: jest.fn(),
        pttl: jest.fn(),
        set: jest.fn(),
        psetex: jest.fn(),
      },
      keyPrefix: 'cache:domains',
      isPaused: false,
      resumeResolver,
    };
    (service as any).activeCacheMigrations.set('cache_1', migration);

    expect(service.listCacheMigrations()).toEqual([migration.status]);
    expect(service.pauseCacheMigration('cache_1')).toMatchObject({
      success: true,
      status: 'paused',
    });
    expect(service.resumeCacheMigration('cache_1')).toMatchObject({
      success: true,
      status: 'running',
    });
    expect(resumeResolver).toHaveBeenCalled();
    expect(service.cancelCacheMigration('cache_1')).toMatchObject({
      success: true,
    });
    expect(migration.abortController.signal.aborted).toBe(true);

    migration.status.status = 'completed';
    expect(service.cancelCacheMigration('cache_1')).toMatchObject({
      success: false,
      message: 'Cache migration cannot be cancelled (status: completed)',
    });
    expect(service.pauseCacheMigration('missing')).toMatchObject({
      success: false,
    });
    expect(service.resumeCacheMigration('missing')).toMatchObject({
      success: false,
    });
    expect(service.pauseCacheMigration('cache_1')).toMatchObject({
      success: false,
      status: 'completed',
    });
    expect(service.resumeCacheMigration('cache_1')).toMatchObject({
      success: false,
      status: 'completed',
    });
  });
});
