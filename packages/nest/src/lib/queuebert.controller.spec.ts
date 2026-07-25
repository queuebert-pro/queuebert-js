import { HttpException, HttpStatus } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';

import {
  createQueuebertController,
  IQueuebertController,
} from './queuebert.controller';
import { QueuebertService } from './queuebert.service';
import {
  QUEUEBERT_OPTIONS,
  QUEUEBERT_QUEUES,
  QueuebertModuleOptions,
  QueuebertProcessor,
} from './types';

describe('QueuebertController', () => {
  let controller: IQueuebertController;
  let queuebertService: jest.Mocked<Partial<QueuebertService>>;
  let mockQueue: any;
  let mockProcessor: jest.Mocked<QueuebertProcessor>;
  let queuesMap: Map<string, { queue: any; processor: QueuebertProcessor }>;

  const defaultOptions: QueuebertModuleOptions = {
    queues: [],
    includeRedisStats: true,
  };

  function createMockQueue(name: string) {
    return {
      name,
      pause: jest.fn().mockResolvedValue(undefined),
      resume: jest.fn().mockResolvedValue(undefined),
      getWaitingCount: jest.fn().mockResolvedValue(10),
      getActiveCount: jest.fn().mockResolvedValue(5),
      getCompletedCount: jest.fn().mockResolvedValue(100),
      getFailedCount: jest.fn().mockResolvedValue(2),
    };
  }

  function createMockProcessor(): jest.Mocked<QueuebertProcessor> {
    return {
      getProcessorStats: jest.fn().mockReturnValue({
        duration: {
          avgMs: 150,
          minMs: 50,
          maxMs: 500,
          p50Ms: 120,
          p95Ms: 400,
          p99Ms: 480,
          recentAvgMs: 140,
          sampleCount: 1000,
        },
        jobs: {
          processed: 1000,
          completed: 980,
          failed: 20,
          failureRate: 0.02,
          successRate: 0.98,
          lastJobTime: '2024-01-01T12:00:00.000Z',
        },
        throughput: {
          jobsPerMinute: 100,
          windowStartTime: '2024-01-01T11:55:00.000Z',
          jobsInWindow: 500,
        },
        cache: {},
        custom: {},
      }),
    } as jest.Mocked<QueuebertProcessor>;
  }

  async function createController(
    options: Partial<QueuebertModuleOptions> = {},
    enabledEndpoints: Record<string, boolean> = {},
  ): Promise<IQueuebertController> {
    const ControllerClass = createQueuebertController('admin/queue');

    // Default all endpoints to enabled unless overridden
    const defaultEnabledEndpoints: Record<string, boolean> = {
      stats: true,
      pause: true,
      resume: true,
      clean: true,
      drain: true,
      metrics: true,
      ...enabledEndpoints,
    };

    queuebertService = {
      getAllQueueStats: jest
        .fn()
        .mockResolvedValue({ queues: {}, capabilities: {}, timestamp: '' }),
      getSingleQueueStats: jest.fn().mockResolvedValue({
        name: 'test-queue',
        paused: false,
        counts: {},
        jobMetrics: {},
        throughput: {},
      }),
      getRedisMemoryStats: jest
        .fn()
        .mockResolvedValue({ memory: {}, usagePercent: '0%' }),
      pauseQueue: jest
        .fn()
        .mockResolvedValue({ status: 'paused', timestamp: '' }),
      pauseQueues: jest
        .fn()
        .mockResolvedValue({ status: 'paused', timestamp: '' }),
      resumeQueue: jest
        .fn()
        .mockResolvedValue({ status: 'resumed', timestamp: '' }),
      resumeQueues: jest
        .fn()
        .mockResolvedValue({ status: 'resumed', timestamp: '' }),
      cleanQueue: jest
        .fn()
        .mockResolvedValue({ cleaned: {}, before: {}, after: {}, options: {} }),
      drainQueue: jest
        .fn()
        .mockResolvedValue({ drained: 0, before: {}, after: {} }),
      getAvailableQueuesForMigration: jest
        .fn()
        .mockResolvedValue({ queues: [], redisInstances: [] }),
      listMigrations: jest.fn().mockReturnValue([]),
      previewMigration: jest
        .fn()
        .mockResolvedValue({ jobsToMigrate: 0, sampleJobs: [] }),
      startMigration: jest
        .fn()
        .mockResolvedValue({ migrationId: 'mig_1', status: 'pending' }),
      executeMigration: jest
        .fn()
        .mockResolvedValue({ migrated: { total: 0 }, errors: [] }),
      getMigrationStatus: jest
        .fn()
        .mockReturnValue({ migrationId: 'mig_1', status: 'running' }),
      pauseMigration: jest
        .fn()
        .mockReturnValue({ success: true, status: 'paused' }),
      resumeMigration: jest
        .fn()
        .mockReturnValue({ success: true, status: 'running' }),
      cancelMigration: jest.fn().mockReturnValue({ success: true }),
      getAllCacheConfigs: jest.fn().mockReturnValue([]),
      listCacheMigrations: jest.fn().mockReturnValue([]),
      findCacheConfig: jest.fn(),
      previewCacheMigration: jest
        .fn()
        .mockResolvedValue({ keyCount: 0, sampleKeys: [] }),
      getCacheMigrationStatus: jest
        .fn()
        .mockReturnValue({ migrationId: 'cache_1', status: 'running' }),
      startCacheMigration: jest
        .fn()
        .mockResolvedValue({ migrationId: 'cache_1', status: 'pending' }),
      pauseCacheMigration: jest
        .fn()
        .mockReturnValue({ success: true, status: 'paused' }),
      resumeCacheMigration: jest
        .fn()
        .mockReturnValue({ success: true, status: 'running' }),
      cancelCacheMigration: jest.fn().mockReturnValue({ success: true }),
      isEndpointEnabled: jest.fn().mockImplementation((endpoint: string) => {
        return defaultEnabledEndpoints[endpoint] ?? false;
      }),
    };

    mockQueue = createMockQueue('test-queue');
    mockProcessor = createMockProcessor();
    queuesMap = new Map([
      ['test-queue', { queue: mockQueue, processor: mockProcessor }],
    ]);

    const module: TestingModule = await Test.createTestingModule({
      controllers: [ControllerClass],
      providers: [
        {
          provide: QueuebertService,
          useValue: queuebertService,
        },
        {
          provide: QUEUEBERT_OPTIONS,
          useValue: { ...defaultOptions, ...options },
        },
        {
          provide: QUEUEBERT_QUEUES,
          useValue: () => queuesMap,
        },
      ],
    }).compile();

    return module.get<IQueuebertController>(ControllerClass);
  }

  beforeEach(async () => {
    controller = await createController();
  });

  describe('getAllStats', () => {
    it('should return stats for all queues', async () => {
      const expectedResult = {
        queues: { 'test-queue': {} },
        capabilities: {},
        timestamp: '',
      };
      (queuebertService.getAllQueueStats as jest.Mock).mockResolvedValue(
        expectedResult,
      );

      const result = await controller.getAllStats();

      expect(queuebertService.getAllQueueStats).toHaveBeenCalledWith(queuesMap);
      expect(result).toEqual(expectedResult);
    });
  });

  describe('pauseAll', () => {
    it('should pause all queues when endpoint is enabled', async () => {
      const expectedResult = { status: 'paused', timestamp: '' };
      (queuebertService.pauseQueues as jest.Mock).mockResolvedValue(
        expectedResult,
      );

      const result = await controller.pauseAll();

      expect(queuebertService.isEndpointEnabled).toHaveBeenCalledWith('pause');
      expect(queuebertService.pauseQueues).toHaveBeenCalledWith([
        { queue: mockQueue, statsKey: 'test-queue' },
      ]);
      expect(result).toEqual(expectedResult);
    });

    it('should throw 404 when pause endpoint is disabled', async () => {
      controller = await createController({}, { pause: false });

      await expect(controller.pauseAll()).rejects.toThrow(HttpException);
      await expect(controller.pauseAll()).rejects.toMatchObject({
        status: HttpStatus.NOT_FOUND,
      });
    });
  });

  describe('resumeAll', () => {
    it('should resume all queues when endpoint is enabled', async () => {
      const expectedResult = { status: 'resumed', timestamp: '' };
      (queuebertService.resumeQueues as jest.Mock).mockResolvedValue(
        expectedResult,
      );

      const result = await controller.resumeAll();

      expect(queuebertService.isEndpointEnabled).toHaveBeenCalledWith('resume');
      expect(queuebertService.resumeQueues).toHaveBeenCalledWith([
        { queue: mockQueue, statsKey: 'test-queue' },
      ]);
      expect(result).toEqual(expectedResult);
    });

    it('should throw 404 when resume endpoint is disabled', async () => {
      controller = await createController({}, { resume: false });

      await expect(controller.resumeAll()).rejects.toThrow(HttpException);
    });
  });

  describe('getQueueStats', () => {
    it('should return stats for a specific queue with Redis stats', async () => {
      const queueStats = {
        name: 'test-queue',
        paused: false,
        counts: { waiting: 10 },
        jobMetrics: {},
        throughput: {},
      };
      const redisStats = { memory: { used: 1024 }, usagePercent: '10%' };
      (queuebertService.getSingleQueueStats as jest.Mock).mockResolvedValue(
        queueStats,
      );
      (queuebertService.getRedisMemoryStats as jest.Mock).mockResolvedValue(
        redisStats,
      );

      const result = (await controller.getQueueStats('test-queue')) as any;

      expect(queuebertService.getSingleQueueStats).toHaveBeenCalledWith(
        mockQueue,
        mockProcessor,
        'test-queue',
      );
      expect(queuebertService.getRedisMemoryStats).toHaveBeenCalledWith(
        mockQueue,
      );
      expect(result.name).toBe('test-queue');
      expect(result.redis).toEqual(redisStats);
      expect(result.timestamp).toBeDefined();
    });

    it('should return stats without Redis stats when disabled', async () => {
      controller = await createController({ includeRedisStats: false });
      const queueStats = {
        name: 'test-queue',
        paused: false,
        counts: { waiting: 10 },
        jobMetrics: {},
        throughput: {},
      };
      (queuebertService.getSingleQueueStats as jest.Mock).mockResolvedValue(
        queueStats,
      );

      const result = (await controller.getQueueStats('test-queue')) as any;

      expect(queuebertService.getRedisMemoryStats).not.toHaveBeenCalled();
      expect(result.redis).toBeUndefined();
      expect(result.timestamp).toBeDefined();
    });

    it('should throw 404 when queue is not found', async () => {
      await expect(
        controller.getQueueStats('non-existent-queue'),
      ).rejects.toThrow(HttpException);
      await expect(
        controller.getQueueStats('non-existent-queue'),
      ).rejects.toMatchObject({
        status: HttpStatus.NOT_FOUND,
      });
    });
  });

  describe('pauseQueue', () => {
    it('should pause a specific queue', async () => {
      const expectedResult = { status: 'paused', timestamp: '' };
      (queuebertService.pauseQueue as jest.Mock).mockResolvedValue(
        expectedResult,
      );

      const result = await controller.pauseQueue('test-queue');

      expect(queuebertService.isEndpointEnabled).toHaveBeenCalledWith('pause');
      expect(queuebertService.pauseQueue).toHaveBeenCalledWith(
        mockQueue,
        'manual-pause',
        'test-queue',
      );
      expect(result).toEqual(expectedResult);
    });

    it('should throw 404 when queue is not found', async () => {
      await expect(controller.pauseQueue('non-existent-queue')).rejects.toThrow(
        HttpException,
      );
    });

    it('should throw 404 when endpoint is disabled', async () => {
      controller = await createController({}, { pause: false });

      await expect(controller.pauseQueue('test-queue')).rejects.toThrow(
        HttpException,
      );
    });
  });

  describe('resumeQueue', () => {
    it('should resume a specific queue', async () => {
      const expectedResult = { status: 'resumed', timestamp: '' };
      (queuebertService.resumeQueue as jest.Mock).mockResolvedValue(
        expectedResult,
      );

      const result = await controller.resumeQueue('test-queue');

      expect(queuebertService.isEndpointEnabled).toHaveBeenCalledWith('resume');
      expect(queuebertService.resumeQueue).toHaveBeenCalledWith(
        mockQueue,
        'test-queue',
      );
      expect(result).toEqual(expectedResult);
    });
  });

  describe('cleanQueue', () => {
    it('should clean a queue with default parameters', async () => {
      const expectedResult = {
        cleaned: { total: 50 },
        before: {},
        after: {},
        options: {},
      };
      (queuebertService.cleanQueue as jest.Mock).mockResolvedValue(
        expectedResult,
      );

      const result = await controller.cleanQueue('test-queue');

      expect(queuebertService.isEndpointEnabled).toHaveBeenCalledWith('clean');
      expect(queuebertService.cleanQueue).toHaveBeenCalledWith(
        mockQueue,
        mockProcessor,
        300000, // default grace
        10000, // default limit
        'test-queue', // statsKey
      );
      expect(result).toEqual(expectedResult);
    });

    it('should clean a queue with custom parameters', async () => {
      const expectedResult = {
        cleaned: { total: 100 },
        before: {},
        after: {},
        options: {},
      };
      (queuebertService.cleanQueue as jest.Mock).mockResolvedValue(
        expectedResult,
      );

      const result = await controller.cleanQueue('test-queue', '60000', '5000');

      expect(queuebertService.cleanQueue).toHaveBeenCalledWith(
        mockQueue,
        mockProcessor,
        60000,
        5000,
        'test-queue', // statsKey
      );
      expect(result).toEqual(expectedResult);
    });

    it('should throw 404 when endpoint is disabled', async () => {
      controller = await createController({}, { clean: false });

      await expect(controller.cleanQueue('test-queue')).rejects.toThrow(
        HttpException,
      );
    });

    it('should reject invalid numeric parameters', async () => {
      await expect(
        controller.cleanQueue('test-queue', 'not-a-number'),
      ).rejects.toMatchObject({
        status: HttpStatus.BAD_REQUEST,
      });
      await expect(
        controller.cleanQueue('test-queue', '0', '0'),
      ).rejects.toMatchObject({
        status: HttpStatus.BAD_REQUEST,
      });
    });
  });

  describe('drainQueue', () => {
    it('should drain a queue', async () => {
      const expectedResult = { drained: 10, before: {}, after: {} };
      (queuebertService.drainQueue as jest.Mock).mockResolvedValue(
        expectedResult,
      );

      const result = await controller.drainQueue('test-queue');

      expect(queuebertService.isEndpointEnabled).toHaveBeenCalledWith('drain');
      expect(queuebertService.drainQueue).toHaveBeenCalledWith(
        mockQueue,
        mockProcessor,
        'test-queue',
      );
      expect(result).toEqual(expectedResult);
    });

    it('should throw 404 when endpoint is disabled', async () => {
      controller = await createController({}, { drain: false });

      await expect(controller.drainQueue('test-queue')).rejects.toThrow(
        HttpException,
      );
    });
  });

  describe('getQueueMetrics', () => {
    it('should return detailed metrics for a queue', async () => {
      const result = (await controller.getQueueMetrics('test-queue')) as any;

      expect(queuebertService.isEndpointEnabled).toHaveBeenCalledWith(
        'metrics',
      );
      expect(mockProcessor.getProcessorStats).toHaveBeenCalled();
      expect(mockQueue.getWaitingCount).toHaveBeenCalled();
      expect(mockQueue.getActiveCount).toHaveBeenCalled();
      expect(mockQueue.getCompletedCount).toHaveBeenCalled();
      expect(mockQueue.getFailedCount).toHaveBeenCalled();

      expect(result).toMatchObject({
        duration: {
          avgMs: 150,
          minMs: 50,
          maxMs: 500,
          p50Ms: 120,
          p95Ms: 400,
          p99Ms: 480,
          recentAvgMs: 140,
          sampleCount: 1000,
        },
        jobs: {
          processed: 1000,
          completed: 980,
          failed: 20,
          failureRate: 0.02,
          successRate: 0.98,
          lastJobTime: '2024-01-01T12:00:00.000Z',
        },
        queue: {
          waiting: 10,
          active: 5,
          completed: 100,
          failed: 2,
          backlog: 15,
        },
        throughput: {
          jobsPerMinute: 100,
          windowStartTime: '2024-01-01T11:55:00.000Z',
          jobsInWindow: 500,
        },
      });
      expect(result.timestamp).toBeDefined();
    });

    it('should throw 404 when metrics endpoint is disabled', async () => {
      controller = await createController({}, { metrics: false });

      await expect(controller.getQueueMetrics('test-queue')).rejects.toThrow(
        HttpException,
      );
    });

    it('should throw 404 when queue is not found', async () => {
      await expect(
        controller.getQueueMetrics('non-existent-queue'),
      ).rejects.toThrow(HttpException);
    });
  });

  describe('createQueuebertController factory', () => {
    it('should create controller with custom base path', () => {
      const CustomController = createQueuebertController('custom/path');

      expect(CustomController).toBeDefined();
      // The controller class should be decorated with the custom path
      const metadata = Reflect.getMetadata('path', CustomController);
      expect(metadata).toBe('custom/path');
    });

    it('should create controller with default base path when not specified', () => {
      const DefaultController = createQueuebertController();

      const metadata = Reflect.getMetadata('path', DefaultController);
      expect(metadata).toBe('admin/queue');
    });
  });

  describe('migration endpoints', () => {
    beforeEach(async () => {
      controller = await createController({}, { migrations: true });
    });

    it('should list available migration queues', async () => {
      (
        queuebertService.getAvailableQueuesForMigration as jest.Mock
      ).mockResolvedValue({
        queues: [{ name: 'test-queue' }],
        redisInstances: [{ id: 'default' }],
      });

      const result = (await controller.getMigrationQueues()) as any;

      expect(
        queuebertService.getAvailableQueuesForMigration,
      ).toHaveBeenCalledWith(queuesMap);
      expect(result.queues).toEqual([{ name: 'test-queue' }]);
      expect(result.redisInstances).toEqual([{ id: 'default' }]);
      expect(result.timestamp).toBeDefined();
    });

    it('should list migrations', async () => {
      (queuebertService.listMigrations as jest.Mock).mockReturnValue([
        { migrationId: 'mig_1' },
      ]);

      const result = (await controller.listMigrations()) as any;

      expect(result.migrations).toEqual([{ migrationId: 'mig_1' }]);
      expect(result.timestamp).toBeDefined();
    });

    it('should preview migrations with validated query params', async () => {
      await controller.previewMigration(
        'test-queue',
        'test-queue',
        undefined,
        undefined,
        'email',
        'waiting, failed',
        '25',
      );

      expect(queuebertService.previewMigration).toHaveBeenCalledWith(
        mockQueue,
        mockQueue,
        expect.objectContaining({
          sourceQueue: 'test-queue',
          targetQueue: 'test-queue',
          jobType: 'email',
          states: ['waiting', 'failed'],
          limit: 25,
        }),
        'default',
        'default',
      );
    });

    it('should reject invalid migration states and integers', async () => {
      await expect(
        controller.previewMigration(
          'test-queue',
          'test-queue',
          undefined,
          undefined,
          undefined,
          'waiting,active',
        ),
      ).rejects.toMatchObject({
        status: HttpStatus.BAD_REQUEST,
      });
      await expect(
        controller.previewMigration(
          'test-queue',
          'test-queue',
          undefined,
          undefined,
          undefined,
          undefined,
          '10abc',
        ),
      ).rejects.toMatchObject({
        status: HttpStatus.BAD_REQUEST,
      });
    });

    it('should reject missing migration queues', async () => {
      await expect(
        controller.previewMigration('', 'target'),
      ).rejects.toMatchObject({
        status: HttpStatus.BAD_REQUEST,
      });
    });

    it('should start background migrations for registered queues', async () => {
      (queuebertService.previewMigration as jest.Mock).mockResolvedValue({
        jobsToMigrate: 5,
      });

      const result = await controller.startMigration(
        'test-queue',
        'test-queue',
        undefined,
        undefined,
        undefined,
        'waiting',
        '10',
        '2',
        '50',
      );

      expect(queuebertService.startMigration).toHaveBeenCalledWith(
        mockQueue,
        mockQueue,
        expect.objectContaining({
          states: ['waiting'],
          limit: 10,
          batchSize: 2,
          delayBetweenBatchesMs: 50,
        }),
        'default',
        'default',
        5,
      );
      expect(result).toEqual({ migrationId: 'mig_1', status: 'pending' });
    });

    it('should reject invalid background migration rate limits', async () => {
      await expect(
        controller.startMigration(
          'test-queue',
          'test-queue',
          undefined,
          undefined,
          undefined,
          undefined,
          undefined,
          '0',
        ),
      ).rejects.toMatchObject({
        status: HttpStatus.BAD_REQUEST,
      });
      await expect(
        controller.startMigration(
          'test-queue',
          'test-queue',
          undefined,
          undefined,
          undefined,
          undefined,
          undefined,
          '1',
          '-1',
        ),
      ).rejects.toMatchObject({
        status: HttpStatus.BAD_REQUEST,
      });
    });

    it('should execute migrations and clean up temporary validation inputs', async () => {
      const result = await controller.executeMigration(
        'test-queue',
        'test-queue',
        undefined,
        undefined,
        undefined,
        'failed',
        '3',
        '1',
        '0',
      );

      expect(queuebertService.executeMigration).toHaveBeenCalledWith(
        mockQueue,
        mockQueue,
        expect.objectContaining({
          states: ['failed'],
          limit: 3,
          batchSize: 1,
          delayBetweenBatchesMs: 0,
        }),
        'default',
        'default',
      );
      expect(result).toEqual({ migrated: { total: 0 }, errors: [] });
    });

    it('should reject invalid synchronous migration integers', async () => {
      await expect(
        controller.executeMigration(
          'test-queue',
          'test-queue',
          undefined,
          undefined,
          undefined,
          undefined,
          'nan',
        ),
      ).rejects.toMatchObject({
        status: HttpStatus.BAD_REQUEST,
      });
    });

    it('should get, pause, resume, and cancel migrations', async () => {
      await expect(controller.getMigrationStatus('mig_1')).resolves.toEqual({
        migrationId: 'mig_1',
        status: 'running',
      });
      await expect(controller.pauseMigration('mig_1')).resolves.toEqual({
        success: true,
        status: 'paused',
      });
      await expect(controller.resumeMigration('mig_1')).resolves.toEqual({
        success: true,
        status: 'running',
      });
      await expect(controller.cancelMigration('mig_1')).resolves.toEqual({
        success: true,
      });
    });

    it('should return 404 for missing migration status', async () => {
      (queuebertService.getMigrationStatus as jest.Mock).mockReturnValue(
        undefined,
      );

      await expect(
        controller.getMigrationStatus('missing'),
      ).rejects.toMatchObject({
        status: HttpStatus.NOT_FOUND,
      });
    });
  });

  describe('cache migration endpoints', () => {
    let redisClient: { scan: jest.Mock; mget: jest.Mock; setex: jest.Mock };

    beforeEach(async () => {
      redisClient = {
        scan: jest.fn().mockResolvedValue(['0', []]),
        mget: jest.fn(),
        setex: jest.fn(),
      };
      controller = await createController(
        { queues: [{ name: 'test-queue' }] },
        { migrations: true },
      );
      mockQueue.client = Promise.resolve(redisClient);
      (queuebertService.findCacheConfig as jest.Mock).mockReturnValue({
        config: { id: 'domains', label: 'Domains', keyPrefix: 'cache:domains' },
        queueName: 'test-queue',
      });
    });

    it('should list caches without exposing cache objects', async () => {
      (queuebertService.getAllCacheConfigs as jest.Mock).mockReturnValue([
        {
          queueName: 'test-queue',
          config: {
            id: 'domains',
            label: 'Domains',
            keyPrefix: 'cache:domains',
            cache: { secret: true },
          },
        },
      ]);

      const result = await (controller as any).listCaches();

      expect(result.caches).toEqual([
        {
          id: 'domains',
          label: 'Domains',
          keyPrefix: 'cache:domains',
          processor: 'test-queue',
        },
      ]);
    });

    it('should preview cache migrations with validated inputs', async () => {
      await (controller as any).previewCacheMigration(
        'domains',
        'default',
        'default',
        'user:*',
        '10',
      );

      expect(queuebertService.previewCacheMigration).toHaveBeenCalledWith(
        expect.objectContaining({
          sourceCache: 'domains',
          sourceRedis: 'default',
          targetRedis: 'default',
          keyPattern: 'user:*',
          limit: 10,
        }),
        queuesMap,
        redisClient,
        'default',
      );
    });

    it('should reject missing and unknown caches', async () => {
      await expect(
        (controller as any).previewCacheMigration(''),
      ).rejects.toMatchObject({
        status: HttpStatus.BAD_REQUEST,
      });
      (queuebertService.findCacheConfig as jest.Mock).mockReturnValue(
        undefined,
      );
      await expect(
        (controller as any).previewCacheMigration('missing'),
      ).rejects.toMatchObject({
        status: HttpStatus.NOT_FOUND,
      });
    });

    it('should start cache migrations and validate rate limits', async () => {
      (queuebertService.previewCacheMigration as jest.Mock).mockResolvedValue({
        keyCount: 7,
      });

      await (controller as any).startCacheMigration(
        'domains',
        'default',
        'default',
        undefined,
        '7',
        '3',
        '25',
      );

      expect(queuebertService.startCacheMigration).toHaveBeenCalledWith(
        expect.objectContaining({
          sourceCache: 'domains',
          limit: 7,
          batchSize: 3,
          delayBetweenBatchesMs: 25,
        }),
        queuesMap,
        redisClient,
        redisClient,
        7,
      );

      await expect(
        (controller as any).startCacheMigration(
          'domains',
          'default',
          'default',
          undefined,
          undefined,
          'bad',
        ),
      ).rejects.toMatchObject({
        status: HttpStatus.BAD_REQUEST,
      });
    });

    it('should get, pause, resume, cancel, and list cache migrations', () => {
      expect((controller as any).listCacheMigrations()).toEqual([]);
      expect((controller as any).getCacheMigrationStatus('cache_1')).toEqual({
        migrationId: 'cache_1',
        status: 'running',
      });
      expect((controller as any).pauseCacheMigration('cache_1')).toEqual({
        success: true,
        status: 'paused',
      });
      expect((controller as any).resumeCacheMigration('cache_1')).toEqual({
        success: true,
        status: 'running',
      });
      expect((controller as any).cancelCacheMigration('cache_1')).toEqual({
        success: true,
      });
    });

    it('should return 404 for missing cache migration status', () => {
      (queuebertService.getCacheMigrationStatus as jest.Mock).mockReturnValue(
        undefined,
      );

      expect(() =>
        (controller as any).getCacheMigrationStatus('missing'),
      ).toThrow(HttpException);
    });
  });

  describe('error messages', () => {
    it('should include available queues in not found error', async () => {
      queuesMap.set('another-queue', {
        queue: createMockQueue('another-queue'),
        processor: createMockProcessor(),
      });

      try {
        await controller.getQueueStats('non-existent-queue');
        fail('Expected error to be thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(HttpException);
        expect((error as HttpException).message).toContain('test-queue');
        expect((error as HttpException).message).toContain('another-queue');
      }
    });

    it('should include endpoint name in disabled error', async () => {
      controller = await createController({}, { pause: false });

      try {
        await controller.pauseAll();
        fail('Expected error to be thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(HttpException);
        expect((error as HttpException).message).toContain('pause');
        expect((error as HttpException).message).toContain('not enabled');
      }
    });
  });
});
