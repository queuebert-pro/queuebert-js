import { Test, TestingModule } from '@nestjs/testing';

import {
  QueuebertIntegrationRegistry,
  QUEUEBERT_INTEGRATION_REGISTRY,
} from '@queuebert/nest';

import {
  QueueAlreadyExistsError,
  WorkerAlreadyExistsError,
  InvalidOptionsError,
} from './errors';
import { QueuebertBullMQService } from './queuebert-bullmq.service';
import { GlobalStatsCollector } from './stats-collector';
import type { QueuebertBullMQModuleOptions } from './types';
import { QUEUEBERT_BULLMQ_OPTIONS, QUEUEBERT_STATS_COLLECTOR } from './types';

// Mock the QueuebertQueue and QueuebertWorker classes
jest.mock('./queuebert-queue', () => {
  return {
    QueuebertQueue: jest.fn().mockImplementation((name: string) => ({
      name,
      close: jest.fn().mockResolvedValue(undefined),
      add: jest
        .fn()
        .mockResolvedValue({ jobId: '1', jobName: 'test', queueName: name }),
    })),
  };
});

jest.mock('./queuebert-worker', () => {
  return {
    QueuebertWorker: jest.fn().mockImplementation((queueName: string) => ({
      queueName,
      isRunning: true,
      isPaused: false,
      close: jest.fn().mockResolvedValue(undefined),
      shutdown: jest.fn().mockResolvedValue(undefined),
      pause: jest.fn().mockResolvedValue(undefined),
      resume: jest.fn().mockResolvedValue(undefined),
      getStats: jest.fn().mockReturnValue({
        totalProcessed: 100,
        totalCompleted: 95,
        totalFailed: 5,
        activeJobs: 2,
        duration: { avgMs: 150 },
        throughput: { jobsPerMinute: 60 },
        byJobName: {},
        startedAt: new Date().toISOString(),
        lastJobTime: null,
        isPaused: false,
        isRunning: true,
      }),
    })),
  };
});

describe('QueuebertBullMQService', () => {
  let service: QueuebertBullMQService;
  let integrationRegistry: QueuebertIntegrationRegistry;
  let statsCollector: GlobalStatsCollector;

  const defaultOptions: QueuebertBullMQModuleOptions = {
    connection: {
      host: 'localhost',
      port: 6379,
    },
  };

  async function createService(
    options: Partial<QueuebertBullMQModuleOptions> = {},
    withRegistry = true,
  ): Promise<QueuebertBullMQService> {
    const providers: any[] = [
      QueuebertBullMQService,
      {
        provide: QUEUEBERT_BULLMQ_OPTIONS,
        useValue: { ...defaultOptions, ...options },
      },
      {
        provide: QUEUEBERT_STATS_COLLECTOR,
        useValue: statsCollector,
      },
    ];

    if (withRegistry) {
      providers.push({
        provide: QUEUEBERT_INTEGRATION_REGISTRY,
        useValue: integrationRegistry,
      });
    }

    const module: TestingModule = await Test.createTestingModule({
      providers,
    }).compile();

    return module.get<QueuebertBullMQService>(QueuebertBullMQService);
  }

  beforeEach(() => {
    jest.clearAllMocks();
    integrationRegistry = new QueuebertIntegrationRegistry();
    statsCollector = {
      registerWorker: jest.fn(),
      unregisterWorker: jest.fn(),
      getAggregatedStats: jest.fn().mockResolvedValue({}),
      getQueueStats: jest.fn().mockResolvedValue(null),
      getRegisteredQueues: jest.fn().mockReturnValue([]),
    } as any;
  });

  describe('input validation', () => {
    it('should throw InvalidOptionsError when connection is missing', async () => {
      await expect(
        createService({ connection: undefined as any }),
      ).rejects.toThrow(InvalidOptionsError);
      await expect(
        createService({ connection: undefined as any }),
      ).rejects.toThrow('connection is required');
    });

    it('should throw InvalidOptionsError when neither url nor host is provided', async () => {
      await expect(createService({ connection: {} as any })).rejects.toThrow(
        InvalidOptionsError,
      );
      await expect(createService({ connection: {} as any })).rejects.toThrow(
        'connection.url or connection.host is required',
      );
    });

    it('should accept connection with url', async () => {
      service = await createService({
        connection: { url: 'redis://localhost:6379' },
      });
      expect(service).toBeDefined();
    });

    it('should accept connection with host', async () => {
      service = await createService({ connection: { host: 'localhost' } });
      expect(service).toBeDefined();
    });
  });

  describe('onModuleInit', () => {
    it('should register integration with registry', async () => {
      service = await createService();

      service.onModuleInit();

      expect(integrationRegistry.has('@queuebert/bullmq')).toBe(true);
      const integration = integrationRegistry.get('@queuebert/bullmq');
      expect(integration?.version).toBe('0.0.1');
      expect(integration?.description).toContain('BullMQ');
    });

    it('should work without integration registry', async () => {
      service = await createService({}, false);

      // Should not throw
      expect(() => service.onModuleInit()).not.toThrow();
    });
  });

  describe('createQueue', () => {
    it('should create a new queue', async () => {
      service = await createService();

      const queue = service.createQueue('test-queue');

      expect(queue).toBeDefined();
      expect(queue.name).toBe('test-queue');
    });

    it('should throw QueueAlreadyExistsError if queue already exists', async () => {
      service = await createService();

      service.createQueue('test-queue');

      expect(() => service.createQueue('test-queue')).toThrow(
        QueueAlreadyExistsError,
      );
      expect(() => service.createQueue('test-queue')).toThrow(
        'Queue "test-queue" already exists',
      );
    });

    it('should merge options with defaults', async () => {
      service = await createService({
        connection: { host: 'localhost', port: 6379 },
        defaultQueueOptions: { defaultJobOptions: { attempts: 3 } },
      });

      const queue = service.createQueue('test-queue', {
        tags: { env: 'test' },
      });

      expect(queue).toBeDefined();
    });
  });

  describe('createWorker', () => {
    it('should create a new worker', async () => {
      service = await createService();
      const processor = jest.fn();

      const worker = service.createWorker('test-queue', processor);

      expect(worker).toBeDefined();
      expect(worker.queueName).toBe('test-queue');
    });

    it('should throw WorkerAlreadyExistsError if worker already exists', async () => {
      service = await createService();
      const processor = jest.fn();

      service.createWorker('test-queue', processor);

      expect(() => service.createWorker('test-queue', processor)).toThrow(
        WorkerAlreadyExistsError,
      );
      expect(() => service.createWorker('test-queue', processor)).toThrow(
        'Worker for queue "test-queue" already exists',
      );
    });

    it('should register worker with stats collector', async () => {
      service = await createService();
      const processor = jest.fn();

      service.createWorker('test-queue', processor);

      expect(statsCollector.registerWorker).toHaveBeenCalledWith(
        'test-queue',
        expect.any(Object),
      );
    });
  });

  describe('getQueue', () => {
    it('should return existing queue', async () => {
      service = await createService();
      service.createQueue('test-queue');

      const queue = service.getQueue('test-queue');

      expect(queue).toBeDefined();
      expect(queue?.name).toBe('test-queue');
    });

    it('should return undefined for non-existent queue', async () => {
      service = await createService();

      const queue = service.getQueue('non-existent');

      expect(queue).toBeUndefined();
    });
  });

  describe('getWorker', () => {
    it('should return existing worker', async () => {
      service = await createService();
      service.createWorker('test-queue', jest.fn());

      const worker = service.getWorker('test-queue');

      expect(worker).toBeDefined();
      expect(worker?.queueName).toBe('test-queue');
    });

    it('should return undefined for non-existent worker', async () => {
      service = await createService();

      const worker = service.getWorker('non-existent');

      expect(worker).toBeUndefined();
    });
  });

  describe('getQueueNames', () => {
    it('should return all queue names', async () => {
      service = await createService();
      service.createQueue('queue-a');
      service.createQueue('queue-b');
      service.createQueue('queue-c');

      const names = service.getQueueNames();

      expect(names).toEqual(['queue-a', 'queue-b', 'queue-c']);
    });

    it('should return empty array when no queues', async () => {
      service = await createService();

      const names = service.getQueueNames();

      expect(names).toEqual([]);
    });
  });

  describe('getWorkerQueueNames', () => {
    it('should return all worker queue names', async () => {
      service = await createService();
      service.createWorker('worker-a', jest.fn());
      service.createWorker('worker-b', jest.fn());

      const names = service.getWorkerQueueNames();

      expect(names).toEqual(['worker-a', 'worker-b']);
    });
  });

  describe('getAllStats', () => {
    it('should delegate to stats collector', async () => {
      const mockStats = { 'test-queue': { workerStats: {}, queueStats: {} } };
      (statsCollector.getAggregatedStats as jest.Mock).mockResolvedValue(
        mockStats,
      );
      service = await createService();

      const stats = await service.getAllStats();

      expect(statsCollector.getAggregatedStats).toHaveBeenCalled();
      expect(stats).toEqual(mockStats);
    });
  });

  describe('getQueueStats', () => {
    it('should delegate to stats collector', async () => {
      const mockStats = {
        queueName: 'test',
        workerStats: null,
        queueStats: {},
      };
      (statsCollector.getQueueStats as jest.Mock).mockResolvedValue(mockStats);
      service = await createService();

      const stats = await service.getQueueStats('test-queue');

      expect(statsCollector.getQueueStats).toHaveBeenCalledWith('test-queue');
      expect(stats).toEqual(mockStats);
    });
  });

  describe('closeQueue', () => {
    it('should close and remove queue', async () => {
      service = await createService();
      const queue = service.createQueue('test-queue');

      await service.closeQueue('test-queue');

      expect(queue.close).toHaveBeenCalled();
      expect(service.getQueue('test-queue')).toBeUndefined();
    });

    it('should do nothing for non-existent queue', async () => {
      service = await createService();

      // Should not throw
      await service.closeQueue('non-existent');
    });
  });

  describe('closeWorker', () => {
    it('should close and remove worker', async () => {
      service = await createService();
      const worker = service.createWorker('test-queue', jest.fn());

      await service.closeWorker('test-queue');

      expect(worker.close).toHaveBeenCalled();
      expect(statsCollector.unregisterWorker).toHaveBeenCalledWith(
        'test-queue',
      );
      expect(service.getWorker('test-queue')).toBeUndefined();
    });

    it('should pass force option to worker', async () => {
      service = await createService();
      const worker = service.createWorker('test-queue', jest.fn());

      await service.closeWorker('test-queue', true);

      expect(worker.close).toHaveBeenCalledWith(true);
    });
  });

  describe('pauseWorker', () => {
    it('should pause the worker', async () => {
      service = await createService();
      const worker = service.createWorker('test-queue', jest.fn());

      await service.pauseWorker('test-queue');

      expect(worker.pause).toHaveBeenCalled();
    });

    it('should pass doNotWaitActive option', async () => {
      service = await createService();
      const worker = service.createWorker('test-queue', jest.fn());

      await service.pauseWorker('test-queue', true);

      expect(worker.pause).toHaveBeenCalledWith(true);
    });

    it('should do nothing for non-existent worker', async () => {
      service = await createService();

      // Should not throw
      await service.pauseWorker('non-existent');
    });
  });

  describe('resumeWorker', () => {
    it('should resume the worker', async () => {
      service = await createService();
      const worker = service.createWorker('test-queue', jest.fn());

      await service.resumeWorker('test-queue');

      expect(worker.resume).toHaveBeenCalled();
    });
  });

  describe('onModuleDestroy', () => {
    it('should close all workers and queues', async () => {
      service = await createService();
      const queue1 = service.createQueue('queue-1');
      const queue2 = service.createQueue('queue-2');
      const worker1 = service.createWorker('worker-1', jest.fn());
      const worker2 = service.createWorker('worker-2', jest.fn());

      await service.onModuleDestroy();

      // Recorded as a shutdown, not an application close
      expect(worker1.shutdown).toHaveBeenCalled();
      expect(worker2.shutdown).toHaveBeenCalled();
      expect(queue1.close).toHaveBeenCalled();
      expect(queue2.close).toHaveBeenCalled();
      expect(service.getQueueNames()).toEqual([]);
      expect(service.getWorkerQueueNames()).toEqual([]);
    });
  });

  describe('connection options', () => {
    it('should use host/port configuration', async () => {
      service = await createService({
        connection: { host: 'redis.example.com', port: 6380 },
      });

      // Create queue to trigger connection options parsing
      service.createQueue('test-queue');

      // The mocked QueuebertQueue should be created with connection options
      const { QueuebertQueue } = require('./queuebert-queue');
      expect(QueuebertQueue).toHaveBeenCalledWith(
        'test-queue',
        expect.objectContaining({
          connection: expect.objectContaining({
            host: 'redis.example.com',
            port: 6380,
          }),
        }),
      );
    });

    it('should pass Redis URLs through without dropping connection semantics', async () => {
      const redisUrl =
        'rediss://app%40example:p%40ss@redis.example.com:6380/1?family=6';
      service = await createService({
        connection: { url: redisUrl },
      });

      service.createQueue('test-queue');

      const { QueuebertQueue } = require('./queuebert-queue');
      expect(QueuebertQueue).toHaveBeenCalledWith(
        'test-queue',
        expect.objectContaining({
          connection: { url: redisUrl },
        }),
      );
    });

    it('should handle Redis URL without password or db', async () => {
      service = await createService({
        connection: { url: 'redis://localhost:6379' },
      });

      service.createQueue('test-queue');

      const { QueuebertQueue } = require('./queuebert-queue');
      expect(QueuebertQueue).toHaveBeenCalledWith(
        'test-queue',
        expect.objectContaining({
          connection: { url: 'redis://localhost:6379' },
        }),
      );
    });

    it('should reject non-Redis URL protocols', async () => {
      service = await createService({
        connection: { url: 'https://redis.example.com' },
      });

      expect(() => service.createQueue('test-queue')).toThrow(
        'connection.url must use the redis: or rediss: protocol',
      );
    });
  });
});
