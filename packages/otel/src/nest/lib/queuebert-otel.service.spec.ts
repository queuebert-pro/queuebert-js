import { Test, TestingModule } from '@nestjs/testing';

import {
  QueuebertIntegrationRegistry,
  QUEUEBERT_INTEGRATION_REGISTRY,
} from '@queuebert/nest';

import { QueuebertOTelService } from './queuebert-otel.service';
import {
  QUEUEBERT_OTEL_METER,
  QUEUEBERT_OTEL_OPTIONS,
  QUEUEBERT_OTEL_TRACER,
} from './types';
import type { QueuebertOTelModuleOptions } from './types';

const mockMetricsRegistry = {
  recordJobCompleted: jest.fn(),
  recordJobFailed: jest.fn(),
  updateQueueCounts: jest.fn(),
  getSnapshot: jest.fn().mockReturnValue({
    queues: {},
    timestamp: '2024-01-01T00:00:00.000Z',
    resource: {},
  }),
  transformAllToQueuebertStats: jest.fn().mockReturnValue({}),
  transformToQueuebertStats: jest.fn().mockReturnValue(null),
  getQueueNames: jest.fn().mockReturnValue([]),
  clear: jest.fn(),
  destroy: jest.fn(),
};

jest.mock('../../lib/metrics-collector', () => ({
  MetricsRegistry: jest.fn().mockImplementation(() => mockMetricsRegistry),
}));

jest.mock('../../lib/instrumentation', () => ({
  createResourceAttributes: jest
    .fn()
    .mockReturnValue({ 'service.name': 'test' }),
}));

const __mocks__ = {
  metricsRegistry: mockMetricsRegistry,
};

describe('QueuebertOTelService', () => {
  let service: QueuebertOTelService;
  let integrationRegistry: QueuebertIntegrationRegistry;

  const defaultOptions: QueuebertOTelModuleOptions = {
    resource: {
      serviceName: 'test-service',
    },
  };

  async function createService(
    options: Partial<QueuebertOTelModuleOptions> = {},
    withRegistry = true,
  ): Promise<QueuebertOTelService> {
    const providers: any[] = [
      QueuebertOTelService,
      {
        provide: QUEUEBERT_OTEL_OPTIONS,
        useValue: { ...defaultOptions, ...options },
      },
      { provide: QUEUEBERT_OTEL_METER, useValue: { name: 'meter' } },
      { provide: QUEUEBERT_OTEL_TRACER, useValue: { name: 'tracer' } },
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

    return module.get<QueuebertOTelService>(QueuebertOTelService);
  }

  beforeEach(() => {
    jest.clearAllMocks();
    integrationRegistry = new QueuebertIntegrationRegistry();
  });

  describe('initialization', () => {
    it('should create service with default options', async () => {
      service = await createService();

      expect(service).toBeDefined();
    });

    it('should merge options with defaults', async () => {
      service = await createService({
        resource: { serviceName: 'custom-service' },
      });

      const config = service.getConfig();
      expect(config.resource?.serviceName).toBe('custom-service');
    });

    it('should work without integration registry', async () => {
      service = await createService({}, false);

      expect(service).toBeDefined();
      // Should not throw when registry is not available
      await service.onModuleInit();
    });
  });

  describe('onModuleInit', () => {
    it('should register integration with registry', async () => {
      service = await createService();

      await service.onModuleInit();

      expect(integrationRegistry.has('@queuebert/otel/nest')).toBe(true);
      const integration = integrationRegistry.get('@queuebert/otel/nest');
      expect(integration?.version).toBe('0.0.1');
      expect(integration?.description).toContain('OpenTelemetry');
    });
  });

  describe('onModuleDestroy', () => {
    it('should clean up resources', async () => {
      service = await createService();

      await service.onModuleDestroy();

      expect(__mocks__.metricsRegistry.destroy).toHaveBeenCalled();
    });
  });

  describe('getResourceAttributes', () => {
    it('should return resource attributes', async () => {
      service = await createService();

      const attrs = service.getResourceAttributes();

      expect(attrs).toEqual({ 'service.name': 'test' });
    });
  });

  describe('recordJobCompleted', () => {
    it('should delegate to metrics registry', async () => {
      service = await createService();

      service.recordJobCompleted('test-queue', 'test-job', 150);

      expect(__mocks__.metricsRegistry.recordJobCompleted).toHaveBeenCalledWith(
        'test-queue',
        'test-job',
        150,
      );
    });
  });

  describe('recordJobFailed', () => {
    it('should delegate to metrics registry', async () => {
      service = await createService();

      service.recordJobFailed('test-queue', 'test-job', 200);

      expect(__mocks__.metricsRegistry.recordJobFailed).toHaveBeenCalledWith(
        'test-queue',
        'test-job',
        200,
      );
    });
  });

  describe('updateQueueCounts', () => {
    it('should delegate to metrics registry', async () => {
      service = await createService();
      const counts = {
        waiting: 10,
        active: 5,
        completed: 100,
        failed: 2,
        delayed: 3,
      };

      service.updateQueueCounts('test-queue', counts);

      expect(__mocks__.metricsRegistry.updateQueueCounts).toHaveBeenCalledWith(
        'test-queue',
        counts,
      );
    });
  });

  describe('getMetricsSnapshot', () => {
    it('should return metrics snapshot', async () => {
      service = await createService();

      const snapshot = service.getMetricsSnapshot();

      expect(snapshot).toEqual({
        queues: {},
        timestamp: '2024-01-01T00:00:00.000Z',
        resource: {},
      });
    });
  });

  describe('getQueuebertStats', () => {
    it('should return transformed stats for all queues', async () => {
      service = await createService();

      const stats = service.getQueuebertStats();

      expect(
        __mocks__.metricsRegistry.transformAllToQueuebertStats,
      ).toHaveBeenCalled();
      expect(stats).toEqual({});
    });
  });

  describe('getQueueStats', () => {
    it('should return stats for specific queue', async () => {
      service = await createService();

      const stats = service.getQueueStats('test-queue');

      expect(
        __mocks__.metricsRegistry.transformToQueuebertStats,
      ).toHaveBeenCalledWith('test-queue');
      expect(stats).toBeNull();
    });
  });

  describe('getQueueNames', () => {
    it('should return list of queue names', async () => {
      service = await createService();
      __mocks__.metricsRegistry.getQueueNames.mockReturnValueOnce([
        'queue-a',
        'queue-b',
      ]);

      const names = service.getQueueNames();

      expect(names).toEqual(['queue-a', 'queue-b']);
    });
  });

  describe('clearMetrics', () => {
    it('should delegate to metrics registry', async () => {
      service = await createService();

      service.clearMetrics();

      expect(__mocks__.metricsRegistry.clear).toHaveBeenCalled();
    });
  });

  describe('getConfig', () => {
    it('should return copy of config', async () => {
      service = await createService({
        instrumentationName: 'my-service.queuebert',
        resource: { serviceName: 'my-service' },
      });

      const config = service.getConfig();

      expect(config.instrumentationName).toBe('my-service.queuebert');
      expect(config.resource?.serviceName).toBe('my-service');
    });
  });
});
