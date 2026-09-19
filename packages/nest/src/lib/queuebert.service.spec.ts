import { Test, TestingModule } from '@nestjs/testing';

import { QueuebertIntegrationRegistry } from './integration-registry';
import { QueuebertService } from './queuebert.service';
import {
  QUEUEBERT_OPTIONS,
  QUEUEBERT_INTEGRATION_REGISTRY,
  QueuebertModuleOptions,
  RateSample,
  PerformanceMetrics,
} from './types';

describe('QueuebertService', () => {
  let service: QueuebertService;

  const defaultOptions: QueuebertModuleOptions = {
    queues: [{ name: 'test-queue', processor: {} as any }],
  };

  async function createService(
    options: Partial<QueuebertModuleOptions> = {},
  ): Promise<QueuebertService> {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        QueuebertService,
        {
          provide: QUEUEBERT_OPTIONS,
          useValue: { ...defaultOptions, ...options },
        },
        {
          provide: QUEUEBERT_INTEGRATION_REGISTRY,
          useValue: new QueuebertIntegrationRegistry(),
        },
      ],
    }).compile();

    return module.get<QueuebertService>(QueuebertService);
  }

  beforeEach(async () => {
    service = await createService();
  });

  describe('parseRedisInfo', () => {
    it('should parse Redis INFO output correctly', () => {
      const info = [
        '# Memory',
        'used_memory:1024000',
        'used_memory_human:1000.00K',
        'used_memory_peak:2048000',
        'used_memory_peak_human:2.00M',
        'maxmemory:10485760',
        'maxmemory_human:10.00M',
        'maxmemory_policy:noeviction',
      ].join('\r\n');

      const result = service.parseRedisInfo(info);

      expect(result['used_memory']).toBe('1024000');
      expect(result['used_memory_human']).toBe('1000.00K');
      expect(result['used_memory_peak']).toBe('2048000');
      expect(result['maxmemory']).toBe('10485760');
      expect(result['maxmemory_policy']).toBe('noeviction');
    });

    it('should skip comment lines', () => {
      const info =
        '# Memory\r\nused_memory:1024\r\n# Server\r\nredis_version:7.0.0';

      const result = service.parseRedisInfo(info);

      expect(result['used_memory']).toBe('1024');
      expect(result['redis_version']).toBe('7.0.0');
      expect(result['# Memory']).toBeUndefined();
    });

    it('should handle empty lines', () => {
      const info = 'used_memory:1024\r\n\r\nused_memory_peak:2048';

      const result = service.parseRedisInfo(info);

      expect(result['used_memory']).toBe('1024');
      expect(result['used_memory_peak']).toBe('2048');
    });
  });

  describe('calculateRatesFromCounters', () => {
    it('should return zeros when no previous sample', () => {
      const current: RateSample = {
        completed: 100,
        failed: 5,
        backlog: 50,
        timestamp: Date.now(),
      };

      const result = service.calculateRatesFromCounters(current, null);

      expect(result.throughputPerMin).toBe(0);
      expect(result.addedPerMin).toBe(0);
    });

    it('should calculate correct rates from counter deltas', () => {
      const now = Date.now();
      const previous: RateSample = {
        completed: 100,
        failed: 5,
        backlog: 50,
        timestamp: now - 60000, // 1 minute ago
      };
      const current: RateSample = {
        completed: 200, // +100 completed in 1 minute
        failed: 10,
        backlog: 70, // backlog increased by 20
        timestamp: now,
      };

      const result = service.calculateRatesFromCounters(current, previous);

      // 100 completed in 1 minute = 100/min
      expect(result.throughputPerMin).toBe(100);
      // backlog delta = 70 - 50 = +20
      // added = completed_delta + backlog_delta = 100 + 20 = 120/min
      expect(result.addedPerMin).toBe(120);
    });

    it('should handle catching up scenario (backlog decreasing)', () => {
      const now = Date.now();
      const previous: RateSample = {
        completed: 100,
        failed: 5,
        backlog: 100,
        timestamp: now - 60000,
      };
      const current: RateSample = {
        completed: 200, // +100 completed
        failed: 10,
        backlog: 50, // backlog decreased by 50
        timestamp: now,
      };

      const result = service.calculateRatesFromCounters(current, previous);

      expect(result.throughputPerMin).toBe(100);
      // added = 100 + (-50) = 50/min
      expect(result.addedPerMin).toBe(50);
    });

    it('should return zeros when time delta is zero or negative', () => {
      const now = Date.now();
      const previous: RateSample = {
        completed: 100,
        failed: 5,
        backlog: 50,
        timestamp: now,
      };
      const current: RateSample = {
        completed: 200,
        failed: 10,
        backlog: 70,
        timestamp: now, // Same timestamp
      };

      const result = service.calculateRatesFromCounters(current, previous);

      expect(result.throughputPerMin).toBe(0);
      expect(result.addedPerMin).toBe(0);
    });

    it('should not return negative rates', () => {
      const now = Date.now();
      const previous: RateSample = {
        completed: 200, // Higher than current (counter reset?)
        failed: 10,
        backlog: 100,
        timestamp: now - 60000,
      };
      const current: RateSample = {
        completed: 100,
        failed: 5,
        backlog: 50,
        timestamp: now,
      };

      const result = service.calculateRatesFromCounters(current, previous);

      expect(result.throughputPerMin).toBeGreaterThanOrEqual(0);
      expect(result.addedPerMin).toBeGreaterThanOrEqual(0);
    });
  });

  describe('calculatePerformanceMetrics', () => {
    it('should return paused status when queue is paused', () => {
      const result = service.calculatePerformanceMetrics(
        100, // throughputPerMin
        50, // addedPerMin
        200, // backlog
        100, // avgDurationMs
        0.05, // failureRate
        true, // isPaused
      );

      expect(result.trend).toBe('paused');
      expect(result.score).toBe(0);
      expect(result.health).toBe('poor');
      expect(result.throughputPerMin).toBe(0);
    });

    it('should return idle with excellent health when no activity and no backlog', () => {
      const result = service.calculatePerformanceMetrics(
        0, // throughputPerMin
        0, // addedPerMin
        0, // backlog
        undefined, // avgDurationMs
        0, // failureRate
        false, // isPaused
      );

      expect(result.trend).toBe('idle');
      expect(result.score).toBe(100);
      expect(result.health).toBe('excellent');
    });

    it('should detect catching_up trend when processing faster than adding', () => {
      const result = service.calculatePerformanceMetrics(
        200, // throughputPerMin (processing fast)
        50, // addedPerMin (adding slow)
        100, // backlog
        100, // avgDurationMs
        0.01, // failureRate
        false, // isPaused
      );

      expect(result.trend).toBe('catching_up');
      expect(result.deltaPerMin).toBe(150); // 200 - 50
      expect(result.estimatedClearTimeMs).toBeDefined();
    });

    it('should detect falling_behind trend when adding faster than processing', () => {
      const result = service.calculatePerformanceMetrics(
        50, // throughputPerMin (processing slow)
        200, // addedPerMin (adding fast)
        1000, // backlog (high)
        100, // avgDurationMs
        0.01, // failureRate
        false, // isPaused
      );

      expect(result.trend).toBe('falling_behind');
      expect(result.deltaPerMin).toBe(-150); // 50 - 200
    });

    it('should detect stable trend when rates are balanced', () => {
      const result = service.calculatePerformanceMetrics(
        100, // throughputPerMin
        100, // addedPerMin (same as throughput)
        50, // backlog (low)
        100, // avgDurationMs
        0.01, // failureRate
        false, // isPaused
      );

      expect(result.trend).toBe('stable');
      expect(result.deltaPerMin).toBe(0);
    });

    it('should penalize score for high failure rate', () => {
      const lowFailureResult = service.calculatePerformanceMetrics(
        100,
        100,
        0,
        100,
        0.01,
        false,
      );
      const highFailureResult = service.calculatePerformanceMetrics(
        100,
        100,
        0,
        100,
        0.5,
        false, // 50% failure rate
      );

      expect(highFailureResult.score).toBeLessThan(lowFailureResult.score);
    });

    it('should penalize score for large backlog', () => {
      const lowBacklogResult = service.calculatePerformanceMetrics(
        100,
        100,
        10,
        100,
        0.01,
        false,
      );
      const highBacklogResult = service.calculatePerformanceMetrics(
        100,
        100,
        5000,
        100,
        0.01,
        false,
      );

      expect(highBacklogResult.score).toBeLessThan(lowBacklogResult.score);
    });

    it('should penalize score for slow job duration', () => {
      const fastResult = service.calculatePerformanceMetrics(
        100,
        100,
        0,
        100,
        0.01,
        false, // 100ms avg
      );
      const slowResult = service.calculatePerformanceMetrics(
        100,
        100,
        0,
        2000,
        0.01,
        false, // 2000ms avg
      );

      expect(slowResult.score).toBeLessThan(fastResult.score);
    });

    it('should calculate estimated clear time when backlog exists', () => {
      const result = service.calculatePerformanceMetrics(
        100, // throughputPerMin
        50, // addedPerMin
        100, // backlog
        100, // avgDurationMs
        0.01, // failureRate
        false, // isPaused
      );

      // delta = 50 jobs/min, backlog = 100
      // estimated clear time = (100 / 50) * 60000ms = 120000ms
      expect(result.estimatedClearTimeMs).toBe(120000);
    });

    it('should return correct health levels based on score', () => {
      const excellentResult = service.calculatePerformanceMetrics(
        0,
        0,
        0,
        undefined,
        0,
        false,
      );
      expect(excellentResult.health).toBe('excellent');
      expect(excellentResult.score).toBeGreaterThanOrEqual(90);

      // Create conditions for different health levels - low throughput, high added rate, high backlog
      const poorResult = service.calculatePerformanceMetrics(
        10,
        200,
        2000,
        1000,
        0.3,
        false,
      );
      // With these conditions, score should be reduced significantly
      expect(poorResult.score).toBeLessThan(70);
      expect(['fair', 'poor', 'critical']).toContain(poorResult.health);
    });
  });

  describe('endpoint configuration', () => {
    it('should enable only read-only endpoints by default', async () => {
      const svc = await createService();

      expect(svc.isEndpointEnabled('stats')).toBe(true);
      expect(svc.isEndpointEnabled('metrics')).toBe(true);
      expect(svc.isEndpointEnabled('pause')).toBe(false);
      expect(svc.isEndpointEnabled('resume')).toBe(false);
      expect(svc.isEndpointEnabled('clean')).toBe(false);
      expect(svc.isEndpointEnabled('drain')).toBe(false);
    });

    it('should always enable stats endpoint', async () => {
      const svc = await createService({ endpoints: [] });

      expect(svc.isEndpointEnabled('stats')).toBe(true);
    });

    it('should respect custom endpoint configuration', async () => {
      const svc = await createService({
        endpoints: ['metrics', 'pause', 'resume'],
      });

      expect(svc.isEndpointEnabled('stats')).toBe(true); // Always enabled
      expect(svc.isEndpointEnabled('metrics')).toBe(true);
      expect(svc.isEndpointEnabled('pause')).toBe(true);
      expect(svc.isEndpointEnabled('resume')).toBe(true);
      expect(svc.isEndpointEnabled('clean')).toBe(false);
      expect(svc.isEndpointEnabled('drain')).toBe(false);
    });

    it('should return correct capabilities', async () => {
      const svc = await createService({
        endpoints: ['pause', 'resume', 'clean'],
      });

      const capabilities = svc.getCapabilities();

      expect(capabilities.endpoints).toContain('stats');
      expect(capabilities.endpoints).toContain('pause');
      expect(capabilities.endpoints).toContain('resume');
      expect(capabilities.endpoints).toContain('clean');
      expect(capabilities.endpoints).not.toContain('drain');

      expect(capabilities.canPause).toBe(true);
      expect(capabilities.canClean).toBe(true);
      expect(capabilities.canDrain).toBe(false);
    });

    it('should require both pause and resume for canPause', async () => {
      const pauseOnlySvc = await createService({ endpoints: ['pause'] });
      expect(pauseOnlySvc.getCapabilities().canPause).toBe(false);

      const resumeOnlySvc = await createService({ endpoints: ['resume'] });
      expect(resumeOnlySvc.getCapabilities().canPause).toBe(false);

      const bothSvc = await createService({ endpoints: ['pause', 'resume'] });
      expect(bothSvc.getCapabilities().canPause).toBe(true);
    });
  });

  describe('Redis key generation', () => {
    it('should generate correct pause reason key', () => {
      const key = (service as any).getPauseReasonKey('my-queue');
      expect(key).toBe('bull:my-queue:pause-reason');
    });
  });

  describe('pause notes', () => {
    function createPausableQueue(
      name: string,
      paused = false,
      stored: string | null = null,
    ) {
      const store = new Map<string, string>();
      if (stored) store.set(`bull:${name}:pause-reason`, stored);
      const client = {
        get: jest.fn(async (key: string) => store.get(key) ?? null),
        set: jest.fn(async (key: string, value: string) => {
          store.set(key, value);
          return 'OK';
        }),
        del: jest.fn(async (key: string) => {
          store.delete(key);
          return 1;
        }),
      };
      let isPaused = paused;
      const queue = {
        name,
        client: Promise.resolve(client),
        isPaused: jest.fn(async () => isPaused),
        pause: jest.fn(async () => {
          isPaused = true;
        }),
        resume: jest.fn(async () => {
          isPaused = false;
        }),
      };
      return { queue, client, store };
    }

    it('pauses and stores a note with when and why', async () => {
      const { queue, store } = createPausableQueue('emails');

      const result = await service.pauseQueue(
        queue as never,
        { reason: 'Deploy', until: '2026-09-19T13:00:00.000Z', source: 'api' },
        'emails',
      );

      expect(queue.pause).toHaveBeenCalledTimes(1);
      const note = JSON.parse(store.get('bull:emails:pause-reason') ?? '{}');
      expect(note).toEqual({
        reason: 'Deploy',
        pausedAt: expect.any(String),
        until: '2026-09-19T13:00:00.000Z',
        source: 'api',
      });
      expect(result).toMatchObject({
        status: 'paused',
        reason: 'Deploy',
        until: '2026-09-19T13:00:00.000Z',
        queues: ['emails'],
      });
    });

    it('stores a note with no reason when none is given', async () => {
      const { queue, store } = createPausableQueue('emails');

      const result = await service.pauseQueue(queue as never);

      const note = JSON.parse(store.get('bull:emails:pause-reason') ?? '{}');
      expect(note).toEqual({ pausedAt: expect.any(String), source: 'api' });
      expect(result).not.toHaveProperty('reason');
    });

    it('updates the note of a paused queue without pausing again', async () => {
      const { queue, store } = createPausableQueue(
        'emails',
        true,
        JSON.stringify({
          pausedAt: '2026-09-19T12:00:00.000Z',
          source: 'api',
        }),
      );

      await service.pauseQueue(queue as never, { reason: 'Incident' });

      expect(queue.pause).not.toHaveBeenCalled();
      expect(JSON.parse(store.get('bull:emails:pause-reason') ?? '{}')).toEqual(
        {
          reason: 'Incident',
          pausedAt: '2026-09-19T12:00:00.000Z',
          source: 'api',
        },
      );
    });

    it('accepts a bare string reason from older callers', async () => {
      const { queue, store } = createPausableQueue('emails');

      await service.pauseQueue(queue as never, 'manual-pause');

      expect(
        JSON.parse(store.get('bull:emails:pause-reason') ?? '{}'),
      ).toMatchObject({
        reason: 'manual-pause',
      });
    });

    it('pauses several queues with one note and clears it on resume', async () => {
      const a = createPausableQueue('a');
      const b = createPausableQueue('b', true, 'old');
      const queues = [
        { queue: a.queue as never, statsKey: 'a' },
        { queue: b.queue as never, statsKey: 'b' },
      ];

      const result = await service.pauseQueues(queues, {
        reason: 'Maintenance',
      });

      expect(a.queue.pause).toHaveBeenCalledTimes(1);
      expect(b.queue.pause).not.toHaveBeenCalled();
      expect(result).toMatchObject({
        reason: 'Maintenance',
        queues: ['a', 'b'],
      });
      expect(
        JSON.parse(a.store.get('bull:a:pause-reason') ?? '{}').reason,
      ).toBe('Maintenance');

      await service.resumeQueues(queues);

      expect(a.store.has('bull:a:pause-reason')).toBe(false);
      expect(b.store.has('bull:b:pause-reason')).toBe(false);
    });

    it('resumes a queue whose until has passed, and only then', async () => {
      const due = createPausableQueue(
        'due',
        true,
        JSON.stringify({ until: '2026-09-19T12:00:00.000Z', source: 'api' }),
      );
      const later = createPausableQueue(
        'later',
        true,
        JSON.stringify({ until: '2026-09-19T13:00:00.000Z', source: 'api' }),
      );
      const open = createPausableQueue(
        'open',
        true,
        JSON.stringify({ reason: 'x' }),
      );
      const running = createPausableQueue('running');
      const getQueues = () =>
        new Map([
          ['due', { queue: due.queue as never }],
          ['later', { queue: later.queue as never }],
          ['open', { queue: open.queue as never }],
          ['running', { queue: running.queue as never }],
        ]);
      const timed = new QueuebertService(
        { queues: [], endpoints: ['pause', 'resume'] },
        new QueuebertIntegrationRegistry(),
        getQueues,
      );

      const resumed = await timed.resumeExpiredPauses(
        Date.parse('2026-09-19T12:30:00.000Z'),
      );

      expect(resumed).toEqual(['due']);
      expect(due.queue.resume).toHaveBeenCalledTimes(1);
      expect(due.store.has('bull:due:pause-reason')).toBe(false);
      expect(later.queue.resume).not.toHaveBeenCalled();
      expect(open.queue.resume).not.toHaveBeenCalled();
      expect(running.queue.resume).not.toHaveBeenCalled();
    });

    it('reports canPauseUntil only with the queues in hand', () => {
      const alone = new QueuebertService(
        { queues: [], endpoints: ['pause', 'resume'] },
        new QueuebertIntegrationRegistry(),
      );
      const withQueues = new QueuebertService(
        { queues: [], endpoints: ['pause', 'resume'] },
        new QueuebertIntegrationRegistry(),
        () => new Map(),
      );
      const readOnly = new QueuebertService(
        { queues: [] },
        new QueuebertIntegrationRegistry(),
        () => new Map(),
      );

      expect(alone.getCapabilities()).toMatchObject({
        canPauseWithReason: true,
        canPauseUntil: false,
      });
      expect(withQueues.getCapabilities()).toMatchObject({
        canPauseWithReason: true,
        canPauseUntil: true,
      });
      expect(readOnly.getCapabilities()).toMatchObject({
        canPauseWithReason: false,
        canPauseUntil: false,
      });
    });

    it('runs the ticker on module init and stops it on destroy', async () => {
      jest.useFakeTimers();
      try {
        const due = createPausableQueue(
          'due',
          true,
          JSON.stringify({ until: '2000-01-01T00:00:00.000Z' }),
        );
        const timed = new QueuebertService(
          { queues: [], endpoints: ['pause', 'resume'] },
          new QueuebertIntegrationRegistry(),
          () => new Map([['due', { queue: due.queue as never }]]),
        );

        timed.onModuleInit();
        await jest.advanceTimersByTimeAsync(30_000);
        expect(due.queue.resume).toHaveBeenCalledTimes(1);

        await timed.onModuleDestroy();
        await jest.advanceTimersByTimeAsync(60_000);
        expect(due.queue.resume).toHaveBeenCalledTimes(1);
      } finally {
        jest.useRealTimers();
      }
    });
  });

  describe('Redis instance mapping', () => {
    it('should use default instance ID for single Redis setup', async () => {
      const svc = await createService();
      const id = (svc as any).getRedisInstanceId({ name: 'test-queue' });
      expect(id).toBe('default');
    });

    it('should use first Redis instance as default when multi-Redis configured', async () => {
      const svc = await createService({
        redis: [
          { id: 'primary', label: 'Primary' },
          { id: 'secondary', label: 'Secondary' },
        ],
      });

      const id = (svc as any).getRedisInstanceId({ name: 'test-queue' });
      expect(id).toBe('primary');
    });

    it('should use explicit redis config when specified', async () => {
      const svc = await createService({
        redis: [
          { id: 'primary', label: 'Primary' },
          { id: 'secondary', label: 'Secondary' },
        ],
      });

      const id = (svc as any).getRedisInstanceId({
        name: 'test-queue',
        redis: 'secondary',
      });
      expect(id).toBe('secondary');
    });

    it('should get correct Redis instance label', async () => {
      const svc = await createService({
        redis: [
          { id: 'primary', label: 'Primary Redis' },
          { id: 'secondary', label: 'Secondary Redis' },
        ],
      });

      expect((svc as any).getRedisInstanceLabel('primary')).toBe(
        'Primary Redis',
      );
      expect((svc as any).getRedisInstanceLabel('secondary')).toBe(
        'Secondary Redis',
      );
      expect((svc as any).getRedisInstanceLabel('unknown')).toBe('unknown');
    });
  });

  describe('performance metrics validation', () => {
    it('should validate correct performance metrics object', () => {
      const validMetrics: PerformanceMetrics = {
        score: 85,
        trend: 'stable',
        throughputPerMin: 100,
        addedPerMin: 90,
        deltaPerMin: 10,
        estimatedClearTimeMs: 60000,
        health: 'good',
      };

      expect((service as any).isValidPerformanceMetrics(validMetrics)).toBe(
        true,
      );
    });

    it('should accept null estimatedClearTimeMs', () => {
      const metrics: PerformanceMetrics = {
        score: 100,
        trend: 'idle',
        throughputPerMin: 0,
        addedPerMin: 0,
        deltaPerMin: 0,
        estimatedClearTimeMs: null,
        health: 'excellent',
      };

      expect((service as any).isValidPerformanceMetrics(metrics)).toBe(true);
    });

    it('should reject invalid metrics objects', () => {
      expect((service as any).isValidPerformanceMetrics(null)).toBe(false);
      expect((service as any).isValidPerformanceMetrics(undefined)).toBe(false);
      expect((service as any).isValidPerformanceMetrics({})).toBe(false);
      expect(
        (service as any).isValidPerformanceMetrics({ score: 'not a number' }),
      ).toBe(false);
    });

    it('should reject invalid trend values', () => {
      const invalidTrend = {
        score: 85,
        trend: 'invalid_trend',
        throughputPerMin: 100,
        addedPerMin: 90,
        deltaPerMin: 10,
        estimatedClearTimeMs: 60000,
        health: 'good',
      };

      expect((service as any).isValidPerformanceMetrics(invalidTrend)).toBe(
        false,
      );
    });

    it('should reject invalid health values', () => {
      const invalidHealth = {
        score: 85,
        trend: 'stable',
        throughputPerMin: 100,
        addedPerMin: 90,
        deltaPerMin: 10,
        estimatedClearTimeMs: 60000,
        health: 'invalid_health',
      };

      expect((service as any).isValidPerformanceMetrics(invalidHealth)).toBe(
        false,
      );
    });
  });
});
