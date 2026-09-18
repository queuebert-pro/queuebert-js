import { UnrecoverableError } from 'bullmq';

import {
  BaseQueueProcessor,
  type BaseQueueProcessorOptions,
  type JobCompletionContext,
  type JobFailureContext,
} from './base-queue-processor';

type EventHandler = (...args: any[]) => void;
type EventTargetMock = {
  handlers: Record<string, EventHandler>;
  on: jest.Mock<EventTargetMock, [string, EventHandler]>;
};

function createEventTargetMock(): EventTargetMock {
  const handlers: Record<string, EventHandler> = {};
  const target = {} as EventTargetMock;

  target.handlers = handlers;
  target.on = jest.fn((event: string, handler: EventHandler) => {
    handlers[event] = handler;
    return target;
  });

  return target;
}

function attachWorker(processor: BaseQueueProcessor, worker: any) {
  Object.defineProperty(processor, 'worker', {
    configurable: true,
    get: () => worker,
  });
}

function muteLogger(processor: BaseQueueProcessor) {
  const logger = (processor as any).logger;

  return {
    log: jest.spyOn(logger, 'log').mockImplementation(() => undefined),
    warn: jest.spyOn(logger, 'warn').mockImplementation(() => undefined),
    error: jest.spyOn(logger, 'error').mockImplementation(() => undefined),
  };
}

class TestProcessor extends BaseQueueProcessor {
  public shouldFail = false;

  constructor(options: Partial<BaseQueueProcessorOptions> = {}) {
    super({
      queueName: 'emails',
      statsLogInterval: 60000,
      ...options,
    });
  }

  protected async processJob(job: { data: { result?: unknown } }) {
    if (this.shouldFail) {
      throw new Error('processing failed');
    }
    return job.data.result ?? true;
  }

  protected override getCustomStats() {
    return {
      customValue: 42,
    };
  }

  protected override getCacheStats() {
    return {
      domains: {
        size: 3,
        hitRate: '75.0%',
        hits: 6,
        misses: 2,
      },
    };
  }
}

class LifecycleProcessor extends TestProcessor {
  public initHook = jest.fn();
  public destroyHook = jest.fn();
  public redisErrors: string[] = [];
  public workerErrors: string[] = [];
  public recoveryExhausted = jest.fn();

  protected override async onProcessorInit() {
    this.initHook();
  }

  protected override onProcessorDestroy() {
    this.destroyHook();
  }

  protected override onRedisError(err: Error) {
    this.redisErrors.push(err.message);
  }

  protected override onWorkerError(err: Error) {
    this.workerErrors.push(err.message);
  }

  protected override onWorkerRecoveryExhausted() {
    this.recoveryExhausted();
  }

  protected override getCustomLogLines() {
    return ['custom heartbeat line'];
  }
}

describe('BaseQueueProcessor', () => {
  afterEach(() => {
    jest.restoreAllMocks();
    jest.useRealTimers();
  });

  it('wraps successful job processing with stats collection', async () => {
    const processor = new TestProcessor();
    const job = {
      id: 'job-1',
      name: 'welcome',
      data: { result: 'ok' },
    } as any;

    await expect(processor.process(job)).resolves.toBe('ok');

    const stats = processor.getProcessorStats();
    expect(stats.jobs).toMatchObject({
      processed: 1,
      completed: 1,
      failed: 0,
      failureRate: 0,
      successRate: 1,
    });
    expect(stats.jobsByType?.['welcome']).toMatchObject({
      processed: 1,
      completed: 1,
      failed: 0,
    });
    expect(stats.custom?.['customValue']).toBe(42);
    expect(stats.cache?.['domains']).toMatchObject({
      size: 3,
      hitRate: '75.0%',
    });
  });

  it('records failed jobs and rethrows the original error', async () => {
    const processor = new TestProcessor();
    processor.shouldFail = true;

    await expect(
      processor.process({
        id: 'job-1',
        name: 'welcome',
        data: {},
      } as any),
    ).rejects.toThrow('processing failed');

    const stats = processor.getProcessorStats();
    expect(stats.jobs).toMatchObject({
      processed: 1,
      completed: 0,
      failed: 1,
      failureRate: 1,
      successRate: 0,
    });
  });

  it('returns empty cache configs by default', () => {
    const processor = new TestProcessor();

    expect(processor.getCacheConfigs()).toEqual([]);
  });

  it('wires lifecycle, Redis, and worker events into processor state', async () => {
    const processor = new LifecycleProcessor();
    const logger = muteLogger(processor);
    const redisClient = createEventTargetMock() as any;
    redisClient.llen = jest.fn().mockResolvedValue(0);
    const worker = createEventTargetMock() as any;
    worker.client = Promise.resolve(redisClient);
    worker.isRunning = jest.fn().mockReturnValue(true);
    worker.run = jest.fn().mockResolvedValue(undefined);
    attachWorker(processor, worker);

    await processor.onModuleInit();

    expect(processor.initHook).toHaveBeenCalledTimes(1);
    for (const event of [
      'connect',
      'ready',
      'error',
      'close',
      'reconnecting',
      'end',
    ]) {
      expect(redisClient.on).toHaveBeenCalledWith(event, expect.any(Function));
    }
    for (const event of [
      'error',
      'failed',
      'ready',
      'stalled',
      'closing',
      'closed',
      'drained',
    ]) {
      expect(worker.on).toHaveBeenCalledWith(event, expect.any(Function));
    }

    redisClient.handlers['connect']();
    expect(processor.getProcessorStats().custom?.['worker']).toMatchObject({
      redisStatus: 'reconnecting',
    });

    redisClient.handlers['ready']();
    expect(processor.getProcessorStats().custom?.['worker']).toMatchObject({
      redisStatus: 'connected',
    });

    redisClient.handlers['error'](new Error('redis down'));
    expect(processor.redisErrors).toEqual(['redis down']);

    redisClient.handlers['close']();
    expect(processor.getProcessorStats().custom?.['worker']).toMatchObject({
      redisStatus: 'disconnected',
    });

    redisClient.handlers['reconnecting']();
    expect(processor.getProcessorStats().custom?.['worker']).toMatchObject({
      redisStatus: 'reconnecting',
    });

    redisClient.handlers['end']();
    expect(processor.getProcessorStats().custom?.['worker']).toMatchObject({
      redisStatus: 'disconnected',
    });

    worker.handlers['ready']();
    worker.handlers['stalled']('job-1');
    worker.handlers['closing']('shutdown');
    worker.handlers['failed']({ id: 'job-2' } as any, new Error('bad job'));
    worker.handlers['error'](new Error('worker down'));
    (processor as any).isShuttingDown = true;
    worker.handlers['closed']();
    worker.handlers['drained']();

    expect(processor.workerErrors).toEqual(['worker down']);
    expect(processor.getProcessorStats().custom?.['worker']).toMatchObject({
      lastCloseTime: expect.any(String),
    });
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining('stalled'),
    );
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining('Worker closing'),
    );
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining('Job job-2 failed'),
    );

    await processor.onModuleDestroy();

    expect(processor.destroyHook).toHaveBeenCalledTimes(1);
    expect(logger.log).toHaveBeenCalledWith('Processor shutting down');
  });

  it('restarts a stopped worker during recovery', async () => {
    jest.useFakeTimers();
    const processor = new LifecycleProcessor({
      maxWorkerRestartAttempts: 2,
      workerRestartDelay: 25,
    });
    muteLogger(processor);
    const worker = {
      client: Promise.resolve({ llen: jest.fn().mockResolvedValue(0) }),
      isRunning: jest.fn().mockReturnValue(false),
      run: jest.fn().mockResolvedValue(undefined),
    };
    attachWorker(processor, worker);
    (processor as any).attemptWorkerRecovery();

    expect(processor.getProcessorStats().custom?.['worker']).toMatchObject({
      restartAttempts: 1,
    });

    await jest.advanceTimersByTimeAsync(25);

    expect(worker.run).toHaveBeenCalledTimes(1);
    expect(processor.getProcessorStats().custom?.['worker']).toMatchObject({
      restartAttempts: 0,
    });

    await processor.onModuleDestroy();
  });

  it('stops recovery after the configured number of restart failures', async () => {
    jest.useFakeTimers();
    const processor = new LifecycleProcessor({
      maxWorkerRestartAttempts: 1,
      workerRestartDelay: 10,
    });
    muteLogger(processor);
    const worker = {
      client: Promise.resolve({ llen: jest.fn().mockResolvedValue(0) }),
      isRunning: jest.fn().mockReturnValue(false),
      run: jest.fn().mockRejectedValue(new Error('restart failed')),
    };
    attachWorker(processor, worker);
    (processor as any).attemptWorkerRecovery();
    await jest.advanceTimersByTimeAsync(10);

    expect(worker.run).toHaveBeenCalledTimes(1);
    expect(processor.workerErrors).toEqual(['restart failed']);
    expect(processor.recoveryExhausted).toHaveBeenCalledTimes(1);

    await processor.onModuleDestroy();
  });

  it('logs heartbeat alerts, queue depth, and custom log lines', async () => {
    const processor = new LifecycleProcessor();
    const logger = muteLogger(processor);
    const redisClient = {
      llen: jest.fn().mockResolvedValueOnce(3).mockResolvedValueOnce(1),
    };
    const worker = {
      client: Promise.resolve(redisClient),
      isRunning: jest.fn().mockReturnValue(false),
    };
    attachWorker(processor, worker);
    (processor as any).lastJobTime = new Date(Date.now() - 121000);
    (processor as any).redisConnectionStatus = 'disconnected';

    await (processor as any).logStats();

    expect(redisClient.llen).toHaveBeenCalledWith('bull:emails:wait');
    expect(redisClient.llen).toHaveBeenCalledWith('bull:emails:active');
    expect(logger.log).toHaveBeenCalledWith(
      expect.stringContaining('Queue: 3 waiting, 1 active'),
    );
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining('possible stall'),
    );
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining('Worker is stopped but 3 jobs are waiting'),
    );
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining('Redis connection status: disconnected'),
    );
    expect(logger.log).toHaveBeenCalledWith('custom heartbeat line');

    await processor.onModuleDestroy();
  });

  describe('per-job hooks', () => {
    class HookProcessor extends BaseQueueProcessor {
      public failures: Array<{ error: unknown; ctx: JobFailureContext }> = [];
      public completions: Array<{
        result: unknown;
        ctx: JobCompletionContext;
      }> = [];
      public failWith: unknown = null;
      public discardOnRun = false;
      public hookThrows = false;

      constructor() {
        super({ queueName: 'emails', statsLogInterval: 60000 });
      }

      protected async processJob(job: any) {
        if (this.discardOnRun) job.discard();
        if (this.failWith) throw this.failWith;
        return 'ok';
      }

      protected override onJobFailed(
        _job: unknown,
        error: unknown,
        ctx: JobFailureContext,
      ) {
        if (this.hookThrows) throw new Error('hook exploded');
        this.failures.push({ error, ctx });
      }

      protected override onJobCompleted(
        _job: unknown,
        result: unknown,
        ctx: JobCompletionContext,
      ) {
        if (this.hookThrows) throw new Error('hook exploded');
        this.completions.push({ result, ctx });
      }
    }

    function createJob(overrides: Record<string, unknown> = {}) {
      return {
        id: 'job-1',
        name: 'welcome',
        data: {},
        attemptsMade: 0,
        opts: { attempts: 3 },
        discard: jest.fn(),
        ...overrides,
      } as any;
    }

    it('reports a retryable attempt as not final', async () => {
      const processor = new HookProcessor();
      processor.failWith = new Error('processing failed');

      await expect(processor.process(createJob())).rejects.toThrow(
        'processing failed',
      );

      expect(processor.failures).toHaveLength(1);
      expect(processor.failures[0].ctx).toMatchObject({
        attempt: 1,
        maxAttempts: 3,
        isFinalAttempt: false,
        discarded: false,
        unrecoverable: false,
      });
      expect(processor.failures[0].ctx.durationMs).toBeGreaterThanOrEqual(0);
    });

    it('counts the current attempt as attemptsMade + 1, matching BullMQ', async () => {
      const processor = new HookProcessor();
      processor.failWith = new Error('processing failed');

      // BullMQ has not incremented attemptsMade yet when the handler throws,
      // and decides retries on attemptsMade + 1 < opts.attempts.
      await expect(
        processor.process(createJob({ attemptsMade: 2 })),
      ).rejects.toThrow('processing failed');

      expect(processor.failures[0].ctx).toMatchObject({
        attempt: 3,
        maxAttempts: 3,
        isFinalAttempt: true,
      });
    });

    it('treats a missing opts.attempts as a single attempt', async () => {
      const processor = new HookProcessor();
      processor.failWith = new Error('processing failed');

      await expect(processor.process(createJob({ opts: {} }))).rejects.toThrow(
        'processing failed',
      );

      expect(processor.failures[0].ctx).toMatchObject({
        attempt: 1,
        maxAttempts: 1,
        isFinalAttempt: true,
      });
    });

    it('marks an UnrecoverableError final on the first attempt', async () => {
      const processor = new HookProcessor();
      processor.failWith = new UnrecoverableError('do not retry');

      await expect(processor.process(createJob())).rejects.toThrow(
        'do not retry',
      );

      expect(processor.failures[0].ctx).toMatchObject({
        attempt: 1,
        maxAttempts: 3,
        unrecoverable: true,
        isFinalAttempt: true,
      });
    });

    it('recognises an UnrecoverableError by name across bullmq copies', async () => {
      const foreign = new Error('do not retry');
      foreign.name = 'UnrecoverableError';
      const processor = new HookProcessor();
      processor.failWith = foreign;

      await expect(processor.process(createJob())).rejects.toThrow(
        'do not retry',
      );

      expect(processor.failures[0].ctx).toMatchObject({
        unrecoverable: true,
        isFinalAttempt: true,
      });
    });

    it('detects job.discard() and marks the attempt final', async () => {
      const processor = new HookProcessor();
      processor.discardOnRun = true;
      processor.failWith = new Error('processing failed');
      const job = createJob();

      await expect(processor.process(job)).rejects.toThrow('processing failed');

      expect(processor.failures[0].ctx).toMatchObject({
        attempt: 1,
        maxAttempts: 3,
        discarded: true,
        isFinalAttempt: true,
      });
      // The original discard still runs, so BullMQ's own flag is set too.
      expect(job.discard).toHaveBeenCalledTimes(1);
    });

    it("restores the job's own discard after processing", async () => {
      const processor = new HookProcessor();
      const original = jest.fn();
      const job = createJob({ discard: original });

      await processor.process(job);

      expect(job.discard).toBe(original);
    });

    it('calls onJobCompleted on success', async () => {
      const processor = new HookProcessor();

      await expect(processor.process(createJob())).resolves.toBe('ok');

      expect(processor.failures).toHaveLength(0);
      expect(processor.completions).toHaveLength(1);
      expect(processor.completions[0].result).toBe('ok');
      expect(processor.completions[0].ctx).toMatchObject({ attempt: 1 });
    });

    it('rethrows the job error even when the failure hook throws', async () => {
      const processor = new HookProcessor();
      processor.failWith = new Error('processing failed');
      processor.hookThrows = true;
      const logger = muteLogger(processor);

      await expect(processor.process(createJob())).rejects.toThrow(
        'processing failed',
      );

      expect(logger.error).toHaveBeenCalledWith(
        expect.stringContaining('onJobFailed hook threw'),
      );
    });

    it('still resolves when the completion hook throws', async () => {
      const processor = new HookProcessor();
      processor.hookThrows = true;
      const logger = muteLogger(processor);

      await expect(processor.process(createJob())).resolves.toBe('ok');

      expect(logger.error).toHaveBeenCalledWith(
        expect.stringContaining('onJobCompleted hook threw'),
      );
    });

    it('awaits an async failure hook before rethrowing', async () => {
      const order: string[] = [];

      class AsyncHookProcessor extends BaseQueueProcessor {
        constructor() {
          super({ queueName: 'emails', statsLogInterval: 60000 });
        }

        protected async processJob() {
          throw new Error('processing failed');
        }

        protected override async onJobFailed() {
          await new Promise((resolve) => setTimeout(resolve, 5));
          order.push('hook');
        }
      }

      const processor = new AsyncHookProcessor();

      await expect(processor.process(createJob())).rejects.toThrow(
        'processing failed',
      );
      order.push('rethrow');

      expect(order).toEqual(['hook', 'rethrow']);
    });

    it('still counts stats when hooks are in play', async () => {
      const processor = new HookProcessor();
      processor.failWith = new Error('processing failed');

      await expect(processor.process(createJob())).rejects.toThrow(
        'processing failed',
      );

      expect(processor.getProcessorStats().jobs).toMatchObject({
        processed: 1,
        completed: 0,
        failed: 1,
      });
    });
  });
});
