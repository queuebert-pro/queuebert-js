import { Logger } from '@nestjs/common';
import { Worker, Job } from 'bullmq';

import { QueuebertWorker } from './queuebert-worker';

// Mock NestJS Logger
const mockLoggerError = jest.fn();
jest.spyOn(Logger.prototype, 'error').mockImplementation(mockLoggerError);

// Mock bullmq Worker
jest.mock('bullmq', () => {
  const mockWorker = {
    pause: jest.fn().mockResolvedValue(undefined),
    resume: jest.fn().mockResolvedValue(undefined),
    close: jest.fn().mockResolvedValue(undefined),
    on: jest.fn(),
  };
  return {
    Worker: jest.fn().mockImplementation(() => mockWorker),
    Job: jest.fn(),
  };
});

/**
 * Presence writes settle over several microtask turns (client lookup, then
 * the MULTI), so a single resolved promise is not enough to observe them.
 */
async function flush(turns = 30): Promise<void> {
  for (let i = 0; i < turns; i++) {
    await Promise.resolve();
  }
}

describe('QueuebertWorker', () => {
  let worker: QueuebertWorker;
  let mockProcessor: jest.Mock;
  let mockBullMQWorker: any;

  beforeEach(() => {
    jest.clearAllMocks();
    mockProcessor = jest.fn().mockResolvedValue('result');

    // Create worker
    worker = new QueuebertWorker('test-queue', mockProcessor, {
      connection: { host: 'localhost', port: 6379 },
      tags: { env: 'test', app: 'myapp' },
    });

    // Get the mock worker instance
    mockBullMQWorker = (Worker as unknown as jest.Mock).mock.results[0].value;
  });

  afterEach(async () => {
    // Clean up
    if (worker) {
      await worker.close();
    }
  });

  describe('constructor', () => {
    it('should create a worker with the given queue name', () => {
      expect(worker.queueName).toBe('test-queue');
    });

    it('should create the underlying BullMQ Worker', () => {
      expect(Worker).toHaveBeenCalledWith(
        'test-queue',
        expect.any(Function),
        expect.objectContaining({
          connection: { host: 'localhost', port: 6379 },
        }),
      );
    });

    it('should set isRunning to true initially', () => {
      expect(worker.isRunning).toBe(true);
    });

    it('should set isPaused to false initially', () => {
      expect(worker.isPaused).toBe(false);
    });

    it('should set up worker event listeners', () => {
      expect(mockBullMQWorker.on).toHaveBeenCalledWith(
        'progress',
        expect.any(Function),
      );
      expect(mockBullMQWorker.on).toHaveBeenCalledWith(
        'stalled',
        expect.any(Function),
      );
      expect(mockBullMQWorker.on).toHaveBeenCalledWith(
        'error',
        expect.any(Function),
      );
    });
  });

  describe('getters', () => {
    it('should return the queue name', () => {
      expect(worker.queueName).toBe('test-queue');
    });

    it('should return isRunning state', () => {
      expect(worker.isRunning).toBe(true);
    });

    it('should return isPaused state', () => {
      expect(worker.isPaused).toBe(false);
    });

    it('should return the underlying worker', () => {
      expect(worker.worker).toBe(mockBullMQWorker);
    });
  });

  describe('getTags', () => {
    it('should return a copy of the tags', () => {
      const tags = worker.getTags();

      expect(tags).toEqual({ env: 'test', app: 'myapp' });
    });

    it('should return empty object if no tags provided', () => {
      const workerNoTags = new QueuebertWorker('no-tags-queue', mockProcessor, {
        connection: { host: 'localhost', port: 6379 },
      });
      const tags = workerNoTags.getTags();

      expect(tags).toEqual({});
    });

    it('should not allow modifying internal tags', () => {
      const tags = worker.getTags();
      tags['newKey'] = 'newValue';

      expect(worker.getTags()).not.toHaveProperty('newKey');
    });
  });

  describe('pause', () => {
    it('should pause the underlying worker', async () => {
      await worker.pause();

      expect(mockBullMQWorker.pause).toHaveBeenCalled();
    });

    it('should set isPaused to true', async () => {
      await worker.pause();

      expect(worker.isPaused).toBe(true);
    });

    it('should pass doNotWaitActive option', async () => {
      await worker.pause(true);

      expect(mockBullMQWorker.pause).toHaveBeenCalledWith(true);
    });
  });

  describe('resume', () => {
    it('should resume the underlying worker', async () => {
      await worker.pause();
      await worker.resume();

      expect(mockBullMQWorker.resume).toHaveBeenCalled();
    });

    it('should set isPaused to false', async () => {
      await worker.pause();
      expect(worker.isPaused).toBe(true);

      await worker.resume();
      expect(worker.isPaused).toBe(false);
    });

    it('should keep paused state when the underlying resume fails', async () => {
      await worker.pause();
      mockBullMQWorker.resume.mockRejectedValueOnce(new Error('resume failed'));

      await expect(worker.resume()).rejects.toThrow('resume failed');
      expect(worker.isPaused).toBe(true);
    });
  });

  describe('close', () => {
    it('should close the underlying worker', async () => {
      await worker.close();

      expect(mockBullMQWorker.close).toHaveBeenCalled();
    });

    it('should set isRunning to false', async () => {
      await worker.close();

      expect(worker.isRunning).toBe(false);
    });

    it('should pass force option to underlying worker', async () => {
      await worker.close(true);

      expect(mockBullMQWorker.close).toHaveBeenCalledWith(true);
    });

    it('records an application close and tells listeners once', async () => {
      const listener = jest.fn();
      worker.onStopped(listener);

      await worker.close();
      // BullMQ's own closed event arriving afterwards must not double up
      const closedHandler = mockBullMQWorker.on.mock.calls.find(
        ([event]: [string]) => event === 'closed',
      )?.[1];
      closedHandler?.();
      await flush();

      expect(worker.lastStop).toMatchObject({
        reason: 'closed',
        description: 'Closed by the application',
        jobsProcessed: 0,
      });
      expect(listener).toHaveBeenCalledTimes(1);
      expect(listener).toHaveBeenCalledWith(worker.lastStop);
    });

    it('records a shutdown when closed through shutdown()', async () => {
      await worker.shutdown();

      expect(mockBullMQWorker.close).toHaveBeenCalled();
      expect(worker.lastStop?.reason).toBe('shutdown');
    });

    it('blames a lost connection when BullMQ closes the worker itself', async () => {
      const handlers = Object.fromEntries(
        mockBullMQWorker.on.mock.calls.map(
          ([event, handler]: [string, any]) => [event, handler],
        ),
      );

      handlers['ioredis:close']();
      handlers['closed']();
      await flush();

      expect(worker.isRunning).toBe(false);
      expect(worker.lastStop?.reason).toBe('lost_connection');
    });

    it('drops a stop listener', async () => {
      const listener = jest.fn();
      worker.onStopped(listener);
      worker.offStopped(listener);

      await worker.close();

      expect(listener).not.toHaveBeenCalled();
    });
  });

  describe('getStats', () => {
    it('should return worker statistics', () => {
      const stats = worker.getStats();

      expect(stats).toMatchObject({
        totalProcessed: expect.any(Number),
        totalCompleted: expect.any(Number),
        totalFailed: expect.any(Number),
        activeJobs: expect.any(Number),
        duration: expect.any(Object),
        throughput: expect.any(Object),
        byJobName: expect.any(Object),
        startedAt: expect.any(String),
        lastJobTime: null,
        isPaused: false,
        isRunning: true,
      });
    });

    it('should start with zero processed jobs', () => {
      const stats = worker.getStats();

      expect(stats.totalProcessed).toBe(0);
      expect(stats.totalCompleted).toBe(0);
      expect(stats.totalFailed).toBe(0);
      expect(stats.activeJobs).toBe(0);
    });

    it('should have null lastJobTime initially', () => {
      const stats = worker.getStats();

      expect(stats.lastJobTime).toBeNull();
    });

    it('should include valid startedAt timestamp', () => {
      const stats = worker.getStats();
      const startedAt = new Date(stats.startedAt);

      expect(startedAt).toBeInstanceOf(Date);
      expect(startedAt.getTime()).toBeLessThanOrEqual(Date.now());
    });
  });

  describe('event listeners', () => {
    it('should register event listeners with on()', () => {
      const listener = jest.fn();

      worker.on('job:started', listener);

      // The listener should be registered (we can verify by checking internal state through stats)
      expect(() => worker.off('job:started', listener)).not.toThrow();
    });

    it('should unregister event listeners with off()', () => {
      const listener = jest.fn();

      worker.on('job:completed', listener);
      worker.off('job:completed', listener);

      // Should not throw when removing non-existent listener
      expect(() => worker.off('job:completed', listener)).not.toThrow();
    });

    it('should handle off() for events never registered', () => {
      const listener = jest.fn();

      // Should not throw
      expect(() => worker.off('job:failed', listener)).not.toThrow();
    });
  });

  describe('options', () => {
    it('should use default trackByJobName (true)', () => {
      const defaultWorker = new QueuebertWorker(
        'default-queue',
        mockProcessor,
        {
          connection: { host: 'localhost', port: 6379 },
        },
      );
      const stats = defaultWorker.getStats();

      // byJobName should be an object (tracking enabled)
      expect(stats.byJobName).toEqual({});
    });

    it('should respect trackByJobName = false option', () => {
      const noTrackWorker = new QueuebertWorker(
        'no-track-queue',
        mockProcessor,
        {
          connection: { host: 'localhost', port: 6379 },
          trackByJobName: false,
        },
      );
      const stats = noTrackWorker.getStats();

      // byJobName should be empty when tracking is disabled
      expect(stats.byJobName).toEqual({});
    });

    it('should use custom statsWindow option', () => {
      const customWindow = new QueuebertWorker(
        'custom-window-queue',
        mockProcessor,
        {
          connection: { host: 'localhost', port: 6379 },
          statsWindow: { windowMs: 60000, maxSamples: 500 },
        },
      );

      // The worker should be created without errors
      expect(customWindow.isRunning).toBe(true);
    });
  });

  describe('wrapped processor', () => {
    let wrappedProcessor: any;
    let mockJob: Partial<Job>;

    beforeEach(() => {
      // Get the wrapped processor that was passed to Worker
      const workerCall = (Worker as unknown as jest.Mock).mock.calls[0];
      wrappedProcessor = workerCall[1];

      mockJob = {
        id: 'job-123',
        name: 'test-job',
        data: { foo: 'bar' },
        attemptsMade: 0,
      };
    });

    it('should call the original processor', async () => {
      await wrappedProcessor(mockJob, 'token');

      expect(mockProcessor).toHaveBeenCalledWith(mockJob, 'token');
    });

    it('should return the processor result', async () => {
      mockProcessor.mockResolvedValue('custom-result');

      const result = await wrappedProcessor(mockJob, 'token');

      expect(result).toBe('custom-result');
    });

    it('should emit job:started event', async () => {
      const startListener = jest.fn();
      worker.on('job:started', startListener);

      await wrappedProcessor(mockJob, 'token');

      expect(startListener).toHaveBeenCalledWith(
        expect.objectContaining({
          event: 'job:started',
          jobId: 'job-123',
          jobName: 'test-job',
          queueName: 'test-queue',
        }),
      );
    });

    it('should emit job:completed event on success', async () => {
      const completeListener = jest.fn();
      worker.on('job:completed', completeListener);

      await wrappedProcessor(mockJob, 'token');

      expect(completeListener).toHaveBeenCalledWith(
        expect.objectContaining({
          event: 'job:completed',
          jobId: 'job-123',
          jobName: 'test-job',
          queueName: 'test-queue',
          result: 'result',
          duration: expect.any(Number),
        }),
      );
    });

    it('should emit job:failed event on error', async () => {
      const error = new Error('Job failed');
      mockProcessor.mockRejectedValue(error);
      const failListener = jest.fn();
      worker.on('job:failed', failListener);

      await expect(wrappedProcessor(mockJob, 'token')).rejects.toThrow(
        'Job failed',
      );

      expect(failListener).toHaveBeenCalledWith(
        expect.objectContaining({
          event: 'job:failed',
          jobId: 'job-123',
          jobName: 'test-job',
          queueName: 'test-queue',
          error: expect.any(Error),
          duration: expect.any(Number),
        }),
      );
    });

    it('reports the retry lifecycle on job:failed', async () => {
      mockProcessor.mockRejectedValue(new Error('Job failed'));
      const failListener = jest.fn();
      worker.on('job:failed', failListener);

      await expect(
        wrappedProcessor({ ...mockJob, opts: { attempts: 3 } }, 'token'),
      ).rejects.toThrow('Job failed');

      expect(failListener).toHaveBeenCalledWith(
        expect.objectContaining({
          attempt: 1,
          maxAttempts: 3,
          isFinalAttempt: false,
        }),
      );
    });

    it('emits job:retrying when another attempt is coming', async () => {
      mockProcessor.mockRejectedValue(new Error('Job failed'));
      const retryListener = jest.fn();
      worker.on('job:retrying', retryListener);

      await expect(
        wrappedProcessor(
          { ...mockJob, attemptsMade: 1, opts: { attempts: 3 } },
          'token',
        ),
      ).rejects.toThrow('Job failed');

      expect(retryListener).toHaveBeenCalledWith(
        expect.objectContaining({
          event: 'job:retrying',
          jobId: 'job-123',
          jobName: 'test-job',
          queueName: 'test-queue',
          attempt: 2,
          maxAttempts: 3,
          isFinalAttempt: false,
          error: expect.any(Error),
        }),
      );
    });

    it('does not emit job:retrying on the final attempt', async () => {
      mockProcessor.mockRejectedValue(new Error('Job failed'));
      const retryListener = jest.fn();
      const failListener = jest.fn();
      worker.on('job:retrying', retryListener);
      worker.on('job:failed', failListener);

      await expect(
        wrappedProcessor(
          { ...mockJob, attemptsMade: 2, opts: { attempts: 3 } },
          'token',
        ),
      ).rejects.toThrow('Job failed');

      expect(retryListener).not.toHaveBeenCalled();
      expect(failListener).toHaveBeenCalledWith(
        expect.objectContaining({ attempt: 3, isFinalAttempt: true }),
      );
    });

    it('does not emit job:retrying for an unrecoverable error', async () => {
      const unrecoverable = new Error('do not retry');
      unrecoverable.name = 'UnrecoverableError';
      mockProcessor.mockRejectedValue(unrecoverable);
      const retryListener = jest.fn();
      worker.on('job:retrying', retryListener);

      await expect(
        wrappedProcessor({ ...mockJob, opts: { attempts: 3 } }, 'token'),
      ).rejects.toThrow('do not retry');

      expect(retryListener).not.toHaveBeenCalled();
    });

    it('does not emit job:retrying when the handler discarded the job', async () => {
      mockProcessor.mockImplementation(async (job: { discard: () => void }) => {
        job.discard();
        throw new Error('Job failed');
      });
      const retryListener = jest.fn();
      worker.on('job:retrying', retryListener);

      await expect(
        wrappedProcessor(
          { ...mockJob, opts: { attempts: 3 }, discard: jest.fn() },
          'token',
        ),
      ).rejects.toThrow('Job failed');

      expect(retryListener).not.toHaveBeenCalled();
    });

    it('should track job duration', async () => {
      await wrappedProcessor(mockJob, 'token');

      const stats = worker.getStats();
      expect(stats.totalProcessed).toBe(1);
      expect(stats.totalCompleted).toBe(1);
    });

    it('should track failed jobs', async () => {
      mockProcessor.mockRejectedValue(new Error('fail'));

      await expect(wrappedProcessor(mockJob, 'token')).rejects.toThrow();

      const stats = worker.getStats();
      expect(stats.totalProcessed).toBe(1);
      expect(stats.totalFailed).toBe(1);
    });

    it('should update lastJobTime after processing', async () => {
      expect(worker.getStats().lastJobTime).toBeNull();

      await wrappedProcessor(mockJob, 'token');

      expect(worker.getStats().lastJobTime).not.toBeNull();
    });

    it('should handle jobs with undefined id', async () => {
      const jobWithoutId = { ...mockJob, id: undefined };

      await wrappedProcessor(jobWithoutId, 'token');

      const stats = worker.getStats();
      expect(stats.totalProcessed).toBe(1);
    });

    it('should track stats by job name', async () => {
      const job1 = { ...mockJob, id: 'job-1', name: 'job-type-a' };
      const job2 = { ...mockJob, id: 'job-2', name: 'job-type-b' };
      const job3 = { ...mockJob, id: 'job-3', name: 'job-type-a' };

      await wrappedProcessor(job1, 'token');
      await wrappedProcessor(job2, 'token');
      await wrappedProcessor(job3, 'token');

      const stats = worker.getStats();
      expect(stats.byJobName).toHaveProperty('job-type-a');
      expect(stats.byJobName).toHaveProperty('job-type-b');
    });

    it('should cap job-name cardinality and aggregate overflow names', async () => {
      const cappedWorker = new QueuebertWorker('capped-queue', mockProcessor, {
        connection: { host: 'localhost', port: 6379 },
        maxJobNameEntries: 2,
      });
      const workerCall = (Worker as unknown as jest.Mock).mock.calls.at(-1);
      const cappedProcessor = workerCall?.[1];

      await cappedProcessor({ ...mockJob, id: 'job-1', name: 'a' }, 'token');
      await cappedProcessor({ ...mockJob, id: 'job-2', name: 'b' }, 'token');
      await cappedProcessor({ ...mockJob, id: 'job-3', name: 'c' }, 'token');
      await cappedProcessor({ ...mockJob, id: 'job-4', name: 'd' }, 'token');

      expect(cappedWorker.getStats().byJobName).toMatchObject({
        a: { processed: 1 },
        b: { processed: 1 },
        __other__: { processed: 2 },
      });
      await cappedWorker.close();
    });

    it('should convert non-Error exceptions to Error', async () => {
      mockProcessor.mockRejectedValue('string error');
      const failListener = jest.fn();
      worker.on('job:failed', failListener);

      await expect(wrappedProcessor(mockJob, 'token')).rejects.toBe(
        'string error',
      );

      expect(failListener).toHaveBeenCalledWith(
        expect.objectContaining({
          error: expect.any(Error),
        }),
      );
    });
  });

  describe('worker events', () => {
    it('should set up progress event handler', () => {
      const progressHandler = mockBullMQWorker.on.mock.calls.find(
        (call: any[]) => call[0] === 'progress',
      )?.[1];

      expect(progressHandler).toBeDefined();
    });

    it('should set up stalled event handler', () => {
      const stalledHandler = mockBullMQWorker.on.mock.calls.find(
        (call: any[]) => call[0] === 'stalled',
      )?.[1];

      expect(stalledHandler).toBeDefined();
    });

    it('should set up error event handler', () => {
      const errorHandler = mockBullMQWorker.on.mock.calls.find(
        (call: any[]) => call[0] === 'error',
      )?.[1];

      expect(errorHandler).toBeDefined();
    });

    it('should emit job:progress event', async () => {
      const progressListener = jest.fn();
      worker.on('job:progress', progressListener);

      const progressHandler = mockBullMQWorker.on.mock.calls.find(
        (call: any[]) => call[0] === 'progress',
      )?.[1];

      await progressHandler({ id: 'job-1', name: 'test', data: {} }, 50);

      expect(progressListener).toHaveBeenCalledWith(
        expect.objectContaining({
          event: 'job:progress',
          jobId: 'job-1',
          progress: 50,
        }),
      );
    });

    it('should emit job:stalled event', async () => {
      const stalledListener = jest.fn();
      worker.on('job:stalled', stalledListener);

      const stalledHandler = mockBullMQWorker.on.mock.calls.find(
        (call: any[]) => call[0] === 'stalled',
      )?.[1];

      await stalledHandler('job-1', 'prev-state');

      expect(stalledListener).toHaveBeenCalledWith(
        expect.objectContaining({
          event: 'job:stalled',
          jobId: 'job-1',
          queueName: 'test-queue',
        }),
      );
    });

    it('should handle worker errors without crashing', () => {
      mockLoggerError.mockClear();
      const errorHandler = mockBullMQWorker.on.mock.calls.find(
        (call: any[]) => call[0] === 'error',
      )?.[1];

      // Should not throw and should log the error
      expect(() => errorHandler(new Error('test error'))).not.toThrow();
      expect(mockLoggerError).toHaveBeenCalled();
    });
  });

  describe('async listener handling', () => {
    it('should wait for async listeners to complete', async () => {
      const asyncListener = jest.fn().mockImplementation(async () => {
        await new Promise((resolve) => setTimeout(resolve, 10));
      });
      worker.on('job:started', asyncListener);

      // Get wrapped processor
      const workerCall = (Worker as unknown as jest.Mock).mock.calls[0];
      const wrappedProcessor = workerCall[1];
      const mockJob = { id: 'job-1', name: 'test', data: {}, attemptsMade: 0 };

      await wrappedProcessor(mockJob, 'token');

      expect(asyncListener).toHaveBeenCalled();
    });

    it('should catch and log listener errors', async () => {
      mockLoggerError.mockClear();
      const errorListener = jest.fn().mockImplementation(() => {
        throw new Error('Listener error');
      });
      worker.on('job:started', errorListener);

      // Get wrapped processor
      const workerCall = (Worker as unknown as jest.Mock).mock.calls[0];
      const wrappedProcessor = workerCall[1];
      const mockJob = { id: 'job-1', name: 'test', data: {}, attemptsMade: 0 };

      // Should not throw despite listener error
      await wrappedProcessor(mockJob, 'token');

      expect(mockLoggerError).toHaveBeenCalled();
    });
  });
});
