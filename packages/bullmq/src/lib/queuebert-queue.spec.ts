import { Queue } from 'bullmq';

import { QueuebertQueue } from './queuebert-queue';

// Mock bullmq Queue
jest.mock('bullmq', () => {
  const mockQueue = {
    add: jest.fn().mockResolvedValue({ id: 'job-1' }),
    addBulk: jest.fn().mockResolvedValue([{ id: 'job-1' }, { id: 'job-2' }]),
    getJob: jest
      .fn()
      .mockResolvedValue({ id: 'job-1', name: 'test', data: {} }),
    getJobCounts: jest.fn().mockResolvedValue({
      waiting: 10,
      active: 5,
      completed: 100,
      failed: 2,
      delayed: 3,
      paused: 0,
      prioritized: 1,
    }),
    getWaiting: jest.fn().mockResolvedValue([]),
    getActive: jest.fn().mockResolvedValue([]),
    getCompleted: jest.fn().mockResolvedValue([]),
    getFailed: jest.fn().mockResolvedValue([]),
    getDelayed: jest.fn().mockResolvedValue([]),
    clean: jest.fn().mockResolvedValue(['job-1', 'job-2']),
    drain: jest.fn().mockResolvedValue(undefined),
    pause: jest.fn().mockResolvedValue(undefined),
    resume: jest.fn().mockResolvedValue(undefined),
    isPaused: jest.fn().mockResolvedValue(false),
    obliterate: jest.fn().mockResolvedValue(undefined),
    close: jest.fn().mockResolvedValue(undefined),
  };
  return {
    Queue: jest.fn().mockImplementation(() => mockQueue),
    Job: jest.fn(),
  };
});

describe('QueuebertQueue', () => {
  let queue: QueuebertQueue;
  let mockBullMQQueue: any;

  beforeEach(() => {
    jest.clearAllMocks();

    queue = new QueuebertQueue('test-queue', {
      connection: { host: 'localhost', port: 6379 },
      tags: { env: 'test', app: 'myapp' },
      defaultJobOptions: {
        attempts: 3,
        backoff: { type: 'exponential', delay: 1000 },
      },
    });

    // Get the mock queue instance
    mockBullMQQueue = (Queue as unknown as jest.Mock).mock.results[0].value;
  });

  describe('constructor', () => {
    it('should create a queue with the given name', () => {
      expect(queue.name).toBe('test-queue');
    });

    it('should create the underlying BullMQ Queue', () => {
      expect(Queue).toHaveBeenCalledWith(
        'test-queue',
        expect.objectContaining({
          connection: { host: 'localhost', port: 6379 },
        }),
      );
    });

    it('should not pass defaultJobOptions to BullMQ Queue', () => {
      const queueCall = (Queue as unknown as jest.Mock).mock.calls[0];
      expect(queueCall[1]).not.toHaveProperty('defaultJobOptions');
    });

    it('should not pass tags to BullMQ Queue', () => {
      const queueCall = (Queue as unknown as jest.Mock).mock.calls[0];
      expect(queueCall[1]).not.toHaveProperty('tags');
    });

    it('should handle minimal options', () => {
      const minimalQueue = new QueuebertQueue('minimal-queue', {
        connection: { host: 'localhost', port: 6379 },
      });

      expect(minimalQueue.name).toBe('minimal-queue');
      expect(minimalQueue.getTags()).toEqual({});
      expect(minimalQueue.getDefaultJobOptions()).toBeUndefined();
    });
  });

  describe('getters', () => {
    it('should return the queue name', () => {
      expect(queue.name).toBe('test-queue');
    });

    it('should return the underlying queue', () => {
      expect(queue.queue).toBe(mockBullMQQueue);
    });
  });

  describe('getTags', () => {
    it('should return a copy of the tags', () => {
      const tags = queue.getTags();

      expect(tags).toEqual({ env: 'test', app: 'myapp' });
    });

    it('should not allow modifying internal tags', () => {
      const tags = queue.getTags();
      tags['newKey'] = 'newValue';

      expect(queue.getTags()).not.toHaveProperty('newKey');
    });
  });

  describe('getDefaultJobOptions', () => {
    it('should return a copy of the default job options', () => {
      const opts = queue.getDefaultJobOptions();

      expect(opts).toEqual({
        attempts: 3,
        backoff: { type: 'exponential', delay: 1000 },
      });
    });

    it('should return undefined if no default options set', () => {
      const noDefaultsQueue = new QueuebertQueue('no-defaults', {
        connection: { host: 'localhost', port: 6379 },
      });

      expect(noDefaultsQueue.getDefaultJobOptions()).toBeUndefined();
    });

    it('should not allow modifying internal options', () => {
      const opts = queue.getDefaultJobOptions();
      if (opts) {
        opts.attempts = 10;
      }

      expect(queue.getDefaultJobOptions()?.attempts).toBe(3);
    });
  });

  describe('add', () => {
    it('should add a job to the queue', async () => {
      const result = await queue.add('test-job', { foo: 'bar' });

      expect(mockBullMQQueue.add).toHaveBeenCalledWith(
        'test-job',
        { foo: 'bar' },
        expect.objectContaining({
          attempts: 3,
          backoff: { type: 'exponential', delay: 1000 },
        }),
      );
      expect(result.jobId).toBe('job-1');
      expect(result.jobName).toBe('test-job');
      expect(result.queueName).toBe('test-queue');
      expect(result.data).toEqual({ foo: 'bar' });
    });

    it('should merge custom options with defaults', async () => {
      await queue.add('test-job', { foo: 'bar' }, { attempts: 5, delay: 1000 });

      expect(mockBullMQQueue.add).toHaveBeenCalledWith(
        'test-job',
        { foo: 'bar' },
        expect.objectContaining({
          attempts: 5,
          delay: 1000,
          backoff: { type: 'exponential', delay: 1000 },
        }),
      );
    });

    it('should return dispatch result with timestamp', async () => {
      const before = new Date();
      const result = await queue.add('test-job', { foo: 'bar' });
      const after = new Date();

      expect(result.dispatchedAt.getTime()).toBeGreaterThanOrEqual(
        before.getTime(),
      );
      expect(result.dispatchedAt.getTime()).toBeLessThanOrEqual(
        after.getTime(),
      );
    });

    it('should include options in dispatch result', async () => {
      const result = await queue.add(
        'test-job',
        { foo: 'bar' },
        { priority: 1 },
      );

      expect(result.options).toEqual(
        expect.objectContaining({
          priority: 1,
          attempts: 3,
        }),
      );
    });

    it('should handle job with empty id', async () => {
      mockBullMQQueue.add.mockResolvedValueOnce({ id: undefined });

      const result = await queue.add('test-job', { foo: 'bar' });

      expect(result.jobId).toBe('');
    });
  });

  describe('addBulk', () => {
    it('should add multiple jobs to the queue', async () => {
      const jobs = [
        { name: 'job-a', data: { value: 1 } },
        { name: 'job-b', data: { value: 2 } },
      ];

      const result = await queue.addBulk(jobs);

      expect(mockBullMQQueue.addBulk).toHaveBeenCalledWith([
        {
          name: 'job-a',
          data: { value: 1 },
          opts: expect.objectContaining({ attempts: 3 }),
        },
        {
          name: 'job-b',
          data: { value: 2 },
          opts: expect.objectContaining({ attempts: 3 }),
        },
      ]);
      expect(result.totalDispatched).toBe(2);
      expect(result.jobs).toHaveLength(2);
      expect(result.queueName).toBe('test-queue');
    });

    it('should merge job-specific options with defaults', async () => {
      const jobs = [
        { name: 'job-a', data: { value: 1 }, opts: { priority: 1 } },
        { name: 'job-b', data: { value: 2 }, opts: { delay: 5000 } },
      ];

      await queue.addBulk(jobs);

      expect(mockBullMQQueue.addBulk).toHaveBeenCalledWith([
        {
          name: 'job-a',
          data: { value: 1 },
          opts: expect.objectContaining({ priority: 1, attempts: 3 }),
        },
        {
          name: 'job-b',
          data: { value: 2 },
          opts: expect.objectContaining({ delay: 5000, attempts: 3 }),
        },
      ]);
    });

    it('should return correct job results', async () => {
      const jobs = [
        { name: 'job-a', data: { value: 1 } },
        { name: 'job-b', data: { value: 2 } },
      ];

      const result = await queue.addBulk(jobs);

      expect(result.jobs[0].jobName).toBe('job-a');
      expect(result.jobs[0].data).toEqual({ value: 1 });
      expect(result.jobs[1].jobName).toBe('job-b');
      expect(result.jobs[1].data).toEqual({ value: 2 });
    });

    it('should include dispatch timestamp', async () => {
      const before = new Date();
      const result = await queue.addBulk([
        { name: 'test-1', data: {} },
        { name: 'test-2', data: {} },
      ]);
      const after = new Date();

      expect(result.dispatchedAt.getTime()).toBeGreaterThanOrEqual(
        before.getTime(),
      );
      expect(result.dispatchedAt.getTime()).toBeLessThanOrEqual(
        after.getTime(),
      );
    });
  });

  describe('getJob', () => {
    it('should get a job by ID', async () => {
      const job = await queue.getJob('job-1');

      expect(mockBullMQQueue.getJob).toHaveBeenCalledWith('job-1');
      expect(job?.id).toBe('job-1');
    });

    it('should return undefined for non-existent job', async () => {
      mockBullMQQueue.getJob.mockResolvedValueOnce(undefined);

      const job = await queue.getJob('non-existent');

      expect(job).toBeUndefined();
    });
  });

  describe('getJobs', () => {
    it('should get multiple jobs by IDs', async () => {
      mockBullMQQueue.getJob
        .mockResolvedValueOnce({ id: 'job-1' })
        .mockResolvedValueOnce({ id: 'job-2' })
        .mockResolvedValueOnce(undefined);

      const jobs = await queue.getJobs(['job-1', 'job-2', 'job-3']);

      expect(jobs).toHaveLength(3);
      expect(jobs[0]?.id).toBe('job-1');
      expect(jobs[1]?.id).toBe('job-2');
      expect(jobs[2]).toBeUndefined();
    });
  });

  describe('getJobCounts', () => {
    it('should return job counts', async () => {
      const counts = await queue.getJobCounts();

      expect(mockBullMQQueue.getJobCounts).toHaveBeenCalled();
      expect(counts).toEqual({
        waiting: 10,
        active: 5,
        completed: 100,
        failed: 2,
        delayed: 3,
        paused: 0,
        prioritized: 1,
      });
    });
  });

  describe('getWaiting', () => {
    it('should get waiting jobs', async () => {
      await queue.getWaiting();

      expect(mockBullMQQueue.getWaiting).toHaveBeenCalledWith(0, -1);
    });

    it('should accept start and end parameters', async () => {
      await queue.getWaiting(5, 10);

      expect(mockBullMQQueue.getWaiting).toHaveBeenCalledWith(5, 10);
    });
  });

  describe('getActive', () => {
    it('should get active jobs', async () => {
      await queue.getActive();

      expect(mockBullMQQueue.getActive).toHaveBeenCalledWith(0, -1);
    });
  });

  describe('getCompleted', () => {
    it('should get completed jobs', async () => {
      await queue.getCompleted();

      expect(mockBullMQQueue.getCompleted).toHaveBeenCalledWith(0, -1);
    });
  });

  describe('getFailed', () => {
    it('should get failed jobs', async () => {
      await queue.getFailed();

      expect(mockBullMQQueue.getFailed).toHaveBeenCalledWith(0, -1);
    });
  });

  describe('getDelayed', () => {
    it('should get delayed jobs', async () => {
      await queue.getDelayed();

      expect(mockBullMQQueue.getDelayed).toHaveBeenCalledWith(0, -1);
    });
  });

  describe('clean', () => {
    it('should clean completed jobs by default', async () => {
      const cleaned = await queue.clean(60000, 100);

      expect(mockBullMQQueue.clean).toHaveBeenCalledWith(
        60000,
        100,
        'completed',
      );
      expect(cleaned).toEqual(['job-1', 'job-2']);
    });

    it('should clean jobs of specified type', async () => {
      await queue.clean(60000, 100, 'failed');

      expect(mockBullMQQueue.clean).toHaveBeenCalledWith(60000, 100, 'failed');
    });
  });

  describe('drain', () => {
    it('should drain waiting jobs', async () => {
      await queue.drain();

      expect(mockBullMQQueue.drain).toHaveBeenCalledWith(false);
    });

    it('should drain delayed jobs when specified', async () => {
      await queue.drain(true);

      expect(mockBullMQQueue.drain).toHaveBeenCalledWith(true);
    });
  });

  describe('pause', () => {
    it('should pause the queue', async () => {
      await queue.pause();

      expect(mockBullMQQueue.pause).toHaveBeenCalled();
    });
  });

  describe('resume', () => {
    it('should resume the queue', async () => {
      await queue.resume();

      expect(mockBullMQQueue.resume).toHaveBeenCalled();
    });
  });

  describe('isPaused', () => {
    it('should return paused state', async () => {
      const result = await queue.isPaused();

      expect(mockBullMQQueue.isPaused).toHaveBeenCalled();
      expect(result).toBe(false);
    });

    it('should return true when paused', async () => {
      mockBullMQQueue.isPaused.mockResolvedValueOnce(true);

      const result = await queue.isPaused();

      expect(result).toBe(true);
    });
  });

  describe('obliterate', () => {
    it('should obliterate the queue', async () => {
      await queue.obliterate();

      expect(mockBullMQQueue.obliterate).toHaveBeenCalledWith(undefined);
    });

    it('should pass force option', async () => {
      await queue.obliterate({ force: true });

      expect(mockBullMQQueue.obliterate).toHaveBeenCalledWith({ force: true });
    });
  });

  describe('close', () => {
    it('should close the queue', async () => {
      await queue.close();

      expect(mockBullMQQueue.close).toHaveBeenCalled();
    });
  });
});
