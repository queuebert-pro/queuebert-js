import { HttpException, HttpStatus, Logger } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';

import { QueuebertIntegrationRegistry } from './integration-registry';
import {
  createQueuebertController,
  IQueuebertController,
} from './queuebert.controller';
import { QueuebertService } from './queuebert.service';
import {
  QUEUEBERT_OPTIONS,
  QUEUEBERT_QUEUES,
  QUEUEBERT_INTEGRATION_REGISTRY,
  READONLY_OPTIONAL_ENDPOINTS,
  ALL_OPTIONAL_ENDPOINTS,
} from './types';
import type {
  JobBulkRetryResult,
  JobDetailResult,
  JobInfo,
  JobListResult,
  JobRemoveResult,
  JobRetryResult,
  QueuebertModuleOptions,
} from './types';

describe('job inspection', () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  function createJob(overrides: Record<string, unknown> = {}) {
    return {
      id: 'j1',
      name: 'send-email',
      data: { to: 'person@example.com', token: 'secret' },
      opts: { attempts: 3 },
      timestamp: 1_000,
      processedOn: 2_000,
      finishedOn: 3_000,
      attemptsMade: 2,
      failedReason: 'boom',
      stacktrace: ['at thing (file.ts:1:1)'],
      progress: 50,
      returnvalue: { receipt: 'abc' },
      delay: 0,
      getState: jest.fn().mockResolvedValue('failed'),
      retry: jest.fn().mockResolvedValue(undefined),
      remove: jest.fn().mockResolvedValue(undefined),
      ...overrides,
    };
  }

  function createQueue(overrides: Record<string, unknown> = {}) {
    return {
      name: 'emails',
      getWaiting: jest.fn().mockResolvedValue([]),
      getWaitingChildren: jest.fn().mockResolvedValue([]),
      getActive: jest.fn().mockResolvedValue([]),
      getDelayed: jest.fn().mockResolvedValue([]),
      getPrioritized: jest.fn().mockResolvedValue([]),
      getCompleted: jest.fn().mockResolvedValue([]),
      getFailed: jest.fn().mockResolvedValue([createJob()]),
      getJobCountByTypes: jest.fn().mockResolvedValue(7),
      getJob: jest.fn().mockResolvedValue(createJob()),
      ...overrides,
    };
  }

  function createService(options: Partial<QueuebertModuleOptions> = {}) {
    return new QueuebertService(
      { queues: [{ name: 'emails' }], ...options },
      new QueuebertIntegrationRegistry(),
    );
  }

  async function createController(
    options: Partial<QueuebertModuleOptions> = {},
    queue: Record<string, unknown> = createQueue(),
  ): Promise<IQueuebertController> {
    const ControllerClass = createQueuebertController('admin/queue');
    const resolvedOptions: QueuebertModuleOptions = {
      queues: [{ name: 'emails' }],
      ...options,
    };
    const queues = new Map([['emails', { queue: queue as never }]]);

    const module: TestingModule = await Test.createTestingModule({
      controllers: [ControllerClass],
      providers: [
        QueuebertService,
        { provide: QUEUEBERT_OPTIONS, useValue: resolvedOptions },
        {
          provide: QUEUEBERT_INTEGRATION_REGISTRY,
          useValue: new QueuebertIntegrationRegistry(),
        },
        { provide: QUEUEBERT_QUEUES, useValue: () => queues },
      ],
    }).compile();

    return module.get<IQueuebertController>(ControllerClass);
  }

  describe('endpoint gating', () => {
    it("keeps 'jobs' out of the default endpoint set", () => {
      // The readonly list doubles as the default, so anything in it is on for
      // every existing consumer on upgrade.
      expect(READONLY_OPTIONAL_ENDPOINTS).not.toContain('jobs');
      expect(ALL_OPTIONAL_ENDPOINTS).toContain('jobs');
    });

    it('reports canInspectJobs false by default', () => {
      const capabilities = createService().getCapabilities();

      expect(capabilities.canInspectJobs).toBe(false);
      expect(capabilities.canInspectJobData).toBe(false);
      expect(capabilities.endpoints).not.toContain('jobs');
    });

    it('404s both job routes when the endpoint is not enabled', async () => {
      const controller = await createController();

      await expect(controller.listQueueJobs('emails')).rejects.toMatchObject({
        status: HttpStatus.NOT_FOUND,
      });
      await expect(
        controller.getQueueJob('emails', 'j1'),
      ).rejects.toMatchObject({ status: HttpStatus.NOT_FOUND });
    });

    it('reports canInspectJobs once enabled, and data separately', () => {
      expect(
        createService({ endpoints: ['jobs'] }).getCapabilities(),
      ).toMatchObject({ canInspectJobs: true, canInspectJobData: false });

      expect(
        createService({
          endpoints: ['jobs'],
          includeJobData: true,
        }).getCapabilities(),
      ).toMatchObject({ canInspectJobs: true, canInspectJobData: true });
    });

    it('does not report canInspectJobData while the endpoint is off', () => {
      const capabilities = createService({
        includeJobData: true,
      }).getCapabilities();

      expect(capabilities.canInspectJobData).toBe(false);
    });
  });

  describe('listing', () => {
    it('defaults to the failed state and a bounded page', async () => {
      const queue = createQueue();
      const controller = await createController({ endpoints: ['jobs'] }, queue);

      const result = (await controller.listQueueJobs(
        'emails',
      )) as JobListResult;

      expect(queue.getFailed).toHaveBeenCalledWith(0, 49);
      expect(result).toMatchObject({
        queue: 'emails',
        state: 'failed',
        total: 7,
        start: 0,
        end: 49,
      });
      expect(queue.getJobCountByTypes).toHaveBeenCalledWith('failed');
      expect(result.jobs).toHaveLength(1);
    });

    it('maps failure detail onto the job info', async () => {
      const controller = await createController({ endpoints: ['jobs'] });

      const result = (await controller.listQueueJobs(
        'emails',
      )) as JobListResult;

      expect(result.jobs[0]).toMatchObject({
        id: 'j1',
        name: 'send-email',
        state: 'failed',
        timestamp: 1_000,
        processedOn: 2_000,
        finishedOn: 3_000,
        attemptsMade: 2,
        maxAttempts: 3,
        failedReason: 'boom',
        stacktrace: ['at thing (file.ts:1:1)'],
        progress: 50,
      });
    });

    it('defaults maxAttempts to 1 when opts.attempts is unset', async () => {
      const queue = createQueue({
        getFailed: jest.fn().mockResolvedValue([createJob({ opts: {} })]),
      });
      const controller = await createController({ endpoints: ['jobs'] }, queue);

      const result = (await controller.listQueueJobs(
        'emails',
      )) as JobListResult;

      expect(result.jobs[0].maxAttempts).toBe(1);
    });

    it('routes each state to its own BullMQ getter', async () => {
      const queue = createQueue();
      const controller = await createController({ endpoints: ['jobs'] }, queue);

      await controller.listQueueJobs('emails', 'completed');
      await controller.listQueueJobs('emails', 'waiting');
      await controller.listQueueJobs('emails', 'active');
      await controller.listQueueJobs('emails', 'delayed');
      await controller.listQueueJobs('emails', 'prioritized');
      await controller.listQueueJobs('emails', 'waiting-children');

      expect(queue.getCompleted).toHaveBeenCalled();
      expect(queue.getWaiting).toHaveBeenCalled();
      expect(queue.getActive).toHaveBeenCalled();
      expect(queue.getDelayed).toHaveBeenCalled();
      expect(queue.getPrioritized).toHaveBeenCalled();
      expect(queue.getWaitingChildren).toHaveBeenCalled();
    });

    it('flags that a jobType filter applies within the page only', async () => {
      const queue = createQueue({
        getFailed: jest
          .fn()
          .mockResolvedValue([
            createJob({ id: 'a', name: 'send-email' }),
            createJob({ id: 'b', name: 'other' }),
          ]),
      });
      const controller = await createController({ endpoints: ['jobs'] }, queue);

      const result = (await controller.listQueueJobs(
        'emails',
        'failed',
        'send-email',
      )) as JobListResult;

      expect(result.jobs.map((job) => job.id)).toEqual(['a']);
      // total counts the whole state, deliberately ignoring the filter.
      expect(result.total).toBe(7);
      expect(result.jobTypeFilter).toBe('send-email');
    });

    it('omits the filter flag when no jobType is given', async () => {
      const controller = await createController({ endpoints: ['jobs'] });

      const result = (await controller.listQueueJobs(
        'emails',
      )) as JobListResult;

      expect(result).not.toHaveProperty('jobTypeFilter');
    });

    it('passes an explicit window through to BullMQ', async () => {
      const queue = createQueue();
      const controller = await createController({ endpoints: ['jobs'] }, queue);

      await controller.listQueueJobs('emails', 'failed', undefined, '10', '19');

      expect(queue.getFailed).toHaveBeenCalledWith(10, 19);
    });
  });

  describe('request validation', () => {
    it('rejects an unknown state', async () => {
      const controller = await createController({ endpoints: ['jobs'] });

      await expect(
        controller.listQueueJobs('emails', 'nonsense'),
      ).rejects.toMatchObject({ status: HttpStatus.BAD_REQUEST });
    });

    it('rejects a window wider than the page cap', async () => {
      const controller = await createController({ endpoints: ['jobs'] });

      await expect(
        controller.listQueueJobs('emails', 'failed', undefined, '0', '100'),
      ).rejects.toMatchObject({ status: HttpStatus.BAD_REQUEST });
    });

    it('rejects an inverted window', async () => {
      const controller = await createController({ endpoints: ['jobs'] });

      await expect(
        controller.listQueueJobs('emails', 'failed', undefined, '10', '5'),
      ).rejects.toMatchObject({ status: HttpStatus.BAD_REQUEST });
    });

    it('404s an unknown queue', async () => {
      const controller = await createController({ endpoints: ['jobs'] });

      await expect(controller.listQueueJobs('nope')).rejects.toBeInstanceOf(
        HttpException,
      );
    });
  });

  describe('single job', () => {
    it('resolves the current state via getState', async () => {
      const job = createJob({
        getState: jest.fn().mockResolvedValue('active'),
      });
      const queue = createQueue({ getJob: jest.fn().mockResolvedValue(job) });
      const controller = await createController({ endpoints: ['jobs'] }, queue);

      const result = (await controller.getQueueJob(
        'emails',
        'j1',
      )) as JobDetailResult;

      expect(queue.getJob).toHaveBeenCalledWith('j1');
      expect(job.getState).toHaveBeenCalled();
      expect(result.job).toMatchObject({ id: 'j1', state: 'active' });
    });

    it('does not call getState while listing', async () => {
      const job = createJob();
      const queue = createQueue({
        getFailed: jest.fn().mockResolvedValue([job]),
      });
      const controller = await createController({ endpoints: ['jobs'] }, queue);

      await controller.listQueueJobs('emails');

      expect(job.getState).not.toHaveBeenCalled();
    });

    it('404s a job that is gone', async () => {
      const queue = createQueue({
        getJob: jest.fn().mockResolvedValue(undefined),
      });
      const controller = await createController({ endpoints: ['jobs'] }, queue);

      await expect(
        controller.getQueueJob('emails', 'missing'),
      ).rejects.toMatchObject({ status: HttpStatus.NOT_FOUND });
    });
  });

  describe('privacy tiers', () => {
    it('withholds data and returnvalue by default', async () => {
      const controller = await createController({ endpoints: ['jobs'] });

      const result = (await controller.listQueueJobs(
        'emails',
      )) as JobListResult;

      expect(result.jobs[0]).not.toHaveProperty('data');
      expect(result.jobs[0]).not.toHaveProperty('returnvalue');
      // failedReason and stacktrace are the point of the endpoint.
      expect(result.jobs[0].failedReason).toBe('boom');
      expect(result.jobs[0].stacktrace).toEqual(['at thing (file.ts:1:1)']);
    });

    it('includes data and returnvalue when includeJobData is set', async () => {
      const controller = await createController({
        endpoints: ['jobs'],
        includeJobData: true,
      });

      const result = (await controller.listQueueJobs(
        'emails',
      )) as JobListResult;

      expect(result.jobs[0].data).toEqual({
        to: 'person@example.com',
        token: 'secret',
      });
      expect(result.jobs[0].returnvalue).toEqual({ receipt: 'abc' });
    });

    it('honours the deprecated migration-preview alias', async () => {
      const controller = await createController({
        endpoints: ['jobs'],
        includeJobDataInMigrationPreview: true,
      });

      const result = (await controller.listQueueJobs(
        'emails',
      )) as JobListResult;

      expect(result.jobs[0].data).toEqual({
        to: 'person@example.com',
        token: 'secret',
      });
    });

    it('lets includeJobData win over the alias', async () => {
      const controller = await createController({
        endpoints: ['jobs'],
        includeJobData: false,
        includeJobDataInMigrationPreview: true,
      });

      const result = (await controller.listQueueJobs(
        'emails',
      )) as JobListResult;

      expect(result.jobs[0]).not.toHaveProperty('data');
    });

    it('withholds data on the single-job route too', async () => {
      const controller = await createController({ endpoints: ['jobs'] });

      const result = (await controller.getQueueJob(
        'emails',
        'j1',
      )) as JobDetailResult;

      expect(result.job).not.toHaveProperty('data');
    });

    it('runs a configured jobRedaction hook over what remains', async () => {
      const jobRedaction = jest.fn(
        (info: JobInfo): JobInfo => ({
          ...info,
          failedReason: '[scrubbed]',
        }),
      );
      const controller = await createController({
        endpoints: ['jobs'],
        jobRedaction,
      });

      const result = (await controller.listQueueJobs(
        'emails',
      )) as JobListResult;

      expect(jobRedaction).toHaveBeenCalledTimes(1);
      // The hook sees the already-tiered job, not the raw payload.
      expect(jobRedaction.mock.calls[0][0]).not.toHaveProperty('data');
      expect(result.jobs[0].failedReason).toBe('[scrubbed]');
    });

    it('falls back to identity fields when the hook throws', async () => {
      const controller = await createController({
        endpoints: ['jobs'],
        includeJobData: true,
        jobRedaction: () => {
          throw new Error('scrubber exploded');
        },
      });
      const errorSpy = jest
        .spyOn(Logger.prototype, 'error')
        .mockImplementation(() => undefined);

      const result = (await controller.listQueueJobs(
        'emails',
      )) as JobListResult;

      expect(result.jobs[0]).toEqual({
        id: 'j1',
        name: 'send-email',
        state: 'failed',
      });
      expect(errorSpy).toHaveBeenCalled();
    });

    it('applies the same tiers to migration preview samples', async () => {
      const service = createService({ includeJobData: true });
      const sourceQueue = createQueue({
        getWaiting: jest
          .fn()
          .mockResolvedValue([createJob({ id: 'w1', name: 'welcome' })]),
        isPaused: jest.fn().mockResolvedValue(false),
      });
      const targetQueue = createQueue({ name: 'emails-new' });

      const preview = await service.previewMigration(
        sourceQueue as never,
        targetQueue as never,
        {
          sourceQueue: 'emails',
          targetQueue: 'emails-new',
          states: ['waiting'],
        },
        'default',
        'target',
      );

      expect(preview.sampleJobs[0]).toMatchObject({ id: 'w1' });
      expect(preview.sampleJobs[0].data).toEqual({
        to: 'person@example.com',
        token: 'secret',
      });
    });
  });

  describe('shared read path', () => {
    it('reuses listJobs for migration reads without limiting by default', async () => {
      const service = createService();
      const queue = createQueue();

      const jobs = await service.listJobs(queue as never, { state: 'failed' });

      expect(queue.getFailed).toHaveBeenCalledWith(0, -1);
      expect(jobs[0].data).toEqual({
        to: 'person@example.com',
        token: 'secret',
      });
    });

    it('returns raw data from listJobs so migration is unaffected by tiers', async () => {
      const service = createService({ includeJobData: false });
      const queue = createQueue();

      const jobs = await service.listJobs(queue as never, { state: 'failed' });

      // listJobs is the internal reader; tiers are applied on the way out.
      expect(jobs[0]).toHaveProperty('data');
    });
  });

  describe('retry and remove', () => {
    it('keeps both mutating endpoints out of the default set', () => {
      expect(READONLY_OPTIONAL_ENDPOINTS).not.toContain('retry');
      expect(READONLY_OPTIONAL_ENDPOINTS).not.toContain('remove');
      expect(ALL_OPTIONAL_ENDPOINTS).toContain('retry');
      expect(ALL_OPTIONAL_ENDPOINTS).toContain('remove');
    });

    it('reports the control capabilities independently', () => {
      expect(createService().getCapabilities()).toMatchObject({
        canRetryJobs: false,
        canRemoveJobs: false,
      });
      expect(
        createService({ endpoints: ['retry'] }).getCapabilities(),
      ).toMatchObject({ canRetryJobs: true, canRemoveJobs: false });
      expect(
        createService({ endpoints: ['remove'] }).getCapabilities(),
      ).toMatchObject({ canRetryJobs: false, canRemoveJobs: true });
    });

    it('404s the control routes when not enabled', async () => {
      // Enabling read-only inspection must not imply the mutating routes.
      const controller = await createController({ endpoints: ['jobs'] });

      await expect(
        controller.retryQueueJob('emails', 'j1'),
      ).rejects.toMatchObject({ status: HttpStatus.NOT_FOUND });
      await expect(controller.retryQueueJobs('emails')).rejects.toMatchObject({
        status: HttpStatus.NOT_FOUND,
      });
      await expect(
        controller.removeQueueJob('emails', 'j1'),
      ).rejects.toMatchObject({ status: HttpStatus.NOT_FOUND });
    });

    describe('single retry', () => {
      it('retries a failed job and preserves its attempt count', async () => {
        const job = createJob();
        const queue = createQueue({ getJob: jest.fn().mockResolvedValue(job) });
        const controller = await createController(
          { endpoints: ['retry'] },
          queue,
        );

        const result = (await controller.retryQueueJob(
          'emails',
          'j1',
        )) as JobRetryResult;

        expect(job.retry).toHaveBeenCalledWith('failed', {});
        expect(result).toMatchObject({
          queue: 'emails',
          jobId: 'j1',
          name: 'send-email',
          retriedFrom: 'failed',
          attemptsMade: 2,
          resetAttempts: false,
        });
      });

      it('resets both attempt counters when asked', async () => {
        const job = createJob();
        const queue = createQueue({ getJob: jest.fn().mockResolvedValue(job) });
        const controller = await createController(
          { endpoints: ['retry'] },
          queue,
        );

        const result = (await controller.retryQueueJob(
          'emails',
          'j1',
          'failed',
          'true',
        )) as JobRetryResult;

        expect(job.retry).toHaveBeenCalledWith('failed', {
          resetAttemptsMade: true,
          resetAttemptsStarted: true,
        });
        expect(result).toMatchObject({ resetAttempts: true, attemptsMade: 0 });
      });

      it('retries a completed job when that state is requested', async () => {
        const job = createJob({
          getState: jest.fn().mockResolvedValue('completed'),
        });
        const queue = createQueue({ getJob: jest.fn().mockResolvedValue(job) });
        const controller = await createController(
          { endpoints: ['retry'] },
          queue,
        );

        const result = (await controller.retryQueueJob(
          'emails',
          'j1',
          'completed',
        )) as JobRetryResult;

        expect(job.retry).toHaveBeenCalledWith('completed', {});
        expect(result.retriedFrom).toBe('completed');
      });

      it('409s with a usable hint when the job is in the other finished state', async () => {
        const job = createJob({
          getState: jest.fn().mockResolvedValue('completed'),
        });
        const queue = createQueue({ getJob: jest.fn().mockResolvedValue(job) });
        const controller = await createController(
          { endpoints: ['retry'] },
          queue,
        );

        await expect(
          controller.retryQueueJob('emails', 'j1'),
        ).rejects.toMatchObject({
          status: HttpStatus.CONFLICT,
          message: expect.stringContaining('state=completed'),
        });
        expect(job.retry).not.toHaveBeenCalled();
      });

      it('409s for a job that has not finished', async () => {
        const job = createJob({
          getState: jest.fn().mockResolvedValue('active'),
        });
        const queue = createQueue({ getJob: jest.fn().mockResolvedValue(job) });
        const controller = await createController(
          { endpoints: ['retry'] },
          queue,
        );

        await expect(
          controller.retryQueueJob('emails', 'j1'),
        ).rejects.toMatchObject({
          status: HttpStatus.CONFLICT,
          message: expect.stringContaining('cannot be retried'),
        });
        expect(job.retry).not.toHaveBeenCalled();
      });

      it('404s a job that is gone', async () => {
        const queue = createQueue({
          getJob: jest.fn().mockResolvedValue(undefined),
        });
        const controller = await createController(
          { endpoints: ['retry'] },
          queue,
        );

        await expect(
          controller.retryQueueJob('emails', 'nope'),
        ).rejects.toMatchObject({ status: HttpStatus.NOT_FOUND });
      });

      it('rejects an unsupported retry state', async () => {
        const controller = await createController({ endpoints: ['retry'] });

        await expect(
          controller.retryQueueJob('emails', 'j1', 'waiting'),
        ).rejects.toMatchObject({ status: HttpStatus.BAD_REQUEST });
      });

      it('rejects a non-boolean resetAttempts', async () => {
        const controller = await createController({ endpoints: ['retry'] });

        await expect(
          controller.retryQueueJob('emails', 'j1', 'failed', 'yes'),
        ).rejects.toMatchObject({ status: HttpStatus.BAD_REQUEST });
      });

      it('maps a lost race with another retry onto 409', async () => {
        const job = createJob({
          retry: jest
            .fn()
            .mockRejectedValue(
              new Error('Job j1 is not in the failed state. reprocessJob'),
            ),
        });
        const queue = createQueue({ getJob: jest.fn().mockResolvedValue(job) });
        const controller = await createController(
          { endpoints: ['retry'] },
          queue,
        );

        await expect(
          controller.retryQueueJob('emails', 'j1'),
        ).rejects.toMatchObject({ status: HttpStatus.CONFLICT });
      });

      it('does not disguise an unexpected error as a conflict', async () => {
        const job = createJob({
          retry: jest.fn().mockRejectedValue(new Error('ECONNRESET')),
        });
        const queue = createQueue({ getJob: jest.fn().mockResolvedValue(job) });
        const controller = await createController(
          { endpoints: ['retry'] },
          queue,
        );

        const error = await controller
          .retryQueueJob('emails', 'j1')
          .catch((err: unknown) => err);

        // A 409 built from this error would carry the same message, so the
        // assertion has to be that it was not mapped at all.
        expect(error).not.toBeInstanceOf(HttpException);
        expect((error as Error).message).toBe('ECONNRESET');
      });
    });

    describe('bulk retry', () => {
      it('retries the bounded page and reports counts', async () => {
        const jobs = [createJob({ id: 'a' }), createJob({ id: 'b' })];
        const queue = createQueue({
          getFailed: jest.fn().mockResolvedValue(jobs),
        });
        const controller = await createController(
          { endpoints: ['retry'] },
          queue,
        );

        const result = (await controller.retryQueueJobs(
          'emails',
        )) as JobBulkRetryResult;

        expect(queue.getFailed).toHaveBeenCalledWith(0, 49);
        expect(jobs[0].retry).toHaveBeenCalledWith('failed');
        expect(jobs[1].retry).toHaveBeenCalledWith('failed');
        expect(result).toMatchObject({
          queue: 'emails',
          state: 'failed',
          limit: 50,
          examined: 2,
          retried: 2,
          failed: 0,
          failures: [],
        });
      });

      it('treats limit as a real cap on jobs read', async () => {
        const queue = createQueue();
        const controller = await createController(
          { endpoints: ['retry'] },
          queue,
        );

        const result = (await controller.retryQueueJobs(
          'emails',
          'failed',
          undefined,
          '10',
        )) as JobBulkRetryResult;

        // A cap, unlike BullMQ's retryJobs({ count }) batch size.
        expect(queue.getFailed).toHaveBeenCalledWith(0, 9);
        expect(result.limit).toBe(10);
      });

      it('rejects a limit above the cap', async () => {
        const controller = await createController({ endpoints: ['retry'] });

        await expect(
          controller.retryQueueJobs('emails', 'failed', undefined, '1001'),
        ).rejects.toMatchObject({ status: HttpStatus.BAD_REQUEST });
      });

      it('filters by jobType and flags that it applies within the page', async () => {
        const wanted = createJob({ id: 'a', name: 'sync' });
        const other = createJob({ id: 'b', name: 'other' });
        const queue = createQueue({
          getFailed: jest.fn().mockResolvedValue([wanted, other]),
        });
        const controller = await createController(
          { endpoints: ['retry'] },
          queue,
        );

        const result = (await controller.retryQueueJobs(
          'emails',
          'failed',
          'sync',
        )) as JobBulkRetryResult;

        expect(wanted.retry).toHaveBeenCalled();
        expect(other.retry).not.toHaveBeenCalled();
        expect(result).toMatchObject({
          examined: 1,
          retried: 1,
          jobTypeFilter: 'sync',
        });
      });

      it('keeps going past a failure and reports it', async () => {
        const good = createJob({ id: 'a' });
        const bad = createJob({
          id: 'b',
          retry: jest.fn().mockRejectedValue(new Error('locked')),
        });
        const queue = createQueue({
          getFailed: jest.fn().mockResolvedValue([bad, good]),
        });
        const controller = await createController(
          { endpoints: ['retry'] },
          queue,
        );
        jest
          .spyOn(Logger.prototype, 'warn')
          .mockImplementation(() => undefined);

        const result = (await controller.retryQueueJobs(
          'emails',
        )) as JobBulkRetryResult;

        expect(good.retry).toHaveBeenCalled();
        expect(result).toMatchObject({
          examined: 2,
          retried: 1,
          failed: 1,
        });
        expect(result.failures).toEqual([
          { jobId: 'b', name: 'send-email', message: 'locked' },
        ]);
      });

      it('caps how many failure details it returns', async () => {
        const service = createService({ endpoints: ['retry'] });
        const failing = Array.from({ length: 5 }, (_, i) =>
          createJob({
            id: `j${i}`,
            retry: jest.fn().mockRejectedValue(new Error('locked')),
          }),
        );
        const queue = createQueue({
          getFailed: jest.fn().mockResolvedValue(failing),
        });
        jest
          .spyOn(Logger.prototype, 'warn')
          .mockImplementation(() => undefined);

        const result = await service.retryJobsByState(
          queue as never,
          'emails',
          { limit: 10, maxReportedFailures: 2 },
        );

        expect(result.failed).toBe(5);
        expect(result.failures).toHaveLength(2);
      });

      it('retries completed jobs when that state is requested', async () => {
        const job = createJob({ id: 'c1' });
        const queue = createQueue({
          getCompleted: jest.fn().mockResolvedValue([job]),
        });
        const controller = await createController(
          { endpoints: ['retry'] },
          queue,
        );

        const result = (await controller.retryQueueJobs(
          'emails',
          'completed',
        )) as JobBulkRetryResult;

        expect(queue.getCompleted).toHaveBeenCalledWith(0, 49);
        expect(job.retry).toHaveBeenCalledWith('completed');
        expect(result.state).toBe('completed');
      });
    });

    describe('remove', () => {
      it('removes a job and reports the state it was in', async () => {
        const job = createJob();
        const queue = createQueue({ getJob: jest.fn().mockResolvedValue(job) });
        const controller = await createController(
          { endpoints: ['remove'] },
          queue,
        );

        const result = (await controller.removeQueueJob(
          'emails',
          'j1',
        )) as JobRemoveResult;

        expect(job.remove).toHaveBeenCalled();
        expect(result).toMatchObject({
          queue: 'emails',
          jobId: 'j1',
          name: 'send-email',
          removedFrom: 'failed',
        });
      });

      it('attempts removal of an active job rather than pre-rejecting it', async () => {
        // BullMQ can remove an active job that no worker holds a lock on.
        const job = createJob({
          getState: jest.fn().mockResolvedValue('active'),
        });
        const queue = createQueue({ getJob: jest.fn().mockResolvedValue(job) });
        const controller = await createController(
          { endpoints: ['remove'] },
          queue,
        );

        const result = (await controller.removeQueueJob(
          'emails',
          'j1',
        )) as JobRemoveResult;

        expect(job.remove).toHaveBeenCalled();
        expect(result.removedFrom).toBe('active');
      });

      it('409s when the job is locked by a worker', async () => {
        const job = createJob({
          getState: jest.fn().mockResolvedValue('active'),
          remove: jest
            .fn()
            .mockRejectedValue(
              new Error(
                'Job j1 could not be removed because it is locked by another worker',
              ),
            ),
        });
        const queue = createQueue({ getJob: jest.fn().mockResolvedValue(job) });
        const controller = await createController(
          { endpoints: ['remove'] },
          queue,
        );

        await expect(
          controller.removeQueueJob('emails', 'j1'),
        ).rejects.toMatchObject({
          status: HttpStatus.CONFLICT,
          message: expect.stringContaining('locked by another worker'),
        });
      });

      it('409s when the job belongs to a job scheduler', async () => {
        const job = createJob({
          remove: jest
            .fn()
            .mockRejectedValue(
              new Error(
                'Job j1 belongs to a job scheduler and cannot be removed directly. removeJob',
              ),
            ),
        });
        const queue = createQueue({ getJob: jest.fn().mockResolvedValue(job) });
        const controller = await createController(
          { endpoints: ['remove'] },
          queue,
        );

        await expect(
          controller.removeQueueJob('emails', 'j1'),
        ).rejects.toMatchObject({ status: HttpStatus.CONFLICT });
      });

      it('404s a job that is gone', async () => {
        const queue = createQueue({
          getJob: jest.fn().mockResolvedValue(undefined),
        });
        const controller = await createController(
          { endpoints: ['remove'] },
          queue,
        );

        await expect(
          controller.removeQueueJob('emails', 'nope'),
        ).rejects.toMatchObject({ status: HttpStatus.NOT_FOUND });
      });

      it('does not disguise an unexpected error as a conflict', async () => {
        const job = createJob({
          remove: jest.fn().mockRejectedValue(new Error('ECONNRESET')),
        });
        const queue = createQueue({ getJob: jest.fn().mockResolvedValue(job) });
        const controller = await createController(
          { endpoints: ['remove'] },
          queue,
        );

        const error = await controller
          .removeQueueJob('emails', 'j1')
          .catch((err: unknown) => err);

        expect(error).not.toBeInstanceOf(HttpException);
        expect((error as Error).message).toBe('ECONNRESET');
      });
    });
  });

  describe('lastFailure on queue stats', () => {
    function statsQueue(overrides: Record<string, unknown> = {}) {
      return createQueue({
        isPaused: jest.fn().mockResolvedValue(false),
        getWaitingCount: jest.fn().mockResolvedValue(0),
        getActiveCount: jest.fn().mockResolvedValue(0),
        getCompletedCount: jest.fn().mockResolvedValue(0),
        getFailedCount: jest.fn().mockResolvedValue(3),
        getDelayedCount: jest.fn().mockResolvedValue(0),
        ...overrides,
      });
    }

    it('is omitted entirely when the jobs endpoint is off', async () => {
      // 'stats' cannot be disabled, so a failure reason must not ride along on
      // it without the same opt-in the inspection endpoint requires.
      const service = createService();
      const queue = statsQueue();

      const stats = await service.getSingleQueueStats(queue as never);

      expect(stats).not.toHaveProperty('lastFailure');
      expect(queue.getFailed).not.toHaveBeenCalled();
    });

    it('reports the newest failure when the endpoint is on', async () => {
      const service = createService({ endpoints: ['jobs'] });
      const queue = statsQueue();

      const stats = await service.getSingleQueueStats(queue as never);

      // getFailed returns the failed set newest-first, so index 0 is latest.
      expect(queue.getFailed).toHaveBeenCalledWith(0, 0);
      expect(stats.lastFailure).toEqual({
        jobId: 'j1',
        name: 'send-email',
        failedReason: 'boom',
        finishedOn: 3_000,
      });
    });

    it('costs nothing on a queue with no failures', async () => {
      const service = createService({ endpoints: ['jobs'] });
      const queue = statsQueue({
        getFailedCount: jest.fn().mockResolvedValue(0),
      });

      const stats = await service.getSingleQueueStats(queue as never);

      expect(stats.lastFailure).toBeNull();
      expect(queue.getFailed).not.toHaveBeenCalled();
    });

    it('never withholds job data through lastFailure', async () => {
      const service = createService({ endpoints: ['jobs'] });
      const queue = statsQueue();

      const stats = await service.getSingleQueueStats(queue as never);

      expect(stats.lastFailure).not.toHaveProperty('data');
    });

    it('runs the jobRedaction hook over it', async () => {
      const service = createService({
        endpoints: ['jobs'],
        jobRedaction: (info) => ({ ...info, failedReason: '[scrubbed]' }),
      });
      const queue = statsQueue();

      const stats = await service.getSingleQueueStats(queue as never);

      expect(stats.lastFailure?.failedReason).toBe('[scrubbed]');
    });

    it('does not fail the whole stats response when the read throws', async () => {
      const service = createService({ endpoints: ['jobs'] });
      const queue = statsQueue({
        getFailed: jest.fn().mockRejectedValue(new Error('redis down')),
      });
      jest.spyOn(Logger.prototype, 'warn').mockImplementation(() => undefined);

      const stats = await service.getSingleQueueStats(queue as never);

      expect(stats.counts.failed).toBe(3);
      expect(stats.lastFailure).toBeNull();
    });
  });

  describe('workers on queue stats', () => {
    function presenceQueue(
      redis: Record<string, jest.Mock>,
      overrides: Record<string, unknown> = {},
    ) {
      return createQueue({
        isPaused: jest.fn().mockResolvedValue(false),
        getWaitingCount: jest.fn().mockResolvedValue(0),
        getActiveCount: jest.fn().mockResolvedValue(0),
        getCompletedCount: jest.fn().mockResolvedValue(0),
        getFailedCount: jest.fn().mockResolvedValue(0),
        getDelayedCount: jest.fn().mockResolvedValue(0),
        client: Promise.resolve(redis),
        opts: { prefix: 'bull' },
        ...overrides,
      });
    }

    function presenceRedis(
      workers: Record<string, string>,
      lastStop: string | null,
    ) {
      return {
        hgetall: jest.fn().mockResolvedValue(workers),
        get: jest.fn().mockResolvedValue(lastStop),
        hset: jest.fn(),
        hdel: jest.fn().mockResolvedValue(0),
        set: jest.fn().mockResolvedValue('OK'),
        multi: jest.fn(),
      };
    }

    it('is omitted when the queue has no presence', async () => {
      const service = createService();
      const queue = presenceQueue(presenceRedis({}, null));

      const stats = await service.getSingleQueueStats(queue as never);

      expect(stats).not.toHaveProperty('workers');
    });

    it('is omitted when the queue has no usable client', async () => {
      const service = createService();
      const queue = presenceQueue({}, { client: undefined });

      const stats = await service.getSingleQueueStats(queue as never);

      expect(stats).not.toHaveProperty('workers');
    });

    it('reports the live count and the last stop', async () => {
      const service = createService();
      const stop = {
        workerId: 'api-1:42:abc',
        host: 'api-1',
        reason: 'lost_connection',
        description: 'Lost connection to Redis',
        at: '2026-09-19T12:00:00.000Z',
      };
      const live = JSON.stringify({
        host: 'api-2',
        pid: 7,
        startedAt: new Date().toISOString(),
        heartbeatAt: new Date().toISOString(),
      });
      const redis = presenceRedis(
        { 'api-2:7:xyz': live },
        JSON.stringify(stop),
      );
      const queue = presenceQueue(redis);

      const stats = await service.getSingleQueueStats(queue as never);

      expect(stats.workers).toEqual({ count: 1, lastStop: stop });
      expect(redis.hgetall).toHaveBeenCalledWith('bull:emails:qb:workers');
    });

    it('leaves stats without the field when the read fails', async () => {
      const service = createService();
      const redis = presenceRedis({}, null);
      redis.hgetall.mockRejectedValue(new Error('redis down'));
      jest.spyOn(Logger.prototype, 'warn').mockImplementation(() => undefined);
      const queue = presenceQueue(redis);

      const stats = await service.getSingleQueueStats(queue as never);

      expect(stats).not.toHaveProperty('workers');
      expect(stats.counts.waiting).toBe(0);
    });
  });
});
