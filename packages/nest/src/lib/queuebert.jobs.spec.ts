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
  JobDetailResult,
  JobInfo,
  JobListResult,
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
});
