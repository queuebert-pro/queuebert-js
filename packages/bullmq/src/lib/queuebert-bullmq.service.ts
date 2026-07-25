import {
  Injectable,
  Inject,
  OnModuleDestroy,
  OnModuleInit,
  Optional,
  Logger,
} from '@nestjs/common';
import { Processor } from 'bullmq';

import {
  QUEUEBERT_INTEGRATION_REGISTRY,
  QueuebertIntegrationRegistry,
} from '@queuebert/nest';

import {
  QueueAlreadyExistsError,
  WorkerAlreadyExistsError,
  InvalidOptionsError,
} from './errors';
import { QueuebertQueue } from './queuebert-queue';
import { QueuebertWorker } from './queuebert-worker';
import { GlobalStatsCollector } from './stats-collector';
import type {
  QueuebertBullMQModuleOptions,
  QueuebertQueueOptions,
  QueuebertWorkerOptions,
  QueuebertWorkerInterface,
  StatsSnapshot,
} from './types';
import { QUEUEBERT_BULLMQ_OPTIONS, QUEUEBERT_STATS_COLLECTOR } from './types';

const QUEUEBERT_BULLMQ_VERSION = (
  require('../../package.json') as { version: string }
).version;

/**
 * Service for creating and managing QueuebertQueue and QueuebertWorker instances
 */
@Injectable()
export class QueuebertBullMQService implements OnModuleDestroy, OnModuleInit {
  private readonly logger = new Logger(QueuebertBullMQService.name);
  private queues: Map<string, QueuebertQueue> = new Map();
  private workers: Map<string, QueuebertWorker> = new Map();

  constructor(
    @Inject(QUEUEBERT_BULLMQ_OPTIONS)
    private readonly options: QueuebertBullMQModuleOptions,
    @Inject(QUEUEBERT_STATS_COLLECTOR)
    private readonly statsCollector: GlobalStatsCollector,
    @Optional()
    @Inject(QUEUEBERT_INTEGRATION_REGISTRY)
    private readonly integrationRegistry?: QueuebertIntegrationRegistry,
  ) {
    this.validateOptions(options);
  }

  /**
   * Validate module options
   */
  private validateOptions(options: QueuebertBullMQModuleOptions): void {
    if (!options.connection) {
      throw new InvalidOptionsError('connection is required');
    }

    const { connection } = options;
    if (!connection.url && !connection.host) {
      throw new InvalidOptionsError(
        'connection.url or connection.host is required',
      );
    }
  }

  /**
   * Register this integration with the Queuebert registry on module init
   */
  onModuleInit(): void {
    this.integrationRegistry?.register({
      name: '@queuebert/bullmq',
      version: QUEUEBERT_BULLMQ_VERSION,
      description: 'BullMQ integration with enhanced stats collection',
    });
  }

  /**
   * Create a new QueuebertQueue instance
   */
  createQueue<T = unknown>(
    name: string,
    options: Partial<QueuebertQueueOptions> = {},
  ): QueuebertQueue<T> {
    if (this.queues.has(name)) {
      throw new QueueAlreadyExistsError(name);
    }

    const mergedOptions: QueuebertQueueOptions = {
      ...this.options.defaultQueueOptions,
      ...options,
      connection: this.getConnectionOptions(),
    };

    const queue = new QueuebertQueue<T>(name, mergedOptions);
    this.queues.set(name, queue as QueuebertQueue<unknown>);
    this.logger.log(`Created queue: ${name}`);

    return queue;
  }

  /**
   * Create a new QueuebertWorker instance
   */
  createWorker<T = unknown, R = unknown>(
    queueName: string,
    processor: Processor<T, R>,
    options: Partial<QueuebertWorkerOptions> = {},
  ): QueuebertWorker<T, R> {
    if (this.workers.has(queueName)) {
      throw new WorkerAlreadyExistsError(queueName);
    }

    const mergedOptions: QueuebertWorkerOptions = {
      ...this.options.defaultWorkerOptions,
      ...options,
      connection: this.getConnectionOptions(),
      statsWindow: options.statsWindow ?? this.options.statsConfig,
    };

    const worker = new QueuebertWorker<T, R>(
      queueName,
      processor,
      mergedOptions,
    );
    this.workers.set(queueName, worker as QueuebertWorker<unknown, unknown>);

    // Register with the global stats collector
    this.statsCollector.registerWorker(
      queueName,
      worker as QueuebertWorkerInterface,
    );
    this.logger.log(`Created worker for queue: ${queueName}`);

    return worker;
  }

  /**
   * Get an existing queue by name
   */
  getQueue<T = unknown>(name: string): QueuebertQueue<T> | undefined {
    return this.queues.get(name) as QueuebertQueue<T> | undefined;
  }

  /**
   * Get an existing worker by queue name
   */
  getWorker<T = unknown, R = unknown>(
    queueName: string,
  ): QueuebertWorker<T, R> | undefined {
    return this.workers.get(queueName) as QueuebertWorker<T, R> | undefined;
  }

  /**
   * Get all registered queue names
   */
  getQueueNames(): string[] {
    return Array.from(this.queues.keys());
  }

  /**
   * Get all registered worker queue names
   */
  getWorkerQueueNames(): string[] {
    return Array.from(this.workers.keys());
  }

  /**
   * Get stats for all workers
   */
  async getAllStats(): Promise<Record<string, StatsSnapshot>> {
    return this.statsCollector.getAggregatedStats();
  }

  /**
   * Get stats for a specific queue's worker
   */
  async getQueueStats(queueName: string): Promise<StatsSnapshot | null> {
    return this.statsCollector.getQueueStats(queueName);
  }

  /**
   * Close a specific queue
   */
  async closeQueue(name: string): Promise<void> {
    const queue = this.queues.get(name);
    if (queue) {
      await queue.close();
      this.queues.delete(name);
    }
  }

  /**
   * Close a specific worker
   */
  async closeWorker(queueName: string, force?: boolean): Promise<void> {
    const worker = this.workers.get(queueName);
    if (worker) {
      this.statsCollector.unregisterWorker(queueName);
      await worker.close(force);
      this.workers.delete(queueName);
    }
  }

  /**
   * Pause a worker
   */
  async pauseWorker(
    queueName: string,
    doNotWaitActive?: boolean,
  ): Promise<void> {
    const worker = this.workers.get(queueName);
    if (worker) {
      await worker.pause(doNotWaitActive);
    }
  }

  /**
   * Resume a worker
   */
  async resumeWorker(queueName: string): Promise<void> {
    const worker = this.workers.get(queueName);
    if (worker) {
      await worker.resume();
    }
  }

  /**
   * Clean up on module destroy
   */
  async onModuleDestroy(): Promise<void> {
    const workerCount = this.workers.size;
    const queueCount = this.queues.size;

    if (workerCount > 0 || queueCount > 0) {
      this.logger.log(
        `Shutting down ${workerCount} workers and ${queueCount} queues`,
      );
    }

    // Close all workers first
    const workerClosePromises = Array.from(this.workers.values()).map(
      (worker) => worker.close(),
    );
    await Promise.allSettled(workerClosePromises);
    this.workers.clear();

    // Then close all queues
    const queueClosePromises = Array.from(this.queues.values()).map((queue) =>
      queue.close(),
    );
    await Promise.allSettled(queueClosePromises);
    this.queues.clear();

    this.logger.log('Shutdown complete');
  }

  /**
   * Get Redis connection options from module configuration
   */
  private getConnectionOptions(): {
    url?: string;
    host?: string;
    port?: number;
    username?: string;
    password?: string;
    db?: number;
  } {
    const { connection } = this.options;

    // If URL is provided, parse it
    if (connection.url) {
      const url = new URL(connection.url);
      if (url.protocol !== 'redis:' && url.protocol !== 'rediss:') {
        throw new InvalidOptionsError(
          'connection.url must use the redis: or rediss: protocol',
        );
      }

      // BullMQ passes its `url` option directly to ioredis. Keeping the URL
      // intact preserves TLS, usernames, percent-encoded credentials, query
      // parameters, and other supported ioredis URL behavior.
      return { url: connection.url };
    }

    return {
      host: connection.host,
      port: connection.port,
      username: connection.username,
      password: connection.password,
      db: connection.db,
    };
  }
}
