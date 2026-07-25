import { getQueueToken } from '@nestjs/bullmq';
import {
  DynamicModule,
  Logger,
  Module,
  OnModuleDestroy,
  OnModuleInit,
  Type,
} from '@nestjs/common';
import { ModuleRef } from '@nestjs/core';
import { Queue } from 'bullmq';

import { QueuebertIntegrationRegistry } from './integration-registry';
import { createQueuebertController } from './queuebert.controller';
import { QueuebertService } from './queuebert.service';
import {
  QueuebertModuleOptions,
  QueuebertModuleAsyncOptions,
  QueuebertAuthConfig,
  QueuebertProcessor,
  QUEUEBERT_OPTIONS,
  QUEUEBERT_QUEUES,
  QUEUEBERT_INTEGRATION_REGISTRY,
  DEFAULT_REDIS_INSTANCE_ID,
  ALL_OPTIONAL_ENDPOINTS,
} from './types';

const QUEUEBERT_NEST_VERSION = (
  require('../../package.json') as { version: string }
).version;

const logger = new Logger('QueuebertModule');

function validateQueuebertOptions(options: QueuebertModuleOptions): void {
  if (!Array.isArray(options.queues) || options.queues.length === 0) {
    throw new Error('Queuebert requires at least one configured queue');
  }

  const redisIds = new Set<string>();
  for (const redis of options.redis ?? []) {
    if (!redis.id || redisIds.has(redis.id)) {
      throw new Error(`Duplicate or empty Queuebert Redis id: '${redis.id}'`);
    }
    redisIds.add(redis.id);
  }

  const statsKeys = new Set<string>();
  for (const queue of options.queues) {
    if (!queue.name) {
      throw new Error('Queuebert queue names must not be empty');
    }

    const statsKey = queue.statsKey ?? queue.name;
    if (statsKeys.has(statsKey)) {
      throw new Error(`Duplicate Queuebert queue statsKey: '${statsKey}'`);
    }
    statsKeys.add(statsKey);

    if (queue.redis && !redisIds.has(queue.redis)) {
      throw new Error(
        `Queue '${statsKey}' references unknown Redis instance '${queue.redis}'`,
      );
    }
  }

  for (const endpoint of options.endpoints ?? []) {
    if (!ALL_OPTIONAL_ENDPOINTS.includes(endpoint)) {
      throw new Error(`Unknown Queuebert endpoint: '${endpoint}'`);
    }
  }
}

function applyControllerAuth(
  controllerClass: Type<unknown>,
  auth?: QueuebertAuthConfig,
): void {
  if (auth?.guards?.length) {
    Reflect.defineMetadata('__guards__', auth.guards, controllerClass);
  }

  if (auth?.decorators?.length) {
    for (const decorator of auth.decorators) {
      decorator(controllerClass);
    }
  }
}

/**
 * Provider class that resolves queues and processors after module initialization
 * This ensures all dependencies are available before attempting resolution
 */
export class QueuebertQueuesProvider implements OnModuleInit, OnModuleDestroy {
  private readonly queuesMap = new Map<
    string,
    { queue: Queue; processor?: QueuebertProcessor }
  >();
  private readonly ownedQueues: Queue[] = [];

  constructor(
    private readonly moduleRef: ModuleRef,
    private readonly options: QueuebertModuleOptions,
  ) {
    validateQueuebertOptions(options);
  }

  onModuleInit() {
    this.queuesMap.clear();
    logger.log(`Resolving ${this.options.queues.length} queue(s)...`);

    const resolutionErrors: string[] = [];

    for (const queueConfig of this.options.queues) {
      let queue: Queue | undefined;
      let processor: QueuebertProcessor | undefined;

      // Determine which Redis instance this queue should use
      const targetRedisId =
        queueConfig.redis ??
        this.options.redis?.[0]?.id ??
        DEFAULT_REDIS_INSTANCE_ID;
      const redisConfig = this.options.redis?.find(
        (r) => r.id === targetRedisId,
      );

      // Check if a Queue instance was directly provided
      if (queueConfig.queue) {
        queue = queueConfig.queue;
        logger.debug(`Using provided Queue instance for '${queueConfig.name}'`);
      } else if (redisConfig?.connection) {
        // If a Redis connection config exists for this queue's target Redis instance,
        // create a Queue instance using that connection instead of looking up from DI.
        // This handles the case where the same queue name exists on multiple Redis instances.
        queue = new Queue(queueConfig.name, {
          connection: redisConfig.connection,
        });
        this.ownedQueues.push(queue);
        logger.debug(
          `Created Queue instance for '${queueConfig.name}' using Redis '${targetRedisId}' connection config`,
        );
      } else {
        // Look up queue from NestJS DI (default behavior for primary/default Redis)
        const queueToken = getQueueToken(queueConfig.name);
        logger.debug(
          `Looking for queue '${queueConfig.name}' with token: ${String(queueToken)}`,
        );

        try {
          queue = this.moduleRef.get<Queue>(queueToken, { strict: false });
        } catch (err) {
          logger.warn(`Failed to get queue '${queueConfig.name}': ${err}`);
        }
      }

      // Only try to get processor if one was configured
      if (queueConfig.processor) {
        try {
          processor = this.moduleRef.get(queueConfig.processor, {
            strict: false,
          });
        } catch (err) {
          logger.warn(
            `Failed to get processor for '${queueConfig.name}': ${err}`,
          );
        }

        if (!processor) {
          logger.warn(
            `Processor for '${queueConfig.name}' not found - ensure it implements QueuebertProcessor and is provided`,
          );
        }
      }

      if (!queue) {
        resolutionErrors.push(
          `Queue '${queueConfig.name}' could not be resolved; register it with BullModule, provide queue, or configure its Redis connection`,
        );
        continue;
      }

      if (queueConfig.processor && !processor) {
        resolutionErrors.push(
          `Processor for queue '${queueConfig.name}' could not be resolved`,
        );
        continue;
      }

      const key = queueConfig.statsKey || queueConfig.name;
      this.queuesMap.set(key, { queue, processor });
      if (processor) {
        logger.log(
          `Registered queue '${key}' with processor (Redis: ${targetRedisId})`,
        );
      } else {
        logger.log(
          `Registered queue '${key}' (monitoring only - Redis: ${targetRedisId})`,
        );
      }
    }

    if (resolutionErrors.length > 0) {
      throw new Error(
        `Queuebert configuration could not be initialized:\n- ${resolutionErrors.join('\n- ')}`,
      );
    }

    logger.log(`Queuebert initialized with ${this.queuesMap.size} queue(s)`);
  }

  async onModuleDestroy(): Promise<void> {
    await Promise.all(
      this.ownedQueues.map(async (queue) => {
        try {
          await queue.close();
        } catch (err) {
          logger.warn(`Failed to close queue '${queue.name}': ${err}`);
        }
      }),
    );

    this.ownedQueues.length = 0;
  }

  getQueues(): Map<string, { queue: Queue; processor?: QueuebertProcessor }> {
    return this.queuesMap;
  }
}

@Module({})
export class QueuebertModule {
  /**
   * Register Queuebert with static configuration
   *
   * @example
   * ```typescript
   * QueuebertModule.forRoot({
   *   path: 'admin/queue',
   *   queues: [
   *     { name: 'my-queue', processor: MyProcessor },
   *   ],
   *   auth: {
   *     guards: [AuthGuard],
   *   },
   * })
   * ```
   */
  static forRoot(options: QueuebertModuleOptions): DynamicModule {
    validateQueuebertOptions(options);
    const controllerClass = createQueuebertController(
      options.path || 'admin/queue',
    );
    applyControllerAuth(controllerClass, options.auth);

    return {
      module: QueuebertModule,
      controllers: [controllerClass],
      providers: [
        {
          provide: QUEUEBERT_OPTIONS,
          useValue: options,
        },
        {
          provide: QueuebertQueuesProvider,
          useFactory: (moduleRef: ModuleRef) => {
            return new QueuebertQueuesProvider(moduleRef, options);
          },
          inject: [ModuleRef],
        },
        {
          provide: QUEUEBERT_QUEUES,
          useFactory: (provider: QueuebertQueuesProvider) => {
            // Return a getter function that retrieves the map after onModuleInit
            return () => provider.getQueues();
          },
          inject: [QueuebertQueuesProvider],
        },
        {
          provide: QUEUEBERT_INTEGRATION_REGISTRY,
          useFactory: () => {
            const registry = new QueuebertIntegrationRegistry();
            // Register the main Queuebert NestJS package
            registry.register({
              name: '@queuebert/nest',
              version: QUEUEBERT_NEST_VERSION,
              description: 'NestJS module for BullMQ queue monitoring',
            });
            return registry;
          },
        },
        QueuebertService,
      ],
      exports: [
        QueuebertService,
        QUEUEBERT_OPTIONS,
        QUEUEBERT_QUEUES,
        QUEUEBERT_INTEGRATION_REGISTRY,
      ],
    };
  }

  /**
   * Register Queuebert with async configuration
   *
   * @example
   * ```typescript
   * QueuebertModule.forRootAsync({
   *   path: 'admin/queue',
   *   imports: [ConfigModule],
   *   useFactory: (config: ConfigService) => ({
   *     queues: [...],
   *   }),
   *   inject: [ConfigService],
   * })
   * ```
   */
  static forRootAsync(
    asyncOptions: QueuebertModuleAsyncOptions,
  ): DynamicModule {
    const controllerClass = createQueuebertController(
      asyncOptions.path || 'admin/queue',
    );
    applyControllerAuth(controllerClass, asyncOptions.auth);

    return {
      module: QueuebertModule,
      imports: asyncOptions.imports || [],
      controllers: [controllerClass],
      providers: [
        {
          provide: QUEUEBERT_OPTIONS,
          useFactory: async (...args: unknown[]) => {
            const options = await asyncOptions.useFactory(...args);
            return {
              ...options,
              path: asyncOptions.path ?? options.path,
              auth: asyncOptions.auth ?? options.auth,
            };
          },
          inject: asyncOptions.inject || [],
        },
        {
          provide: QueuebertQueuesProvider,
          useFactory: (
            moduleRef: ModuleRef,
            options: QueuebertModuleOptions,
          ) => {
            return new QueuebertQueuesProvider(moduleRef, options);
          },
          inject: [ModuleRef, QUEUEBERT_OPTIONS],
        },
        {
          provide: QUEUEBERT_QUEUES,
          useFactory: (provider: QueuebertQueuesProvider) => {
            return () => provider.getQueues();
          },
          inject: [QueuebertQueuesProvider],
        },
        {
          provide: QUEUEBERT_INTEGRATION_REGISTRY,
          useFactory: () => {
            const registry = new QueuebertIntegrationRegistry();
            // Register the main Queuebert NestJS package
            registry.register({
              name: '@queuebert/nest',
              version: QUEUEBERT_NEST_VERSION,
              description: 'NestJS module for BullMQ queue monitoring',
            });
            return registry;
          },
        },
        QueuebertService,
      ],
      exports: [
        QueuebertService,
        QUEUEBERT_OPTIONS,
        QUEUEBERT_QUEUES,
        QUEUEBERT_INTEGRATION_REGISTRY,
      ],
    };
  }

  /**
   * Register Queuebert with a custom controller class
   *
   * Use this when you need full control over the controller,
   * such as extending the base functionality or using custom decorators
   *
   * @example
   * ```typescript
   * @Controller('my-custom-path')
   * @UseGuards(MyAuthGuard)
   * class MyQueueController extends createQueuebertController('') {
   *   // Custom methods...
   * }
   *
   * QueuebertModule.forRootWithController({
   *   queues: [...],
   *   controller: MyQueueController,
   * })
   * ```
   */
  static forRootWithController(
    options: Omit<QueuebertModuleOptions, 'path' | 'auth'> & {
      controller: Type<unknown>;
    },
  ): DynamicModule {
    validateQueuebertOptions(options);
    return {
      module: QueuebertModule,
      controllers: [options.controller],
      providers: [
        {
          provide: QUEUEBERT_OPTIONS,
          useValue: options,
        },
        {
          provide: QueuebertQueuesProvider,
          useFactory: (moduleRef: ModuleRef) => {
            return new QueuebertQueuesProvider(moduleRef, options);
          },
          inject: [ModuleRef],
        },
        {
          provide: QUEUEBERT_QUEUES,
          useFactory: (provider: QueuebertQueuesProvider) => {
            return () => provider.getQueues();
          },
          inject: [QueuebertQueuesProvider],
        },
        {
          provide: QUEUEBERT_INTEGRATION_REGISTRY,
          useFactory: () => {
            const registry = new QueuebertIntegrationRegistry();
            // Register the main Queuebert NestJS package
            registry.register({
              name: '@queuebert/nest',
              version: QUEUEBERT_NEST_VERSION,
              description: 'NestJS module for BullMQ queue monitoring',
            });
            return registry;
          },
        },
        QueuebertService,
      ],
      exports: [
        QueuebertService,
        QUEUEBERT_OPTIONS,
        QUEUEBERT_QUEUES,
        QUEUEBERT_INTEGRATION_REGISTRY,
      ],
    };
  }
}
