import { Module, DynamicModule, Global } from '@nestjs/common';

import { QueuebertBullMQService } from './queuebert-bullmq.service';
import { GlobalStatsCollector } from './stats-collector';
import type {
  QueuebertBullMQModuleOptions,
  QueuebertBullMQModuleAsyncOptions,
} from './types';
import { QUEUEBERT_BULLMQ_OPTIONS, QUEUEBERT_STATS_COLLECTOR } from './types';

/**
 * NestJS module for QueuebertBullMQ integration
 *
 * Provides enhanced BullMQ queue and worker wrappers with:
 * - Automatic job duration tracking
 * - Job lifecycle events
 * - Rolling window statistics
 * - Integration with the main Queuebert monitoring module
 *
 * @example
 * ```typescript
 * // Synchronous configuration
 * @Module({
 *   imports: [
 *     QueuebertBullMQModule.forRoot({
 *       connection: {
 *         host: 'localhost',
 *         port: 6379,
 *       },
 *     }),
 *   ],
 * })
 * export class AppModule {}
 *
 * // Async configuration
 * @Module({
 *   imports: [
 *     QueuebertBullMQModule.forRootAsync({
 *       imports: [ConfigModule],
 *       useFactory: (config: ConfigService) => ({
 *         connection: {
 *           host: config.get('REDIS_HOST'),
 *           port: config.get('REDIS_PORT'),
 *         },
 *       }),
 *       inject: [ConfigService],
 *     }),
 *   ],
 * })
 * export class AppModule {}
 * ```
 */
@Global()
@Module({})
export class QueuebertBullMQModule {
  /**
   * Configure the module with synchronous options
   */
  static forRoot(options: QueuebertBullMQModuleOptions): DynamicModule {
    return {
      module: QueuebertBullMQModule,
      providers: [
        {
          provide: QUEUEBERT_BULLMQ_OPTIONS,
          useValue: options,
        },
        {
          provide: QUEUEBERT_STATS_COLLECTOR,
          useFactory: () => new GlobalStatsCollector(),
        },
        QueuebertBullMQService,
      ],
      exports: [QueuebertBullMQService, QUEUEBERT_STATS_COLLECTOR],
    };
  }

  /**
   * Configure the module with async options
   */
  static forRootAsync(
    options: QueuebertBullMQModuleAsyncOptions,
  ): DynamicModule {
    return {
      module: QueuebertBullMQModule,
      imports: options.imports as DynamicModule['imports'],
      providers: [
        {
          provide: QUEUEBERT_BULLMQ_OPTIONS,
          useFactory: options.useFactory,
          inject: options.inject,
        },
        {
          provide: QUEUEBERT_STATS_COLLECTOR,
          useFactory: () => new GlobalStatsCollector(),
        },
        QueuebertBullMQService,
      ],
      exports: [QueuebertBullMQService, QUEUEBERT_STATS_COLLECTOR],
    };
  }
}
