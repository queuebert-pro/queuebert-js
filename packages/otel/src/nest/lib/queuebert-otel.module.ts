import { Module, DynamicModule, Global } from '@nestjs/common';
import { metrics, trace } from '@opentelemetry/api';

import { QueuebertOTelService } from './queuebert-otel.service';
import type {
  QueuebertOTelModuleOptions,
  QueuebertOTelModuleAsyncOptions,
} from './types';
import {
  QUEUEBERT_OTEL_METER,
  QUEUEBERT_OTEL_OPTIONS,
  QUEUEBERT_OTEL_TRACER,
} from './types';

/**
 * NestJS module for OpenTelemetry integration with Queuebert
 *
 * Records Queuebert metrics through the application's existing OpenTelemetry
 * MeterProvider and exposes the selected Meter and Tracer as Nest providers.
 * The application remains responsible for starting and shutting down its SDK.
 *
 * @example
 * ```typescript
 * // Use the global OpenTelemetry providers configured by the application
 * @Module({
 *   imports: [
 *     QueuebertOTelModule.forRoot({
 *       resource: {
 *         serviceName: 'my-worker',
 *         deploymentEnvironment: 'production',
 *       },
 *     }),
 *   ],
 * })
 * export class AppModule {}
 * ```
 *
 * @example
 * ```typescript
 * // Custom instrumentation scope
 * @Module({
 *   imports: [
 *     QueuebertOTelModule.forRoot({
 *       instrumentationName: 'my-worker.queuebert',
 *     }),
 *   ],
 * })
 * export class AppModule {}
 * ```
 *
 * @example
 * ```typescript
 * // Async configuration
 * @Module({
 *   imports: [
 *     QueuebertOTelModule.forRootAsync({
 *       imports: [ConfigModule],
 *       useFactory: (config: ConfigService) => ({
 *         resource: {
 *           serviceName: config.get('SERVICE_NAME'),
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
export class QueuebertOTelModule {
  /**
   * Configure the module with synchronous options
   */
  static forRoot(options: QueuebertOTelModuleOptions): DynamicModule {
    return {
      module: QueuebertOTelModule,
      providers: [
        {
          provide: QUEUEBERT_OTEL_OPTIONS,
          useValue: options,
        },
        {
          provide: QUEUEBERT_OTEL_METER,
          useValue:
            options.meter ??
            metrics.getMeter(
              options.instrumentationName ?? '@queuebert/otel',
              options.instrumentationVersion,
            ),
        },
        {
          provide: QUEUEBERT_OTEL_TRACER,
          useValue:
            options.tracer ??
            trace.getTracer(
              options.instrumentationName ?? '@queuebert/otel',
              options.instrumentationVersion,
            ),
        },
        QueuebertOTelService,
      ],
      exports: [
        QueuebertOTelService,
        QUEUEBERT_OTEL_OPTIONS,
        QUEUEBERT_OTEL_METER,
        QUEUEBERT_OTEL_TRACER,
      ],
    };
  }

  /**
   * Configure the module with async options
   */
  static forRootAsync(options: QueuebertOTelModuleAsyncOptions): DynamicModule {
    return {
      module: QueuebertOTelModule,
      imports: options.imports as DynamicModule['imports'],
      providers: [
        {
          provide: QUEUEBERT_OTEL_OPTIONS,
          useFactory: options.useFactory,
          inject: options.inject,
        },
        {
          provide: QUEUEBERT_OTEL_METER,
          useFactory: (moduleOptions: QueuebertOTelModuleOptions) =>
            moduleOptions.meter ??
            metrics.getMeter(
              moduleOptions.instrumentationName ?? '@queuebert/otel',
              moduleOptions.instrumentationVersion,
            ),
          inject: [QUEUEBERT_OTEL_OPTIONS],
        },
        {
          provide: QUEUEBERT_OTEL_TRACER,
          useFactory: (moduleOptions: QueuebertOTelModuleOptions) =>
            moduleOptions.tracer ??
            trace.getTracer(
              moduleOptions.instrumentationName ?? '@queuebert/otel',
              moduleOptions.instrumentationVersion,
            ),
          inject: [QUEUEBERT_OTEL_OPTIONS],
        },
        QueuebertOTelService,
      ],
      exports: [
        QueuebertOTelService,
        QUEUEBERT_OTEL_OPTIONS,
        QUEUEBERT_OTEL_METER,
        QUEUEBERT_OTEL_TRACER,
      ],
    };
  }
}
