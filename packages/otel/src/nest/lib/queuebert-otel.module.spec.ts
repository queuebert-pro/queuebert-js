import { QueuebertOTelModule } from './queuebert-otel.module';
import { QueuebertOTelService } from './queuebert-otel.service';
import {
  QUEUEBERT_OTEL_METER,
  QUEUEBERT_OTEL_OPTIONS,
  QUEUEBERT_OTEL_TRACER,
} from './types';

describe('QueuebertOTelModule', () => {
  it('creates a global module with static options', () => {
    const options = {
      resource: {
        serviceName: 'worker',
      },
    };

    const module = QueuebertOTelModule.forRoot(options);

    expect(module.module).toBe(QueuebertOTelModule);
    expect(module.providers).toEqual(
      expect.arrayContaining([
        {
          provide: QUEUEBERT_OTEL_OPTIONS,
          useValue: options,
        },
        expect.objectContaining({ provide: QUEUEBERT_OTEL_METER }),
        expect.objectContaining({ provide: QUEUEBERT_OTEL_TRACER }),
        QueuebertOTelService,
      ]),
    );
    expect(module.exports).toEqual([
      QueuebertOTelService,
      QUEUEBERT_OTEL_OPTIONS,
      QUEUEBERT_OTEL_METER,
      QUEUEBERT_OTEL_TRACER,
    ]);
  });

  it('creates a global module with async options', () => {
    class ConfigModule {}
    const useFactory = jest.fn(() => ({ instrumentationName: 'test' }));

    const module = QueuebertOTelModule.forRootAsync({
      imports: [ConfigModule],
      useFactory,
      inject: ['ConfigService'],
    });

    expect(module.imports).toEqual([ConfigModule]);
    const optionsProvider = (module.providers as any[]).find(
      (provider) => provider.provide === QUEUEBERT_OTEL_OPTIONS,
    );
    expect(optionsProvider.useFactory).toBe(useFactory);
    expect(optionsProvider.inject).toEqual(['ConfigService']);
  });
});
