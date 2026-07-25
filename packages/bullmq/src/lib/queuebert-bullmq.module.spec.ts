import { QueuebertBullMQModule } from './queuebert-bullmq.module';
import { QueuebertBullMQService } from './queuebert-bullmq.service';
import { GlobalStatsCollector } from './stats-collector';
import { QUEUEBERT_BULLMQ_OPTIONS, QUEUEBERT_STATS_COLLECTOR } from './types';

describe('QueuebertBullMQModule', () => {
  it('creates a global module with static options', () => {
    const options = {
      connection: {
        host: 'localhost',
        port: 6379,
      },
    };

    const module = QueuebertBullMQModule.forRoot(options);

    expect(module.module).toBe(QueuebertBullMQModule);
    expect(module.providers).toEqual([
      {
        provide: QUEUEBERT_BULLMQ_OPTIONS,
        useValue: options,
      },
      expect.objectContaining({
        provide: QUEUEBERT_STATS_COLLECTOR,
      }),
      QueuebertBullMQService,
    ]);
    expect(module.exports).toEqual([
      QueuebertBullMQService,
      QUEUEBERT_STATS_COLLECTOR,
    ]);

    const statsProvider = (module.providers as any[]).find(
      (provider) => provider.provide === QUEUEBERT_STATS_COLLECTOR,
    );
    expect(statsProvider.useFactory()).toBeInstanceOf(GlobalStatsCollector);
  });

  it('creates a global module with async options', async () => {
    const useFactory = jest.fn().mockResolvedValue({
      connection: { host: 'localhost' },
    });

    const module = QueuebertBullMQModule.forRootAsync({
      imports: ['ConfigModule'],
      useFactory,
      inject: ['ConfigService'],
    });

    expect(module.imports).toEqual(['ConfigModule']);
    const optionsProvider = (module.providers as any[]).find(
      (provider) => provider.provide === QUEUEBERT_BULLMQ_OPTIONS,
    );
    expect(optionsProvider.useFactory).toBe(useFactory);
    expect(optionsProvider.inject).toEqual(['ConfigService']);
  });
});
