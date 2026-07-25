import { ModuleRef } from '@nestjs/core';

import { QueuebertIntegrationRegistry } from './integration-registry';
import { QueuebertModule, QueuebertQueuesProvider } from './queuebert.module';
import { QueuebertService } from './queuebert.service';
import {
  QUEUEBERT_INTEGRATION_REGISTRY,
  QUEUEBERT_OPTIONS,
  QUEUEBERT_QUEUES,
} from './types';

describe('QueuebertModule', () => {
  it('creates a dynamic module with configured controller path and providers', () => {
    class AdminGuard {}
    const decorator = jest.fn();

    const module = QueuebertModule.forRoot({
      path: 'internal/queuebert',
      queues: [{ name: 'emails', queue: { name: 'emails' } as any }],
      auth: {
        guards: [AdminGuard],
        decorators: [decorator],
      },
    });

    expect(module.module).toBe(QueuebertModule);
    expect(module.controllers).toHaveLength(1);
    expect(Reflect.getMetadata('path', module.controllers![0])).toBe(
      'internal/queuebert',
    );
    expect(Reflect.getMetadata('__guards__', module.controllers![0])).toEqual([
      AdminGuard,
    ]);
    expect(decorator).toHaveBeenCalledWith(module.controllers![0]);
    expect(module.exports).toEqual([
      QueuebertService,
      QUEUEBERT_OPTIONS,
      QUEUEBERT_QUEUES,
      QUEUEBERT_INTEGRATION_REGISTRY,
    ]);
  });

  it('registers the core integration in forRoot', () => {
    const module = QueuebertModule.forRoot({
      queues: [{ name: 'emails', queue: { name: 'emails' } as any }],
    });
    const registryProvider = (module.providers as any[]).find(
      (provider) => provider.provide === QUEUEBERT_INTEGRATION_REGISTRY,
    );

    const registry =
      registryProvider.useFactory() as QueuebertIntegrationRegistry;

    expect(registry.get('@queuebert/nest')).toMatchObject({
      name: '@queuebert/nest',
      version: '0.0.1',
    });
  });

  it('creates an async dynamic module with a registered controller', async () => {
    class AdminGuard {}
    const decorator = jest.fn();
    const module = QueuebertModule.forRootAsync({
      imports: [],
      path: 'async/queuebert',
      auth: {
        guards: [AdminGuard],
        decorators: [decorator],
      },
      useFactory: () => ({
        queues: [],
      }),
      inject: [],
    });

    expect(module.module).toBe(QueuebertModule);
    expect(module.imports).toEqual([]);
    expect(module.controllers).toHaveLength(1);
    const controller = module.controllers![0];

    expect(Reflect.getMetadata('path', controller)).toBe('async/queuebert');
    expect(Reflect.getMetadata('__guards__', controller)).toEqual([AdminGuard]);
    expect(decorator).toHaveBeenCalledWith(controller);

    const optionsProvider = (module.providers as any[]).find(
      (provider) => provider.provide === QUEUEBERT_OPTIONS,
    );

    await expect(optionsProvider.useFactory()).resolves.toMatchObject({
      path: 'async/queuebert',
      queues: [],
      auth: {
        guards: [AdminGuard],
      },
    });
  });
});

describe('QueuebertQueuesProvider', () => {
  type ProviderWithOwnedQueues = {
    ownedQueues: Array<{ name: string; close: () => Promise<void> }>;
  };

  it('uses directly provided queue instances and processors', () => {
    class EmailProcessor {}
    const queue = { name: 'emails' };
    const processor = new EmailProcessor();
    const moduleRef = {
      get: jest.fn((token) => {
        if (token === EmailProcessor) return processor;
        return undefined;
      }),
    } as unknown as ModuleRef;
    const provider = new QueuebertQueuesProvider(moduleRef, {
      queues: [
        {
          name: 'emails',
          statsKey: 'primary-emails',
          queue: queue as any,
          processor: EmailProcessor as any,
        },
      ],
    });

    provider.onModuleInit();

    expect(provider.getQueues().get('primary-emails')).toEqual({
      queue,
      processor,
    });
  });

  it('fails initialization when a configured queue cannot be resolved', () => {
    const moduleRef = {
      get: jest.fn(() => {
        throw new Error('missing');
      }),
    } as unknown as ModuleRef;
    const provider = new QueuebertQueuesProvider(moduleRef, {
      queues: [{ name: 'missing' }],
    });

    expect(() => provider.onModuleInit()).toThrow(
      'Queuebert configuration could not be initialized',
    );
  });

  it('closes queues it owns during module shutdown', async () => {
    const queue = {
      name: 'secondary-emails',
      close: jest.fn().mockResolvedValue(undefined),
    };
    const moduleRef = {
      get: jest.fn(),
    } as unknown as ModuleRef;
    const provider = new QueuebertQueuesProvider(moduleRef, {
      queues: [{ name: 'unused', queue: { name: 'unused' } as any }],
    });
    const providerWithOwnedQueues =
      provider as unknown as ProviderWithOwnedQueues;

    providerWithOwnedQueues.ownedQueues.push(queue);

    await provider.onModuleDestroy();

    expect(queue.close).toHaveBeenCalledTimes(1);
    expect(providerWithOwnedQueues.ownedQueues).toEqual([]);
  });
});
