import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';

import { Queue } from 'bullmq';
import Redis from 'ioredis';

import {
  QueuebertIntegrationRegistry,
  QueuebertService,
} from '@queuebert/nest';

const redisUrl = new URL(
  process.env['QUEUEBERT_REDIS_URL'] ?? 'redis://127.0.0.1:6379',
);
if (redisUrl.protocol !== 'redis:' && redisUrl.protocol !== 'rediss:') {
  throw new Error('QUEUEBERT_REDIS_URL must use redis: or rediss:');
}

const connection = {
  host: redisUrl.hostname,
  port: redisUrl.port ? Number(redisUrl.port) : 6379,
  ...(redisUrl.username
    ? { username: decodeURIComponent(redisUrl.username) }
    : {}),
  ...(redisUrl.password
    ? { password: decodeURIComponent(redisUrl.password) }
    : {}),
  ...(redisUrl.protocol === 'rediss:' ? { tls: {} } : {}),
};
const suffix = randomUUID();
const source = new Queue(`queuebert-smoke-source-${suffix}`, { connection });
const target = new Queue(`queuebert-smoke-target-${suffix}`, { connection });
const redis = new Redis(connection);
const service = new QueuebertService(
  {
    queues: [],
    redis: [{ id: 'smoke-source' }, { id: 'smoke-target' }],
    endpoints: ['migrations'],
  },
  new QueuebertIntegrationRegistry(),
);

try {
  assert.equal(await redis.ping(), 'PONG');
  await Promise.all([source.waitUntilReady(), target.waitUntilReady()]);

  const timestamp = Date.now() - 5000;
  await source.add(
    'waiting-job',
    { kind: 'waiting' },
    { jobId: 'waiting-1', attempts: 3, timestamp },
  );
  await source.add(
    'delayed-job',
    { kind: 'delayed' },
    { jobId: 'delayed-1', delay: 60000, timestamp },
  );

  const result = await service.executeMigration(
    source,
    target,
    {
      sourceQueue: source.name,
      targetQueue: target.name,
      sourceRedis: 'smoke-source',
      targetRedis: 'smoke-target',
      states: ['waiting', 'delayed'],
      limit: 2,
      batchSize: 1,
    },
    'smoke-source',
    'smoke-target',
  );

  const [sourceWaiting, sourceDelayed, targetWaiting, targetDelayed] =
    await Promise.all([
      source.getJob('waiting-1'),
      source.getJob('delayed-1'),
      target.getJob('waiting-1'),
      target.getJob('delayed-1'),
    ]);

  assert.equal(result.migrated.total, 2);
  assert.equal(result.migrated.waiting, 1);
  assert.equal(result.migrated.delayed, 1);
  assert.equal(sourceWaiting, undefined);
  assert.equal(sourceDelayed, undefined);
  assert.ok(targetWaiting);
  assert.ok(targetDelayed);
  assert.equal(targetWaiting.opts.attempts, 3);
  assert.equal(targetWaiting.timestamp, timestamp);
  assert.equal(await source.isPaused(), false);
  assert.equal(await target.isPaused(), false);

  process.stdout.write('Redis migration smoke test passed\n');
} finally {
  await service.onModuleDestroy();
  await Promise.allSettled([source.pause(), target.pause()]);
  await Promise.allSettled([
    source.obliterate({ force: true }),
    target.obliterate({ force: true }),
  ]);
  await Promise.allSettled([source.close(), target.close(), redis.quit()]);
}
