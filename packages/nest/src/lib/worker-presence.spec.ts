import { WorkerStopReason } from './types';
import {
  classifyWorkerStop,
  describeWorkerStopReason,
  isPresenceRedis,
  readWorkerPresence,
  WorkerPresence,
  workerPresenceKeys,
} from './worker-presence';

/**
 * An in-memory stand-in for the handful of Redis commands presence uses,
 * recording every write so tests can assert on what reached Redis.
 */
function createRedis(
  initial: { workers?: Record<string, string>; lastStop?: string | null } = {},
) {
  const hashes = new Map<string, Map<string, string>>();
  const strings = new Map<string, string>();
  const keys = workerPresenceKeys('emails');

  if (initial.workers) {
    hashes.set(keys.workers, new Map(Object.entries(initial.workers)));
  }
  if (initial.lastStop) {
    strings.set(keys.lastStop, initial.lastStop);
  }

  const redis = {
    hgetall: jest.fn(async (key: string) =>
      Object.fromEntries(hashes.get(key) ?? []),
    ),
    hset: jest.fn(async (key: string, field: string, value: string) => {
      const hash = hashes.get(key) ?? new Map<string, string>();
      hash.set(field, value);
      hashes.set(key, hash);
      return 1;
    }),
    hdel: jest.fn(async (key: string, ...fields: string[]) => {
      const hash = hashes.get(key);
      for (const field of fields) hash?.delete(field);
      return fields.length;
    }),
    get: jest.fn(async (key: string) => strings.get(key) ?? null),
    set: jest.fn(async (key: string, value: string) => {
      strings.set(key, value);
      return 'OK';
    }),
    multi: jest.fn(() => {
      const ops: Array<() => Promise<unknown>> = [];
      const chain = {
        set: (key: string, value: string) => {
          ops.push(() => redis.set(key, value));
          return chain;
        },
        hdel: (key: string, ...fields: string[]) => {
          ops.push(() => redis.hdel(key, ...fields));
          return chain;
        },
        exec: jest.fn(async () => {
          for (const op of ops) await op();
          return [];
        }),
      };
      return chain;
    }),
    hashes,
    strings,
    keys,
  };

  return redis;
}

const T0 = Date.parse('2026-09-19T12:00:00.000Z');

function entry(heartbeatAt: string, extra: Record<string, unknown> = {}) {
  return JSON.stringify({
    host: 'api-1',
    pid: 42,
    startedAt: '2026-09-19T11:00:00.000Z',
    heartbeatAt,
    ...extra,
  });
}

describe('classifyWorkerStop', () => {
  it('maps each signal to its reason', () => {
    expect(classifyWorkerStop({ shuttingDown: true }, T0)).toBe(
      WorkerStopReason.Shutdown,
    );
    expect(classifyWorkerStop({ closeRequested: true }, T0)).toBe(
      WorkerStopReason.Closed,
    );
    expect(classifyWorkerStop({ connectionLost: true }, T0)).toBe(
      WorkerStopReason.LostConnection,
    );
    expect(classifyWorkerStop({ recoveryExhausted: true }, T0)).toBe(
      WorkerStopReason.RecoveryFailed,
    );
    expect(classifyWorkerStop({ lastErrorAt: T0 - 2_000 }, T0)).toBe(
      WorkerStopReason.Error,
    );
    expect(classifyWorkerStop({}, T0)).toBe(WorkerStopReason.Unknown);
  });

  it('ignores an error too old to explain the close', () => {
    expect(classifyWorkerStop({ lastErrorAt: T0 - 60_000 }, T0)).toBe(
      WorkerStopReason.Unknown,
    );
    expect(
      classifyWorkerStop(
        { lastErrorAt: T0 - 60_000, recentErrorWindowMs: 120_000 },
        T0,
      ),
    ).toBe(WorkerStopReason.Error);
  });

  it('gives a shutdown precedence over everything else', () => {
    expect(
      classifyWorkerStop(
        {
          shuttingDown: true,
          closeRequested: true,
          connectionLost: true,
          lastErrorAt: T0,
        },
        T0,
      ),
    ).toBe(WorkerStopReason.Shutdown);
  });

  it('blames a lost connection rather than the error it caused', () => {
    expect(
      classifyWorkerStop({ connectionLost: true, lastErrorAt: T0 - 100 }, T0),
    ).toBe(WorkerStopReason.LostConnection);
  });

  it('describes every reason in one sentence', () => {
    for (const reason of Object.values(WorkerStopReason)) {
      expect(describeWorkerStopReason(reason)).toMatch(/^[A-Z][^.]+$/);
    }
  });
});

describe('workerPresenceKeys', () => {
  it('lives under the queue prefix with a qb segment', () => {
    expect(workerPresenceKeys('emails')).toEqual({
      workers: 'bull:emails:qb:workers',
      lastStop: 'bull:emails:qb:last-stop',
    });
    expect(workerPresenceKeys('emails', 'app').lastStop).toBe(
      'app:emails:qb:last-stop',
    );
  });
});

describe('isPresenceRedis', () => {
  it('accepts a client with the hash and string commands', () => {
    expect(isPresenceRedis(createRedis())).toBe(true);
  });

  it('rejects anything else without throwing', () => {
    expect(isPresenceRedis(undefined)).toBe(false);
    expect(isPresenceRedis(null)).toBe(false);
    expect(isPresenceRedis({ llen: jest.fn() })).toBe(false);
    expect(isPresenceRedis('redis')).toBe(false);
  });
});

describe('readWorkerPresence', () => {
  it('is undefined for a queue with no presence at all', async () => {
    const redis = createRedis();

    await expect(readWorkerPresence(redis, 'emails')).resolves.toBeUndefined();
    expect(redis.hdel).not.toHaveBeenCalled();
  });

  it('counts live workers and reports the recorded stop', async () => {
    const stop = {
      workerId: 'api-0:7:abc',
      host: 'api-0',
      reason: WorkerStopReason.LostConnection,
      description: 'Lost connection to Redis',
      at: '2026-09-19T11:30:00.000Z',
    };
    const redis = createRedis({
      workers: {
        'api-1:42:aaa': entry(new Date(T0 - 5_000).toISOString()),
        'api-2:43:bbb': entry(new Date(T0 - 20_000).toISOString()),
      },
      lastStop: JSON.stringify(stop),
    });

    await expect(
      readWorkerPresence(redis, 'emails', { now: T0 }),
    ).resolves.toEqual({ count: 2, lastStop: stop });
    expect(redis.hdel).not.toHaveBeenCalled();
    expect(redis.set).not.toHaveBeenCalled();
  });

  it('prunes a dead worker and records it as an unknown stop', async () => {
    const deadAt = new Date(T0 - 90_000).toISOString();
    const redis = createRedis({
      workers: {
        'api-1:42:aaa': entry(new Date(T0 - 5_000).toISOString()),
        'api-2:43:bbb': entry(deadAt, { jobsProcessed: 17, lastJobAt: null }),
      },
    });

    const result = await readWorkerPresence(redis, 'emails', { now: T0 });

    expect(result).toEqual({
      count: 1,
      lastStop: {
        workerId: 'api-2:43:bbb',
        host: 'api-1',
        reason: WorkerStopReason.Unknown,
        description: 'Stopped without reporting why',
        at: deadAt,
        jobsProcessed: 17,
        lastJobAt: null,
      },
    });
    expect(redis.hdel).toHaveBeenCalledWith(
      'bull:emails:qb:workers',
      'api-2:43:bbb',
    );
    expect(redis.set).toHaveBeenCalledWith(
      'bull:emails:qb:last-stop',
      expect.stringContaining('"reason":"unknown"'),
    );
  });

  it('keeps a recorded stop that is newer than the dead worker', async () => {
    const stop = {
      workerId: 'api-9:1:zzz',
      reason: WorkerStopReason.Closed,
      description: 'Closed by the application',
      at: new Date(T0 - 10_000).toISOString(),
    };
    const redis = createRedis({
      workers: {
        'api-2:43:bbb': entry(new Date(T0 - 90_000).toISOString()),
      },
      lastStop: JSON.stringify(stop),
    });

    const result = await readWorkerPresence(redis, 'emails', { now: T0 });

    expect(result).toEqual({ count: 0, lastStop: stop });
    expect(redis.set).not.toHaveBeenCalled();
    expect(redis.hdel).toHaveBeenCalledTimes(1);
  });

  it('drops an entry it cannot parse without inventing a stop', async () => {
    const redis = createRedis({
      workers: { broken: 'not json', old: entry('never') },
    });

    const result = await readWorkerPresence(redis, 'emails', { now: T0 });

    expect(result).toEqual({ count: 0 });
    expect(redis.hdel).toHaveBeenCalledWith(
      'bull:emails:qb:workers',
      'broken',
      'old',
    );
    expect(redis.set).not.toHaveBeenCalled();
  });

  it('reads under a custom prefix', async () => {
    const redis = createRedis();

    await readWorkerPresence(redis, 'emails', { prefix: 'app' });

    expect(redis.hgetall).toHaveBeenCalledWith('app:emails:qb:workers');
    expect(redis.get).toHaveBeenCalledWith('app:emails:qb:last-stop');
  });
});

describe('WorkerPresence', () => {
  afterEach(() => {
    jest.useRealTimers();
  });

  function createPresence(
    redis: ReturnType<typeof createRedis> | null,
    overrides: Partial<ConstructorParameters<typeof WorkerPresence>[0]> = {},
  ) {
    let tick = 0;
    return new WorkerPresence({
      queueName: 'emails',
      workerId: 'api-1:42:abc',
      host: 'api-1',
      pid: 42,
      getClient: () => redis,
      now: () => new Date(T0 + tick++ * 1_000),
      counters: () => ({ jobsProcessed: 5, lastJobAt: null }),
      ...overrides,
    });
  }

  it('registers on start and heartbeats on the interval', async () => {
    jest.useFakeTimers();
    const redis = createRedis();
    const presence = createPresence(redis, { heartbeatIntervalMs: 1_000 });

    await presence.start();

    expect(presence.isActive).toBe(true);
    expect(redis.hset).toHaveBeenCalledTimes(1);
    const [, field, value] = redis.hset.mock.calls[0];
    expect(field).toBe('api-1:42:abc');
    expect(JSON.parse(value)).toMatchObject({
      host: 'api-1',
      pid: 42,
      jobsProcessed: 5,
      startedAt: '2026-09-19T12:00:00.000Z',
    });

    await jest.advanceTimersByTimeAsync(2_000);
    expect(redis.hset).toHaveBeenCalledTimes(3);

    await presence.recordStop(WorkerStopReason.Shutdown);
    await jest.advanceTimersByTimeAsync(5_000);
    expect(redis.hset).toHaveBeenCalledTimes(3);
  });

  it('records a stop in one transaction and reports it once', async () => {
    const redis = createRedis();
    const presence = createPresence(redis);
    await presence.start();

    const record = await presence.recordStop(WorkerStopReason.LostConnection);

    expect(record).toEqual({
      workerId: 'api-1:42:abc',
      host: 'api-1',
      reason: WorkerStopReason.LostConnection,
      description: 'Lost connection to Redis',
      at: expect.any(String),
      jobsProcessed: 5,
      lastJobAt: null,
    });
    expect(redis.multi).toHaveBeenCalledTimes(1);
    expect(redis.strings.get(redis.keys.lastStop)).toBe(JSON.stringify(record));
    expect(redis.hashes.get(redis.keys.workers)?.has('api-1:42:abc')).toBe(
      false,
    );
    expect(presence.isActive).toBe(false);

    await expect(
      presence.recordStop(WorkerStopReason.Shutdown),
    ).resolves.toBeNull();
    expect(redis.multi).toHaveBeenCalledTimes(1);
  });

  it('replaces a recorded stop only when asked to', async () => {
    const redis = createRedis();
    const presence = createPresence(redis);
    await presence.start();
    await presence.recordStop(WorkerStopReason.Error);

    const record = await presence.recordStop(WorkerStopReason.RecoveryFailed, {
      replace: true,
    });

    expect(record?.reason).toBe(WorkerStopReason.RecoveryFailed);
    expect(redis.strings.get(redis.keys.lastStop)).toContain(
      '"recovery_failed"',
    );
  });

  it('re-registers when started again after a stop', async () => {
    const redis = createRedis();
    const presence = createPresence(redis);
    await presence.start();
    await presence.recordStop(WorkerStopReason.Error);

    await presence.start();

    expect(presence.isActive).toBe(true);
    expect(redis.hashes.get(redis.keys.workers)?.has('api-1:42:abc')).toBe(
      true,
    );
    await presence.recordStop(WorkerStopReason.Shutdown);
  });

  it('still returns the record when there is no usable client', async () => {
    const presence = createPresence(null);
    await presence.start();

    const record = await presence.recordStop(WorkerStopReason.Closed);

    expect(record?.reason).toBe(WorkerStopReason.Closed);
  });

  it('reports a failed write and keeps going', async () => {
    const redis = createRedis();
    redis.hset.mockRejectedValueOnce(new Error('READONLY'));
    const onError = jest.fn();
    const presence = createPresence(redis, { onError });

    await presence.start();

    expect(onError).toHaveBeenCalledWith(expect.any(Error));
    expect(presence.isActive).toBe(true);
    await presence.recordStop(WorkerStopReason.Shutdown);
  });

  it('resolves a client promise', async () => {
    const redis = createRedis();
    const presence = createPresence(null, {
      getClient: () => Promise.resolve(redis),
    });

    await presence.start();

    expect(redis.hset).toHaveBeenCalledTimes(1);
    await presence.recordStop(WorkerStopReason.Shutdown);
  });
});
