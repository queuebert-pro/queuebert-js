import { hostname } from 'os';

import { QueueWorkersStats, WorkerStopReason, WorkerStopRecord } from './types';

/** How often a live worker refreshes its presence entry. */
export const WORKER_HEARTBEAT_INTERVAL_MS = 15_000;

/**
 * A presence entry older than this is treated as a dead worker: the process
 * was killed before it could record a stop.
 */
export const WORKER_PRESENCE_STALE_MS = 60_000;

/** A worker error this recent explains a close that followed it. */
const RECENT_ERROR_WINDOW_MS = 10_000;

/**
 * One human sentence per reason, so logs, the API and the apps say the same
 * thing.
 */
export function describeWorkerStopReason(reason: WorkerStopReason): string {
  switch (reason) {
    case WorkerStopReason.Shutdown:
      return 'Process shutting down';
    case WorkerStopReason.Closed:
      return 'Closed by the application';
    case WorkerStopReason.LostConnection:
      return 'Lost connection to Redis';
    case WorkerStopReason.RecoveryFailed:
      return 'Restart attempts exhausted';
    case WorkerStopReason.Error:
      return 'Stopped after a worker error';
    case WorkerStopReason.Unknown:
    default:
      return 'Stopped without reporting why';
  }
}

/**
 * What a worker knows about itself when its `closed` event fires.
 */
export interface WorkerStopSignals {
  /** The owning process is shutting down. */
  shuttingDown?: boolean;
  /** Application code asked the worker to close. */
  closeRequested?: boolean;
  /** The Redis connection ended or is currently disconnected. */
  connectionLost?: boolean;
  /** Automatic restarts were tried and gave up. */
  recoveryExhausted?: boolean;
  /** When the worker last emitted `error` (ms since epoch), if ever. */
  lastErrorAt?: number | null;
  /** How recent an error has to be to explain the close. */
  recentErrorWindowMs?: number;
}

/**
 * Pick the reason a worker stopped from the signals seen around its close.
 *
 * Precedence matters when signals overlap: a shutdown wins over everything,
 * an explicit close over an incidental one, and a lost connection over a
 * generic error, since the error is usually the connection loss itself.
 */
export function classifyWorkerStop(
  signals: WorkerStopSignals,
  now: number = Date.now(),
): WorkerStopReason {
  if (signals.shuttingDown) return WorkerStopReason.Shutdown;
  if (signals.closeRequested) return WorkerStopReason.Closed;
  if (signals.connectionLost) return WorkerStopReason.LostConnection;
  if (signals.recoveryExhausted) return WorkerStopReason.RecoveryFailed;

  const window = signals.recentErrorWindowMs ?? RECENT_ERROR_WINDOW_MS;
  if (
    typeof signals.lastErrorAt === 'number' &&
    now - signals.lastErrorAt <= window
  ) {
    return WorkerStopReason.Error;
  }

  return WorkerStopReason.Unknown;
}

export interface WorkerPresenceKeys {
  /** Hash of live workers keyed by worker id. */
  workers: string;
  /** The last stop recorded for the queue. */
  lastStop: string;
}

/**
 * Presence lives under the queue's own key prefix so it is deleted with the
 * queue and never collides with BullMQ's keys, which have no `qb:` segment.
 */
export function workerPresenceKeys(
  queueName: string,
  prefix = 'bull',
): WorkerPresenceKeys {
  const base = `${prefix}:${queueName}:qb`;
  return {
    workers: `${base}:workers`,
    lastStop: `${base}:last-stop`,
  };
}

/**
 * What a live worker writes about itself on each heartbeat.
 */
export interface WorkerPresenceEntry {
  host: string;
  pid: number;
  startedAt: string;
  heartbeatAt: string;
  jobsProcessed?: number;
  lastJobAt?: string | null;
}

/**
 * The Redis commands presence needs, satisfied by ioredis and its cluster
 * client. Kept structural so tests and other clients can stand in.
 */
export interface PresenceRedis {
  hgetall(key: string): Promise<Record<string, string>>;
  hset(key: string, field: string, value: string): Promise<unknown>;
  hdel(key: string, ...fields: string[]): Promise<unknown>;
  get(key: string): Promise<string | null>;
  set(key: string, value: string): Promise<unknown>;
  multi(): PresenceMulti;
}

export interface PresenceMulti {
  set(key: string, value: string): PresenceMulti;
  hdel(key: string, ...fields: string[]): PresenceMulti;
  exec(): Promise<unknown>;
}

const PRESENCE_COMMANDS: Array<keyof PresenceRedis> = [
  'hgetall',
  'hset',
  'hdel',
  'get',
  'set',
  'multi',
];

/**
 * Whether a value is a client presence can use. Anything else, including a
 * missing client, means presence is silently skipped.
 */
export function isPresenceRedis(client: unknown): client is PresenceRedis {
  if (!client || typeof client !== 'object') return false;
  const candidate = client as Record<string, unknown>;
  return PRESENCE_COMMANDS.every(
    (command) => typeof candidate[command] === 'function',
  );
}

function parseJSON<T>(raw: string | null | undefined): T | null {
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw) as unknown;
    return parsed && typeof parsed === 'object' ? (parsed as T) : null;
  } catch {
    return null;
  }
}

function parseTime(value: unknown): number | null {
  if (typeof value !== 'string') return null;
  const time = Date.parse(value);
  return Number.isNaN(time) ? null : time;
}

export interface ReadWorkerPresenceOptions {
  /** The queue's key prefix. @default 'bull' */
  prefix?: string;
  /** The current time (ms since epoch). @default Date.now() */
  now?: number;
  /** @default WORKER_PRESENCE_STALE_MS */
  staleAfterMs?: number;
}

/**
 * Read a queue's worker presence: the live count after pruning dead
 * entries, and the last recorded stop.
 *
 * A dead entry (no heartbeat within `staleAfterMs`) is removed, and when it
 * is newer than the recorded stop it becomes the stop, with `Unknown` as the
 * reason: the process went away without saying why.
 *
 * Returns undefined when the queue has no presence at all, so callers can
 * omit the field rather than report zero workers for a queue nothing
 * Queuebert-aware has ever served.
 */
export async function readWorkerPresence(
  client: PresenceRedis,
  queueName: string,
  options: ReadWorkerPresenceOptions = {},
): Promise<QueueWorkersStats | undefined> {
  const keys = workerPresenceKeys(queueName, options.prefix);
  const now = options.now ?? Date.now();
  const staleAfterMs = options.staleAfterMs ?? WORKER_PRESENCE_STALE_MS;

  const [entries, lastStopRaw] = await Promise.all([
    client.hgetall(keys.workers),
    client.get(keys.lastStop),
  ]);

  let lastStop = parseJSON<WorkerStopRecord>(lastStopRaw);
  const workerIds = Object.keys(entries ?? {});

  if (workerIds.length === 0 && !lastStop) {
    return undefined;
  }

  let count = 0;
  const stale: string[] = [];
  let newestDead: {
    id: string;
    entry: WorkerPresenceEntry;
    at: number;
  } | null = null;

  for (const id of workerIds) {
    const entry = parseJSON<WorkerPresenceEntry>(entries[id]);
    const heartbeatAt = parseTime(entry?.heartbeatAt);

    if (!entry || heartbeatAt === null) {
      stale.push(id);
      continue;
    }

    if (now - heartbeatAt > staleAfterMs) {
      stale.push(id);
      if (!newestDead || heartbeatAt > newestDead.at) {
        newestDead = { id, entry, at: heartbeatAt };
      }
      continue;
    }

    count++;
  }

  if (stale.length > 0) {
    await client.hdel(keys.workers, ...stale);
  }

  if (newestDead) {
    const recordedAt = parseTime(lastStop?.at);
    if (recordedAt === null || recordedAt < newestDead.at) {
      lastStop = {
        workerId: newestDead.id,
        host: newestDead.entry.host,
        reason: WorkerStopReason.Unknown,
        description: describeWorkerStopReason(WorkerStopReason.Unknown),
        at: new Date(newestDead.at).toISOString(),
        jobsProcessed: newestDead.entry.jobsProcessed,
        lastJobAt: newestDead.entry.lastJobAt ?? null,
      };
      await client.set(keys.lastStop, JSON.stringify(lastStop));
    }
  }

  return lastStop ? { count, lastStop } : { count };
}

export interface WorkerPresenceOptions {
  queueName: string;
  /** The queue's key prefix. @default 'bull' */
  prefix?: string;
  /** @default `${host}:${pid}:${random}` */
  workerId?: string;
  /** @default os.hostname() */
  host?: string;
  /** @default process.pid */
  pid?: number;
  /** @default WORKER_HEARTBEAT_INTERVAL_MS */
  heartbeatIntervalMs?: number;
  /**
   * The Redis client to write through, resolved on every write so a
   * reconnected client is picked up. Anything that is not a usable client
   * skips the write.
   */
  getClient: () => unknown | Promise<unknown>;
  /** Counters to include in the presence entry and the stop record. */
  counters?: () => { jobsProcessed?: number; lastJobAt?: string | null };
  /** Called with any Redis failure; presence never throws into a worker. */
  onError?: (error: unknown) => void;
  /** @default () => new Date() */
  now?: () => Date;
}

export interface RecordStopOptions {
  /**
   * Write the stop even when one was already recorded for this presence,
   * for a later, more final verdict such as exhausted recovery.
   */
  replace?: boolean;
}

/**
 * A worker's own presence: a heartbeat while it runs and one stop record
 * when it goes, written under the queue's keys for `readWorkerPresence`.
 *
 * Every write is best effort. Presence is observability, and a worker must
 * keep processing jobs whether or not it can be seen.
 */
export class WorkerPresence {
  readonly workerId: string;
  private readonly keys: WorkerPresenceKeys;
  private readonly host: string;
  private readonly pid: number;
  private readonly heartbeatIntervalMs: number;
  private readonly now: () => Date;

  private timer: NodeJS.Timeout | null = null;
  private active = false;
  private startedAt = '';

  constructor(private readonly options: WorkerPresenceOptions) {
    this.host = options.host ?? hostname();
    this.pid = options.pid ?? process.pid;
    this.workerId =
      options.workerId ??
      `${this.host}:${this.pid}:${Math.random().toString(36).slice(2, 8)}`;
    this.keys = workerPresenceKeys(options.queueName, options.prefix);
    this.heartbeatIntervalMs =
      options.heartbeatIntervalMs ?? WORKER_HEARTBEAT_INTERVAL_MS;
    this.now = options.now ?? (() => new Date());
  }

  /** Whether the worker is currently registered as live. */
  get isActive(): boolean {
    return this.active;
  }

  /**
   * Register the worker and start the heartbeat. Calling it on an active
   * presence is a no-op; calling it after a stop re-registers, which is what
   * a recovered worker wants.
   */
  async start(): Promise<void> {
    if (this.active) return;
    this.active = true;
    this.startedAt = this.now().toISOString();

    await this.heartbeat();

    this.timer = setInterval(() => {
      void this.heartbeat();
    }, this.heartbeatIntervalMs);
    this.timer.unref?.();
  }

  /** Refresh the presence entry. */
  async heartbeat(): Promise<void> {
    if (!this.active) return;

    const client = await this.resolveClient();
    if (!client) return;

    const entry: WorkerPresenceEntry = {
      host: this.host,
      pid: this.pid,
      startedAt: this.startedAt,
      heartbeatAt: this.now().toISOString(),
      ...this.counters(),
    };

    try {
      await client.hset(
        this.keys.workers,
        this.workerId,
        JSON.stringify(entry),
      );
    } catch (error) {
      this.options.onError?.(error);
    }
  }

  /**
   * Record why the worker stopped and drop its presence entry, in one
   * transaction. Returns the record, or null when the presence had already
   * stopped and `replace` was not asked for.
   */
  async recordStop(
    reason: WorkerStopReason,
    options: RecordStopOptions = {},
  ): Promise<WorkerStopRecord | null> {
    if (!this.active && !options.replace) return null;

    this.active = false;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }

    const record: WorkerStopRecord = {
      workerId: this.workerId,
      host: this.host,
      reason,
      description: describeWorkerStopReason(reason),
      at: this.now().toISOString(),
      ...this.counters(),
    };

    const client = await this.resolveClient();
    if (client) {
      try {
        await client
          .multi()
          .set(this.keys.lastStop, JSON.stringify(record))
          .hdel(this.keys.workers, this.workerId)
          .exec();
      } catch (error) {
        this.options.onError?.(error);
      }
    }

    return record;
  }

  private counters(): { jobsProcessed?: number; lastJobAt?: string | null } {
    try {
      return this.options.counters?.() ?? {};
    } catch {
      return {};
    }
  }

  private async resolveClient(): Promise<PresenceRedis | null> {
    try {
      const client = await this.options.getClient();
      return isPresenceRedis(client) ? client : null;
    } catch {
      return null;
    }
  }
}
