import {
  InjectionToken,
  OptionalFactoryDependency,
  Type,
} from '@nestjs/common';
import { ModuleMetadata } from '@nestjs/common/interfaces';
import type { Queue } from 'bullmq';
import type { RedisOptions } from 'ioredis';

/**
 * Duration statistics from job processing
 */
export interface DurationStats {
  avgMs: number | undefined;
  minMs: number | undefined;
  maxMs: number | undefined;
  p50Ms: number | undefined;
  p95Ms: number | undefined;
  p99Ms: number | undefined;
  recentAvgMs: number | undefined;
  sampleCount: number;
}

/**
 * Job processing statistics
 */
export interface JobStats {
  processed: number;
  completed: number;
  failed: number;
  /** Fraction of finished jobs that failed, from 0 to 1. */
  failureRate: number;
  /** Fraction of finished jobs that completed, from 0 to 1. */
  successRate: number;
  lastJobTime: string | null;
}

/**
 * Throughput statistics
 */
export interface ThroughputStats {
  jobsPerMinute: number;
  windowStartTime: string;
  jobsInWindow: number;
}

/**
 * Cache statistics (optional, for processors that use caching)
 */
export interface CacheStats {
  size: number;
  hitRate: string;
  hits: number;
  misses: number;
  batchHits?: number;
  batchMisses?: number;
}

/**
 * Configuration for a cache that can be managed by Queuebert.
 * Processors return this from getCacheConfigs() to register their caches.
 */
export interface QueuebertCacheConfig {
  /** Unique identifier for this cache (e.g., 'domains', 'socialProfiles') */
  id: string;
  /** Human-readable label for display (e.g., 'Domain Cache') */
  label?: string;
  /** Redis key prefix (e.g., 'cache:queuebert:domains') */
  keyPrefix: string;
  /** Reference to the cache instance for stats and operations */
  cache: QueuebertManagedCache;
}

/**
 * Interface that caches must implement to be managed by Queuebert.
 * This allows Queuebert to collect stats and perform migrations.
 */
export interface QueuebertManagedCache {
  /** Get current cache size (L1 entries) */
  readonly size: number;
  /** Get cache statistics */
  getStats(): {
    l1: {
      hits: number;
      misses: number;
      size: number;
      hitRate: string;
      evictions: number;
      maxSize: number;
      utilizationPercent: string;
    };
    l2: {
      hits: number;
      misses: number;
      errors: number;
      hitRate: string;
    };
  };
  /** Get the Redis key prefix */
  getKeyPrefix(): string;
  /** Get the L2 TTL in seconds */
  getL2TtlSeconds(): number;
}

/**
 * Cache instance statistics returned in API responses
 */
export interface CacheInstanceStats {
  /** Cache identifier */
  id: string;
  /** Human-readable label */
  label: string;
  /** Redis key prefix */
  keyPrefix: string;
  /** Redis instance ID this cache is on */
  redis: string;
  /** Processor/queue this cache belongs to */
  processor: string;
  /** L1 (in-memory) cache stats */
  l1: {
    size: number;
    maxSize: number;
    utilizationPercent: string;
    hitRate: string;
    hits: number;
    misses: number;
    evictions: number;
  };
  /** L2 (Redis) cache stats */
  l2: {
    hitRate: string;
    hits: number;
    misses: number;
    errors: number;
    /** Estimated key count in Redis (from last scan, if available) */
    keyCount?: number;
  };
  /** Combined hit rate across L1 and L2 */
  combinedHitRate: string;
}

/**
 * Interface that processors must implement to provide stats to Queuebert
 */
export interface QueuebertProcessorStats {
  duration: DurationStats;
  jobs: JobStats;
  throughput: ThroughputStats;
  jobsByType?: Record<
    string,
    { processed: number; completed: number; failed: number }
  >;
  cache?: Record<string, CacheStats>;
  /** Custom stats specific to the processor */
  custom?: Record<string, unknown>;
}

/**
 * Interface that processors must implement to be compatible with Queuebert
 */
export interface QueuebertProcessor {
  getProcessorStats(): QueuebertProcessorStats;
  /**
   * Optional: Return cache configurations for Queuebert to manage.
   * Enables cache stats in API responses and cache migration features.
   */
  getCacheConfigs?(): QueuebertCacheConfig[];
}

/**
 * Queue counts from BullMQ
 */
export interface QueueCounts {
  waiting: number;
  active: number;
  completed: number;
  failed: number;
  delayed: number;
  total: number;
}

/**
 * Performance trend indicator
 */
export type PerformanceTrend =
  | 'catching_up'
  | 'falling_behind'
  | 'stable'
  | 'idle'
  | 'paused';

/**
 * Health status indicator
 */
export type HealthStatus = 'excellent' | 'good' | 'fair' | 'poor' | 'critical';

/**
 * Performance metrics for a queue
 */
export interface PerformanceMetrics {
  score: number;
  trend: PerformanceTrend;
  throughputPerMin: number;
  addedPerMin: number;
  deltaPerMin: number;
  estimatedClearTimeMs: number | null;
  health: HealthStatus;
}

/**
 * Job metrics for a queue
 */
export interface QueueJobMetrics {
  duration: Omit<DurationStats, 'sampleCount'>;
  failureRate: number;
  successRate: number;
  processed: number;
  completed: number;
  failed: number;
  byType?: Record<
    string,
    { processed: number; completed: number; failed: number }
  >;
  lastJobTime: string | null;
  sampleCount: number;
  performance: PerformanceMetrics;
}

/**
 * Job type statistics discovered from the queue
 * This provides counts of jobs by type in different states
 */
export interface QueueJobTypeStats {
  /** Number of waiting jobs of this type */
  waiting: number;
  /** Number of active jobs of this type */
  active: number;
  /** Number of delayed jobs of this type */
  delayed: number;
  /** Number of completed jobs of this type (from processor stats, not queue) */
  completed?: number;
  /** Number of failed jobs of this type (from processor stats, not queue) */
  failed?: number;
}

/**
 * Job types discovery result
 */
export interface JobTypesDiscovery {
  /** Job type stats keyed by job name */
  types: Record<string, QueueJobTypeStats>;
  /** Whether the counts are sampled (true) or complete (false) */
  sampled: boolean;
  /** Sample size used if sampled */
  sampleSize?: number;
}

/**
 * Stats for a single queue
 */
export interface SingleQueueStats {
  name: string;
  /** Redis instance ID this queue belongs to */
  redis: string;
  paused: boolean;
  counts: QueueCounts;
  jobMetrics: QueueJobMetrics;
  throughput: ThroughputStats;
  /** Discovered job types in the queue */
  jobTypes?: JobTypesDiscovery;
  custom?: Record<string, unknown>;
}

/**
 * Redis memory statistics
 */
export interface RedisMemoryStats {
  used: number;
  usedHuman: string;
  peak: number;
  peakHuman: string;
  rss: number;
  rssHuman: string;
  maxmemory: number;
  maxmemoryHuman: string;
  maxmemoryPolicy: string;
  fragmentationRatio: number;
}

/**
 * Configuration for a Redis instance in multi-Redis setups
 */
export interface RedisInstanceConfig {
  /** Unique identifier for this Redis instance */
  id: string;
  /** Human-readable label for display (e.g., "Primary (us-east)") */
  label?: string;
  /**
   * Redis connection options.
   * If not provided, the connection will be inferred from the first queue
   * assigned to this instance.
   */
  connection?: RedisOptions;
}

/**
 * Stats for a single Redis instance (used in multi-Redis responses)
 */
export interface RedisInstanceStats {
  /** Unique identifier for this Redis instance */
  id: string;
  /** Human-readable label */
  label: string;
  /** Memory statistics */
  memory: RedisMemoryStats;
  /** Memory usage as percentage (or 'unlimited') */
  usagePercent: string;
}

/**
 * Available Queuebert endpoint types
 * Note: 'stats' is always enabled and cannot be disabled
 */
export type QueuebertEndpoint =
  | 'stats' // GET /stats - read queue statistics (always enabled)
  | 'metrics' // GET /:queue/metrics - detailed metrics for a queue
  | 'pause' // POST /pause or /:queue/pause - pause queue(s)
  | 'resume' // POST /resume or /:queue/resume - resume queue(s)
  | 'clean' // POST /:queue/clean - remove old completed/failed jobs
  | 'drain' // POST /:queue/drain - remove all waiting jobs
  | 'jobs' // GET /:queue/jobs - read-only job inspection (off by default)
  | 'migrations'; // GET/POST /migrations - migrate jobs between queues/redis instances

/**
 * Optional endpoints that can be enabled/disabled
 * Note: 'stats' is always enabled and not included here
 */
export type OptionalQueuebertEndpoint = Exclude<QueuebertEndpoint, 'stats'>;

/**
 * All optional endpoints (for defaults)
 */
export const ALL_OPTIONAL_ENDPOINTS: OptionalQueuebertEndpoint[] = [
  'metrics',
  'pause',
  'resume',
  'clean',
  'drain',
  'jobs',
  'migrations',
];

/**
 * Read-only optional endpoints (safe to expose publicly).
 *
 * This doubles as the default when `endpoints` is not configured, so anything
 * added here is enabled for every existing consumer on upgrade.
 *
 * 'jobs' is deliberately excluded even though it is read-only: 'metrics'
 * exposes aggregates, whereas 'jobs' exposes per-job identifiers, failure
 * reasons and stack traces. Those are not the same risk class, so job
 * inspection must be opted into explicitly.
 */
export const READONLY_OPTIONAL_ENDPOINTS: OptionalQueuebertEndpoint[] = [
  'metrics',
];

/**
 * Endpoint capabilities metadata included in stats response
 */
export interface QueuebertCapabilities {
  /** List of available endpoints */
  endpoints: QueuebertEndpoint[];
  /** Whether pause/resume operations are available */
  canPause: boolean;
  /** Whether clean operation is available */
  canClean: boolean;
  /** Whether drain operation is available */
  canDrain: boolean;
  /** Whether read-only job inspection is available */
  canInspectJobs: boolean;
  /**
   * Whether job inspection responses include raw payloads (`data`) and handler
   * return values. False means those fields are withheld, not that they are
   * empty.
   */
  canInspectJobData: boolean;
  /** Whether migration operations are available */
  canMigrate: boolean;
  /** Whether cache migration operations are available */
  canMigrateCache: boolean;
}

/**
 * Integration package information
 */
export interface IntegrationPackage {
  /** Package name (e.g., '@queuebert/nest') */
  name: string;
  /** Current installed version */
  version: string;
  /** Optional description of the package */
  description?: string;
  /** Optional URL for more info or updates */
  url?: string;
}

/**
 * Multi-queue stats response
 */
export interface MultiQueueStats {
  queues: Record<string, SingleQueueStats>;
  /**
   * Redis instance statistics keyed by instance ID.
   * Single Redis uses 'default' as the instance ID.
   */
  redis?: Record<string, RedisInstanceStats>;
  /**
   * Cache instance statistics keyed by cache ID.
   * Only present if processors implement getCacheConfigs().
   */
  caches?: Record<string, CacheInstanceStats>;
  /** Available endpoints and capabilities */
  capabilities: QueuebertCapabilities;
  /** Installed integration packages */
  integrations?: IntegrationPackage[];
  /** Per-queue collection failures; healthy queues are still returned. */
  errors?: Record<string, string>;
  timestamp: string;
}

/**
 * Default Redis instance ID when no multi-Redis config is provided
 */
export const DEFAULT_REDIS_INSTANCE_ID = 'default';
export const DEFAULT_REDIS_INSTANCE_LABEL = 'Default';

/**
 * Clean operation result
 */
export interface CleanResult {
  cleaned: {
    completed: number;
    failed: number;
    total: number;
  };
  before: QueueCounts;
  after: QueueCounts;
  options: {
    graceMs: number;
    maxJobs: number;
  };
}

/**
 * Drain operation result
 */
export interface DrainResult {
  drained: number;
  before: QueueCounts;
  after: QueueCounts;
}

/**
 * Status change result (pause/resume)
 */
export interface StatusResult {
  status: 'paused' | 'resumed';
  reason?: string;
  queues?: string[];
  timestamp: string;
}

/**
 * Configuration for a single queue in Queuebert
 */
export interface QueuebertQueueConfig {
  /** The queue name (must match BullMQ queue name) */
  name: string;
  /**
   * The processor that handles this queue's jobs.
   * Optional for monitoring-only queues (e.g., migration targets).
   * When omitted, the queue will still be monitored but job metrics
   * from the processor will not be available.
   */
  processor?: Type<QueuebertProcessor>;
  /** Optional custom stats key in the response (defaults to queue name) */
  statsKey?: string;
  /**
   * Redis instance ID this queue belongs to (for multi-Redis setups).
   * Must match an id in the `redis` array configuration.
   * If not specified and multi-Redis is configured, defaults to the first Redis instance.
   */
  redis?: string;
  /**
   * Direct Queue instance to use instead of resolving from NestJS DI.
   * Useful for monitoring queues that:
   * - Have the same name on different Redis instances
   * - Weren't registered via BullModule
   * - Need custom connection options
   *
   * When provided, the `name` field is still used for display purposes,
   * but the queue is not looked up from the DI container.
   *
   * @example
   * ```typescript
   * // Create a queue instance for monitoring orphaned jobs on old Redis
   * const oldRedisQueue = new Queue('jobs', { connection: oldRedisConnection })
   *
   * QueuebertModule.forRoot({
   *   queues: [
   *     { name: 'jobs', processor: JobProcessor },  // Current queue from DI
   *     { name: 'jobs', queue: oldRedisQueue, statsKey: 'jobs-old', redis: 'old' },  // Old Redis
   *   ],
   *   redis: [
   *     { id: 'current', label: 'Current Redis' },
   *     { id: 'old', label: 'Old Redis (migration source)' },
   *   ],
   * })
   * ```
   */
  queue?: Queue;
}

/**
 * Auth configuration for Queuebert endpoints
 */
export interface QueuebertAuthConfig {
  /** Guards to apply to all Queuebert endpoints */
  guards?: Type<unknown>[];
  /** Custom decorator to apply to the controller class (e.g., for roles) */
  decorators?: ClassDecorator[];
}

/**
 * Module configuration options for QueuebertModule.forRoot()
 */
export interface QueuebertModuleOptions {
  /**
   * The base path for all queue endpoints (default: 'admin/queue')
   * Note: Do not include leading slash
   */
  path?: string;

  /**
   * Redis instances to monitor (for multi-Redis setups).
   * If not specified, Redis connection is inferred from the first queue.
   *
   * @example
   * // Monitor multiple Redis instances
   * redis: [
   *   { id: 'primary', label: 'Primary (us-east)' },
   *   { id: 'secondary', label: 'Backup (us-west)', connection: backupRedisOptions },
   * ]
   *
   * @example
   * // Migration scenario - same queue on different Redis
   * redis: [
   *   { id: 'current', label: 'Current Redis' },
   *   { id: 'target', label: 'Migration Target', connection: newRedisOptions },
   * ],
   * queues: [
   *   { name: 'jobs', processor: JobProcessor, redis: 'current' },
   *   { name: 'jobs', processor: JobProcessor, redis: 'target', statsKey: 'jobs-new' },
   * ]
   */
  redis?: RedisInstanceConfig[];

  /** Queues to manage with Queuebert */
  queues: QueuebertQueueConfig[];

  /** Optional auth configuration */
  auth?: QueuebertAuthConfig;

  /**
   * Whether to include Redis memory stats in responses (default: true)
   * Disable if you don't want to expose Redis info
   */
  includeRedisStats?: boolean;

  /**
   * Optional endpoints to enable (default: read-only metrics only)
   * Note: 'stats' is always enabled and cannot be disabled
   *
   * @example
   * // Read-only mode - only stats and metrics
   * endpoints: ['metrics']
   *
   * @example
   * // Add read-only job inspection for debugging failed jobs
   * endpoints: ['metrics', 'jobs']
   *
   * @example
   * // Full control except drain
   * endpoints: ['metrics', 'pause', 'resume', 'clean']
   */
  endpoints?: OptionalQueuebertEndpoint[];

  /**
   * Integration packages to report in the stats response.
   * This allows client apps to see what packages are installed
   * and potentially notify when updates are available.
   *
   * @example
   * integrations: [
   *   { name: '@queuebert/nest', version: '0.0.1' },
   *   { name: '@queuebert/bullmq', version: '0.0.1' },
   * ]
   */
  integrations?: IntegrationPackage[];

  /**
   * Configuration for job type discovery.
   * When enabled, Queuebert will sample jobs from the queue to discover
   * what job types exist and their counts in different states.
   *
   * @default { enabled: true, sampleSize: 100, threshold: 1000 }
   */
  jobTypeDiscovery?: {
    /**
     * Whether to enable job type discovery (default: true)
     */
    enabled?: boolean;
    /**
     * Maximum number of jobs to sample per state (waiting, active, delayed)
     * Lower values = faster but less accurate for large queues
     * @default 100
     */
    sampleSize?: number;
    /**
     * If total jobs in a state exceeds this threshold, use sampling
     * Otherwise, count all jobs for accurate counts
     * @default 1000
     */
    threshold?: number;
  };

  /**
   * Include raw BullMQ job payloads (`data`) and handler return values
   * (`returnvalue`) in job inspection responses and migration preview
   * sampleJobs.
   *
   * Disabled by default because job data commonly contains sensitive
   * application-specific values. Migration execution is unaffected either way:
   * it copies original job data by reading each job directly from BullMQ.
   */
  includeJobData?: boolean;

  /**
   * @deprecated Use `includeJobData`, which governs both job inspection and
   * migration preview. Retained as an alias so existing configuration keeps
   * working; `includeJobData` wins when both are set.
   */
  includeJobDataInMigrationPreview?: boolean;

  /**
   * Optional hook to scrub job info before it leaves the API.
   *
   * Runs on every job returned by the 'jobs' endpoint and by migration
   * preview, after the `includeJobData` tier has been applied. Use it to run
   * an application-specific scrubber over `failedReason`, `stacktrace` or an
   * opted-in `data` payload.
   *
   * The hook must be synchronous and should not throw. If it does throw, the
   * job is reduced to its identity fields (`id`, `name`, `state`) rather than
   * being returned unscrubbed.
   */
  jobRedaction?: (info: JobInfo) => JobInfo;
}

/**
 * Async module configuration options
 */
export interface QueuebertModuleAsyncOptions extends Pick<
  ModuleMetadata,
  'imports'
> {
  /**
   * Static controller path for async registration.
   *
   * NestJS controller metadata must be known when the dynamic module is created,
   * so async factories cannot set the route path after DI has resolved.
   */
  path?: string;
  /** Static auth configuration applied to the generated controller. */
  auth?: QueuebertAuthConfig;
  useFactory: (
    ...args: unknown[]
  ) => Promise<QueuebertModuleOptions> | QueuebertModuleOptions;
  inject?: (InjectionToken | OptionalFactoryDependency)[];
}

/**
 * Counter-based rate sample stored in Redis
 */
export interface RateSample {
  completed: number;
  failed: number;
  backlog: number;
  timestamp: number;
}

/**
 * Job states that can be listed through the job inspection endpoint.
 *
 * These map one-to-one onto BullMQ's own per-state getters. Note that BullMQ
 * orders each state differently: 'completed' and 'failed' come back
 * newest-first, every other state oldest-first. Pagination via start/end
 * follows whatever order BullMQ uses for the requested state.
 */
export type InspectableJobState =
  | 'waiting'
  | 'waiting-children'
  | 'active'
  | 'delayed'
  | 'prioritized'
  | 'completed'
  | 'failed';

/**
 * All states accepted by the job inspection endpoint
 */
export const INSPECTABLE_JOB_STATES: InspectableJobState[] = [
  'waiting',
  'waiting-children',
  'active',
  'delayed',
  'prioritized',
  'completed',
  'failed',
];

/**
 * Job state for migration operations
 */
export type MigrationJobState = 'waiting' | 'delayed' | 'failed';

/**
 * A single job as reported by job inspection and migration preview.
 *
 * Privacy tiers:
 * - Identity, timing and attempt fields are always present.
 * - `failedReason` and `stacktrace` are present when BullMQ has them; enabling
 *   the 'jobs' endpoint is itself the opt-in for those.
 * - `data` and `returnvalue` are withheld unless `includeJobData` is set.
 */
export interface JobInfo {
  id: string;
  name: string;
  /**
   * The state the job was read from. 'unknown' is only possible from the
   * single-job route, where BullMQ reports the state it resolved.
   */
  state: InspectableJobState | 'unknown';
  /** When the job was created (ms since epoch) */
  timestamp?: number;
  /** When the job was picked up by a worker (ms since epoch) */
  processedOn?: number;
  /** When the job completed or failed (ms since epoch) */
  finishedOn?: number;
  /**
   * Attempts BullMQ has recorded as failed. During a failure this still holds
   * the count *before* the current attempt; see `maxAttempts`.
   */
  attemptsMade?: number;
  /** Configured attempt ceiling (`opts.attempts`), defaulting to 1 */
  maxAttempts?: number;
  /** Failure reason recorded by BullMQ */
  failedReason?: string;
  /** Stack traces recorded by BullMQ, one entry per failed attempt */
  stacktrace?: string[];
  /** Progress reported by the handler */
  progress?: unknown;
  /** Configured delay in ms */
  delay?: number;
  /**
   * Job payload. Withheld unless `includeJobData` is set, because job data
   * commonly contains application PII or secrets.
   */
  data?: unknown;
  /**
   * Handler return value. Withheld unless `includeJobData` is set, for the
   * same reason as `data`.
   */
  returnvalue?: unknown;
}

/**
 * Options for listing jobs from a queue
 */
export interface ListJobsOptions {
  /** State to read from (default: 'failed') */
  state?: InspectableJobState;
  /**
   * Only return jobs whose name matches. Applied *after* the start/end window
   * is read from Redis, so it filters within the page rather than across the
   * whole state.
   */
  jobType?: string;
  /** Zero-based inclusive start index (default: 0) */
  start?: number;
  /** Zero-based inclusive end index, -1 for all (default: -1) */
  end?: number;
}

/**
 * Individual job info for migration preview
 */
export interface MigrationJobInfo extends JobInfo {
  state: MigrationJobState;
  /** For delayed jobs, the remaining delay in ms */
  remainingDelayMs?: number;
}

/**
 * Paginated response for GET /:queue/jobs
 */
export interface JobListResult {
  /** Queue this page was read from (the configured stats key) */
  queue: string;
  /** State the jobs were read from */
  state: InspectableJobState;
  jobs: JobInfo[];
  /**
   * Total jobs in this state for the queue. This counts the whole state and
   * therefore ignores `jobType` — see `jobTypeFilter`.
   */
  total: number;
  /** Zero-based inclusive start index of this page */
  start: number;
  /** Zero-based inclusive end index of this page */
  end: number;
  /**
   * Set when a jobType filter was applied. While present, `jobs` has been
   * filtered within this page only and `total` does not reflect the filter, so
   * `jobs.length` can be smaller than the page size without the page being the
   * last one.
   */
  jobTypeFilter?: string;
  timestamp: string;
}

/**
 * A single job response for GET /:queue/jobs/:jobId
 */
export interface JobDetailResult {
  queue: string;
  job: JobInfo;
  timestamp: string;
}

/**
 * Migration preview result
 */
export interface MigrationPreview {
  /** Migration parameters used for this preview */
  params: {
    sourceQueue: string;
    sourceRedis: string;
    targetQueue: string;
    targetRedis: string;
    jobType?: string;
    states: MigrationJobState[];
    batchSize?: number;
    delayBetweenBatchesMs?: number;
  };
  /** Total number of jobs that would be migrated */
  jobsToMigrate: number;
  /** Counts by job state */
  byState: Record<MigrationJobState, number>;
  /** Counts by job type/name */
  byJobType: Record<string, number>;
  /** Sample jobs that would be migrated (limited for preview) */
  sampleJobs: MigrationJobInfo[];
  /** Whether this is a cross-Redis migration */
  isCrossRedis: boolean;
  /** Estimated duration in milliseconds (based on batch size and delay) */
  estimatedDurationMs?: number;
  timestamp: string;
}

/**
 * Migration execution result
 */
export interface MigrationResult {
  /** Source queue name */
  sourceQueue: string;
  /** Source Redis instance ID */
  sourceRedis: string;
  /** Target queue name */
  targetQueue: string;
  /** Target Redis instance ID */
  targetRedis: string;
  /** Job type/name filter applied */
  jobType?: string;
  /** Number of jobs migrated by state */
  migrated: {
    waiting: number;
    delayed: number;
    failed: number;
    total: number;
  };
  /** Errors encountered during migration */
  errors: Array<{
    jobId: string;
    error: string;
  }>;
  /** Whether this was a cross-Redis migration */
  isCrossRedis: boolean;
  /** Rate limiting applied (if any) */
  rateLimit?: {
    batchSize: number;
    delayBetweenBatchesMs: number;
    batchesProcessed: number;
  };
  /** Total duration of migration in milliseconds */
  durationMs: number;
  timestamp: string;
}

/**
 * Migration request parameters
 */
export interface MigrationParams {
  /** Source queue name (required) */
  sourceQueue: string;
  /** Target queue name (required) */
  targetQueue: string;
  /** Source Redis instance ID (defaults to 'default') */
  sourceRedis?: string;
  /** Target Redis instance ID (defaults to 'default') */
  targetRedis?: string;
  /** Job type/name to filter (optional - migrates all if not specified) */
  jobType?: string;
  /**
   * Job states to migrate (defaults to waiting and delayed).
   * Failed jobs are opt-in and are requeued as new waiting jobs because BullMQ
   * does not expose a supported API for recreating a job in the failed state.
   */
  states?: MigrationJobState[];
  /** Maximum number of jobs to migrate (optional safety limit) */
  limit?: number;
  /** Number of jobs to process per batch (defaults to all at once) */
  batchSize?: number;
  /** Delay in milliseconds between batches (defaults to 0) */
  delayBetweenBatchesMs?: number;
}

/**
 * Migration execution status
 */
export type MigrationStatusType =
  | 'pending'
  | 'running'
  | 'paused'
  | 'completed'
  | 'failed'
  | 'cancelled';

/**
 * Migration progress information
 */
export interface MigrationProgress {
  /** Total jobs to migrate (estimated from preview) */
  total: number;
  /** Jobs processed so far */
  processed: number;
  /** Percentage complete (0-100) */
  percent: number;
  /** Current batch number (if using batching) */
  currentBatch?: number;
  /** Total batches (estimated, if using batching) */
  totalBatches?: number;
  /** Jobs migrated by state */
  byState: {
    waiting: number;
    delayed: number;
    failed: number;
  };
}

/**
 * Active migration status (returned by status endpoint)
 */
export interface MigrationStatus {
  /** Unique migration ID */
  migrationId: string;
  /** Current status */
  status: MigrationStatusType;
  /** Migration parameters */
  params: MigrationParams;
  /** Progress information (available when running) */
  progress?: MigrationProgress;
  /** Error message (if failed) */
  error?: string;
  /** Errors encountered during migration */
  errors: Array<{ jobId: string; error: string }>;
  /** Whether this is a cross-Redis migration */
  isCrossRedis: boolean;
  /** Rate limiting config (if any) */
  rateLimit?: {
    batchSize: number;
    delayBetweenBatchesMs: number;
  };
  /** When the migration was started */
  startedAt: string;
  /** When the migration was last updated */
  updatedAt: string;
  /** When the migration completed (if completed/failed/cancelled) */
  completedAt?: string;
  /** Final result (available when completed) */
  result?: MigrationResult;
}

/**
 * Response when starting a migration
 */
export interface MigrationStartResponse {
  /** Unique migration ID for tracking */
  migrationId: string;
  /** Initial status */
  status: MigrationStatusType;
  /** Message */
  message: string;
  /** Timestamp */
  timestamp: string;
}

/**
 * Response when cancelling a migration
 */
export interface MigrationCancelResponse {
  /** Migration ID */
  migrationId: string;
  /** Whether cancellation was successful */
  success: boolean;
  /** Message */
  message: string;
  /** Timestamp */
  timestamp: string;
}

/**
 * Response when pausing/resuming a migration
 */
export interface MigrationPauseResumeResponse {
  /** Migration ID */
  migrationId: string;
  /** Whether the operation was successful */
  success: boolean;
  /** New status after operation */
  status: MigrationStatusType;
  /** Message */
  message: string;
  /** Timestamp */
  timestamp: string;
}

// ============================================
// Cache Migration Types
// ============================================

/**
 * Cache migration request parameters
 */
export interface CacheMigrationParams {
  /** Source cache ID (from getCacheConfigs) */
  sourceCache: string;
  /** Source Redis instance ID */
  sourceRedis: string;
  /** Target Redis instance ID */
  targetRedis: string;
  /** Optional: only migrate keys matching this pattern (glob) */
  keyPattern?: string;
  /** Number of keys to process per batch (default: 1000) */
  batchSize?: number;
  /** Delay in milliseconds between batches (default: 100) */
  delayBetweenBatchesMs?: number;
  /** Maximum number of keys to migrate (optional safety limit) */
  limit?: number;
}

/**
 * Cache migration preview result
 */
export interface CacheMigrationPreview {
  /** Source cache ID */
  sourceCache: string;
  /** Source Redis instance ID */
  sourceRedis: string;
  /** Target Redis instance ID */
  targetRedis: string;
  /** Key prefix being migrated */
  keyPrefix: string;
  /** Number of keys found in source */
  keyCount: number;
  /** Sample keys (first N) */
  sampleKeys: string[];
  /** Estimated migration time in ms (based on key count and batch settings) */
  estimatedDurationMs: number;
  /** Whether source and target are different Redis instances */
  isCrossRedis: boolean;
  timestamp: string;
}

/**
 * Cache migration progress information
 */
export interface CacheMigrationProgress {
  /** Total keys to migrate */
  total: number;
  /** Keys processed so far */
  processed: number;
  /** Keys successfully migrated */
  migrated: number;
  /** Keys that failed */
  failed: number;
  /** Percentage complete (0-100) */
  percent: number;
  /** Current batch number */
  currentBatch?: number;
  /** Total batches */
  totalBatches?: number;
}

/**
 * Cache migration status
 */
export interface CacheMigrationStatus {
  /** Unique migration ID */
  migrationId: string;
  /** Current status */
  status: MigrationStatusType;
  /** Migration parameters */
  params: CacheMigrationParams;
  /** Key prefix being migrated */
  keyPrefix: string;
  /** Progress information */
  progress?: CacheMigrationProgress;
  /** Error message (if failed) */
  error?: string;
  /** Individual key errors */
  errors: Array<{ key: string; error: string }>;
  /** Whether this is a cross-Redis migration */
  isCrossRedis: boolean;
  /** Rate limiting config */
  rateLimit?: {
    batchSize: number;
    delayBetweenBatchesMs: number;
  };
  /** When the migration was started */
  startedAt: string;
  /** When the migration was last updated */
  updatedAt: string;
  /** When the migration completed */
  completedAt?: string;
}

/**
 * Cache migration result
 */
export interface CacheMigrationResult {
  /** Source cache ID */
  sourceCache: string;
  /** Source Redis instance ID */
  sourceRedis: string;
  /** Target Redis instance ID */
  targetRedis: string;
  /** Key prefix migrated */
  keyPrefix: string;
  /** Number of keys migrated */
  migrated: number;
  /** Number of keys that failed */
  failed: number;
  /** Errors encountered */
  errors: Array<{ key: string; error: string }>;
  /** Total duration in milliseconds */
  durationMs: number;
  timestamp: string;
}

/**
 * Injection tokens for Queuebert
 */
export const QUEUEBERT_OPTIONS = 'QUEUEBERT_OPTIONS';
export const QUEUEBERT_QUEUES = 'QUEUEBERT_QUEUES';
export const QUEUEBERT_REDIS_INSTANCES = 'QUEUEBERT_REDIS_INSTANCES';
export const QUEUEBERT_INTEGRATION_REGISTRY = 'QUEUEBERT_INTEGRATION_REGISTRY';
