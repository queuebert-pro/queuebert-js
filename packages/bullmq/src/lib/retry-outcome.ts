import { Job, UnrecoverableError } from 'bullmq';

/**
 * Where a failed attempt sits in a job's retry lifecycle.
 *
 * This mirrors BullMQ's own decision in `Job.shouldRetryJob()` so that
 * consumers do not have to re-derive "will this be retried?" themselves.
 */
export interface RetryOutcome {
  /**
   * 1-based number of the attempt that just failed.
   *
   * BullMQ compares `attemptsMade + 1` against `opts.attempts` and does not
   * increment `attemptsMade` until the job is moved to failed, so this is the
   * attempt currently in flight.
   */
  attempt: number;
  /** Configured attempt ceiling (`opts.attempts`), defaulting to 1 */
  maxAttempts: number;
  /**
   * True when BullMQ will not retry this job.
   *
   * Covers all three reasons it declines a retry: the attempt ceiling is
   * reached, the handler called `job.discard()`, or the error is an
   * `UnrecoverableError`.
   *
   * Caveat: a custom `backoffStrategy` returning -1 also stops retries, and
   * that cannot be known without running the strategy. Treat this as a lower
   * bound if you use one.
   */
  isFinalAttempt: boolean;
  /** True when the handler called `job.discard()` during this attempt */
  discarded: boolean;
  /** True when the error is (or reports itself as) an `UnrecoverableError` */
  unrecoverable: boolean;
}

/**
 * Mirrors BullMQ's own check in `Job.shouldRetryJob()`, including the name
 * fallback that catches an `UnrecoverableError` from a duplicate bullmq copy.
 *
 * The constructor is checked for callability before use: this runs on the
 * failure path, and if `UnrecoverableError` were ever missing from the
 * resolved bullmq (a duplicate install, a bundler, a test double), a bare
 * `instanceof` would throw and replace the job's real error with a TypeError.
 * The name check still catches genuine instances in that case.
 */
export function isUnrecoverableError(error: unknown): boolean {
  if (
    typeof UnrecoverableError === 'function' &&
    error instanceof UnrecoverableError
  ) {
    return true;
  }
  return (
    typeof error === 'object' &&
    error !== null &&
    (error as { name?: unknown }).name === 'UnrecoverableError'
  );
}

/**
 * Derive the retry outcome for a failed attempt.
 */
export function resolveRetryOutcome(
  job: Job,
  error: unknown,
  discarded = false,
): RetryOutcome {
  const maxAttempts = job.opts?.attempts ?? 1;
  const attempt = (job.attemptsMade ?? 0) + 1;
  const unrecoverable = isUnrecoverableError(error);

  return {
    attempt,
    maxAttempts,
    isFinalAttempt: discarded || unrecoverable || attempt >= maxAttempts,
    discarded,
    unrecoverable,
  };
}

/**
 * Observe whether a handler calls `job.discard()`.
 *
 * BullMQ keeps the resulting flag protected on its Job class, so the only way
 * to see it is to wrap the method for the duration of the attempt. `restore()`
 * puts the original back.
 */
export function trackDiscard(job: Job): {
  wasDiscarded: () => boolean;
  restore: () => void;
} {
  const original = job.discard;
  if (typeof original !== 'function') {
    return { wasDiscarded: () => false, restore: () => undefined };
  }

  let discarded = false;
  const hadOwnDiscard = Object.prototype.hasOwnProperty.call(job, 'discard');

  job.discard = function trackedDiscard(this: Job): void {
    discarded = true;
    return original.call(this);
  };

  return {
    wasDiscarded: () => discarded,
    restore: () => {
      if (hadOwnDiscard) {
        job.discard = original;
      } else {
        delete (job as Partial<Job>).discard;
      }
    },
  };
}
