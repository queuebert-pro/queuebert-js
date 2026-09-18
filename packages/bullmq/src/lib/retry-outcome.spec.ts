import { UnrecoverableError } from 'bullmq';

import {
  isUnrecoverableError,
  resolveRetryOutcome,
  trackDiscard,
} from './retry-outcome';

function createJob(overrides: Record<string, unknown> = {}) {
  return {
    id: 'job-1',
    name: 'welcome',
    attemptsMade: 0,
    opts: { attempts: 3 },
    discard: jest.fn(),
    ...overrides,
  } as never;
}

describe('isUnrecoverableError', () => {
  it('recognises a real UnrecoverableError', () => {
    expect(isUnrecoverableError(new UnrecoverableError('no'))).toBe(true);
  });

  it('recognises one by name from another bullmq copy', () => {
    const foreign = new Error('no');
    foreign.name = 'UnrecoverableError';
    expect(isUnrecoverableError(foreign)).toBe(true);
  });

  it('does not treat an ordinary error as unrecoverable', () => {
    expect(isUnrecoverableError(new Error('boom'))).toBe(false);
  });

  it('tolerates a non-error value', () => {
    expect(isUnrecoverableError('boom')).toBe(false);
    expect(isUnrecoverableError(null)).toBe(false);
    expect(isUnrecoverableError(undefined)).toBe(false);
  });
});

describe('resolveRetryOutcome', () => {
  it('counts the failed attempt as attemptsMade + 1', () => {
    // BullMQ has not incremented attemptsMade when the handler throws.
    expect(
      resolveRetryOutcome(createJob({ attemptsMade: 2 }), new Error('boom')),
    ).toMatchObject({ attempt: 3, maxAttempts: 3, isFinalAttempt: true });
  });

  it('leaves a mid-run attempt retryable', () => {
    expect(resolveRetryOutcome(createJob(), new Error('boom'))).toMatchObject({
      attempt: 1,
      isFinalAttempt: false,
    });
  });

  it('defaults a missing ceiling to one attempt', () => {
    expect(
      resolveRetryOutcome(createJob({ opts: {} }), new Error('boom')),
    ).toMatchObject({ maxAttempts: 1, isFinalAttempt: true });
  });

  it('marks an UnrecoverableError final on the first attempt', () => {
    expect(
      resolveRetryOutcome(createJob(), new UnrecoverableError('no')),
    ).toMatchObject({ unrecoverable: true, isFinalAttempt: true });
  });

  it('marks a discarded job final', () => {
    expect(
      resolveRetryOutcome(createJob(), new Error('boom'), true),
    ).toMatchObject({ discarded: true, isFinalAttempt: true });
  });
});

describe('trackDiscard', () => {
  it('observes a discard and still calls through', () => {
    const original = jest.fn();
    const job = createJob({ discard: original });
    const tracker = trackDiscard(job);

    expect(tracker.wasDiscarded()).toBe(false);
    (job as unknown as { discard: () => void }).discard();

    expect(tracker.wasDiscarded()).toBe(true);
    expect(original).toHaveBeenCalledTimes(1);
  });

  it('restores the job own method afterwards', () => {
    const original = jest.fn();
    const job = createJob({ discard: original });

    trackDiscard(job).restore();

    expect((job as unknown as { discard: unknown }).discard).toBe(original);
  });

  it('tolerates a job with no discard method', () => {
    const job = createJob({ discard: undefined });
    const tracker = trackDiscard(job);

    expect(tracker.wasDiscarded()).toBe(false);
    expect(() => tracker.restore()).not.toThrow();
  });
});
