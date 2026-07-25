import {
  InvalidOptionsError,
  QueueAlreadyExistsError,
  QueueNotFoundError,
  QueuebertBullMQError,
  WorkerAlreadyExistsError,
  WorkerNotFoundError,
} from './errors';

describe('Queuebert BullMQ errors', () => {
  it.each([
    [
      new QueueAlreadyExistsError('emails'),
      'QueueAlreadyExistsError',
      'Queue "emails" already exists',
    ],
    [
      new WorkerAlreadyExistsError('emails'),
      'WorkerAlreadyExistsError',
      'Worker for queue "emails" already exists',
    ],
    [
      new QueueNotFoundError('emails'),
      'QueueNotFoundError',
      'Queue "emails" not found',
    ],
    [
      new WorkerNotFoundError('emails'),
      'WorkerNotFoundError',
      'Worker for queue "emails" not found',
    ],
    [
      new InvalidOptionsError('connection is required'),
      'InvalidOptionsError',
      'connection is required',
    ],
  ])('sets name and message for %s', (error, name, message) => {
    expect(error).toBeInstanceOf(QueuebertBullMQError);
    expect(error.name).toBe(name);
    expect(error.message).toBe(message);
    expect(error.stack).toContain(name);
  });
});
