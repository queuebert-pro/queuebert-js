/**
 * Base error class for Queuebert BullMQ errors
 */
export class QueuebertBullMQError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'QueuebertBullMQError';
    Error.captureStackTrace(this, this.constructor);
  }
}

/**
 * Thrown when attempting to create a queue that already exists
 */
export class QueueAlreadyExistsError extends QueuebertBullMQError {
  constructor(public readonly queueName: string) {
    super(`Queue "${queueName}" already exists`);
    this.name = 'QueueAlreadyExistsError';
  }
}

/**
 * Thrown when attempting to create a worker for a queue that already has one
 */
export class WorkerAlreadyExistsError extends QueuebertBullMQError {
  constructor(public readonly queueName: string) {
    super(`Worker for queue "${queueName}" already exists`);
    this.name = 'WorkerAlreadyExistsError';
  }
}

/**
 * Thrown when attempting to access a queue that doesn't exist
 */
export class QueueNotFoundError extends QueuebertBullMQError {
  constructor(public readonly queueName: string) {
    super(`Queue "${queueName}" not found`);
    this.name = 'QueueNotFoundError';
  }
}

/**
 * Thrown when attempting to access a worker that doesn't exist
 */
export class WorkerNotFoundError extends QueuebertBullMQError {
  constructor(public readonly queueName: string) {
    super(`Worker for queue "${queueName}" not found`);
    this.name = 'WorkerNotFoundError';
  }
}

/**
 * Thrown when module options are invalid
 */
export class InvalidOptionsError extends QueuebertBullMQError {
  constructor(message: string) {
    super(message);
    this.name = 'InvalidOptionsError';
  }
}
