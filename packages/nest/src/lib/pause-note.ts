import { PauseRequest, QueuePauseInfo } from './types';

/** The longest reason a pause request may carry. */
export const MAX_PAUSE_REASON_LENGTH = 200;

/** How far ahead an auto-resume time may be set. */
export const MAX_PAUSE_UNTIL_MS = 7 * 24 * 60 * 60 * 1000;

/**
 * A pause request the API cannot accept. The controller turns it into a 400
 * with the message as the body, so keep messages fit for a client to show.
 */
export class PauseRequestError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'PauseRequestError';
  }
}

/**
 * The Redis key a queue's pause note lives under. Kept at the name the
 * bare-string reason used, so an upgrade over a paused queue keeps its note.
 */
export function pauseNoteKey(queueName: string, prefix = 'bull'): string {
  return `${prefix}:${queueName}:pause-reason`;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * Trim a reason and collapse whitespace, including control characters, so
 * what is stored is one line of text however it was typed.
 */
function normalizeReason(value: string): string {
  return value.replace(/[\s\p{Cc}]+/gu, ' ').trim();
}

/**
 * Validate a pause request body. Absent, null or empty bodies are fine and
 * mean "pause with no note"; anything else must be an object with an
 * optional `reason` and an optional `until`.
 *
 * A reason over the limit is rejected rather than silently cut, so a client
 * always knows what was stored. `until` must be in the future and no more
 * than seven days out.
 */
export function parsePauseRequest(
  body: unknown,
  now: number = Date.now(),
): PauseRequest {
  if (body === undefined || body === null || body === '') return {};
  if (!isPlainObject(body)) {
    throw new PauseRequestError(
      'Pause body must be an object with optional reason and until fields',
    );
  }

  const request: PauseRequest = {};

  if (body['reason'] !== undefined && body['reason'] !== null) {
    if (typeof body['reason'] !== 'string') {
      throw new PauseRequestError('reason must be a string');
    }
    const reason = normalizeReason(body['reason']);
    if (reason.length > MAX_PAUSE_REASON_LENGTH) {
      throw new PauseRequestError(
        `reason must be at most ${MAX_PAUSE_REASON_LENGTH} characters`,
      );
    }
    if (reason.length > 0) request.reason = reason;
  }

  if (body['until'] !== undefined && body['until'] !== null) {
    if (typeof body['until'] !== 'string') {
      throw new PauseRequestError('until must be an ISO 8601 timestamp');
    }
    const until = Date.parse(body['until']);
    if (Number.isNaN(until)) {
      throw new PauseRequestError('until must be an ISO 8601 timestamp');
    }
    if (until <= now) {
      throw new PauseRequestError('until must be in the future');
    }
    if (until - now > MAX_PAUSE_UNTIL_MS) {
      throw new PauseRequestError('until must be within the next 7 days');
    }
    request.until = new Date(until).toISOString();
  }

  return request;
}

/**
 * Serialise a pause note. Fields that are absent are left out rather than
 * written as null, so a stored note is exactly what stats will report.
 */
export function encodePauseNote(info: QueuePauseInfo): string {
  const note: QueuePauseInfo = {};
  if (info.reason) note.reason = info.reason;
  if (info.pausedAt) note.pausedAt = info.pausedAt;
  if (info.until) note.until = info.until;
  if (info.source) note.source = info.source;
  return JSON.stringify(note);
}

/**
 * Read a stored pause note. A value that is not JSON is the bare-string
 * reason earlier versions wrote, and is kept as the reason with nothing else
 * known about the pause.
 */
export function decodePauseNote(
  raw: string | null | undefined,
): QueuePauseInfo | undefined {
  if (raw === null || raw === undefined || raw === '') return undefined;

  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return { reason: raw };
  }

  if (!isPlainObject(parsed)) {
    return { reason: raw };
  }

  const info: QueuePauseInfo = {};
  if (typeof parsed['reason'] === 'string' && parsed['reason'] !== '') {
    info.reason = parsed['reason'];
  }
  if (typeof parsed['pausedAt'] === 'string')
    info.pausedAt = parsed['pausedAt'];
  if (typeof parsed['until'] === 'string') info.until = parsed['until'];
  if (typeof parsed['source'] === 'string') info.source = parsed['source'];
  return info;
}

/**
 * Whether a note's auto-resume time has passed.
 */
export function isPauseExpired(
  info: QueuePauseInfo | undefined,
  now: number = Date.now(),
): boolean {
  if (!info?.until) return false;
  const until = Date.parse(info.until);
  return !Number.isNaN(until) && until <= now;
}
