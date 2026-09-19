import {
  decodePauseNote,
  encodePauseNote,
  isPauseExpired,
  MAX_PAUSE_REASON_LENGTH,
  parsePauseRequest,
  pauseNoteKey,
  PauseRequestError,
} from './pause-note';

const NOW = Date.parse('2026-09-19T12:00:00.000Z');

describe('parsePauseRequest', () => {
  it('treats a missing or empty body as a pause with no note', () => {
    expect(parsePauseRequest(undefined, NOW)).toEqual({});
    expect(parsePauseRequest(null, NOW)).toEqual({});
    expect(parsePauseRequest('', NOW)).toEqual({});
    expect(parsePauseRequest({}, NOW)).toEqual({});
  });

  it('trims and collapses a reason', () => {
    expect(
      parsePauseRequest({ reason: '  Deploying\n\tapi   v2.3 ' }, NOW),
    ).toEqual({ reason: 'Deploying api v2.3' });
  });

  it('drops a whitespace-only reason', () => {
    expect(parsePauseRequest({ reason: '   ' }, NOW)).toEqual({});
    expect(parsePauseRequest({ reason: null }, NOW)).toEqual({});
  });

  it('rejects a reason over the limit rather than cutting it', () => {
    const reason = 'x'.repeat(MAX_PAUSE_REASON_LENGTH + 1);

    expect(() => parsePauseRequest({ reason }, NOW)).toThrow(PauseRequestError);
    expect(() => parsePauseRequest({ reason }, NOW)).toThrow(/200 characters/);
    expect(
      parsePauseRequest({ reason: 'x'.repeat(MAX_PAUSE_REASON_LENGTH) }, NOW),
    ).toEqual({ reason: 'x'.repeat(MAX_PAUSE_REASON_LENGTH) });
  });

  it('rejects a reason that is not a string', () => {
    expect(() => parsePauseRequest({ reason: 42 }, NOW)).toThrow(
      /reason must be a string/,
    );
  });

  it('normalises a valid until to ISO 8601', () => {
    expect(parsePauseRequest({ until: '2026-09-19T12:30:00Z' }, NOW)).toEqual({
      until: '2026-09-19T12:30:00.000Z',
    });
  });

  it('rejects an until that is malformed, past, or too far out', () => {
    expect(() => parsePauseRequest({ until: 'tomorrow' }, NOW)).toThrow(
      /ISO 8601/,
    );
    expect(() => parsePauseRequest({ until: 1234 }, NOW)).toThrow(/ISO 8601/);
    expect(() =>
      parsePauseRequest({ until: '2026-09-19T12:00:00.000Z' }, NOW),
    ).toThrow(/in the future/);
    expect(() =>
      parsePauseRequest({ until: '2026-09-27T12:00:00.000Z' }, NOW),
    ).toThrow(/7 days/);
    expect(
      parsePauseRequest({ until: '2026-09-26T12:00:00.000Z' }, NOW),
    ).toEqual({ until: '2026-09-26T12:00:00.000Z' });
  });

  it('rejects a body that is not an object', () => {
    expect(() => parsePauseRequest('deploy', NOW)).toThrow(PauseRequestError);
    expect(() => parsePauseRequest(['deploy'], NOW)).toThrow(PauseRequestError);
  });

  it('ignores fields it does not know', () => {
    expect(parsePauseRequest({ reason: 'x', who: 'me' }, NOW)).toEqual({
      reason: 'x',
    });
  });
});

describe('pause note storage', () => {
  it('keeps the key earlier versions used', () => {
    expect(pauseNoteKey('emails')).toBe('bull:emails:pause-reason');
    expect(pauseNoteKey('emails', 'app')).toBe('app:emails:pause-reason');
  });

  it('round-trips a note without writing absent fields', () => {
    const encoded = encodePauseNote({
      reason: 'Deploy',
      pausedAt: '2026-09-19T12:00:00.000Z',
      source: 'api',
    });

    expect(JSON.parse(encoded)).toEqual({
      reason: 'Deploy',
      pausedAt: '2026-09-19T12:00:00.000Z',
      source: 'api',
    });
    expect(decodePauseNote(encoded)).toEqual({
      reason: 'Deploy',
      pausedAt: '2026-09-19T12:00:00.000Z',
      source: 'api',
    });
  });

  it('reads the bare-string reason earlier versions wrote', () => {
    expect(decodePauseNote('manual-pause')).toEqual({ reason: 'manual-pause' });
    expect(decodePauseNote('42')).toEqual({ reason: '42' });
  });

  it('is undefined for nothing stored', () => {
    expect(decodePauseNote(null)).toBeUndefined();
    expect(decodePauseNote(undefined)).toBeUndefined();
    expect(decodePauseNote('')).toBeUndefined();
  });

  it('ignores fields of the wrong type', () => {
    expect(decodePauseNote('{"reason":7,"until":"soon","extra":1}')).toEqual({
      until: 'soon',
    });
  });
});

describe('isPauseExpired', () => {
  it('is true only once until has passed', () => {
    expect(isPauseExpired(undefined, NOW)).toBe(false);
    expect(isPauseExpired({ reason: 'x' }, NOW)).toBe(false);
    expect(isPauseExpired({ until: 'never' }, NOW)).toBe(false);
    expect(isPauseExpired({ until: '2026-09-19T12:00:01.000Z' }, NOW)).toBe(
      false,
    );
    expect(isPauseExpired({ until: '2026-09-19T12:00:00.000Z' }, NOW)).toBe(
      true,
    );
  });
});
