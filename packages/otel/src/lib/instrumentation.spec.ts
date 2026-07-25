import {
  createResourceAttributes,
  METRIC_NAMES,
  SPAN_ATTRIBUTES,
  SPAN_NAMES,
} from './instrumentation';

describe('OpenTelemetry instrumentation helpers', () => {
  it('creates semantic resource attributes with custom values', () => {
    expect(
      createResourceAttributes({
        serviceName: 'worker',
        serviceVersion: '1.0.0',
        serviceNamespace: 'queuebert',
        deploymentEnvironment: 'production',
        custom: {
          region: 'us-east-1',
        },
      }),
    ).toEqual({
      'service.name': 'worker',
      'service.version': '1.0.0',
      'service.namespace': 'queuebert',
      'deployment.environment': 'production',
      region: 'us-east-1',
    });
  });

  it('exports stable metric and span constants', () => {
    expect(METRIC_NAMES.JOB_DURATION).toBe('queuebert.job.duration');
    expect(SPAN_NAMES.WORKER_PROCESS).toBe('bullmq.worker.process');
    expect(SPAN_ATTRIBUTES.QUEUE_NAME).toBe('bullmq.queue.name');
  });
});
