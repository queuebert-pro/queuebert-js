// Types
export type {
  MetricsConfig,
  ResourceAttributes,
  QueuebertOTelConfig,
  OTelMetricsSnapshot,
  OTelQueueMetrics,
  TransformedQueueStats,
} from './lib/types';

// Constants and defaults
export { DEFAULT_OTEL_CONFIG } from './lib/types';

// OpenTelemetry helpers and semantic names
export {
  createResourceAttributes,
  METRIC_NAMES,
  SPAN_NAMES,
  SPAN_ATTRIBUTES,
} from './lib/instrumentation';

// Metrics collection
export {
  MetricsRegistry,
  transformOTelToQueuebert,
} from './lib/metrics-collector';
