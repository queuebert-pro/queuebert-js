import { Injectable, Logger } from '@nestjs/common';

import type { IntegrationPackage } from './types';

/**
 * Registry for Queuebert integration packages.
 *
 * Child modules (like @queuebert/bullmq and @queuebert/otel/nest) can inject
 * this registry and register themselves during module initialization.
 * The registered integrations are included in the /stats endpoint response.
 *
 * @example
 * ```typescript
 * @Injectable()
 * export class MyIntegrationService implements OnModuleInit {
 *   constructor(
 *     @Optional() @Inject(QUEUEBERT_INTEGRATION_REGISTRY)
 *     private registry?: QueuebertIntegrationRegistry,
 *   ) {}
 *
 *   onModuleInit() {
 *     this.registry?.register({
 *       name: '@myorg/my-integration',
 *       version: '1.0.0',
 *       description: 'My custom integration',
 *     })
 *   }
 * }
 * ```
 */
@Injectable()
export class QueuebertIntegrationRegistry {
  private readonly logger = new Logger(QueuebertIntegrationRegistry.name);
  private readonly integrations: IntegrationPackage[] = [];

  /**
   * Register an integration package.
   * Duplicate registrations (same package name) are ignored.
   */
  register(pkg: IntegrationPackage): void {
    // Avoid duplicate registrations
    if (this.integrations.some((i) => i.name === pkg.name)) {
      this.logger.debug(`Integration ${pkg.name} already registered, skipping`);
      return;
    }

    this.integrations.push(pkg);
    this.logger.log(`Registered integration: ${pkg.name}@${pkg.version}`);
  }

  /**
   * Get all registered integration packages.
   */
  getAll(): IntegrationPackage[] {
    return [...this.integrations];
  }

  /**
   * Check if a specific integration is registered.
   */
  has(name: string): boolean {
    return this.integrations.some((i) => i.name === name);
  }

  /**
   * Get a specific integration by name.
   */
  get(name: string): IntegrationPackage | undefined {
    return this.integrations.find((i) => i.name === name);
  }
}
