import { QueuebertIntegrationRegistry } from './integration-registry';
import type { IntegrationPackage } from './types';

describe('QueuebertIntegrationRegistry', () => {
  let registry: QueuebertIntegrationRegistry;

  beforeEach(() => {
    registry = new QueuebertIntegrationRegistry();
  });

  describe('register', () => {
    it('should register a new integration package', () => {
      const pkg: IntegrationPackage = {
        name: '@test/my-integration',
        version: '1.0.0',
        description: 'Test integration',
      };

      registry.register(pkg);

      expect(registry.has('@test/my-integration')).toBe(true);
      expect(registry.getAll()).toHaveLength(1);
      expect(registry.get('@test/my-integration')).toEqual(pkg);
    });

    it('should register multiple integration packages', () => {
      const pkg1: IntegrationPackage = {
        name: '@test/integration-one',
        version: '1.0.0',
      };
      const pkg2: IntegrationPackage = {
        name: '@test/integration-two',
        version: '2.0.0',
        description: 'Second integration',
      };

      registry.register(pkg1);
      registry.register(pkg2);

      expect(registry.getAll()).toHaveLength(2);
      expect(registry.has('@test/integration-one')).toBe(true);
      expect(registry.has('@test/integration-two')).toBe(true);
    });

    it('should ignore duplicate registrations with the same name', () => {
      const pkg1: IntegrationPackage = {
        name: '@test/my-integration',
        version: '1.0.0',
        description: 'First version',
      };
      const pkg2: IntegrationPackage = {
        name: '@test/my-integration',
        version: '2.0.0',
        description: 'Second version',
      };

      registry.register(pkg1);
      registry.register(pkg2);

      expect(registry.getAll()).toHaveLength(1);
      // Should keep the first registration
      expect(registry.get('@test/my-integration')?.version).toBe('1.0.0');
      expect(registry.get('@test/my-integration')?.description).toBe(
        'First version',
      );
    });

    it('should handle packages without optional description', () => {
      const pkg: IntegrationPackage = {
        name: '@test/minimal',
        version: '1.0.0',
      };

      registry.register(pkg);

      const retrieved = registry.get('@test/minimal');
      expect(retrieved).toBeDefined();
      expect(retrieved?.name).toBe('@test/minimal');
      expect(retrieved?.version).toBe('1.0.0');
      expect(retrieved?.description).toBeUndefined();
    });
  });

  describe('getAll', () => {
    it('should return empty array when no integrations registered', () => {
      expect(registry.getAll()).toEqual([]);
    });

    it('should return a copy of the integrations array', () => {
      const pkg: IntegrationPackage = {
        name: '@test/my-integration',
        version: '1.0.0',
      };

      registry.register(pkg);
      const result = registry.getAll();

      // Modifying the returned array should not affect the registry
      result.push({ name: '@test/injected', version: '0.0.0' });

      expect(registry.getAll()).toHaveLength(1);
      expect(registry.has('@test/injected')).toBe(false);
    });

    it('should return integrations in registration order', () => {
      registry.register({ name: '@test/first', version: '1.0.0' });
      registry.register({ name: '@test/second', version: '1.0.0' });
      registry.register({ name: '@test/third', version: '1.0.0' });

      const all = registry.getAll();

      expect(all[0].name).toBe('@test/first');
      expect(all[1].name).toBe('@test/second');
      expect(all[2].name).toBe('@test/third');
    });
  });

  describe('has', () => {
    it('should return false for unregistered package name', () => {
      expect(registry.has('@test/nonexistent')).toBe(false);
    });

    it('should return true for registered package name', () => {
      registry.register({ name: '@test/exists', version: '1.0.0' });

      expect(registry.has('@test/exists')).toBe(true);
    });

    it('should be case-sensitive', () => {
      registry.register({ name: '@test/MyPackage', version: '1.0.0' });

      expect(registry.has('@test/MyPackage')).toBe(true);
      expect(registry.has('@test/mypackage')).toBe(false);
      expect(registry.has('@test/MYPACKAGE')).toBe(false);
    });
  });

  describe('get', () => {
    it('should return undefined for unregistered package name', () => {
      expect(registry.get('@test/nonexistent')).toBeUndefined();
    });

    it('should return the registered package', () => {
      const pkg: IntegrationPackage = {
        name: '@test/my-integration',
        version: '1.2.3',
        description: 'Test description',
      };

      registry.register(pkg);

      expect(registry.get('@test/my-integration')).toEqual(pkg);
    });

    it('should return the correct package when multiple are registered', () => {
      const pkg1: IntegrationPackage = { name: '@test/one', version: '1.0.0' };
      const pkg2: IntegrationPackage = { name: '@test/two', version: '2.0.0' };
      const pkg3: IntegrationPackage = {
        name: '@test/three',
        version: '3.0.0',
      };

      registry.register(pkg1);
      registry.register(pkg2);
      registry.register(pkg3);

      expect(registry.get('@test/one')).toEqual(pkg1);
      expect(registry.get('@test/two')).toEqual(pkg2);
      expect(registry.get('@test/three')).toEqual(pkg3);
    });
  });
});
