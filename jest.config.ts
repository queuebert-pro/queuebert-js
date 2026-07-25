import type { Config } from 'jest';

const config: Config = {
  projects: [
    '<rootDir>/packages/nest/jest.config.cts',
    '<rootDir>/packages/bullmq/jest.config.cts',
    '<rootDir>/packages/otel/jest.config.cts',
  ],
};

export default config;
