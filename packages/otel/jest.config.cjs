module.exports = {
  displayName: 'otel',
  preset: '../../jest.preset.js',
  testEnvironment: 'node',
  transform: {
    '^.+\\.[tj]s$': ['ts-jest', { tsconfig: '<rootDir>/tsconfig.spec.json' }],
  },
  moduleNameMapper: {
    '^@queuebert/nest$': '<rootDir>/../nest/src/index.ts',
    '^@queuebert/otel$': '<rootDir>/src/index.ts',
    '^@queuebert/otel/nest$': '<rootDir>/src/nest/index.ts',
  },
  moduleFileExtensions: ['ts', 'js', 'html'],
  coveragePathIgnorePatterns: ['/dist/'],
  coverageDirectory: '../../coverage/packages/otel',
  coverageThreshold: {
    global: { branches: 60, functions: 75, lines: 75, statements: 75 },
  },
};
