module.exports = {
  displayName: 'bullmq',
  preset: '../../jest.preset.js',
  testEnvironment: 'node',
  transform: {
    '^.+\\.[tj]s$': ['ts-jest', { tsconfig: '<rootDir>/tsconfig.spec.json' }],
  },
  moduleNameMapper: {
    '^@queuebert/nest$': '<rootDir>/../nest/src/index.ts',
  },
  moduleFileExtensions: ['ts', 'js', 'html'],
  coverageDirectory: '../../coverage/packages/bullmq',
  coverageThreshold: {
    global: { branches: 60, functions: 75, lines: 75, statements: 75 },
  },
};
