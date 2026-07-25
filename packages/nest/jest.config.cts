module.exports = {
  displayName: 'nest',
  preset: '../../jest.preset.js',
  testEnvironment: 'node',
  transform: {
    '^.+\\.[tj]s$': ['ts-jest', { tsconfig: '<rootDir>/tsconfig.spec.json' }],
  },
  moduleFileExtensions: ['ts', 'js', 'html'],
  coverageDirectory: '../../coverage/packages/nest',
  coverageThreshold: {
    global: { branches: 60, functions: 75, lines: 75, statements: 75 },
  },
};
