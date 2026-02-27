import type { Config } from '@jest/types';

const config: Config.InitialOptions = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  testMatch: ['**/test/**/*.spec.ts'],
  // Explicitly ignore manual integration helpers and miscellaneous scripts
  testPathIgnorePatterns: ['<rootDir>/test/manual/', '<rootDir>/scripts/'],
  verbose: true,
};

export default config;
