import { defineConfig, devices } from '@playwright/test'

export default defineConfig({
  testDir: './e2e',
  fullyParallel: false,
  workers: 1,
  retries: process.env.CI ? 2 : 0,
  forbidOnly: Boolean(process.env.CI),
  timeout: 60_000,
  expect: {
    timeout: 10_000,
  },
  outputDir: 'test-results',
  reporter: process.env.CI
    ? [['github'], ['html', { open: 'never' }]]
    : [['line'], ['html', { open: 'never' }]],
  use: {
    actionTimeout: 15_000,
    navigationTimeout: 30_000,
    viewport: { width: 1440, height: 1000 },
    timezoneId: 'Europe/Amsterdam',
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    video: 'retain-on-failure',
  },
  // E2E fixtures own an isolated backend and frontend server for each worker.
  // Keep webServer absent so Playwright never starts or reuses shared test state.
  projects: [
    {
      name: 'chromium',
      use: {
        ...devices['Desktop Chrome'],
        viewport: { width: 1440, height: 1000 },
        timezoneId: 'Europe/Amsterdam',
      },
    },
  ],
})
