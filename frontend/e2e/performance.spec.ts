import type { Browser, Locator, Page, Response } from '@playwright/test'
import {
  expect,
  overviewScaleAdditionalGroupsPerMonth,
  overviewScaleEventCount,
  overviewScaleMessageCount,
  overviewScaleMessagesPerMonth,
  overviewScaleThreadsPerMonth,
  overviewScaleUsageFactCount,
  overviewScaleUsageFactsPerMonth,
  overviewScaleYear,
  test,
} from './fixtures'

const STATS_RANGES = ['day', 'week', 'month', 'year', 'all'] as const
const COLD_SAMPLE_COUNT = 3
const PRODUCT_TARGET_MS = 1_000
const HEADROOM_RENDER_BUDGET_MS = 900
const SURFACE_WAIT_TIMEOUT_MS = 10_000

type StatsRange = typeof STATS_RANGES[number]
type OverviewRenderSurface = 'summary' | 'heatmap' | 'topProjects' | 'topSessions'

// Functional E2E keeps a one-second scanner to exercise live polling. The
// performance suite uses the application's real default cadence so repeated
// samples do not manufacture continuous write contention that users never see.
test.use({ e2ePollSeconds: 30 })

interface OverviewTiming {
  render: Record<OverviewRenderSurface, number>
  api: { summaryMs: number; annualMs: number }
}

interface SurfaceTiming {
  renderMs: number
  apiMs: number
}

async function withColdBrowserContext<T>(browser: Browser, run: (page: Page) => Promise<T>) {
  const context = await browser.newContext({
    viewport: { width: 1440, height: 1_000 },
    timezoneId: 'Europe/Amsterdam',
  })
  try {
    return await run(await context.newPage())
  } finally {
    await context.close()
  }
}

async function coldRenderTime(locator: Locator, startedAt: number) {
  await locator.waitFor({ state: 'visible', timeout: SURFACE_WAIT_TIMEOUT_MS })
  return Math.round(performance.now() - startedAt)
}

async function coldApiTime(responsePromise: Promise<Response>, startedAt: number) {
  const response = await responsePromise
  await response.finished()
  if (!response.ok()) {
    const body = await response.text().catch(() => '<response body unavailable>')
    throw new Error(`${response.url()} returned ${response.status()} ${response.statusText()}: ${body}`)
  }
  return Math.round(performance.now() - startedAt)
}

function visibleStatsRows(page: Page, range: StatsRange) {
  const selectedTab = page
    .getByRole('tab', { name: range.toUpperCase(), exact: true })
    .and(page.locator('[aria-selected="true"]'))
  return page
    .locator('.stats-page')
    .filter({ has: selectedTab })
    .locator('.stats-ledger')
    .filter({ has: page.locator('.stats-row') })
}

async function measureColdOverview(page: Page, baseUrl: string): Promise<OverviewTiming> {
  const summaryResponse = page.waitForResponse(candidate => new URL(candidate.url()).pathname === '/api/v1/overview', {
    timeout: SURFACE_WAIT_TIMEOUT_MS,
  })
  const annualResponse = page.waitForResponse(candidate => new URL(candidate.url()).pathname === '/api/v1/overview/year', {
    timeout: SURFACE_WAIT_TIMEOUT_MS,
  })
  const startedAt = performance.now()
  const summaryApiTime = coldApiTime(summaryResponse, startedAt)
  const annualApiTime = coldApiTime(annualResponse, startedAt)
  const observedSurfaces: Array<[OverviewRenderSurface, Locator]> = [
    ['summary', page.locator('.overview-hero:not(.overview-summary-skeleton):not(.overview-summary-error) .today-cost')],
    ['heatmap', page.getByRole('group', { name: `${overviewScaleYear} usage by day` })],
    ['topProjects', page.locator('.drivers-card:not(.overview-card-loading) .driver-row').first()],
    ['topSessions', page.locator('.recent-card:not(.overview-card-loading) .recent-row').first()],
  ]
  const timingEntriesPromise = Promise.all(observedSurfaces.map(async ([name, locator]) => (
    [name, await coldRenderTime(locator, startedAt)] as const
  )))
  const [, timingEntries, summaryApiMs, annualApiMs] = await Promise.all([
    page.goto(`${baseUrl}/`),
    timingEntriesPromise,
    summaryApiTime,
    annualApiTime,
  ])

  await expect(page.getByRole('button', {
    name: new RegExp(`^${overviewScaleYear}-01-15: .* ${overviewScaleThreadsPerMonth} sessions, ${overviewScaleMessagesPerMonth} messages, ${Math.round(overviewScaleUsageFactsPerMonth * 12 / 1_000)}K API tokens$`),
  })).toBeVisible()

  return {
    render: Object.fromEntries(timingEntries) as Record<OverviewRenderSurface, number>,
    api: { summaryMs: summaryApiMs, annualMs: annualApiMs },
  }
}

async function measureColdStats(page: Page, baseUrl: string, range: StatsRange): Promise<SurfaceTiming> {
  const query = range === 'all' ? 'range=all' : `range=${range}&anchor=${overviewScaleYear}-07-15`
  const response = page.waitForResponse(candidate => {
    const url = new URL(candidate.url())
    return url.pathname === '/api/v1/stats' && url.searchParams.get('range') === range
  }, { timeout: SURFACE_WAIT_TIMEOUT_MS })
  const startedAt = performance.now()
  const apiTime = coldApiTime(response, startedAt)
  const renderTime = coldRenderTime(visibleStatsRows(page, range), startedAt)
  const [, renderMs, apiMs] = await Promise.all([
    page.goto(`${baseUrl}/stats?${query}`),
    renderTime,
    apiTime,
  ])
  await expect(page.getByRole('tab', { name: range.toUpperCase() })).toHaveAttribute('aria-selected', 'true')
  return { renderMs, apiMs }
}

async function measureColdOverviewToStats(page: Page, baseUrl: string): Promise<SurfaceTiming> {
  // Stats is still cold here, while the two Overview requests may compete with
  // it. This preserves the real navigation path that exposed the regression.
  await page.goto(`${baseUrl}/`)
  const response = page.waitForResponse(candidate => {
    const url = new URL(candidate.url())
    return url.pathname === '/api/v1/stats' && url.searchParams.get('range') === 'month'
  }, { timeout: SURFACE_WAIT_TIMEOUT_MS })
  const startedAt = performance.now()
  const apiTime = coldApiTime(response, startedAt)
  const renderTime = coldRenderTime(visibleStatsRows(page, 'month'), startedAt)
  const [, renderMs, apiMs] = await Promise.all([
    page.getByRole('navigation', { name: 'Primary navigation' }).getByRole('link', { name: 'Stats' }).click(),
    renderTime,
    apiTime,
  ])
  return { renderMs, apiMs }
}

function summarize(samples: number[]) {
  const sorted = [...samples].sort((left, right) => left - right)
  const percentile = (fraction: number) => sorted[Math.ceil(sorted.length * fraction) - 1]
  return {
    minMs: sorted[0],
    medianMs: percentile(0.5),
    p95Ms: percentile(0.95),
    maxMs: sorted[sorted.length - 1],
  }
}

test.describe('cold analytical performance', () => {
  test.describe.configure({ retries: 0, timeout: 180_000 })

  test(`Overview and every Stats range stay below ${HEADROOM_RENDER_BUDGET_MS}ms across cold samples`, async ({ browser, app }, testInfo) => {
    const overview: OverviewTiming[] = []
    const stats = Object.fromEntries(STATS_RANGES.map(range => [range, [] as SurfaceTiming[]])) as Record<StatsRange, SurfaceTiming[]>
    const overviewToStats: SurfaceTiming[] = []

    for (let sample = 0; sample < COLD_SAMPLE_COUNT; sample += 1) {
      overview.push(await withColdBrowserContext(browser, page => measureColdOverview(page, app.baseUrl)))
      for (const range of STATS_RANGES) {
        stats[range].push(await withColdBrowserContext(browser, page => measureColdStats(page, app.baseUrl, range)))
      }
      overviewToStats.push(await withColdBrowserContext(browser, page => measureColdOverviewToStats(page, app.baseUrl)))
    }

    const renderSamples: Record<string, number[]> = Object.fromEntries(
      (['summary', 'heatmap', 'topProjects', 'topSessions'] as const).map(surface => [
        `Overview ${surface}`,
        overview.map(sample => sample.render[surface]),
      ]),
    )
    const apiSamples: Record<string, number[]> = {
      'Overview summary API': overview.map(sample => sample.api.summaryMs),
      'Overview annual API': overview.map(sample => sample.api.annualMs),
    }
    for (const range of STATS_RANGES) {
      renderSamples[`Stats ${range}`] = stats[range].map(sample => sample.renderMs)
      apiSamples[`Stats ${range} API`] = stats[range].map(sample => sample.apiMs)
    }
    renderSamples['Overview to Stats navigation'] = overviewToStats.map(sample => sample.renderMs)
    apiSamples['Overview to Stats API'] = overviewToStats.map(sample => sample.apiMs)

    const report = {
      productTargetMs: PRODUCT_TARGET_MS,
      enforcedHeadroomBudgetMs: HEADROOM_RENDER_BUDGET_MS,
      coldSampleCount: COLD_SAMPLE_COUNT,
      coldDefinition: 'a new isolated Chromium context for every measured page load or navigation',
      scale: {
        year: overviewScaleYear,
        threads: 12 * overviewScaleThreadsPerMonth,
        messages: overviewScaleMessageCount,
        usageFacts: overviewScaleUsageFactCount,
        events: overviewScaleEventCount,
        usageGroups: 12 * (overviewScaleThreadsPerMonth + overviewScaleAdditionalGroupsPerMonth),
        activityPairs: 12 * (overviewScaleThreadsPerMonth + overviewScaleAdditionalGroupsPerMonth),
      },
      raw: { overview, stats, overviewToStats },
      summaries: {
        render: Object.fromEntries(Object.entries(renderSamples).map(([surface, samples]) => [surface, summarize(samples)])),
        api: Object.fromEntries(Object.entries(apiSamples).map(([surface, samples]) => [surface, summarize(samples)])),
      },
    }
    await testInfo.attach('cold-analytical-performance.json', {
      body: Buffer.from(JSON.stringify(report, null, 2)),
      contentType: 'application/json',
    })
    console.log(`Cold analytical timings:\n${JSON.stringify({ render: renderSamples, api: apiSamples }, null, 2)}`)

    const slowSamples = Object.entries(renderSamples).flatMap(([surface, samples]) => (
      samples.flatMap((elapsedMs, index) => (
        elapsedMs >= HEADROOM_RENDER_BUDGET_MS ? [`${surface} sample ${index + 1}: ${elapsedMs}ms`] : []
      ))
    ))
    const summaryText = Object.entries(report.summaries.render)
      .map(([surface, timing]) => `${surface}: ${timing.minMs}/${timing.medianMs}/${timing.p95Ms}/${timing.maxMs}ms min/median/p95/max`)
      .join('\n')
    expect(
      slowSamples,
      `The ${PRODUCT_TARGET_MS}ms product target is enforced at ${HEADROOM_RENDER_BUDGET_MS}ms for headroom.\n${summaryText}`,
    ).toEqual([])
  })
})
