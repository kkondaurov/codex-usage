import {
  expect,
  overviewScaleYear,
  test,
} from './fixtures'
import type { Locator, Page } from '@playwright/test'
import { mkdir, writeFile } from 'node:fs/promises'
import { join } from 'node:path'

const RICH_SESSION = '019f6768-ef84-74d3-ab05-e4b5fb717fa8'
const ABORTED_SESSION = '019f6767-979c-7df1-a512-9830528bda62'
const LIVE_SESSION = '019f7ffe-1111-7111-8111-111111111111'
const LIVE_TURN = '019f7ffe-2222-7222-8222-222222222222'

interface ActivityCaretGeometry {
  label: string
  level: number
  tokenText: string
  caret: { left: number; right: number }
  tokenCell: { left: number; right: number }
  detailsCell: { left: number; right: number }
}

async function activityCaretGeometry(rows: Locator) {
  await expect(rows.first()).toBeVisible()
  return rows.evaluateAll((elements): ActivityCaretGeometry[] => elements.map(element => {
    const tokenCell = element.querySelector<HTMLElement>(':scope > .event-tokens')
    const detailsCell = element.querySelector<HTMLElement>(':scope > .event-details-cell')
    const caret = element.querySelector<SVGElement>(':scope > .event-copy .activity-event-trigger > svg:last-child')
    const label = element.querySelector<HTMLButtonElement>('.activity-event-trigger')?.ariaLabel ?? 'unlabelled activity row'

    if (!tokenCell || !detailsCell || !caret) {
      throw new Error(`Could not measure disclosure geometry for ${label}`)
    }

    const caretBox = caret.getBoundingClientRect()
    const tokenBox = tokenCell.getBoundingClientRect()
    const detailsBox = detailsCell.getBoundingClientRect()
    return {
      label,
      level: Number(element.getAttribute('data-activity-depth')),
      tokenText: tokenCell.textContent?.trim() ?? '',
      caret: { left: caretBox.left, right: caretBox.right },
      tokenCell: { left: tokenBox.left, right: tokenBox.right },
      detailsCell: { left: detailsBox.left, right: detailsBox.right },
    }
  }))
}

function expectActivityCaretsInDetailsColumn(geometry: ActivityCaretGeometry[]) {
  const seamTolerance = 0.5
  for (const row of geometry) {
    const context = `${row.label} at nesting level ${row.level}`
    expect(row.caret.left, `${context}: caret starts inside the Details cell`).toBeGreaterThanOrEqual(row.detailsCell.left - seamTolerance)
    expect(row.caret.right, `${context}: caret ends inside the Details cell`).toBeLessThanOrEqual(row.detailsCell.right + seamTolerance)
    expect(row.caret.left, `${context}: caret does not overlap the API token cell`).toBeGreaterThanOrEqual(row.tokenCell.right - seamTolerance)
  }
}

async function expectNoDocumentHorizontalOverflow(page: Page) {
  const dimensions = await page.evaluate(() => ({
    clientWidth: document.documentElement.clientWidth,
    scrollWidth: document.documentElement.scrollWidth,
  }))
  expect(dimensions.scrollWidth).toBeLessThanOrEqual(dimensions.clientWidth + 1)
}

async function expectScrollableLedger(ledger: Locator) {
  await expect(ledger).toBeVisible()
  const dimensions = await ledger.evaluate(element => ({
    clientWidth: element.clientWidth,
    scrollWidth: element.scrollWidth,
    overflowX: getComputedStyle(element).overflowX,
  }))
  expect(dimensions.overflowX).toBe('auto')
  expect(dimensions.scrollWidth).toBeGreaterThan(dimensions.clientWidth)
  const scrollLeft = await ledger.evaluate(element => {
    element.scrollLeft = element.scrollWidth
    return element.scrollLeft
  })
  expect(scrollLeft).toBeGreaterThan(0)
}

test('boots the production application without browser-side network escapes', async ({ page, app }) => {
  const unexpectedRequests: string[] = []
  const pageErrors: string[] = []

  page.on('request', request => {
    const url = new URL(request.url())
    if (!['http:', 'https:'].includes(url.protocol)) return
    if (!['127.0.0.1', 'localhost', '[::1]', '::1'].includes(url.hostname)) {
      unexpectedRequests.push(request.url())
    }
  })
  page.on('pageerror', error => pageErrors.push(error.message))

  await page.goto(`${app.baseUrl}/`)

  await expect(page.getByRole('heading', { name: 'Overview', exact: true })).toBeVisible()
  await expect(page.getByRole('group', { name: `${overviewScaleYear} usage by day` })).toBeVisible()
  await page.waitForLoadState('networkidle')

  expect(unexpectedRequests).toEqual([])
  expect(pageErrors).toEqual([])

  const headerBox = await page.locator('.app-header').boundingBox()
  expect(headerBox).not.toBeNull()
  expect(headerBox).toMatchObject({ x: 0, y: 0, width: 1440, height: 64 })

  const heroColors = await page.locator('.today-panel').evaluate(element => {
    const style = getComputedStyle(element)
    return {
      background: style.backgroundColor,
      railColor: style.borderLeftColor,
      railWidth: style.borderLeftWidth,
    }
  })
  expect(heroColors).toEqual({
    background: 'rgb(252, 201, 66)',
    railColor: 'rgb(246, 75, 28)',
    railWidth: '22px',
  })
})

test('responsive routes contain horizontal overflow and preserve keyboard access', async ({ page, app }) => {
  for (const width of [1280, 1024, 680, 390]) {
    await page.setViewportSize({ width, height: 900 })
    await page.goto(`${app.baseUrl}/`)
    await expect(page.getByRole('heading', { name: 'Overview', exact: true })).toBeVisible()
    const annualLedger = page.getByRole('region', { name: `${overviewScaleYear} yearly usage ledger` })
    await expect(page.getByRole('group', { name: `${overviewScaleYear} usage by day` })).toBeVisible()
    if (width < 1280) await expectScrollableLedger(annualLedger)
    else await expect(annualLedger).toBeVisible()
    if (width <= 680) {
      const mobileHealth = page.locator('.updated-label')
      await expect(mobileHealth).toBeVisible()
      await expect(mobileHealth).not.toHaveText('')
    }
    await expectNoDocumentHorizontalOverflow(page)

    await page.evaluate(() => (document.activeElement as HTMLElement | null)?.blur())
    await page.keyboard.press('Tab')
    const skipLink = page.getByRole('link', { name: 'Skip to main content' })
    await expect(skipLink).toBeFocused()
    await skipLink.press('Enter')
    await expect(page.locator('main#main-content')).toBeFocused()

    await page.goto(`${app.baseUrl}/sessions`)
    await expect(page.getByRole('heading', { name: 'Sessions', exact: true })).toBeVisible()
    const sessionsLedger = page.getByRole('region', { name: 'Scrollable sessions ledger' })
    if (width < 1280) await expectScrollableLedger(sessionsLedger)
    else await expect(sessionsLedger).toBeVisible()
    await expectNoDocumentHorizontalOverflow(page)

    const projectTrigger = page.locator('.project-filter .filter-button')
    await projectTrigger.click()
    const projectMenu = page.locator('.project-menu')
    const projectSearch = page.getByRole('combobox', { name: 'Search projects' })
    await expect(projectSearch).toBeFocused()
    const menuBox = await projectMenu.boundingBox()
    expect(menuBox).not.toBeNull()
    expect(menuBox!.x).toBeGreaterThanOrEqual(0)
    expect(menuBox!.x + menuBox!.width).toBeLessThanOrEqual(width + 1)
    await projectSearch.fill('dashboard')
    await expect(page.getByRole('option', { name: 'codex-dashboard' })).toBeVisible()
    await projectSearch.press('Enter')
    await expect(page).toHaveURL(/(?:\?|&)project=codex-dashboard(?:&|$)/)
    await expectNoDocumentHorizontalOverflow(page)

    await page.goto(`${app.baseUrl}/stats?range=month&anchor=${overviewScaleYear}-07-15`)
    await expect(page.getByRole('heading', { name: 'Stats', exact: true })).toBeVisible()
    const statsLedger = page.getByRole('region', { name: 'Scrollable statistics ledger' })
    if (width < 1280) await expectScrollableLedger(statsLedger)
    else await expect(statsLedger).toBeVisible()
    await expectNoDocumentHorizontalOverflow(page)

    await page.goto(`${app.baseUrl}/sessions/${RICH_SESSION}?tab=activity`)
    await expect(page.getByRole('table', { name: 'Session activity' })).toBeVisible()
    if (width === 680) {
      await expectScrollableLedger(page.getByRole('region', { name: 'Scrollable session activity ledger' }))
    }
    await expectNoDocumentHorizontalOverflow(page)
  }
})

test('Settings preserves search focus during refetch and keeps modal actions reachable on a short viewport', async ({ page, app }) => {
  let requestSeen = false
  let releaseRequest!: () => void
  const requestGate = new Promise<void>(resolve => { releaseRequest = resolve })
  await page.route('**/api/v1/prices?*', async route => {
    const url = new URL(route.request().url())
    if (url.searchParams.get('q') === 'focus-check') {
      requestSeen = true
      await requestGate
    }
    await route.continue()
  })

  await page.goto(`${app.baseUrl}/settings`)
  const search = page.getByRole('textbox', { name: 'Search model prices' })
  await expect(search).toBeVisible()
  await search.fill('focus-check')
  await expect.poll(() => requestSeen).toBe(true)

  const priceLedger = page.getByRole('region', { name: 'Scrollable model price ledger' })
  await expect(search).toBeFocused()
  await expect(page.getByRole('table', { name: 'Model prices' })).toBeVisible()
  await expect(priceLedger).toHaveAttribute('aria-busy', 'true')

  releaseRequest()
  await expect(priceLedger).not.toHaveAttribute('aria-busy')
  await expect(search).toBeFocused()

  await page.setViewportSize({ width: 680, height: 360 })
  await page.getByRole('button', { name: 'ADD PRICE' }).click()
  const dialog = page.getByRole('dialog', { name: 'Add model price' })
  const geometry = await dialog.evaluate(element => ({
    clientHeight: element.clientHeight,
    scrollHeight: element.scrollHeight,
    overflowY: getComputedStyle(element).overflowY,
  }))
  expect(geometry.clientHeight).toBeLessThanOrEqual(328)
  expect(geometry.scrollHeight).toBeGreaterThan(geometry.clientHeight)
  expect(geometry.overflowY).toBe('auto')
  await dialog.evaluate(element => { element.scrollTop = element.scrollHeight })
  await expect(dialog.getByRole('button', { name: 'SAVE PRICE' })).toBeInViewport()
  await dialog.getByRole('button', { name: 'CANCEL' }).click()
})

test('session Summary discloses and expands compact model and tool category lists', async ({ page, app }) => {
  await page.route(`**/api/v1/sessions/${RICH_SESSION}/summary`, async route => {
    const response = await route.fetch()
    const body = await response.json() as {
      models: unknown[]
      toolSummary: unknown[]
    }
    body.models = Array.from({ length: 8 }, (_, index) => ({
      model: `model-${index}`,
      effort: null,
      inputTokens: 10,
      cachedInputTokens: 0,
      outputTokens: 2,
      reasoningTokens: 1,
      totalTokens: 12,
      costUsd: '0.01',
      unpricedTokens: 0,
    }))
    body.toolSummary = Array.from({ length: 20 }, (_, index) => ({
      tool: `tool-${index}`,
      count: 20 - index,
      failedCount: 0,
      totalDurationMs: index,
    }))
    await route.fulfill({ response, json: body })
  })

  await page.goto(`${app.baseUrl}/sessions/${RICH_SESSION}`)
  await expect(page.getByText('model-5', { exact: true })).toBeVisible()
  await expect(page.getByText('model-6', { exact: true })).toHaveCount(0)
  await expect(page.getByText('tool-17', { exact: true })).toBeVisible()
  await expect(page.getByText('tool-18', { exact: true })).toHaveCount(0)

  await page.getByRole('button', { name: 'SHOWING 6 OF 8 · SHOW ALL' }).click()
  await page.getByRole('button', { name: 'SHOWING 18 OF 20 · SHOW ALL' }).click()
  await expect(page.getByText('model-7', { exact: true })).toBeVisible()
  await expect(page.getByText('tool-19', { exact: true })).toBeVisible()
})

test('primary navigation and a reloaded Activity deep link preserve route identity', async ({ page, app }) => {
  await page.goto(`${app.baseUrl}/`)
  const primaryNavigation = page.getByRole('navigation', { name: 'Primary navigation' })

  await primaryNavigation.getByRole('link', { name: 'Sessions' }).click()
  await expect(page.getByRole('heading', { name: 'Sessions', exact: true })).toBeVisible()
  await primaryNavigation.getByRole('link', { name: 'Stats' }).click()
  await expect(page.getByRole('heading', { name: 'Stats', exact: true })).toBeVisible()
  await page.getByRole('link', { name: 'Codex usage overview' }).click()
  await expect(page.getByRole('heading', { name: 'Overview', exact: true })).toBeVisible()

  await page.goto(`${app.baseUrl}/sessions/${RICH_SESSION}?tab=activity`)
  await page.reload()

  await expect(page).toHaveURL(new RegExp(`/sessions/${RICH_SESSION}\\?tab=activity$`))
  await expect(page.getByRole('heading', {
    name: 'Revisit the usage application from ingestion through the browser UI.',
  })).toBeVisible()
  await expect(page.getByRole('link', {
    name: `Open session ${RICH_SESSION} in Codex`,
  })).toHaveAttribute('href', `codex://threads/${RICH_SESSION}`)
  await expect(page.getByRole('tab', { name: 'ACTIVITY' })).toHaveAttribute('aria-selected', 'true')
  await expect(page.getByRole('button', {
    name: /Revisit the usage application from ingestion through the browser UI\./,
  })).toBeVisible()
})

test('Sessions supports sorting, debounced search, inset geometry, and detail navigation', async ({ page, app }) => {
  await page.goto(`${app.baseUrl}/sessions`)
  await expect(page.getByRole('heading', { name: 'Sessions', exact: true })).toBeVisible()

  const ledger = page.locator('.sessions-ledger')
  await expect(ledger).toBeVisible()
  const ledgerBox = await ledger.boundingBox()
  const documentWidth = await page.evaluate(() => document.documentElement.clientWidth)
  expect(ledgerBox).not.toBeNull()
  expect(ledgerBox?.x).toBe(32)
  expect(ledgerBox?.width).toBe(documentWidth - 64)

  const costSort = page.getByRole('button', { name: 'COST' })
  await costSort.click()
  await expect(costSort).toHaveAttribute('aria-pressed', 'true')
  await expect(page).toHaveURL(/(?:\?|&)sort=cost(?:&|$)/)

  await page.getByRole('textbox', { name: 'Search sessions' }).fill(RICH_SESSION)
  await expect(page).toHaveURL(new RegExp(`(?:\\?|&)q=${RICH_SESSION}(?:&|$)`))
  await expect(page.getByText('1 RESULTS', { exact: true })).toBeVisible()

  const sessionLink = page.getByRole('link', {
    name: /Revisit the usage application from ingestion through the browser UI\./,
  })
  await expect(sessionLink).toBeVisible()
  await sessionLink.click()

  await expect(page).toHaveURL(`${app.baseUrl}/sessions/${RICH_SESSION}`)
  await expect(page.getByRole('heading', {
    name: 'Revisit the usage application from ingestion through the browser UI.',
  })).toBeVisible()
})

test('pagination stays focused and inert while replacement pages load', async ({ page, app }) => {
  let sessionsRequestSeen = false
  let releaseSessions!: () => void
  const sessionsGate = new Promise<void>(resolve => { releaseSessions = resolve })
  await page.route('**/api/v1/sessions?*', async route => {
    const url = new URL(route.request().url())
    if (url.pathname === '/api/v1/sessions' && url.searchParams.get('page') === '2') {
      sessionsRequestSeen = true
      await sessionsGate
    }
    await route.continue()
  })

  await page.goto(`${app.baseUrl}/sessions`)
  const sessionsPagination = page.getByRole('navigation', { name: 'Pagination' })
  const sessionsPageTwo = sessionsPagination.getByRole('button', { name: '02' })
  await sessionsPageTwo.focus()
  await sessionsPageTwo.click()
  await expect.poll(() => sessionsRequestSeen).toBe(true)
  await expect(sessionsPageTwo).toBeFocused()
  await expect(sessionsPageTwo).not.toHaveAttribute('disabled')
  await expect(sessionsPageTwo).toHaveAttribute('aria-disabled', 'true')
  releaseSessions()
  await expect(sessionsPagination).not.toHaveAttribute('aria-busy')
  await expect(sessionsPageTwo).toBeFocused()

  let pricesRequestSeen = false
  let releasePrices!: () => void
  const pricesGate = new Promise<void>(resolve => { releasePrices = resolve })
  await page.route('**/api/v1/prices?*', async route => {
    const url = new URL(route.request().url())
    const requestedPage = Number(url.searchParams.get('page') ?? 1)
    if (requestedPage === 2) {
      pricesRequestSeen = true
      await pricesGate
    }
    const response = await route.fetch()
    const body = await response.json() as Record<string, unknown>
    await route.fulfill({ response, json: { ...body, page: requestedPage, total: 50, totalPages: 2 } })
  })

  await page.goto(`${app.baseUrl}/settings?tab=price-data`)
  const pricesPagination = page.getByRole('navigation', { name: 'Pagination' })
  const pricesPageTwo = pricesPagination.getByRole('button', { name: '02' })
  await pricesPageTwo.focus()
  await pricesPageTwo.click()
  await expect.poll(() => pricesRequestSeen).toBe(true)
  await expect(pricesPageTwo).toBeFocused()
  await expect(pricesPageTwo).not.toHaveAttribute('disabled')
  await expect(pricesPageTwo).toHaveAttribute('aria-disabled', 'true')
  releasePrices()
  await expect(pricesPagination).not.toHaveAttribute('aria-busy')
  await expect(pricesPageTwo).toBeFocused()

  let activityRequestSeen = false
  let releaseActivity!: () => void
  const activityGate = new Promise<void>(resolve => { releaseActivity = resolve })
  await page.route(`**/api/v1/sessions/${RICH_SESSION}/activity?*`, async route => {
    const url = new URL(route.request().url())
    const requestedPage = Number(url.searchParams.get('page') ?? 1)
    if (requestedPage === 2) {
      activityRequestSeen = true
      await activityGate
    }
    const response = await route.fetch()
    const body = await response.json() as Record<string, unknown>
    await route.fulfill({ response, json: { ...body, page: requestedPage, total: 50, totalPages: 2 } })
  })

  await page.goto(`${app.baseUrl}/sessions/${RICH_SESSION}?tab=activity`)
  const activityPagination = page.getByRole('navigation', { name: 'Pagination' })
  const activityPageTwo = activityPagination.getByRole('button', { name: '02' })
  await activityPageTwo.focus()
  await activityPageTwo.click()
  await expect.poll(() => activityRequestSeen).toBe(true)
  await expect(activityPageTwo).toBeFocused()
  await expect(activityPageTwo).not.toHaveAttribute('disabled')
  await expect(activityPageTwo).toHaveAttribute('aria-disabled', 'true')
  releaseActivity()
  await expect(activityPagination).not.toHaveAttribute('aria-busy')
  await expect(activityPageTwo).toBeFocused()
})

test('keyboard focus, filter clearing, tabs, and Activity disclosures remain honest and visible', async ({ page, app }) => {
  await page.goto(`${app.baseUrl}/sessions?date=2026-07-15&project=codex-dashboard`)

  const search = page.getByRole('textbox', { name: 'Search sessions' })
  await search.focus()
  await expect(search).toBeFocused()
  const focusOutline = await search.locator('..').evaluate(element => {
    const style = getComputedStyle(element)
    return { style: style.outlineStyle, width: style.outlineWidth }
  })
  expect(focusOutline).toEqual({ style: 'solid', width: '3px' })

  const dateTrigger = page.locator('.filter-row .filter-wrap').first().locator('.filter-button')
  const projectTrigger = page.locator('.project-filter .filter-button')
  const dateClear = page.getByRole('button', { name: 'Clear date range' })
  const projectClear = page.getByRole('button', { name: 'Clear project filter' })
  await expect(dateClear).toBeVisible()
  await expect(projectClear).toBeVisible()
  for (const [clearButton, expectedWidth] of [[dateClear, '120px'], [projectClear, '104px']] as const) {
    const style = await clearButton.evaluate(element => {
      const computed = getComputedStyle(element)
      return { background: computed.backgroundColor, width: computed.width }
    })
    expect(style).toEqual({ background: 'rgb(246, 75, 28)', width: expectedWidth })
  }

  await projectClear.focus()
  await projectClear.click()
  await expect(projectTrigger).toBeFocused()
  await projectTrigger.press('ArrowDown')
  const projectMenu = page.getByRole('listbox', { name: 'Projects' })
  const projectSearch = page.getByRole('combobox', { name: 'Search projects' })
  await expect(projectMenu).toBeVisible()
  await expect(projectSearch).toBeFocused()
  await expect(projectSearch).toHaveAttribute('aria-activedescendant', 'project-option-0')
  expect(await projectMenu.getByRole('option').evaluateAll(options => options.map(option => (option as HTMLElement).tabIndex)))
    .toEqual(Array(await projectMenu.getByRole('option').count()).fill(-1))
  await page.keyboard.press('Tab')
  await expect(projectMenu).toBeHidden()
  await expect(page.locator('[role="option"]:focus')).toHaveCount(0)

  await projectTrigger.focus()
  await projectTrigger.press('ArrowDown')
  await expect(projectMenu).toBeVisible()
  await projectSearch.fill('dashboard')
  await expect(projectMenu.getByRole('option')).toHaveCount(1)
  await expect(projectMenu.getByRole('option', { name: 'codex-dashboard' })).toBeVisible()
  await projectSearch.press('Enter')
  await expect(page).toHaveURL(/(?:\?|&)project=codex-dashboard(?:&|$)/)
  await expect(projectTrigger).toBeFocused()

  await dateClear.focus()
  await dateClear.click()
  await expect(dateTrigger).toBeFocused()

  await page.goto(`${app.baseUrl}/stats?range=month&anchor=${overviewScaleYear}-07-15`)
  const monthTab = page.getByRole('tab', { name: 'MONTH' })
  await monthTab.focus()
  await monthTab.press('ArrowRight')
  const yearTab = page.getByRole('tab', { name: 'YEAR' })
  await expect(yearTab).toBeFocused()
  await expect(yearTab).toHaveAttribute('aria-selected', 'true')

  await page.goto(`${app.baseUrl}/sessions/${RICH_SESSION}?tab=activity`)
  await expect(page.getByRole('table', { name: 'Session activity' })).toBeVisible()
  await expect(page.locator('[role="treegrid"]')).toHaveCount(0)
  await expect(page.locator('[aria-level]')).toHaveCount(0)
  const exchange = page.getByRole('button', {
    name: 'Toggle Revisit the usage application from ingestion through the browser UI. details',
  })
  await exchange.click()
  await expect(exchange).toHaveAttribute('aria-expanded', 'true')
  await expect(page.locator('.activity-child-list[role="list"] > [role="listitem"]').first()).toBeVisible()
})

test('session Summary and Activity retain the rich hierarchy and authored user text', async ({ page, app }) => {
  await page.goto(`${app.baseUrl}/sessions/${RICH_SESSION}`)

  await expect(page.getByRole('tab', { name: 'SUMMARY' })).toHaveAttribute('aria-selected', 'true')
  await expect(page.getByText('FIRST PROMPT', { exact: true })).toBeVisible()
  await expect(page.getByText('LATEST ASSISTANT RESULT', { exact: true })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'MODELS & REASONING' })).toBeVisible()
  await expect(page.getByText('gpt-5.6-sol', { exact: true })).toBeVisible()

  await page.getByRole('tab', { name: 'ACTIVITY' }).click()
  const exchange = page.getByRole('button', {
    name: 'Toggle Revisit the usage application from ingestion through the browser UI. details',
  })
  await exchange.click()
  await expect(exchange).toHaveAttribute('aria-expanded', 'true')
  await expect(page.getByRole('button', {
    name: /Assistant update: I found the copied-history ownership failure/,
  })).toBeVisible()
  await expect(page.getByText(/^Work · \d+ events?$/).first()).toBeVisible()
  await expect(page.getByText('Context compacted', { exact: true })).toBeVisible()
  await expect(page.getByRole('button', {
    name: 'Final answer: The model now keeps the rich trace while the main UI stays quiet.',
    exact: true,
  })).toBeVisible()

  await page.goto(`${app.baseUrl}/sessions/${ABORTED_SESSION}?tab=activity`)
  const interruptedExchange = page.getByRole('button', {
    name: /Why is this giant usage bucket not visible in Sessions\?/,
  })
  await interruptedExchange.click()
  const userMessage = page.getByRole('button', {
    name: /User message: Why is this giant usage bucket not visible in Sessions\?/,
  })
  await userMessage.click()

  await expect(page.locator('.user-message-primary').getByText(
    'Why is this giant usage bucket not visible in Sessions?',
    { exact: true },
  )).toBeVisible()
  await expect(page.getByRole('region', { name: 'Supporting material' })).toHaveCount(0)
})

test('nested Activity disclosure controls stay in the Details column', async ({ page, app }) => {
  await page.goto(`${app.baseUrl}/sessions/${RICH_SESSION}?tab=activity`)

  const exchange = page.getByRole('button', {
    name: 'Toggle Revisit the usage application from ingestion through the browser UI. details',
  })
  await exchange.click()
  await expect(exchange).toHaveAttribute('aria-expanded', 'true')

  const workGroupToggles = page.getByRole('button', {
    name: /^Toggle Work · \d+ events? details$/,
  })
  await expect(workGroupToggles.first()).toBeVisible()
  const tokenBearingWorkIndex = await workGroupToggles.evaluateAll(buttons => buttons.findIndex(button => (
    button.closest('.activity-event')?.querySelector('.event-tokens')?.textContent?.trim() !== '—'
  )))
  expect(tokenBearingWorkIndex, 'fixture contains a token-bearing expandable Work group').toBeGreaterThanOrEqual(0)

  const workGroupToggle = workGroupToggles.nth(tokenBearingWorkIndex)
  const workGroupRow = workGroupToggle.locator('..').locator('..')
  await expect(workGroupRow).toHaveAttribute('data-activity-depth', '2')
  await expect(workGroupRow.locator('.event-tokens')).not.toHaveText('—')
  await workGroupToggle.click()
  await expect(workGroupToggle).toHaveAttribute('aria-expanded', 'true')

  const nestedExpandableRows = page.locator('.activity-event-details .activity-event.expandable')
  await expect(page.locator('.activity-event-details .activity-event.expandable[data-activity-depth="3"]').first()).toBeVisible()

  const geometry = await activityCaretGeometry(nestedExpandableRows)
  const levels = new Set(geometry.map(row => row.level))
  expect(levels.has(2), 'fixture exercises an immediate child disclosure').toBe(true)
  expect(levels.has(3), 'fixture exercises a disclosure inside an expanded Work group').toBe(true)
  expect(
    geometry.some(row => row.level === 2 && row.tokenText !== '—'),
    'fixture exercises a token-bearing expandable nested row',
  ).toBe(true)
  expectActivityCaretsInDetailsColumn(geometry)
})

test('Stats switches an explicit July range through a loading state and drills into Sessions', async ({ page, app }) => {
  await page.route('**/api/v1/stats?*', async route => {
    const url = new URL(route.request().url())
    if (url.searchParams.get('range') === 'year') {
      await new Promise(resolve => setTimeout(resolve, 200))
    }
    await route.continue()
  })

  await page.goto(`${app.baseUrl}/stats?range=month&anchor=${overviewScaleYear}-07-15`)
  await expect(page.getByRole('tab', { name: 'MONTH' })).toHaveAttribute('aria-selected', 'true')
  await expect(page.getByText(`July ${overviewScaleYear}`, { exact: true })).toBeVisible()

  const yearTab = page.getByRole('tab', { name: 'YEAR' })
  await yearTab.click()
  await expect(yearTab).toHaveAttribute('aria-selected', 'true')
  await expect(page.getByLabel('Loading')).toBeVisible()
  await expect(page.getByText(String(overviewScaleYear), { exact: true })).toBeVisible()

  const selectedStyle = await yearTab.evaluate(element => {
    const style = getComputedStyle(element)
    return { background: style.backgroundColor, shadow: style.boxShadow }
  })
  expect(selectedStyle.background).toBe('rgb(252, 201, 66)')
  expect(selectedStyle.shadow).toContain('rgb(246, 75, 28) 8px 0px 0px 0px inset')

  await page.getByRole('button', { name: 'View Jul sessions' }).click()
  await expect(page.getByRole('heading', { name: 'Sessions', exact: true })).toBeVisible()
  const drilldown = new URL(page.url()).searchParams
  const start = drilldown.get('start')
  const end = drilldown.get('end')
  expect(start).not.toBeNull()
  expect(end).not.toBeNull()
  expect(new Intl.DateTimeFormat('sv-SE', { timeZone: 'Europe/Amsterdam' }).format(new Date(start!))).toBe(`${overviewScaleYear}-07-01`)
  expect(new Intl.DateTimeFormat('sv-SE', { timeZone: 'Europe/Amsterdam' }).format(new Date(new Date(end!).getTime() - 1))).toBe(`${overviewScaleYear}-07-31`)
  await expect(page.getByRole('button', { name: 'JUL 1–31' })).toBeVisible()
  await expect(page.getByRole('button', { name: 'Clear date range' })).toBeVisible()
})

test('the live scanner and Sessions polling surface a newly completed rollout without reloading', async ({ page, app, request }) => {
  await page.clock.install({ time: new Date('2026-07-18T14:00:00+02:00') })
  await page.goto(`${app.baseUrl}/sessions?q=${LIVE_SESSION}`)
  await expect(page.getByText('NO SESSIONS FOUND', { exact: true })).toBeVisible()

  const timestamp = '2026-07-18T12:00:00.000Z'
  const prompt = 'Observe a newly ingested rollout without reloading the Sessions page.'
  const answer = 'The isolated scanner projected this rollout successfully.'
  const records = [
    { timestamp, type: 'session_meta', payload: { id: LIVE_SESSION, session_id: LIVE_SESSION, timestamp, cwd: '/tmp/e2e-live', originator: 'Codex Desktop', source: 'vscode', model_provider: 'openai', git: { branch: 'main' } } },
    { timestamp: '2026-07-18T12:00:00.010Z', type: 'event_msg', payload: { type: 'task_started', turn_id: LIVE_TURN, model_context_window: 258400 } },
    { timestamp: '2026-07-18T12:00:00.020Z', type: 'turn_context', payload: { turn_id: LIVE_TURN, model: 'gpt-5.6-sol', effort: 'low', cwd: '/tmp/e2e-live' } },
    { timestamp: '2026-07-18T12:00:00.030Z', type: 'response_item', payload: { type: 'message', id: 'msg_e2e_live_user', role: 'user', content: [{ type: 'input_text', text: prompt }] } },
    { timestamp: '2026-07-18T12:00:00.040Z', type: 'event_msg', payload: { type: 'user_message', message: prompt } },
    { timestamp: '2026-07-18T12:00:01.000Z', type: 'event_msg', payload: { type: 'agent_message', message: answer } },
    { timestamp: '2026-07-18T12:00:01.010Z', type: 'response_item', payload: { type: 'message', id: 'msg_e2e_live_final', role: 'assistant', phase: 'final_answer', content: [{ type: 'output_text', text: answer }] } },
    { timestamp: '2026-07-18T12:00:01.020Z', type: 'event_msg', payload: { type: 'task_complete', turn_id: LIVE_TURN, last_agent_message: answer } },
  ]
  const liveDirectory = join(app.activeRoot, 'live')
  await mkdir(liveDirectory, { recursive: true })
  await writeFile(
    join(liveDirectory, `rollout-2026-07-18T14-00-00-${LIVE_SESSION}.jsonl`),
    `${records.map(record => JSON.stringify(record)).join('\n')}\n`,
  )

  await expect.poll(async () => {
    const response = await request.get(`${app.baseUrl}/api/v1/sessions?q=${LIVE_SESSION}&pageSize=50`)
    if (!response.ok()) return -1
    const body = await response.json() as { total: number }
    return body.total
  }, { timeout: 10_000 }).toBe(1)

  await page.clock.fastForward(30_100)
  await expect(page.getByRole('link', { name: new RegExp(prompt) })).toBeVisible()
})

test('Settings identifies its upstream and round-trips exact decimal prices through the editor', async ({ page, app, request }, testInfo) => {
  const modelId = `e2e-price-${testInfo.workerIndex}-${testInfo.retry}`
  const effectiveFrom = '2026-01-01T00:00:00Z'
  const modelUrl = `${app.baseUrl}/api/v1/prices/${encodeURIComponent(modelId)}`
  const cleanupUrl = `${modelUrl}?effectiveFrom=${encodeURIComponent(effectiveFrom)}`
  await request.delete(cleanupUrl)

  try {
    await page.goto(`${app.baseUrl}/settings`)
    await expect(page.getByRole('heading', { name: 'Settings', exact: true })).toBeVisible()
    await expect(page.locator('.price-toolbar').getByText('LITELLM', { exact: true })).toBeVisible()

    await page.getByRole('button', { name: 'ADD PRICE' }).click()
    const dialog = page.getByRole('dialog', { name: 'Add model price' })
    await dialog.getByLabel('MODEL ID').fill(modelId)
    await dialog.getByLabel('EFFECTIVE FROM').fill('2026-01-01')
    await dialog.getByLabel('INPUT / 1M').fill('0.123456')
    await dialog.getByLabel('CACHED / 1M').fill('0.012345')
    await dialog.getByLabel('OUTPUT / 1M').fill('1.234567')
    await dialog.getByRole('button', { name: 'SAVE PRICE' }).click()
    await expect(dialog).toBeHidden()

    await page.getByRole('textbox', { name: 'Search model prices' }).fill(modelId)
    await expect(page.getByText(modelId, { exact: true })).toBeVisible()

    const listedResponse = await request.get(
      `${app.baseUrl}/api/v1/prices?q=${encodeURIComponent(modelId)}&page=1&pageSize=25`,
    )
    expect(listedResponse.ok()).toBe(true)
    const listed = await listedResponse.json() as {
      items: Array<{
        modelId: string
        inputPerMillion: string
        cachedInputPerMillion: string | null
        outputPerMillion: string
      }>
    }
    expect(listed.items).toContainEqual(expect.objectContaining({
      modelId,
      inputPerMillion: '0.123456',
      cachedInputPerMillion: '0.012345',
      outputPerMillion: '1.234567',
    }))

    await page.getByRole('button', { name: `Edit ${modelId}` }).click()
    const editDialog = page.getByRole('dialog', { name: 'Edit model price' })
    await expect(editDialog.getByLabel('INPUT / 1M')).toHaveValue('0.123456')
    await expect(editDialog.getByLabel('CACHED / 1M')).toHaveValue('0.012345')
    await expect(editDialog.getByLabel('OUTPUT / 1M')).toHaveValue('1.234567')
    await editDialog.getByRole('button', { name: 'CANCEL' }).click()
  } finally {
    await request.delete(cleanupUrl)
  }
})
