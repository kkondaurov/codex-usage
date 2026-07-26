import { act, fireEvent, render, screen, within } from '@testing-library/react'
import { StrictMode } from 'react'
import { MemoryRouter } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { api } from '../api'
import type { OverviewResponse, OverviewYearResponse, SessionRow, Totals } from '../types'
import { buildAnnualHeatmapLayout, OverviewPage, placeHeatmapCard } from './OverviewPage'

const totals: Totals = {
  inputTokens: 10,
  cachedInputTokens: 0,
  outputTokens: 2,
  reasoningTokens: 0,
  blendedTokens: 12,
  totalTokens: 12,
  costUsd: '0.01',
  unpricedTokens: 0,
  pricingComplete: true,
}

const summaryResponse: OverviewResponse = {
  updatedAt: '2026-07-16T10:00:00Z',
  periods: {
    today: { start: '2026-07-15T22:00:00Z', end: '2026-07-16T22:00:00Z', sessionCount: 1, messageCount: 2, totals: { ...totals, costUsd: '12.34', totalTokens: 1_234 } },
    week: { sessionCount: 3, messageCount: 4, totals: { ...totals, costUsd: '23.45' } },
    month: { sessionCount: 5, messageCount: 6, totals: { ...totals, costUsd: '34.56' } },
  },
}

function session(id: string, title: string, overrides: Partial<SessionRow> = {}): SessionRow {
  return {
    id,
    rootThreadId: id,
    startedAt: '2026-07-15T12:00:00Z',
    lastEventAt: '2026-07-16T10:00:00Z',
    title,
    project: 'codex-usage',
    branch: 'main',
    messageCount: 10,
    turnCount: 2,
    agentCount: 1,
    toolCount: 3,
    totalTokens: 2_500,
    costUsd: '9.87',
    unpricedTokens: 0,
    lifetimeCostUsd: '9.87',
    lifetimeUnpricedTokens: 0,
    ...overrides,
  }
}

function yearResponse(year = 2026): OverviewYearResponse {
  return {
    year,
    heatmap: [{ date: `${year}-07-16`, costUsd: '12.34', sessionCount: 2, messageCount: 7, totalTokens: 98_765 }],
    topProjects: [
      { project: '/Users/example/src/codex-usage', costUsd: '90', share: .9 },
      { project: 'personal-hq', costUsd: '8', share: .08 },
      { project: 'peregrine', costUsd: '2', share: .02 },
    ],
    topSessions: [
      session(`${year}-one`, `${year} winner`, { startedAt: `${year - 1}-12-31T23:00:00Z` }),
      session(`${year}-two`, `${year} runner-up`),
      session(`${year}-three`, `${year} third place`),
    ],
  }
}

function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (reason: Error) => void
  const promise = new Promise<T>((nextResolve, nextReject) => {
    resolve = nextResolve
    reject = nextReject
  })
  return { promise, resolve, reject }
}

async function flush() {
  await act(async () => {
    await Promise.resolve()
    await Promise.resolve()
  })
}

function renderOverview() {
  return render(<MemoryRouter><OverviewPage /></MemoryRouter>)
}

beforeEach(() => {
  vi.useFakeTimers()
  vi.setSystemTime(new Date('2026-07-16T12:00:00+02:00'))
})

afterEach(() => {
  vi.useRealTimers()
  vi.restoreAllMocks()
})

describe('annual heatmap geometry', () => {
  it('uses calendar days rather than elapsed DST milliseconds for month columns', () => {
    const layout = buildAnnualHeatmapLayout(2026, [], new Date('2026-07-16T12:00:00+02:00'))
    expect(layout.months.find(month => month.name === 'JUN')?.week).toBe(22)
    expect(layout.cells.find(cell => cell.day.date === '2026-06-01')?.week).toBe(22)
  })

  it('marks server-provided dates after today as future and noninteractive', () => {
    const layout = buildAnnualHeatmapLayout(
      2026,
      [{ date: '2026-12-31', costUsd: null, sessionCount: 0, messageCount: 0, totalTokens: 0 }],
      new Date('2026-07-16T12:00:00+02:00'),
    )
    const future = layout.cells.find(cell => cell.day.date === '2026-12-31')?.day
    expect(future?.future).toBe(true)
    expect(future?.costUsd).toBeNull()
  })

  it('flips and clamps the hover card at every viewport edge', () => {
    expect(placeHeatmapCard({ left: 2, right: 21, top: 2, bottom: 21 }, { width: 100, height: 80 }, { width: 320, height: 240 })).toEqual({ left: 31, top: 31 })
    expect(placeHeatmapCard({ left: 299, right: 318, top: 219, bottom: 238 }, { width: 100, height: 80 }, { width: 320, height: 240 })).toEqual({ left: 189, top: 129 })
    expect(placeHeatmapCard({ left: -20, right: -1, top: -20, bottom: -1 }, { width: 400, height: 300 }, { width: 320, height: 240 })).toEqual({ left: 8, top: 8 })
  })
})

describe('OverviewPage independent loading', () => {
  it('starts summary and annual loading together without duplicate requests', async () => {
    const summary = deferred<OverviewResponse>()
    const yearly = deferred<OverviewYearResponse>()
    const summarySpy = vi.spyOn(api, 'overview').mockReturnValue(summary.promise)
    const yearSpy = vi.spyOn(api, 'overviewYear').mockReturnValue(yearly.promise)

    render(<StrictMode><MemoryRouter><OverviewPage /></MemoryRouter></StrictMode>)
    expect(screen.getByRole('heading', { name: 'Overview' })).toBeInTheDocument()
    expect(screen.getByLabelText('Loading overview summary')).toBeInTheDocument()
    expect(screen.getByLabelText('Loading 2026 yearly usage')).toBeInTheDocument()
    expect(screen.getByRole('heading', { name: 'TOP PROJECTS · 2026' })).toBeInTheDocument()
    expect(screen.getByRole('heading', { name: 'TOP SESSIONS · 2026' })).toBeInTheDocument()
    expect(summarySpy).toHaveBeenCalledTimes(1)
    expect(yearSpy).toHaveBeenCalledTimes(1)
    expect(yearSpy).toHaveBeenCalledWith(2026, expect.any(AbortSignal))
    expect(within(screen.getByLabelText('Loading 2026 yearly usage')).queryAllByRole('button')).toEqual([])

    await act(async () => { yearly.resolve(yearResponse()); await Promise.resolve() })
    expect(screen.getByText('2026 winner')).toBeInTheDocument()
    expect(screen.getByLabelText('Loading overview summary')).toBeInTheDocument()
    expect(summarySpy).toHaveBeenCalledTimes(1)
    expect(yearSpy).toHaveBeenCalledTimes(1)

    await act(async () => { summary.resolve(summaryResponse); await Promise.resolve() })
    expect(screen.getByText('$12.34')).toBeInTheDocument()
    expect(screen.queryByText(/Updated/i)).not.toBeInTheDocument()
    expect(screen.queryByLabelText('Loading overview summary')).not.toBeInTheDocument()
    expect(summarySpy).toHaveBeenCalledTimes(1)
    expect(yearSpy).toHaveBeenCalledTimes(1)
  })

  it('reuses both Overview responses until the 30-second freshness boundary', async () => {
    const summarySpy = vi.spyOn(api, 'overview').mockResolvedValue(summaryResponse)
    const yearSpy = vi.spyOn(api, 'overviewYear').mockResolvedValue(yearResponse())

    const first = renderOverview()
    await flush()
    expect(summarySpy).toHaveBeenCalledTimes(1)
    expect(yearSpy).toHaveBeenCalledTimes(1)
    first.unmount()

    await act(async () => { await vi.advanceTimersByTimeAsync(29_999) })
    const second = renderOverview()
    expect(screen.getByText('$12.34')).toBeInTheDocument()
    expect(screen.getByText('2026 winner')).toBeInTheDocument()
    expect(summarySpy).toHaveBeenCalledTimes(1)
    expect(yearSpy).toHaveBeenCalledTimes(1)

    await act(async () => { await vi.advanceTimersByTimeAsync(1) })
    expect(summarySpy).toHaveBeenCalledTimes(2)
    expect(yearSpy).toHaveBeenCalledTimes(2)
    second.unmount()
  })

  it('uses the browser calendar date for today navigation', async () => {
    vi.spyOn(api, 'overview').mockResolvedValue(summaryResponse)
    vi.spyOn(api, 'overviewYear').mockResolvedValue(yearResponse())
    renderOverview()
    await flush()
    expect(screen.getByRole('link', { name: /VIEW TODAY’S SESSIONS/ })).toHaveAttribute('href', '/sessions?date=2026-07-16')
  })

  it('keeps summary and year failures local and retryable', async () => {
    const summaryRetry = deferred<OverviewResponse>()
    const yearRetry = deferred<OverviewYearResponse>()
    vi.spyOn(api, 'overview').mockRejectedValueOnce(new Error('summary broke')).mockReturnValueOnce(summaryRetry.promise)
    vi.spyOn(api, 'overviewYear').mockRejectedValueOnce(new Error('year broke')).mockReturnValueOnce(yearRetry.promise)
    renderOverview()
    await flush()

    expect(screen.getByText('summary broke')).toBeInTheDocument()
    expect(screen.getAllByText('year broke').length).toBeGreaterThan(0)
    expect(screen.getByRole('group', { name: '2026 usage by day' }).querySelectorAll('.heatmap-tile')).toHaveLength(0)
    const retryButtons = screen.getAllByRole('button', { name: 'TRY AGAIN' })
    fireEvent.click(retryButtons[0])
    fireEvent.click(retryButtons[1])
    await act(async () => {
      summaryRetry.resolve(summaryResponse)
      yearRetry.resolve(yearResponse())
      await Promise.resolve()
    })

    expect(screen.getByText('$12.34')).toBeInTheDocument()
    expect(screen.getByText('2026 winner')).toBeInTheDocument()
  })

  it('refetches only the year and removes stale-year rows immediately', async () => {
    const previousYear = deferred<OverviewYearResponse>()
    const summarySpy = vi.spyOn(api, 'overview').mockResolvedValue(summaryResponse)
    const yearSpy = vi.spyOn(api, 'overviewYear').mockImplementation(year => year === 2026 ? Promise.resolve(yearResponse(2026)) : previousYear.promise)
    renderOverview()
    await flush()
    expect(screen.getByText('2026 winner')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: 'Previous year' }))
    await flush()
    expect(screen.queryByText('2026 winner')).not.toBeInTheDocument()
    expect(screen.getByLabelText('Loading 2025 yearly usage')).toBeInTheDocument()
    expect(summarySpy).toHaveBeenCalledTimes(1)
    expect(yearSpy).toHaveBeenNthCalledWith(1, 2026, expect.any(AbortSignal))
    expect(yearSpy).toHaveBeenNthCalledWith(2, 2025, expect.any(AbortSignal))

    await act(async () => { previousYear.resolve(yearResponse(2025)); await Promise.resolve() })
    expect(screen.getByText('2025 winner')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /2025-07-16:/ })).toHaveAttribute('tabindex', '0')
  })

  it('does not navigate before the public 1970 boundary', async () => {
    vi.setSystemTime(new Date('1970-07-16T12:00:00+02:00'))
    vi.spyOn(api, 'overview').mockResolvedValue(summaryResponse)
    const yearSpy = vi.spyOn(api, 'overviewYear').mockResolvedValue(yearResponse(1970))
    renderOverview()
    await flush()

    const previous = screen.getByRole('button', { name: 'Previous year' })
    expect(previous).toHaveAttribute('aria-disabled', 'true')
    expect(previous).not.toBeDisabled()
    previous.focus()
    fireEvent.click(previous)
    await flush()
    expect(previous).toHaveFocus()
    expect(yearSpy).toHaveBeenCalledTimes(1)
    expect(yearSpy).toHaveBeenCalledWith(1970, expect.any(AbortSignal))
    expect(screen.getByRole('region', { name: '1970 yearly usage ledger' })).toBeVisible()
  })
})

describe('OverviewPage heatmap hover card', () => {
  async function loadedTile() {
    vi.spyOn(api, 'overview').mockResolvedValue(summaryResponse)
    vi.spyOn(api, 'overviewYear').mockResolvedValue(yearResponse())
    renderOverview()
    await flush()
    return screen.getByRole('button', { name: /2026-07-16:/ })
  }

  it('opens on hover, remains open across the pointer gap, and exposes every day metric', async () => {
    const tile = await loadedTile()
    fireEvent.pointerEnter(tile, { pointerType: 'mouse' })
    const card = screen.getByRole('dialog', { name: '2026-07-16 usage details' })
    expect(card).toHaveTextContent('THU, JUL 16, 2026')
    expect(card).toHaveTextContent('$12.34')
    expect(card).toHaveTextContent('2 sessions · 7 messages')
    expect(card).toHaveTextContent('98.8K API tokens')
    expect(screen.getByRole('link', { name: /VIEW SESSIONS/ })).toHaveAttribute('href', '/sessions?date=2026-07-16')
    expect(screen.getByText('JUL')).not.toHaveClass('focused-month')

    fireEvent.pointerLeave(tile, { pointerType: 'mouse' })
    fireEvent.pointerEnter(card, { pointerType: 'mouse' })
    act(() => vi.advanceTimersByTime(200))
    expect(screen.getByRole('dialog')).toBeInTheDocument()
    fireEvent.pointerLeave(card, { pointerType: 'mouse' })
    act(() => vi.advanceTimersByTime(200))
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
  })

  it('opens on focus and closes with Escape while returning focus to the tile', async () => {
    const tile = await loadedTile()
    act(() => tile.focus())
    expect(screen.getByRole('dialog')).toBeInTheDocument()
    fireEvent.keyDown(document, { key: 'Escape' })
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
    expect(tile).toHaveFocus()
  })

  it('keeps one nonfuture tile in the tab order and moves spatially with arrow keys', async () => {
    const tile = await loadedTile()
    const grid = screen.getByRole('group', { name: '2026 usage by day' })
    const tabStops = within(grid).getAllByRole('button').filter(button => button.tabIndex === 0)
    expect(tabStops).toEqual([tile])
    expect(screen.getByRole('button', { name: /2026-12-31:/ })).toBeDisabled()

    act(() => tile.focus())
    fireEvent.keyDown(tile, { key: 'ArrowLeft' })
    const previousWeek = screen.getByRole('button', { name: /2026-07-09:/ })
    expect(previousWeek).toHaveFocus()
    expect(previousWeek).toHaveAttribute('tabindex', '0')
    expect(tile).toHaveAttribute('tabindex', '-1')

    fireEvent.keyDown(previousWeek, { key: 'ArrowUp' })
    const previousDay = screen.getByRole('button', { name: /2026-07-08:/ })
    expect(previousDay).toHaveFocus()
    fireEvent.keyDown(previousDay, { key: 'ArrowRight' })
    expect(screen.getByRole('button', { name: /2026-07-15:/ })).toHaveFocus()

    const monday = screen.getByRole('button', { name: /2026-07-13:/ })
    act(() => monday.focus())
    fireEvent.keyDown(monday, { key: 'ArrowUp' })
    expect(monday).toHaveFocus()

    const sunday = screen.getByRole('button', { name: /2026-07-12:/ })
    act(() => sunday.focus())
    fireEvent.keyDown(sunday, { key: 'ArrowDown' })
    expect(sunday).toHaveFocus()
  })

  it('scrolls the arrow-focused tile into the visible heatmap viewport', async () => {
    const original = HTMLElement.prototype.scrollIntoView
    const scrollIntoView = vi.fn()
    Object.defineProperty(HTMLElement.prototype, 'scrollIntoView', { configurable: true, value: scrollIntoView })
    try {
      const tile = await loadedTile()
      act(() => tile.focus())
      fireEvent.keyDown(tile, { key: 'ArrowLeft' })

      expect(screen.getByRole('button', { name: /2026-07-09:/ })).toHaveFocus()
      expect(scrollIntoView).toHaveBeenCalledWith({ block: 'nearest', inline: 'nearest' })
    } finally {
      if (original) Object.defineProperty(HTMLElement.prototype, 'scrollIntoView', { configurable: true, value: original })
      else delete (HTMLElement.prototype as { scrollIntoView?: unknown }).scrollIntoView
    }
  })

  it('moves keyboard activation into the portaled action and Escape returns to the tile', async () => {
    const tile = await loadedTile()
    act(() => tile.focus())
    fireEvent.keyDown(tile, { key: 'Enter' })

    const action = screen.getByRole('link', { name: /VIEW SESSIONS/ })
    expect(action).toHaveFocus()
    expect(tile).toHaveAttribute('aria-expanded', 'true')
    fireEvent.keyDown(document, { key: 'Escape' })
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
    expect(tile).toHaveFocus()
  })

  it('ignores touch hover but click or tap pins the interactive card', async () => {
    const tile = await loadedTile()
    const touchOver = new Event('pointerover', { bubbles: true })
    Object.defineProperty(touchOver, 'pointerType', { value: 'touch' })
    fireEvent(tile, touchOver)
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
    fireEvent.click(tile)
    const card = screen.getByRole('dialog')
    expect(tile).toHaveAttribute('aria-expanded', 'true')
    expect(tile).toHaveAttribute('aria-controls', card.id)

    fireEvent.pointerLeave(tile, { pointerType: 'touch' })
    fireEvent.pointerLeave(card, { pointerType: 'touch' })
    act(() => vi.advanceTimersByTime(500))
    expect(screen.getByRole('dialog')).toBeInTheDocument()
    fireEvent.keyDown(document, { key: 'Escape' })
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
  })

  it('keeps disabled future tiles inert under delegated events', async () => {
    await loadedTile()
    const future = screen.getByRole('button', { name: /2026-12-31:/ })
    expect(future).toBeDisabled()

    fireEvent.pointerOver(future, { pointerType: 'mouse' })
    fireEvent.click(future)
    fireEvent.keyDown(future, { key: 'Enter' })

    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
  })
})

describe('OverviewPage yearly leaders', () => {
  it('shows only three rows, absolute dates, and bounded cost-sorted links', async () => {
    const data = yearResponse()
    data.topProjects[0].share = null
    data.topProjects.push({ project: 'fourth-project', costUsd: '1', share: .01 })
    data.topSessions.push(session('fourth', 'fourth session'))
    vi.spyOn(api, 'overview').mockResolvedValue(summaryResponse)
    vi.spyOn(api, 'overviewYear').mockResolvedValue(data)
    renderOverview()
    await flush()

    expect(screen.queryByText('fourth-project')).not.toBeInTheDocument()
    expect(screen.queryByText('fourth session')).not.toBeInTheDocument()
    expect(screen.getByRole('link', { name: /01 codex-usage/ })).toHaveTextContent('—')
    expect(screen.getAllByText('JUL 16')).toHaveLength(3)
    expect(screen.getByRole('link', { name: /01 2026 winner/ }).querySelector('time')).toHaveAttribute('datetime', '2026-07-16T10:00:00Z')
    expect(screen.getByRole('link', { name: /01 codex-usage/ })).toHaveAttribute('href', '/sessions?start=2026-01-01&end=2026-12-31&project=%2FUsers%2Fexample%2Fsrc%2Fcodex-usage&sort=cost')
    for (const link of screen.getAllByRole('link', { name: /VIEW YEAR/ })) {
      expect(link).toHaveAttribute('href', '/sessions?start=2026-01-01&end=2026-12-31&sort=cost')
    }
  })
})
