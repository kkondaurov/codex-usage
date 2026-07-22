import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { MemoryRouter, useLocation } from 'react-router-dom'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { api } from '../api'
import { canonicalStatsAnchor, dateOnly, shiftAnchor, validDateOnly } from '../calendar'
import { clearAsyncCache } from '../hooks'
import type { StatsRange, StatsResponse, Totals } from '../types'
import { StatsPage } from './StatsPage'

const totals: Totals = {
  inputTokens: 0,
  cachedInputTokens: 0,
  outputTokens: 0,
  reasoningTokens: 0,
  blendedTokens: 0,
  totalTokens: 0,
  costUsd: '0',
  unpricedTokens: 0,
  pricingComplete: true,
}

function deferred<T>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((nextResolve) => { resolve = nextResolve })
  return { promise, resolve }
}

function LocationProbe() {
  const location = useLocation()
  return <output data-testid="location">{location.search}</output>
}

function statsResponse(range: StatsRange, anchor: string, label: string, rowLabel: string): StatsResponse {
  return {
    range,
    anchor,
    label,
    totals,
    rows: [{
      ...totals,
      periodStart: '2020-01-01T00:00:00Z',
      periodEnd: '2020-01-02T00:00:00Z',
      label: rowLabel,
      sessionCount: 1,
    }],
    trend: [],
  }
}

afterEach(() => {
  clearAsyncCache()
  vi.restoreAllMocks()
})

describe('StatsPage', () => {
  it('uses one canonical URL identity per month and year without clamp drift', () => {
    expect(canonicalStatsAnchor('2026-03-31', 'month')).toBe('2026-03-01')
    expect(canonicalStatsAnchor('2024-02-29', 'year')).toBe('2024-01-01')
    expect(shiftAnchor('2026-03-31', 'month', -1)).toBe('2026-02-01')
    expect(shiftAnchor(shiftAnchor('2026-03-31', 'month', -1), 'month', 1)).toBe('2026-03-01')
    expect(shiftAnchor('2024-02-29', 'year', 1)).toBe('2025-01-01')
    expect(shiftAnchor('2024-02-29', 'year', -1)).toBe('2023-01-01')
  })

  it('shares the backend public year domain for navigable dates', () => {
    expect(validDateOnly('1970-01-01')).toBe(true)
    expect(validDateOnly('9998-12-31')).toBe(true)
    expect(validDateOnly('1969-12-31')).toBe(false)
    expect(validDateOnly('9999-01-01')).toBe(false)
    expect(canonicalStatsAnchor('1970-01-01', 'week')).toBe('1970-01-05')
    expect(canonicalStatsAnchor('2026-07-15', 'week')).toBe('2026-07-13')
  })

  it.each<StatsRange>(['day', 'week', 'month', 'year'])('disables %s navigation before the public lower bound', async range => {
    const expectedAnchor = range === 'week' ? '1970-01-05' : '1970-01-01'
    const stats = vi.spyOn(api, 'stats').mockResolvedValue({
      range,
      anchor: expectedAnchor,
      label: 'Lower bound',
      totals,
      rows: [],
      trend: [],
    })
    render(
      <MemoryRouter initialEntries={[`/stats?range=${range}&anchor=1970-01-01`]}>
        <StatsPage />
        <LocationProbe />
      </MemoryRouter>,
    )

    await waitFor(() => expect(stats).toHaveBeenCalledWith(range, expectedAnchor, expect.any(AbortSignal)))
    const previous = screen.getByRole('button', { name: 'PREVIOUS' })
    expect(previous).toBeDisabled()
    fireEvent.click(previous)
    expect(screen.getByTestId('location')).toHaveTextContent(`?range=${range}&anchor=${expectedAnchor}`)
  })

  it.each<StatsRange>(['day', 'week', 'month', 'year'])('disables %s navigation after the public upper bound', async range => {
    const expectedAnchor = range === 'week'
      ? '9998-12-28'
      : range === 'month'
        ? '9998-12-01'
        : range === 'year'
          ? '9998-01-01'
          : '9998-12-31'
    const stats = vi.spyOn(api, 'stats').mockResolvedValue({
      range,
      anchor: expectedAnchor,
      label: 'Upper bound',
      totals,
      rows: [],
      trend: [],
    })
    render(
      <MemoryRouter initialEntries={[`/stats?range=${range}&anchor=9998-12-31`]}>
        <StatsPage />
        <LocationProbe />
      </MemoryRouter>,
    )

    await waitFor(() => expect(stats).toHaveBeenCalledWith(range, expectedAnchor, expect.any(AbortSignal)))
    const next = screen.getByRole('button', { name: 'NEXT' })
    expect(next).toBeDisabled()
    fireEvent.click(next)
    expect(screen.getByTestId('location')).toHaveTextContent(`?range=${range}&anchor=${expectedAnchor}`)
  })

  it('canonicalizes a midweek URL and loads the weekly range from Monday', async () => {
    const stats = vi.spyOn(api, 'stats').mockResolvedValue({
      range: 'week',
      anchor: '2026-07-13',
      label: 'Week of Jul 13, 2026',
      totals,
      rows: [],
      trend: [],
    })

    render(
      <MemoryRouter initialEntries={['/stats?range=week&anchor=2026-07-15']}>
        <StatsPage />
        <LocationProbe />
      </MemoryRouter>,
    )

    expect(screen.getByRole('tab', { name: 'WEEK' })).toHaveAttribute('aria-selected', 'true')
    await waitFor(() => expect(stats).toHaveBeenCalledWith('week', '2026-07-13', expect.any(AbortSignal)))
    await waitFor(() => expect(screen.getByTestId('location')).toHaveTextContent('?range=week&anchor=2026-07-13'))
  })

  it('replaces malformed range and anchor parameters with a safe canonical URL', async () => {
    const today = canonicalStatsAnchor(dateOnly(new Date()), 'month')!
    const stats = vi.spyOn(api, 'stats').mockResolvedValue({
      range: 'month',
      anchor: today,
      label: 'This month',
      totals,
      rows: [],
      trend: [],
    })

    render(
      <MemoryRouter initialEntries={['/stats?range=nonsense&anchor=2026-02-30']}>
        <StatsPage />
        <LocationProbe />
      </MemoryRouter>,
    )

    await waitFor(() => expect(stats).toHaveBeenCalledWith('month', today, expect.any(AbortSignal)))
    await waitFor(() => expect(screen.getByTestId('location')).toHaveTextContent(`?range=month&anchor=${today}`))
    expect(screen.getByRole('tab', { name: 'MONTH' })).toHaveAttribute('aria-selected', 'true')
  })

  it('removes irrelevant anchors from the all-time URL', async () => {
    const stats = vi.spyOn(api, 'stats').mockResolvedValue({
      range: 'all',
      anchor: '2026-07-18',
      label: 'All time',
      totals,
      rows: [],
      trend: [],
    })

    render(
      <MemoryRouter initialEntries={['/stats?range=all&anchor=2026-07-18']}>
        <StatsPage />
        <LocationProbe />
      </MemoryRouter>,
    )

    await waitFor(() => expect(stats).toHaveBeenCalledWith('all', undefined, expect.any(AbortSignal)))
    await waitFor(() => expect(screen.getByTestId('location')).toHaveTextContent('?range=all'))
  })

  it('compares navigation by canonical period boundaries near month and year ends', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    vi.setSystemTime(new Date('2026-07-20T12:00:00'))
    try {
      const stats = vi.spyOn(api, 'stats').mockImplementation((range, anchor = '') => Promise.resolve({
        range,
        anchor,
        label: `${range} ${anchor}`,
        totals,
        rows: [],
        trend: [],
      }))

      const view = render(
        <MemoryRouter initialEntries={['/stats?range=month&anchor=2026-07-30']}>
          <StatsPage />
          <LocationProbe />
        </MemoryRouter>,
      )

      await waitFor(() => expect(stats).toHaveBeenCalledWith('month', '2026-07-01', expect.any(AbortSignal)))
      expect(screen.getByTestId('location')).toHaveTextContent('?range=month&anchor=2026-07-01')
      expect(screen.getByRole('button', { name: 'NEXT' })).toBeDisabled()

      view.unmount()
      clearAsyncCache()
      stats.mockClear()
      render(
        <MemoryRouter initialEntries={['/stats?range=year&anchor=2025-12-31']}>
          <StatsPage />
          <LocationProbe />
        </MemoryRouter>,
      )

      await waitFor(() => expect(stats).toHaveBeenCalledWith('year', '2025-01-01', expect.any(AbortSignal)))
      const next = screen.getByRole('button', { name: 'NEXT' })
      expect(next).toBeEnabled()
      fireEvent.click(next)
      await waitFor(() => expect(stats).toHaveBeenLastCalledWith('year', '2026-01-01', expect.any(AbortSignal)))
      expect(screen.getByTestId('location')).toHaveTextContent('?range=year&anchor=2026-01-01')
      expect(screen.getByRole('button', { name: 'NEXT' })).toBeDisabled()
    } finally {
      vi.useRealTimers()
    }
  })

  it('keeps the controlled tabpanel mounted and marks it busy while rows load', async () => {
    const response = deferred<StatsResponse>()
    vi.spyOn(api, 'stats').mockReturnValue(response.promise)

    render(
      <MemoryRouter initialEntries={['/stats?range=month&anchor=2026-07-18']}>
        <StatsPage />
      </MemoryRouter>,
    )

    const panel = screen.getByRole('tabpanel', { name: 'month statistics' })
    expect(screen.getByRole('tab', { name: 'MONTH' })).toHaveAttribute('aria-controls', panel.id)
    expect(panel).toHaveAttribute('aria-busy', 'true')
    expect(within(panel).getByLabelText('Loading')).toBeInTheDocument()

    await act(async () => {
      response.resolve(statsResponse('month', '2026-07-01', 'July 2026', 'JULY ROW'))
    })
    expect(await within(panel).findByRole('button', { name: /JULY ROW/ })).toBeVisible()
    expect(panel).not.toHaveAttribute('aria-busy')
  })

  it('uses roving tab focus and arrow keys to change the selected range', async () => {
    const stats = vi.spyOn(api, 'stats').mockImplementation((range, anchor = '2026-07-18') => Promise.resolve({
      range,
      anchor,
      label: `${range} range`,
      totals,
      rows: [],
      trend: [],
    }))
    render(
      <MemoryRouter initialEntries={['/stats?range=month&anchor=2026-07-18']}>
        <StatsPage />
      </MemoryRouter>,
    )
    await waitFor(() => expect(stats).toHaveBeenCalledWith('month', '2026-07-01', expect.any(AbortSignal)))

    const month = screen.getByRole('tab', { name: 'MONTH' })
    month.focus()
    fireEvent.keyDown(month, { key: 'ArrowRight' })

    const year = screen.getByRole('tab', { name: 'YEAR' })
    expect(year).toHaveFocus()
    expect(year).toHaveAttribute('aria-selected', 'true')
    expect(year).toHaveAttribute('tabindex', '0')
    expect(month).toHaveAttribute('tabindex', '-1')
    await waitFor(() => expect(stats).toHaveBeenLastCalledWith('year', canonicalStatsAnchor(dateOnly(new Date()), 'year'), expect.any(AbortSignal)))
  })

  it('keeps empty past periods, hides empty future periods, and retains future activity', async () => {
    const now = Date.now()
    vi.spyOn(api, 'stats').mockResolvedValue({
      range: 'month',
      anchor: '2026-07-18',
      label: 'July 2026',
      totals,
      rows: [
        {
          ...totals,
          periodStart: new Date(now - 86_400_000).toISOString(),
          periodEnd: new Date(now).toISOString(),
          label: 'PAST EMPTY PERIOD',
          sessionCount: 0,
        },
        {
          ...totals,
          periodStart: new Date(now + 86_400_000).toISOString(),
          periodEnd: new Date(now + 172_800_000).toISOString(),
          label: 'FUTURE EMPTY PERIOD',
          sessionCount: 0,
        },
        {
          ...totals,
          periodStart: new Date(now + 172_800_000).toISOString(),
          periodEnd: new Date(now + 259_200_000).toISOString(),
          label: 'FUTURE ACTIVITY PERIOD',
          sessionCount: 1,
        },
      ],
      trend: [],
    })

    render(
      <MemoryRouter initialEntries={['/stats?range=month&anchor=2026-07-18']}>
        <StatsPage />
      </MemoryRouter>,
    )

    const pastLink = await screen.findByRole('button', { name: /PAST EMPTY PERIOD/ })
    const pastRow = pastLink.closest('[role="row"]') as HTMLElement
    expect(screen.getByRole('table', { name: 'month usage statistics' })).toContainElement(pastRow)
    expect(within(pastRow).getAllByRole('cell')).toHaveLength(9)
    expect(screen.queryByRole('button', { name: /FUTURE EMPTY PERIOD/ })).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: /FUTURE ACTIVITY PERIOD/ })).toBeVisible()
  })

  it('replaces stale rows with the loading ledger while changing ranges', async () => {
    const today = canonicalStatsAnchor(dateOnly(new Date()), 'month')!
    const currentYear = canonicalStatsAnchor(dateOnly(new Date()), 'year')!
    const year = deferred<StatsResponse>()
    const stats = vi.spyOn(api, 'stats')
      .mockResolvedValueOnce(statsResponse('month', today, 'July 2026', 'MONTH ROW'))
      .mockReturnValueOnce(year.promise)

    render(
      <MemoryRouter initialEntries={[`/stats?range=month&anchor=${today}`]}>
        <StatsPage />
      </MemoryRouter>,
    )

    expect(await screen.findByRole('button', { name: /MONTH ROW/ })).toBeInTheDocument()
    fireEvent.click(screen.getByRole('tab', { name: 'YEAR' }))

    expect(screen.getByRole('tab', { name: 'YEAR' })).toHaveAttribute('aria-selected', 'true')
    expect(screen.queryByRole('button', { name: /MONTH ROW/ })).not.toBeInTheDocument()
    expect(screen.getByLabelText('Loading')).toBeInTheDocument()

    await act(async () => { year.resolve(statsResponse('year', currentYear, '2026', 'YEAR ROW')) })
    expect(await screen.findByRole('button', { name: /YEAR ROW/ })).toBeInTheDocument()

    fireEvent.click(screen.getByRole('tab', { name: 'MONTH' }))
    expect(screen.getByRole('button', { name: /MONTH ROW/ })).toBeInTheDocument()
    expect(screen.queryByLabelText('Loading')).not.toBeInTheDocument()
    expect(stats).toHaveBeenCalledTimes(2)
  })

  it('replaces stale rows with the loading ledger while navigating periods', async () => {
    const june = deferred<StatsResponse>()
    const stats = vi.spyOn(api, 'stats')
      .mockResolvedValueOnce(statsResponse('month', '2026-07-01', 'July 2026', 'JULY ROW'))
      .mockReturnValueOnce(june.promise)

    render(
      <MemoryRouter initialEntries={['/stats?range=month&anchor=2026-07-18']}>
        <StatsPage />
      </MemoryRouter>,
    )

    expect(await screen.findByRole('button', { name: /JULY ROW/ })).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'PREVIOUS' }))

    expect(screen.queryByRole('button', { name: /JULY ROW/ })).not.toBeInTheDocument()
    expect(screen.getByLabelText('Loading')).toBeInTheDocument()
    await waitFor(() => expect(stats).toHaveBeenLastCalledWith('month', '2026-06-01', expect.any(AbortSignal)))

    await act(async () => { june.resolve(statsResponse('month', '2026-06-01', 'June 2026', 'JUNE ROW')) })
    expect(await screen.findByRole('button', { name: /JUNE ROW/ })).toBeInTheDocument()
  })
})
