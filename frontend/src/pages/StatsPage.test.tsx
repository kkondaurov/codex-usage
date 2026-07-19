import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { MemoryRouter, useLocation } from 'react-router-dom'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { api } from '../api'
import { dateOnly, shiftAnchor } from '../calendar'
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
  costUsd: 0,
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
  it('clamps month and year navigation at calendar boundaries', () => {
    expect(shiftAnchor('2026-03-31', 'month', -1)).toBe('2026-02-28')
    expect(shiftAnchor('2024-03-31', 'month', -1)).toBe('2024-02-29')
    expect(shiftAnchor('2024-02-29', 'year', 1)).toBe('2025-02-28')
    expect(shiftAnchor('2024-02-29', 'year', -1)).toBe('2023-02-28')
  })

  it('exposes the weekly range from the product navigation and loads it directly', async () => {
    const stats = vi.spyOn(api, 'stats').mockResolvedValue({
      range: 'week',
      anchor: '2026-07-13',
      label: 'Week of Jul 13, 2026',
      totals,
      rows: [],
      trend: [],
    })

    render(
      <MemoryRouter initialEntries={['/stats?range=week&anchor=2026-07-13']}>
        <StatsPage />
      </MemoryRouter>,
    )

    expect(screen.getByRole('tab', { name: 'WEEK' })).toHaveAttribute('aria-selected', 'true')
    await waitFor(() => expect(stats).toHaveBeenCalledWith('week', '2026-07-13', expect.any(AbortSignal)))
  })

  it('replaces malformed range and anchor parameters with a safe canonical URL', async () => {
    const today = dateOnly(new Date())
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
    await waitFor(() => expect(stats).toHaveBeenCalledWith('month', '2026-07-18', expect.any(AbortSignal)))

    const month = screen.getByRole('tab', { name: 'MONTH' })
    month.focus()
    fireEvent.keyDown(month, { key: 'ArrowRight' })

    const year = screen.getByRole('tab', { name: 'YEAR' })
    expect(year).toHaveFocus()
    expect(year).toHaveAttribute('aria-selected', 'true')
    expect(year).toHaveAttribute('tabindex', '0')
    expect(month).toHaveAttribute('tabindex', '-1')
    await waitFor(() => expect(stats).toHaveBeenLastCalledWith('year', dateOnly(new Date()), expect.any(AbortSignal)))
  })

  it('keeps empty past periods but does not render future periods', async () => {
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
          label: 'FUTURE PERIOD',
          sessionCount: 0,
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
    expect(screen.queryByRole('button', { name: /FUTURE PERIOD/ })).not.toBeInTheDocument()
  })

  it('replaces stale rows with the loading ledger while changing ranges', async () => {
    const today = dateOnly(new Date())
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

    await act(async () => { year.resolve(statsResponse('year', today, '2026', 'YEAR ROW')) })
    expect(await screen.findByRole('button', { name: /YEAR ROW/ })).toBeInTheDocument()

    fireEvent.click(screen.getByRole('tab', { name: 'MONTH' }))
    expect(screen.getByRole('button', { name: /MONTH ROW/ })).toBeInTheDocument()
    expect(screen.queryByLabelText('Loading')).not.toBeInTheDocument()
    expect(stats).toHaveBeenCalledTimes(2)
  })

  it('replaces stale rows with the loading ledger while navigating periods', async () => {
    const june = deferred<StatsResponse>()
    const stats = vi.spyOn(api, 'stats')
      .mockResolvedValueOnce(statsResponse('month', '2026-07-18', 'July 2026', 'JULY ROW'))
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
    await waitFor(() => expect(stats).toHaveBeenLastCalledWith('month', '2026-06-18', expect.any(AbortSignal)))

    await act(async () => { june.resolve(statsResponse('month', '2026-06-18', 'June 2026', 'JUNE ROW')) })
    expect(await screen.findByRole('button', { name: /JUNE ROW/ })).toBeInTheDocument()
  })
})
