import { act, render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { api } from '../api'
import type { StatusResponse } from '../types'
import { AppHeader } from './AppHeader'

async function renderHeader(status: StatusResponse) {
  vi.spyOn(api, 'status').mockResolvedValue(status)
  render(<MemoryRouter><AppHeader /></MemoryRouter>)
  await act(async () => {
    await Promise.resolve()
    await Promise.resolve()
  })
}

beforeEach(() => {
  vi.useFakeTimers()
  vi.setSystemTime(new Date('2026-07-19T08:00:00Z'))
})

afterEach(() => {
  vi.useRealTimers()
  vi.restoreAllMocks()
})

describe('AppHeader ingestion status', () => {
  it('shows an in-progress scan instead of a stale successful-ingest timestamp', async () => {
    await renderHeader({
      state: 'scanning',
      lastIngestAt: '2026-07-19T03:00:00Z',
      lastIngestAttemptAt: '2026-07-19T08:00:00Z',
      lastEventAt: '2026-07-19T07:59:00Z',
      filesScanned: 3_425,
      filesFailed: 0,
    })

    expect(screen.getByText('Updating…')).toBeInTheDocument()
    expect(screen.queryByText(/^Updated /)).not.toBeInTheDocument()
  })

  it('uses the last completed ingest as the idle freshness timestamp', async () => {
    await renderHeader({
      state: 'idle',
      lastIngestAt: '2026-07-19T07:55:00Z',
      lastIngestAttemptAt: '2026-07-19T07:55:00Z',
      lastEventAt: '2026-07-19T07:59:00Z',
      filesScanned: 3_425,
      filesFailed: 0,
    })

    expect(screen.getByText('Updated 5m ago')).toBeInTheDocument()
    expect(document.querySelector('.header-status')).not.toHaveAttribute('aria-live')
    expect(document.querySelector('.header-status .sr-only')).toHaveAttribute('aria-live', 'polite')
    expect(document.querySelector('.header-status .sr-only')).toHaveTextContent('Usage data is up to date')
  })

  it('keeps age ticks visual without changing the live status message', async () => {
    const status: StatusResponse = {
      state: 'idle',
      lastIngestAt: '2026-07-19T07:59:01Z',
      lastIngestAttemptAt: '2026-07-19T07:59:01Z',
      lastEventAt: '2026-07-19T07:59:01Z',
      filesScanned: 3_425,
      filesFailed: 0,
    }
    vi.spyOn(api, 'status').mockResolvedValue(status)
    render(<MemoryRouter><AppHeader /></MemoryRouter>)
    await act(async () => { await Promise.resolve(); await Promise.resolve() })

    const liveStatus = document.querySelector('.header-status .sr-only')
    expect(screen.getByText('Updated 59s ago')).toBeInTheDocument()
    expect(liveStatus).toHaveTextContent('Usage data is up to date')

    await act(async () => {
      vi.advanceTimersByTime(5_000)
      await Promise.resolve()
      await Promise.resolve()
    })

    expect(screen.getByText('Updated 1m ago')).toBeInTheDocument()
    expect(liveStatus).toHaveTextContent('Usage data is up to date')
  })

  it('makes a failed ingest visible instead of presenting the last success as current', async () => {
    await renderHeader({
      state: 'error',
      lastIngestAt: '2026-07-19T03:00:00Z',
      lastIngestAttemptAt: '2026-07-19T07:58:00Z',
      lastEventAt: '2026-07-19T07:59:00Z',
      filesScanned: 3_425,
      filesFailed: 2,
    })

    const label = screen.getByText('Update failed 2m ago')
    expect(label).toHaveClass('attention')
    expect(label).toHaveAttribute('title', 'Updated 5h ago. 2 source files failed.')
  })

  it('marks cached status as stale when a polling request fails', async () => {
    vi.spyOn(api, 'status')
      .mockResolvedValueOnce({
        state: 'idle',
        lastIngestAt: '2026-07-19T07:55:00Z',
        lastIngestAttemptAt: '2026-07-19T07:55:00Z',
        lastEventAt: '2026-07-19T07:59:00Z',
        filesScanned: 3_425,
        filesFailed: 0,
      })
      .mockRejectedValueOnce(new Error('status request failed'))
    render(<MemoryRouter><AppHeader /></MemoryRouter>)
    await act(async () => { await Promise.resolve(); await Promise.resolve() })

    await act(async () => {
      vi.advanceTimersByTime(5_000)
      await Promise.resolve()
      await Promise.resolve()
    })

    expect(screen.getByText('Status stale · Updated 5m ago')).toHaveAttribute('title', 'status request failed')
  })
})
