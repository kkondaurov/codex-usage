import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { MemoryRouter, useLocation } from 'react-router-dom'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { api } from '../api'
import type { SessionRow } from '../types'
import { SessionsPage } from './SessionsPage'

function session(overrides: Partial<SessionRow> = {}): SessionRow {
  return {
    id: 'session-1',
    rootThreadId: 'session-1',
    startedAt: '2026-07-15T12:00:00Z',
    lastEventAt: '2026-07-16T10:00:00Z',
    title: 'Cost history session',
    project: 'codex-usage',
    branch: 'main',
    messageCount: 10,
    turnCount: 2,
    agentCount: 1,
    toolCount: 3,
    totalTokens: 2_500,
    costUsd: 2,
    unpricedTokens: 0,
    lifetimeCostUsd: 8,
    lifetimeUnpricedTokens: 0,
    ...overrides,
  }
}

function renderSessions(item: SessionRow, entry = '/sessions?date=2026-07-16') {
  vi.spyOn(api, 'sessions').mockResolvedValue({
    items: [item],
    page: 1,
    pageSize: 50,
    total: 1,
    totalPages: 1,
    projects: ['codex-usage'],
  })
  return render(<MemoryRouter initialEntries={[entry]}><SessionsPage /></MemoryRouter>)
}

function LocationProbe() {
  const location = useLocation()
  return <output data-testid="location">{location.search}</output>
}

afterEach(() => vi.restoreAllMocks())

describe('SessionsPage cost history', () => {
  it('shows filtered cost first and a quieter lifetime cost when their rendered values differ', async () => {
    const { container } = renderSessions(session())
    const link = await screen.findByRole('link', { name: /Cost history session/ })
    const row = link.closest('[role="row"]') as HTMLElement
    const cost = row.querySelector('.session-cost')!

    expect(container.querySelector('.sessions-ledger')).toHaveClass('page-ledger-frame')
    expect(screen.getByRole('table', { name: 'Sessions' })).toContainElement(row)
    expect(screen.getAllByRole('columnheader')).toHaveLength(7)
    expect(within(row).getAllByRole('cell')).toHaveLength(7)
    expect(within(cost as HTMLElement).getByText('$2.00')).toBeVisible()
    expect(within(cost as HTMLElement).getByText('$8.00')).toHaveClass('lifetime-cost')
    expect(within(row).getByText('1 / 3')).toBeVisible()
  })

  it('does not repeat the cost when the filtered and lifetime values render identically', async () => {
    renderSessions(session({ costUsd: 8 }))
    const link = await screen.findByRole('link', { name: /Cost history session/ })
    const row = link.closest('[role="row"]') as HTMLElement

    expect(within(row).getAllByText('$8.00')).toHaveLength(1)
    expect(row.querySelector('.lifetime-cost')).not.toBeInTheDocument()
  })

  it('shows incomplete lifetime pricing quietly when the filtered period itself is priced', async () => {
    renderSessions(session({ lifetimeCostUsd: null, lifetimeUnpricedTokens: 1_000 }))
    const link = await screen.findByRole('link', { name: /Cost history session/ })
    const row = link.closest('[role="row"]') as HTMLElement

    expect(within(row).getByText('$2.00')).toBeVisible()
    expect(within(row).getByText('—')).toHaveClass('lifetime-cost')
    expect(within(row).queryByText('Unknown price')).not.toBeInTheDocument()
  })

  it('does not duplicate an unknown value when both filtered and lifetime pricing are incomplete', async () => {
    renderSessions(session({ costUsd: null, unpricedTokens: 400, lifetimeCostUsd: null, lifetimeUnpricedTokens: 1_000 }))
    const link = await screen.findByRole('link', { name: /Cost history session/ })
    const row = link.closest('[role="row"]') as HTMLElement

    expect(within(row).getAllByText('—')).toHaveLength(1)
    expect(row.querySelector('.lifetime-cost')).not.toBeInTheDocument()
    expect(within(row).getByText('Unknown price')).toBeVisible()
  })

  it('labels timestamp ranges with their inclusive last day', async () => {
    renderSessions(
      session(),
      '/sessions?start=2024-12-31T23%3A00%3A00%2B00%3A00&end=2025-12-31T23%3A00%3A00%2B00%3A00',
    )

    const trigger = await screen.findByRole('button', { name: 'JAN 1–DEC 31' })
    const clear = screen.getByRole('button', { name: 'Clear date range' })
    expect(trigger).toHaveAttribute('aria-haspopup', 'dialog')
    expect(trigger).toHaveAttribute('aria-controls', 'session-date-range-dialog')
    expect(clear).toBeVisible()
    expect(clear).toHaveTextContent('CLEAR')

    clear.focus()
    fireEvent.click(clear)

    await waitFor(() => expect(trigger).toHaveFocus())
    expect(screen.queryByRole('button', { name: 'Clear date range' })).not.toBeInTheDocument()
  })

  it('keeps the project clear control visible and restores focus after clearing', async () => {
    renderSessions(session(), '/sessions?project=codex-usage')

    const trigger = await screen.findByRole('button', { name: 'codex-usage' })
    const clear = screen.getByRole('button', { name: 'Clear project filter' })
    expect(clear).toBeVisible()
    expect(clear).toHaveTextContent('CLEAR')

    clear.focus()
    fireEvent.click(clear)

    await waitFor(() => expect(trigger).toHaveFocus())
    expect(screen.queryByRole('button', { name: 'Clear project filter' })).not.toBeInTheDocument()
  })

  it('does not mutate the project collection returned by the API', async () => {
    const projects = Object.freeze(['codex-usage']) as unknown as string[]
    vi.spyOn(api, 'sessions').mockResolvedValue({
      items: [session()],
      page: 1,
      pageSize: 50,
      total: 1,
      totalPages: 1,
      projects,
    })
    render(<MemoryRouter initialEntries={['/sessions?project=another-project']}><SessionsPage /></MemoryRouter>)

    expect(await screen.findByRole('link', { name: /Cost history session/ })).toBeVisible()
    fireEvent.click(screen.getByRole('button', { name: 'another-project' }))
    const menu = document.querySelector('.project-menu') as HTMLElement
    expect(within(menu).getByRole('menuitemradio', { name: 'codex-usage' })).toBeVisible()
    expect(within(menu).getByRole('menuitemradio', { name: 'another-project' })).toBeVisible()
    expect(projects).toEqual(['codex-usage'])
  })

  it('quietly removes malformed date parameters instead of sending them to the API', async () => {
    const loadSessions = vi.spyOn(api, 'sessions').mockResolvedValue({
      items: [session()],
      page: 1,
      pageSize: 50,
      total: 1,
      totalPages: 1,
      projects: ['codex-usage'],
    })

    render(
      <MemoryRouter initialEntries={['/sessions?date=2026-02-30&start=definitely-not-a-date&end=2026-13-50']}>
        <SessionsPage />
      </MemoryRouter>,
    )

    expect(await screen.findByRole('link', { name: /Cost history session/ })).toBeVisible()
    expect(screen.getByRole('button', { name: /ALL DATES SELECT/ })).toBeVisible()
    expect(loadSessions).toHaveBeenCalledTimes(1)
    expect(loadSessions).toHaveBeenCalledWith(expect.objectContaining({
      date: undefined,
      start: undefined,
      end: undefined,
    }), expect.any(AbortSignal))
  })

  it('treats the project=all sentinel as no filter and canonicalizes the URL', async () => {
    const loadSessions = vi.spyOn(api, 'sessions').mockResolvedValue({
      items: [session()],
      page: 1,
      pageSize: 50,
      total: 1,
      totalPages: 1,
      projects: ['codex-usage'],
    })

    render(
      <MemoryRouter initialEntries={['/sessions?project=all']}>
        <SessionsPage />
        <LocationProbe />
      </MemoryRouter>,
    )

    expect(await screen.findByRole('link', { name: /Cost history session/ })).toBeVisible()
    expect(screen.getByRole('button', { name: /ALL PROJECTS SELECT/ })).toBeVisible()
    expect(loadSessions).toHaveBeenCalledTimes(1)
    expect(loadSessions).toHaveBeenCalledWith(expect.objectContaining({ project: undefined }), expect.any(AbortSignal))
    await waitFor(() => expect(screen.getByTestId('location')).toHaveTextContent(''))
  })

  it('supports project-menu arrow keys and restores focus when Escape closes it', async () => {
    vi.spyOn(api, 'sessions').mockResolvedValue({
      items: [session()],
      page: 1,
      pageSize: 50,
      total: 1,
      totalPages: 1,
      projects: ['alpha', 'codex-usage'],
    })
    render(<MemoryRouter initialEntries={['/sessions?project=codex-usage']}><SessionsPage /></MemoryRouter>)

    const trigger = await screen.findByRole('button', { name: 'codex-usage' })
    trigger.focus()
    fireEvent.keyDown(trigger, { key: 'ArrowDown' })

    const menu = await screen.findByRole('menu', { name: 'Projects' })
    const options = within(menu).getAllByRole('menuitemradio')
    await waitFor(() => expect(options[0]).toHaveFocus())
    expect(options.map(option => option.tabIndex)).toEqual([0, -1, -1])
    expect(within(menu).getByRole('menuitemradio', { name: 'codex-usage' })).toHaveAttribute('aria-checked', 'true')

    fireEvent.keyDown(options[0], { key: 'ArrowDown' })
    expect(options[1]).toHaveFocus()
    await waitFor(() => expect(options.map(option => option.tabIndex)).toEqual([-1, 0, -1]))
    fireEvent.keyDown(options[1], { key: 'End' })
    expect(options.at(-1)).toHaveFocus()
    await waitFor(() => expect(options.map(option => option.tabIndex)).toEqual([-1, -1, 0]))
    fireEvent.keyDown(options.at(-1)!, { key: 'Escape' })

    await waitFor(() => expect(trigger).toHaveFocus())
    expect(screen.queryByRole('menu', { name: 'Projects' })).not.toBeInTheDocument()
  })
})
