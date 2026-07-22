import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
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
    costUsd: '2',
    unpricedTokens: 0,
    lifetimeCostUsd: '8',
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
    expect(cost).toHaveTextContent('PERIOD$2.00')
    expect(cost).toHaveTextContent('ALL TIME$8.00')
    expect(within(row).getByText('1 / 3')).toBeVisible()
  })

  it('does not repeat the cost when the filtered and lifetime values render identically', async () => {
    renderSessions(session({ costUsd: '8' }))
    const link = await screen.findByRole('link', { name: /Cost history session/ })
    const row = link.closest('[role="row"]') as HTMLElement

    expect(within(row).getAllByText('$8.00')).toHaveLength(1)
    expect(row.querySelector('.lifetime-cost')).not.toBeInTheDocument()
  })

  it('keeps sortable headers mounted and focused while a new query is loading', async () => {
    let resolveSorted!: (value: Awaited<ReturnType<typeof api.sessions>>) => void
    const sorted = new Promise<Awaited<ReturnType<typeof api.sessions>>>(resolve => { resolveSorted = resolve })
    vi.spyOn(api, 'sessions')
      .mockResolvedValueOnce({ items: [session()], page: 1, pageSize: 50, total: 1, totalPages: 1, projects: ['codex-usage'] })
      .mockReturnValueOnce(sorted)

    render(<MemoryRouter initialEntries={['/sessions']}><SessionsPage /></MemoryRouter>)

    const costSort = await screen.findByRole('button', { name: 'COST' })
    costSort.focus()
    fireEvent.click(costSort)
    await waitFor(() => expect(api.sessions).toHaveBeenCalledTimes(2))
    expect(costSort).toHaveFocus()
    expect(screen.getByRole('table', { name: 'Sessions' })).toBeVisible()
    expect(screen.getByRole('table', { name: 'Sessions' }).closest('.sessions-ledger')).toHaveAttribute('aria-busy', 'true')

    resolveSorted({ items: [session()], page: 1, pageSize: 50, total: 1, totalPages: 1, projects: ['codex-usage'] })
    await waitFor(() => expect(screen.getByRole('table', { name: 'Sessions' }).closest('.sessions-ledger')).not.toHaveAttribute('aria-busy'))
    expect(costSort).toHaveFocus()
  })

  it('retains successful project choices while replacement rows are loading', async () => {
    let resolveSorted!: (value: Awaited<ReturnType<typeof api.sessions>>) => void
    const sorted = new Promise<Awaited<ReturnType<typeof api.sessions>>>(resolve => { resolveSorted = resolve })
    vi.spyOn(api, 'sessions')
      .mockResolvedValueOnce({ items: [session()], page: 1, pageSize: 50, total: 1, totalPages: 1, projects: ['alpha', 'codex-usage'] })
      .mockReturnValueOnce(sorted)

    render(<MemoryRouter initialEntries={['/sessions']}><SessionsPage /></MemoryRouter>)

    fireEvent.click(await screen.findByRole('button', { name: 'COST' }))
    await waitFor(() => expect(api.sessions).toHaveBeenCalledTimes(2))
    expect(screen.queryByRole('link', { name: /Cost history session/ })).not.toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: /ALL PROJECTS SELECT/ }))
    const menu = screen.getByRole('listbox', { name: 'Projects' })
    expect(within(menu).getByRole('option', { name: 'alpha' })).toBeVisible()
    expect(within(menu).getByRole('option', { name: 'codex-usage' })).toBeVisible()

    resolveSorted({ items: [session()], page: 1, pageSize: 50, total: 1, totalPages: 1, projects: ['alpha', 'codex-usage'] })
    expect(await screen.findByRole('link', { name: /Cost history session/ })).toBeVisible()
  })

  it('retains successful project choices after replacement rows fail', async () => {
    let rejectSorted!: (reason: Error) => void
    const sorted = new Promise<Awaited<ReturnType<typeof api.sessions>>>((_resolve, reject) => { rejectSorted = reject })
    vi.spyOn(api, 'sessions')
      .mockResolvedValueOnce({ items: [session()], page: 1, pageSize: 50, total: 1, totalPages: 1, projects: ['alpha', 'codex-usage'] })
      .mockReturnValueOnce(sorted)

    render(<MemoryRouter initialEntries={['/sessions']}><SessionsPage /></MemoryRouter>)

    fireEvent.click(await screen.findByRole('button', { name: 'COST' }))
    await waitFor(() => expect(api.sessions).toHaveBeenCalledTimes(2))
    rejectSorted(new Error('sorted sessions failed'))

    expect(await screen.findByRole('alert')).toHaveTextContent('sorted sessions failed')
    expect(screen.queryByRole('link', { name: /Cost history session/ })).not.toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: /ALL PROJECTS SELECT/ }))
    const menu = screen.getByRole('listbox', { name: 'Projects' })
    expect(within(menu).getByRole('option', { name: 'alpha' })).toBeVisible()
    expect(within(menu).getByRole('option', { name: 'codex-usage' })).toBeVisible()
  })

  it('keeps the focused paginator mounted while replacing rows for a new page', async () => {
    let resolveSecond!: (value: Awaited<ReturnType<typeof api.sessions>>) => void
    const second = new Promise<Awaited<ReturnType<typeof api.sessions>>>(resolve => { resolveSecond = resolve })
    vi.spyOn(api, 'sessions')
      .mockResolvedValueOnce({ items: [session()], page: 1, pageSize: 50, total: 100, totalPages: 2, projects: ['codex-usage'] })
      .mockReturnValueOnce(second)

    render(<MemoryRouter initialEntries={['/sessions']}><SessionsPage /></MemoryRouter>)

    const nextPage = await screen.findByRole('button', { name: '02' })
    nextPage.focus()
    fireEvent.click(nextPage)
    await waitFor(() => expect(api.sessions).toHaveBeenCalledTimes(2))

    expect(nextPage).toHaveFocus()
    expect(nextPage).not.toBeDisabled()
    expect(nextPage).toHaveAttribute('aria-disabled', 'true')
    expect(screen.getByRole('navigation', { name: 'Pagination' })).toHaveAttribute('aria-busy', 'true')
    expect(screen.queryByRole('link', { name: /Cost history session/ })).not.toBeInTheDocument()
    expect(screen.getByRole('table', { name: 'Sessions' })).toBeVisible()

    resolveSecond({ items: [session({ id: 'session-2', title: 'Second page session' })], page: 2, pageSize: 50, total: 100, totalPages: 2, projects: ['codex-usage'] })
    expect(await screen.findByRole('link', { name: /Second page session/ })).toBeVisible()
    expect(nextPage).toHaveFocus()
    expect(nextPage).not.toHaveAttribute('aria-disabled')
  })

  it('keeps retained pagination inert after a replacement page fails', async () => {
    let rejectSecond!: (reason: Error) => void
    const second = new Promise<Awaited<ReturnType<typeof api.sessions>>>((_resolve, reject) => { rejectSecond = reject })
    vi.spyOn(api, 'sessions')
      .mockResolvedValueOnce({ items: [session()], page: 1, pageSize: 50, total: 100, totalPages: 2, projects: ['codex-usage'] })
      .mockReturnValueOnce(second)

    render(<MemoryRouter initialEntries={['/sessions']}><SessionsPage /></MemoryRouter>)

    const nextPage = await screen.findByRole('button', { name: '02' })
    nextPage.focus()
    fireEvent.click(nextPage)
    await waitFor(() => expect(api.sessions).toHaveBeenCalledTimes(2))
    rejectSecond(new Error('page two failed'))

    expect(await screen.findByRole('alert')).toHaveTextContent('page two failed')
    expect(nextPage).toHaveFocus()
    expect(nextPage).not.toBeDisabled()
    expect(nextPage).toHaveAttribute('aria-disabled', 'true')
    expect(screen.getByRole('navigation', { name: 'Pagination' })).toHaveAttribute('aria-busy', 'true')
    expect(screen.queryByRole('link', { name: /Cost history session/ })).not.toBeInTheDocument()

    fireEvent.click(nextPage)
    expect(api.sessions).toHaveBeenCalledTimes(2)
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

  it('labels an open-ended range as starting from the selected date', async () => {
    renderSessions(session(), '/sessions?start=2026-07-01')

    expect(await screen.findByRole('button', { name: 'SINCE JUL 1' })).toBeVisible()
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
    expect(within(menu).getByRole('option', { name: 'codex-usage' })).toBeVisible()
    expect(within(menu).getByRole('option', { name: 'another-project' })).toBeVisible()
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

  it('lets a valid direct date win and removes redundant range parameters', async () => {
    const loadSessions = vi.spyOn(api, 'sessions').mockResolvedValue({
      items: [session()],
      page: 1,
      pageSize: 50,
      total: 1,
      totalPages: 1,
      projects: ['codex-usage'],
    })

    render(
      <MemoryRouter initialEntries={['/sessions?date=2026-07-16&start=2026-07-01&end=2026-07-20&project=codex-usage']}>
        <SessionsPage />
        <LocationProbe />
      </MemoryRouter>,
    )

    expect(await screen.findByRole('link', { name: /Cost history session/ })).toBeVisible()
    expect(loadSessions).toHaveBeenCalledTimes(1)
    expect(loadSessions).toHaveBeenCalledWith(expect.objectContaining({
      date: '2026-07-16',
      start: undefined,
      end: undefined,
    }), expect.any(AbortSignal))
    await waitFor(() => expect(screen.getByTestId('location')).toHaveTextContent('?date=2026-07-16&project=codex-usage'))
  })

  it('drops a non-increasing timestamp end while preserving the open-ended start', async () => {
    const loadSessions = vi.spyOn(api, 'sessions').mockResolvedValue({
      items: [session()],
      page: 1,
      pageSize: 50,
      total: 1,
      totalPages: 1,
      projects: ['codex-usage'],
    })

    render(
      <MemoryRouter initialEntries={['/sessions?start=2026-07-16T00%3A00%3A00Z&end=2026-07-16T00%3A00%3A00Z']}>
        <SessionsPage />
        <LocationProbe />
      </MemoryRouter>,
    )

    expect(await screen.findByRole('link', { name: /Cost history session/ })).toBeVisible()
    expect(loadSessions).toHaveBeenCalledTimes(1)
    expect(loadSessions).toHaveBeenCalledWith(expect.objectContaining({
      start: '2026-07-16T00:00:00Z',
      end: undefined,
    }), expect.any(AbortSignal))
    await waitFor(() => {
      expect(screen.getByTestId('location')).toHaveTextContent('?start=2026-07-16T00%3A00%3A00Z')
      expect(screen.getByTestId('location')).not.toHaveTextContent('end=')
    })
  })

  it('keeps an equal date-only range because backend end dates are inclusive', async () => {
    const loadSessions = vi.spyOn(api, 'sessions').mockResolvedValue({
      items: [session()],
      page: 1,
      pageSize: 50,
      total: 1,
      totalPages: 1,
      projects: ['codex-usage'],
    })

    render(<MemoryRouter initialEntries={['/sessions?start=2026-07-16&end=2026-07-16']}><SessionsPage /></MemoryRouter>)

    expect(await screen.findByRole('link', { name: /Cost history session/ })).toBeVisible()
    expect(loadSessions).toHaveBeenCalledWith(expect.objectContaining({
      start: '2026-07-16',
      end: '2026-07-16',
    }), expect.any(AbortSignal))
  })

  it('treats project=all as a literal project filter', async () => {
    const reactError = vi.spyOn(console, 'error').mockImplementation(() => {})
    const loadSessions = vi.spyOn(api, 'sessions').mockResolvedValue({
      items: [session({ project: 'all' })],
      page: 1,
      pageSize: 50,
      total: 1,
      totalPages: 1,
      projects: ['all', 'codex-usage'],
    })

    render(
      <MemoryRouter initialEntries={['/sessions?project=all']}>
        <SessionsPage />
        <LocationProbe />
      </MemoryRouter>,
    )

    expect(await screen.findByRole('link', { name: /Cost history session/ })).toBeVisible()
    expect(screen.getByRole('button', { name: 'all' })).toBeVisible()
    expect(loadSessions).toHaveBeenCalledTimes(1)
    expect(loadSessions).toHaveBeenCalledWith(expect.objectContaining({ project: 'all' }), expect.any(AbortSignal))
    expect(screen.getByTestId('location')).toHaveTextContent('?project=all')

    fireEvent.click(screen.getByRole('button', { name: 'all' }))
    expect(screen.getByRole('option', { name: 'ALL PROJECTS' })).toBeVisible()
    expect(screen.getByRole('option', { name: 'all' })).toBeVisible()
    expect(reactError.mock.calls.flat().join(' ')).not.toContain('same key')
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

    const search = await screen.findByRole('combobox', { name: 'Search projects' })
    const options = within(screen.getByRole('listbox', { name: 'Projects' })).getAllByRole('option')
    await waitFor(() => expect(search).toHaveFocus())
    expect(search).toHaveAttribute('aria-activedescendant', 'project-option-0')
    expect(within(screen.getByRole('listbox', { name: 'Projects' })).getByRole('option', { name: 'codex-usage' })).toHaveAttribute('aria-selected', 'true')

    fireEvent.keyDown(search, { key: 'ArrowDown' })
    await waitFor(() => expect(search).toHaveAttribute('aria-activedescendant', 'project-option-1'))
    fireEvent.keyDown(search, { key: 'End' })
    await waitFor(() => expect(search).toHaveAttribute('aria-activedescendant', `project-option-${options.length - 1}`))
    fireEvent.keyDown(search, { key: 'Escape' })

    await waitFor(() => expect(trigger).toHaveFocus())
    expect(screen.queryByRole('listbox', { name: 'Projects' })).not.toBeInTheDocument()
  })

  it('scrolls the selected project option into view when the searchable menu opens', async () => {
    const originalScrollIntoView = Object.getOwnPropertyDescriptor(HTMLElement.prototype, 'scrollIntoView')
    const scrollIntoView = vi.fn()
    Object.defineProperty(HTMLElement.prototype, 'scrollIntoView', { configurable: true, value: scrollIntoView })
    vi.spyOn(window, 'requestAnimationFrame').mockImplementation(callback => {
      callback(0)
      return 1
    })
    try {
      vi.spyOn(api, 'sessions').mockResolvedValue({
        items: [session()],
        page: 1,
        pageSize: 50,
        total: 1,
        totalPages: 1,
        projects: ['alpha', 'codex-usage', 'personal-hq'],
      })
      render(<MemoryRouter initialEntries={['/sessions?project=codex-usage']}><SessionsPage /></MemoryRouter>)

      fireEvent.click(await screen.findByRole('button', { name: 'codex-usage' }))

      const selected = screen.getByRole('option', { name: 'codex-usage' })
      expect(selected).toHaveAttribute('aria-selected', 'true')
      expect(selected).toHaveClass('active')
      expect(scrollIntoView).toHaveBeenCalledWith({ block: 'nearest' })
      expect(scrollIntoView.mock.contexts).toContain(selected)
    } finally {
      if (originalScrollIntoView) Object.defineProperty(HTMLElement.prototype, 'scrollIntoView', originalScrollIntoView)
      else delete (HTMLElement.prototype as Partial<HTMLElement>).scrollIntoView
    }
  })

  it('filters projects by name and selects the active result from the keyboard', async () => {
    vi.spyOn(api, 'sessions').mockResolvedValue({
      items: [session()],
      page: 1,
      pageSize: 50,
      total: 1,
      totalPages: 1,
      projects: ['alpha', 'codex-usage', 'personal-hq'],
    })
    render(<MemoryRouter initialEntries={['/sessions']}><SessionsPage /><LocationProbe /></MemoryRouter>)

    fireEvent.click(await screen.findByRole('button', { name: /ALL PROJECTS SELECT/ }))
    const search = screen.getByRole('combobox', { name: 'Search projects' })
    fireEvent.change(search, { target: { value: 'usage' } })

    const listbox = screen.getByRole('listbox', { name: 'Projects' })
    expect(within(listbox).getAllByRole('option')).toHaveLength(1)
    expect(within(listbox).getByRole('option', { name: 'codex-usage' })).toBeVisible()
    expect(within(listbox).queryByRole('option', { name: 'alpha' })).not.toBeInTheDocument()
    fireEvent.keyDown(search, { key: 'Enter' })

    await waitFor(() => expect(screen.getByTestId('location')).toHaveTextContent('project=codex-usage'))
  })

  it('keeps the filtered active project valid while Sessions polling refreshes the options', async () => {
    vi.useFakeTimers()
    try {
      const loadSessions = vi.spyOn(api, 'sessions').mockImplementation(() => Promise.resolve({
        items: [session()],
        page: 1,
        pageSize: 50,
        total: 1,
        totalPages: 1,
        projects: ['alpha', 'codex-usage', 'personal-hq'],
      }))
      render(
        <MemoryRouter initialEntries={['/sessions?project=codex-usage']}>
          <SessionsPage />
          <LocationProbe />
        </MemoryRouter>,
      )
      await act(async () => { await Promise.resolve(); await Promise.resolve(); await Promise.resolve() })

      fireEvent.click(screen.getByRole('button', { name: 'codex-usage' }))
      const search = screen.getByRole('combobox', { name: 'Search projects' })
      fireEvent.change(search, { target: { value: 'personal' } })
      await act(async () => { await Promise.resolve() })

      const result = screen.getByRole('option', { name: 'personal-hq' })
      expect(search).toHaveAttribute('aria-activedescendant', result.id)
      expect(result).toHaveClass('active')

      await act(async () => { await vi.advanceTimersByTimeAsync(30_000) })

      expect(loadSessions).toHaveBeenCalledTimes(2)
      expect(search).toHaveAttribute('aria-activedescendant', result.id)
      expect(result).toHaveClass('active')
      fireEvent.keyDown(search, { key: 'Enter' })
      await act(async () => { await Promise.resolve(); await Promise.resolve() })
      expect(screen.getByTestId('location')).toHaveTextContent('project=personal-hq')
    } finally {
      vi.useRealTimers()
    }
  })

  it('canonicalizes invalid sort and page parameters without changing the interpreted query', async () => {
    const loadSessions = vi.spyOn(api, 'sessions').mockResolvedValue({
      items: [session()],
      page: 1,
      pageSize: 50,
      total: 1,
      totalPages: 1,
      projects: ['codex-usage'],
    })
    render(<MemoryRouter initialEntries={['/sessions?sort=banana&page=01&project=codex-usage']}><SessionsPage /><LocationProbe /></MemoryRouter>)

    expect(await screen.findByRole('link', { name: /Cost history session/ })).toBeVisible()
    expect(loadSessions).toHaveBeenCalledWith(expect.objectContaining({ sort: 'recent', page: 1 }), expect.any(AbortSignal))
    await waitFor(() => expect(screen.getByTestId('location')).toHaveTextContent('?project=codex-usage'))
  })
})
