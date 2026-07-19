import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter, useNavigate } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { api } from '../api'
import { useCachedAsync } from '../hooks'
import type { PricesResponse, SettingsResponse } from '../types'
import { SettingsPage } from './SettingsPage'

const prices: PricesResponse = {
  items: [],
  aliases: [],
  observedUnknown: [],
  page: 1,
  pageSize: 25,
  total: 0,
  totalPages: 0,
  lastRefreshAt: null,
  lastRefreshErrorAt: null,
  refreshError: null,
  refreshErrorKind: null,
  source: null,
}

const settings: SettingsResponse = {
  databasePath: '/tmp/codex-usage.db',
  activeRoot: '/tmp/sessions',
  archiveRoot: null,
  timezone: 'CEST',
  lastIngestAt: '2026-07-19T12:00:00Z',
  sessionCount: 2270,
  databaseBytes: 3_435_973_837,
  pricing: { knownCostUsd: 1, unpricedTokens: 0, complete: true },
}

function CachedSurface({ cacheKey, loader }: { cacheKey: string; loader: () => Promise<string> }) {
  const { data } = useCachedAsync(cacheKey, loader, [cacheKey], 30_000)
  return <span>{data}</span>
}

function SettingsHistoryHarness() {
  const navigate = useNavigate()
  return <><button type="button" onClick={() => navigate(-1)}>BACK</button><SettingsPage /></>
}

afterEach(() => {
  vi.useRealTimers()
  vi.restoreAllMocks()
})
beforeEach(() => {
  vi.spyOn(api, 'pricedModelIds').mockResolvedValue([])
  vi.spyOn(api, 'settings').mockResolvedValue(settings)
})

describe('SettingsPage price editor', () => {
  it('surfaces the local database footprint and explains that retained history can grow', async () => {
    vi.spyOn(api, 'prices').mockResolvedValue(prices)
    render(<MemoryRouter initialEntries={['/settings']}><SettingsPage /></MemoryRouter>)

    const notice = await screen.findByRole('status')
    expect(notice).toHaveTextContent('LOCAL DATABASE · 3.20 GB')
    expect(notice).toHaveTextContent('database can continue growing as sessions are ingested')
    expect(notice).toHaveTextContent('/tmp/codex-usage.db')
  })

  it('keeps the price ledger usable when local database metadata fails and retries it independently', async () => {
    vi.spyOn(api, 'prices').mockResolvedValue(prices)
    vi.mocked(api.settings)
      .mockRejectedValueOnce(new Error('metadata endpoint is offline'))
      .mockResolvedValue(settings)

    render(<MemoryRouter initialEntries={['/settings']}><SettingsPage /></MemoryRouter>)

    expect(await screen.findByRole('table', { name: 'Model prices' })).toBeVisible()
    const warning = await screen.findByRole('alert')
    expect(warning).toHaveTextContent('LOCAL DATABASE DETAILS UNAVAILABLE')
    expect(warning).toHaveTextContent('metadata endpoint is offline')

    fireEvent.click(screen.getByRole('button', { name: 'TRY AGAIN' }))
    expect(await screen.findByRole('status')).toHaveTextContent('LOCAL DATABASE · 3.20 GB')
    await waitFor(() => expect(api.settings).toHaveBeenCalledTimes(2))
  })

  it('shows price provenance without an effective-date column and retains exact values in the editor', async () => {
    vi.spyOn(api, 'prices').mockResolvedValue({
      ...prices,
      items: [{
        modelId: 'gpt-test',
        effectiveFrom: '1970-01-01T00:00:00Z',
        effectiveTo: null,
        inputPerMillion: '1.00',
        cachedInputPerMillion: '0.10',
        outputPerMillion: '2.00',
        currency: 'USD',
        source: 'remote:https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json',
      }],
      total: 1,
      totalPages: 1,
    })

    render(<MemoryRouter initialEntries={['/settings']}><SettingsPage /></MemoryRouter>)

    expect(await screen.findByText('gpt-test')).toBeInTheDocument()
    expect(screen.queryByRole('columnheader', { name: 'EFFECTIVE' })).not.toBeInTheDocument()
    expect(screen.getByRole('columnheader', { name: 'SOURCE' })).toBeInTheDocument()
    expect(screen.queryByText('1970-01-01')).not.toBeInTheDocument()
    expect(screen.getAllByText('LITELLM').some(element => element.classList.contains('price-source'))).toBe(true)

    expect(screen.queryByRole('button', { name: 'Delete gpt-test' })).not.toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Override gpt-test price' }))
    expect(screen.getByLabelText('INPUT / 1M')).toHaveFocus()
    expect(screen.getByLabelText('EFFECTIVE FROM')).toHaveValue('1970-01-01')
    expect(screen.getByLabelText('EFFECTIVE FROM')).toHaveAttribute('readonly')
    expect(screen.getByLabelText('INPUT / 1M')).toHaveValue('1.00')
    expect(screen.getByLabelText('CACHED / 1M')).toHaveValue('0.10')
    expect(screen.getByLabelText('OUTPUT / 1M')).toHaveValue('2.00')
  })

  it('allows destructive controls only for manual price rows', async () => {
    vi.spyOn(api, 'prices').mockResolvedValue({
      ...prices,
      items: [{
        modelId: 'gpt-manual',
        effectiveFrom: '2026-07-19T00:00:00Z',
        effectiveTo: null,
        inputPerMillion: '1.00',
        cachedInputPerMillion: null,
        outputPerMillion: '2.00',
        currency: 'USD',
        source: 'manual',
      }],
      total: 1,
      totalPages: 1,
    })

    render(<MemoryRouter initialEntries={['/settings']}><SettingsPage /></MemoryRouter>)

    expect(await screen.findByRole('button', { name: 'Edit gpt-manual' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Delete gpt-manual' })).toBeInTheDocument()
    expect(screen.getByRole('tab', { name: 'PRICE DATA' })).toHaveAttribute('aria-controls', 'settings-price-panel')
    expect(screen.getByRole('tabpanel', { name: 'Price data' })).toBeInTheDocument()
  })

  it('labels the public upstream and refreshes it directly', async () => {
    vi.spyOn(api, 'prices').mockResolvedValue({
      ...prices,
      lastRefreshAt: '2026-07-18T18:00:00Z',
      source: 'https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json',
    })
    const refreshPrices = vi.spyOn(api, 'refreshPrices').mockResolvedValue({ updated: 42 })

    render(<MemoryRouter initialEntries={['/settings']}><SettingsPage /></MemoryRouter>)

    expect(await screen.findByText('LITELLM')).toHaveAttribute(
      'title',
      'https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json',
    )
    fireEvent.click(screen.getByRole('button', { name: 'REFRESH PRICES' }))
    await waitFor(() => expect(refreshPrices).toHaveBeenCalledOnce())
  })

  it('reloads and displays persisted refresh health after the refresh request fails', async () => {
    const loadPrices = vi.spyOn(api, 'prices')
      .mockResolvedValueOnce({ ...prices, lastRefreshAt: '2026-07-18T18:00:00Z' })
      .mockResolvedValue({
        ...prices,
        lastRefreshAt: '2026-07-18T18:00:00Z',
        lastRefreshErrorAt: '2026-07-19T12:00:00Z',
        refreshError: 'LiteLLM could not be reached',
        refreshErrorKind: 'network',
      })
    vi.spyOn(api, 'refreshPrices').mockRejectedValue(new Error('refresh request failed'))

    const { container } = render(<MemoryRouter initialEntries={['/settings']}><SettingsPage /></MemoryRouter>)

    fireEvent.click(await screen.findByRole('button', { name: 'REFRESH PRICES' }))
    await waitFor(() => expect(loadPrices).toHaveBeenCalledTimes(2))
    const warning = await screen.findByRole('alert')
    expect(warning).toHaveTextContent('PRICE REFRESH FAILED · NETWORK')
    expect(warning).toHaveTextContent('LiteLLM could not be reached')
    expect(container.querySelector('.inline-error')).not.toBeInTheDocument()
  })

  it('debounces model-price search instead of requesting on every keystroke', async () => {
    const loadPrices = vi.spyOn(api, 'prices').mockResolvedValue(prices)

    render(<MemoryRouter initialEntries={['/settings']}><SettingsPage /></MemoryRouter>)

    fireEvent.change(await screen.findByRole('textbox', { name: 'Search model prices' }), {
      target: { value: 'gpt-5.6-sol' },
    })

    expect(loadPrices).toHaveBeenCalledTimes(1)
    await waitFor(() => expect(loadPrices).toHaveBeenLastCalledWith({ q: 'gpt-5.6-sol', page: 1 }, expect.any(AbortSignal)))
  })

  it('resynchronizes the search field when browser history changes the query', async () => {
    const loadPrices = vi.spyOn(api, 'prices').mockResolvedValue(prices)
    render(
      <MemoryRouter initialEntries={['/settings?tab=price-data&q=alpha', '/settings?tab=price-data&q=beta']} initialIndex={1}>
        <SettingsHistoryHarness />
      </MemoryRouter>,
    )

    expect(await screen.findByRole('textbox', { name: 'Search model prices' })).toHaveValue('beta')
    fireEvent.click(screen.getByRole('button', { name: 'BACK' }))

    await waitFor(() => expect(screen.getByRole('textbox', { name: 'Search model prices' })).toHaveValue('alpha'))
    await waitFor(() => expect(loadPrices).toHaveBeenLastCalledWith({ q: 'alpha', page: 1 }, expect.any(AbortSignal)))
  })

  it('clamps an out-of-range page to the last available page', async () => {
    const loadPrices = vi.spyOn(api, 'prices').mockImplementation(({ page = 1 }) => Promise.resolve({
      ...prices,
      page,
      pageSize: 25,
      total: 113,
      totalPages: 5,
    }))
    render(<MemoryRouter initialEntries={['/settings?tab=price-data&page=999']}><SettingsPage /></MemoryRouter>)

    await waitFor(() => expect(loadPrices).toHaveBeenLastCalledWith({ q: undefined, page: 5 }, expect.any(AbortSignal)))
    expect(await screen.findByText(/101–113 \/ 113/)).toBeInTheDocument()
  })

  it('defaults a new price to the local calendar date around UTC midnight', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    vi.setSystemTime(new Date('2026-07-18T22:30:00Z'))
    try {
      vi.spyOn(api, 'prices').mockResolvedValue(prices)
      render(<MemoryRouter initialEntries={['/settings']}><SettingsPage /></MemoryRouter>)
      fireEvent.click(await screen.findByRole('button', { name: /ADD PRICE/ }))
      expect(screen.getByLabelText('EFFECTIVE FROM')).toHaveValue('2026-07-19')
    } finally {
      vi.useRealTimers()
    }
  })

  it('loads canonical alias suggestions from every price page', async () => {
    vi.spyOn(api, 'prices').mockResolvedValue(prices)
    vi.mocked(api.pricedModelIds).mockResolvedValue(['gpt-first-page', 'gpt-last-page'])
    render(<MemoryRouter initialEntries={['/settings']}><SettingsPage /></MemoryRouter>)

    fireEvent.click(await screen.findByRole('button', { name: /ADD ALIAS/ }))

    await waitFor(() => expect(api.pricedModelIds).toHaveBeenCalledOnce())
    expect(document.querySelector('option[value="gpt-first-page"]')).toBeInTheDocument()
    expect(document.querySelector('option[value="gpt-last-page"]')).toBeInTheDocument()
  })

  it('rejects blank or negative rates and allows an optional blank cached rate', async () => {
    vi.spyOn(api, 'prices').mockResolvedValue(prices)
    const savePrice = vi.spyOn(api, 'savePrice').mockResolvedValue(undefined)

    render(<MemoryRouter initialEntries={['/settings']}><SettingsPage /></MemoryRouter>)

    fireEvent.click(await screen.findByRole('button', { name: /ADD PRICE/ }))
    fireEvent.change(screen.getByLabelText('MODEL ID'), { target: { value: 'gpt-test' } })
    fireEvent.click(screen.getByRole('button', { name: 'SAVE PRICE' }))

    expect(await screen.findByRole('alert')).toHaveTextContent('Input and output prices are required')
    expect(savePrice).not.toHaveBeenCalled()

    fireEvent.change(screen.getByLabelText('INPUT / 1M'), { target: { value: '-1' } })
    fireEvent.change(screen.getByLabelText('OUTPUT / 1M'), { target: { value: '2' } })
    fireEvent.click(screen.getByRole('button', { name: 'SAVE PRICE' }))
    expect(savePrice).not.toHaveBeenCalled()

    fireEvent.change(screen.getByLabelText('INPUT / 1M'), { target: { value: '1' } })
    fireEvent.change(screen.getByLabelText('OUTPUT / 1M'), { target: { value: '-2' } })
    fireEvent.click(screen.getByRole('button', { name: 'SAVE PRICE' }))
    expect(savePrice).not.toHaveBeenCalled()

    fireEvent.change(screen.getByLabelText('OUTPUT / 1M'), { target: { value: '2' } })
    fireEvent.change(screen.getByLabelText('CACHED / 1M'), { target: { value: '-0.1' } })
    fireEvent.click(screen.getByRole('button', { name: 'SAVE PRICE' }))
    expect(await screen.findByRole('alert')).toHaveTextContent('Cached price must be a non-negative decimal')
    expect(savePrice).not.toHaveBeenCalled()

    fireEvent.change(screen.getByLabelText('CACHED / 1M'), { target: { value: '0.0000001' } })
    fireEvent.click(screen.getByRole('button', { name: 'SAVE PRICE' }))
    expect(savePrice).not.toHaveBeenCalled()

    fireEvent.change(screen.getByLabelText('CACHED / 1M'), { target: { value: '   ' } })
    fireEvent.click(screen.getByRole('button', { name: 'SAVE PRICE' }))

    await waitFor(() => expect(savePrice).toHaveBeenCalledWith('gpt-test', expect.objectContaining({
      inputPerMillion: '1',
      cachedInputPerMillion: null,
      outputPerMillion: '2',
    })))
  })

  it('invalidates cached Overview and Stats values after a successful price mutation', async () => {
    const loadOverview = vi.fn().mockResolvedValue('cached overview cost')
    const loadStats = vi.fn().mockResolvedValue('cached stats cost')
    const cached = render(<>
      <CachedSurface cacheKey="overview" loader={loadOverview} />
      <CachedSurface cacheKey="stats:month:2026-07-18" loader={loadStats} />
    </>)
    expect(await screen.findByText('cached overview cost')).toBeInTheDocument()
    expect(await screen.findByText('cached stats cost')).toBeInTheDocument()
    cached.unmount()

    vi.spyOn(api, 'prices').mockResolvedValue(prices)
    vi.spyOn(api, 'savePrice').mockResolvedValue(undefined)
    const settings = render(<MemoryRouter initialEntries={['/settings']}><SettingsPage /></MemoryRouter>)
    fireEvent.click(await screen.findByRole('button', { name: /ADD PRICE/ }))
    fireEvent.change(screen.getByLabelText('MODEL ID'), { target: { value: 'gpt-cache-test' } })
    fireEvent.change(screen.getByLabelText('INPUT / 1M'), { target: { value: '1' } })
    fireEvent.change(screen.getByLabelText('OUTPUT / 1M'), { target: { value: '2' } })
    fireEvent.click(screen.getByRole('button', { name: 'SAVE PRICE' }))
    await waitFor(() => expect(screen.queryByRole('dialog', { name: 'Add model price' })).not.toBeInTheDocument())
    settings.unmount()

    render(<>
      <CachedSurface cacheKey="overview" loader={loadOverview} />
      <CachedSurface cacheKey="stats:month:2026-07-18" loader={loadStats} />
    </>)
    await waitFor(() => {
      expect(loadOverview).toHaveBeenCalledTimes(2)
      expect(loadStats).toHaveBeenCalledTimes(2)
    })
  })

  it('closes after a successful mutation and reports a subsequent list reload failure distinctly', async () => {
    vi.spyOn(api, 'prices')
      .mockResolvedValueOnce(prices)
      .mockRejectedValueOnce(new Error('reload is offline'))
    const savePrice = vi.spyOn(api, 'savePrice').mockResolvedValue(undefined)

    render(<MemoryRouter initialEntries={['/settings']}><SettingsPage /></MemoryRouter>)
    fireEvent.click(await screen.findByRole('button', { name: /ADD PRICE/ }))
    fireEvent.change(screen.getByLabelText('MODEL ID'), { target: { value: 'gpt-durable' } })
    fireEvent.change(screen.getByLabelText('INPUT / 1M'), { target: { value: '1' } })
    fireEvent.change(screen.getByLabelText('OUTPUT / 1M'), { target: { value: '2' } })
    fireEvent.click(screen.getByRole('button', { name: 'SAVE PRICE' }))

    await waitFor(() => expect(savePrice).toHaveBeenCalledOnce())
    expect(screen.queryByRole('dialog', { name: 'Add model price' })).not.toBeInTheDocument()
    expect(await screen.findByText(/Price saved, but the price list could not be reloaded/)).toHaveTextContent(
      'Price saved, but the price list could not be reloaded: reload is offline',
    )
    expect(savePrice).toHaveBeenCalledTimes(1)
  })

  it('contains price-editor focus and restores it to the opening control', async () => {
    vi.spyOn(api, 'prices').mockResolvedValue(prices)
    render(<MemoryRouter initialEntries={['/settings']}><SettingsPage /></MemoryRouter>)

    const trigger = await screen.findByRole('button', { name: /ADD PRICE/ })
    trigger.focus()
    fireEvent.click(trigger)

    const dialog = screen.getByRole('dialog', { name: 'Add model price' })
    const close = screen.getByRole('button', { name: 'Close' })
    const save = screen.getByRole('button', { name: 'SAVE PRICE' })
    expect(dialog).toHaveAttribute('aria-modal', 'true')
    expect(screen.getByLabelText('MODEL ID')).toHaveFocus()

    close.focus()
    fireEvent.keyDown(document, { key: 'Tab', shiftKey: true })
    expect(save).toHaveFocus()
    fireEvent.keyDown(document, { key: 'Tab' })
    expect(close).toHaveFocus()

    fireEvent.keyDown(document, { key: 'Escape' })
    expect(screen.queryByRole('dialog', { name: 'Add model price' })).not.toBeInTheDocument()
    expect(trigger).toHaveFocus()
  })

  it('contains alias-editor focus and restores it to the opening control', async () => {
    vi.spyOn(api, 'prices').mockResolvedValue(prices)
    render(<MemoryRouter initialEntries={['/settings']}><SettingsPage /></MemoryRouter>)

    const trigger = await screen.findByRole('button', { name: /ADD ALIAS/ })
    trigger.focus()
    fireEvent.click(trigger)

    const dialog = screen.getByRole('dialog', { name: 'Map observed model ID' })
    const close = screen.getByRole('button', { name: 'Close' })
    const save = screen.getByRole('button', { name: 'SAVE MAPPING' })
    expect(dialog).toHaveAttribute('aria-modal', 'true')
    expect(screen.getByLabelText('OBSERVED MODEL ID')).toHaveFocus()

    close.focus()
    fireEvent.keyDown(document, { key: 'Tab', shiftKey: true })
    expect(save).toHaveFocus()
    fireEvent.keyDown(document, { key: 'Tab' })
    expect(close).toHaveFocus()

    fireEvent.keyDown(document, { key: 'Escape' })
    expect(screen.queryByRole('dialog', { name: 'Map observed model ID' })).not.toBeInTheDocument()
    expect(trigger).toHaveFocus()
  })

  it('starts a locked unknown-model mapping at the canonical model field', async () => {
    vi.spyOn(api, 'prices').mockResolvedValue({
      ...prices,
      observedUnknown: [{ modelId: 'unpriced-model', usageCount: 1, totalTokens: 42, lastSeenAt: '2026-07-18T12:00:00Z' }],
    })
    render(<MemoryRouter initialEntries={['/settings']}><SettingsPage /></MemoryRouter>)

    fireEvent.click(await screen.findByRole('button', { name: 'MAP unpriced-model' }))

    expect(screen.getByLabelText('OBSERVED MODEL ID')).toHaveAttribute('readonly')
    expect(screen.getByLabelText('CANONICAL MODEL ID')).toHaveFocus()
  })
})
