import { afterEach, describe, expect, it, vi } from 'vitest'
import { ApiError, api } from './api'

afterEach(() => vi.unstubAllGlobals())

function jsonResponse(value: unknown, status = 200) {
  return new Response(JSON.stringify(value), { status, headers: { 'Content-Type': 'application/json' } })
}

const totals = {
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

const period = {
  label: 'Today',
  start: '2026-07-19T00:00:00Z',
  end: '2026-07-20T00:00:00Z',
  sessionCount: 0,
  messageCount: 0,
  deltaPercent: null,
  deltaCostUsd: null,
  totals,
}

describe('api client', () => {
  it('loads summary and year overview data from independent endpoints', async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(jsonResponse({ updatedAt: null, periods: { today: period, week: period, month: period } }))
      .mockResolvedValueOnce(jsonResponse({ year: 2026, heatmap: [], topProjects: [], topSessions: [] }))
    vi.stubGlobal('fetch', fetchMock)

    await api.overview()
    await api.overviewYear(2026)

    expect(fetchMock.mock.calls[0][0]).toBe('/api/v1/overview')
    expect(fetchMock.mock.calls[1][0]).toBe('/api/v1/overview/year?year=2026')
  })

  it('passes an AbortSignal through analytical requests', async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ updatedAt: null, periods: { today: period, week: period, month: period } }))
    vi.stubGlobal('fetch', fetchMock)
    const controller = new AbortController()

    await api.overview(controller.signal)

    expect(fetchMock).toHaveBeenCalledWith('/api/v1/overview', expect.objectContaining({ signal: controller.signal }))
  })

  it('requests a bounded Activity detail page', async () => {
    const item = {
      id: 'turn-1',
      turnId: 'turn-1',
      rolloutId: 'rollout-1',
      agentRunId: null,
      agentLabel: null,
      timestamp: '2026-07-19T08:00:00Z',
      kind: 'exchange',
      role: 'user',
      label: 'Request',
      body: null,
      status: 'completed',
      toolName: null,
      durationMs: 1,
      model: null,
      effort: null,
      hasDetails: true,
      children: [],
      childPage: 2,
      childPageSize: 50,
      childTotal: 75,
      childHasMore: false,
      usage: null,
      counts: null,
    }
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(item))
    vi.stubGlobal('fetch', fetchMock)
    const controller = new AbortController()

    await expect(api.sessionActivityDetail('session/1', 'turn:1', controller.signal, 2, 50))
      .resolves.toEqual(expect.objectContaining({ childPage: 2, childHasMore: false }))
    expect(fetchMock).toHaveBeenCalledWith(
      '/api/v1/sessions/session%2F1/activity/turn%3A1?childPage=2&childPageSize=50',
      expect.objectContaining({ signal: controller.signal }),
    )
  })

  it('preserves session date ranges and pagination in the v1 request', async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ items: [], page: 2, pageSize: 50, total: 0, totalPages: 1, projects: [] }))
    vi.stubGlobal('fetch', fetchMock)

    await api.sessions({ start: '2026-07-01', end: '2026-07-09', project: 'codex', sort: 'cost', page: 2 })

    const url = String(fetchMock.mock.calls[0][0])
    expect(url).toContain('/api/v1/sessions?')
    expect(url).toContain('start=2026-07-01')
    expect(url).toContain('end=2026-07-09')
    expect(url).toContain('pageSize=50')
  })

  it('sends explicit model aliases to the retroactive pricing endpoint', async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(null, { status: 204 }))
    vi.stubGlobal('fetch', fetchMock)

    await api.saveAlias('codex-auto-review', 'gpt-5.5')

    expect(fetchMock).toHaveBeenCalledWith('/api/v1/aliases/codex-auto-review', expect.objectContaining({
      method: 'PUT',
      body: JSON.stringify({ canonicalModelId: 'gpt-5.5' }),
    }))
  })

  it('surfaces backend error messages', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse({ error: 'bad range' }, 400)))
    await expect(api.sessions({ start: 'nope' })).rejects.toEqual(expect.objectContaining<ApiError>({ name: 'ApiError', status: 400, message: 'bad range' }))
  })

  it('rejects successful responses that drift from the frontend contract', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse({ state: 'idle' })))

    await expect(api.status()).rejects.toThrow('Invalid API response at status.lastIngestAt')
  })

  it('validates the top-level Stats label', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse({
      range: 'month',
      anchor: '2026-07-19',
      label: 202607,
      totals,
      rows: [],
      trend: [],
    })))

    await expect(api.stats('month')).rejects.toThrow('Invalid API response at stats.label')
  })

  it('validates and returns Settings storage metadata', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse({
      databasePath: '/tmp/codex-usage.db',
      activeRoot: '/tmp/sessions',
      archiveRoot: null,
      timezone: 'CEST',
      lastIngestAt: null,
      sessionCount: 12,
      databaseBytes: 4096,
      pricing: { knownCostUsd: 1, unpricedTokens: 0, complete: true },
    })))

    await expect(api.settings()).resolves.toEqual(expect.objectContaining({ databaseBytes: 4096 }))
  })

  it('loads every price page for alias suggestions', async () => {
    const price = (modelId: string) => ({
      modelId,
      effectiveFrom: '1970-01-01T00:00:00Z',
      effectiveTo: null,
      inputPerMillion: '1.000000',
      cachedInputPerMillion: null,
      outputPerMillion: '2.000000',
      currency: 'USD',
      source: 'remote:test',
    })
    const response = (page: number, items: ReturnType<typeof price>[]) => ({
      items,
      aliases: [],
      observedUnknown: [],
      page,
      pageSize: 100,
      total: 3,
      totalPages: 2,
      lastRefreshAt: null,
      lastRefreshErrorAt: null,
      refreshError: null,
      refreshErrorKind: null,
      source: null,
    })
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(jsonResponse(response(1, [price('gpt-b'), price('gpt-a')])))
      .mockResolvedValueOnce(jsonResponse(response(2, [price('gpt-c')])))
    vi.stubGlobal('fetch', fetchMock)

    await expect(api.pricedModelIds()).resolves.toEqual(['gpt-a', 'gpt-b', 'gpt-c'])
    expect(fetchMock.mock.calls.map(([url]) => String(url))).toEqual([
      '/api/v1/prices?page=1&pageSize=100',
      '/api/v1/prices?page=2&pageSize=100',
    ])
  })
})
