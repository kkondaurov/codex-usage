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
  costUsd: '0',
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

  it('passes the opaque Activity cursor when seeking the next child page', async () => {
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

    await api.sessionActivityDetail('session', 'turn', undefined, 2, 50, '["stamp",1,"event"]')

    expect(fetchMock).toHaveBeenCalledWith(
      '/api/v1/sessions/session/activity/turn?childPage=2&childPageSize=50&childCursor=%5B%22stamp%22%2C1%2C%22event%22%5D',
      expect.any(Object),
    )
  })

  it('rejects malformed timestamps nested inside Activity responses', async () => {
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
      children: [{
        id: 'child-1',
        turnId: 'turn-1',
        rolloutId: 'rollout-1',
        agentRunId: null,
        agentLabel: null,
        timestamp: '2026-02-30T08:00:00Z',
        kind: 'tool',
        role: null,
        label: null,
        body: null,
        status: 'completed',
        toolName: 'exec',
        durationMs: 1,
        model: null,
        effort: null,
        hasDetails: false,
        children: [],
        usage: null,
        counts: null,
      }],
      usage: null,
      counts: null,
    }
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse({
      items: [item],
      days: [],
      page: 1,
      pageSize: 25,
      total: 1,
      totalPages: 1,
    })))

    await expect(api.sessionActivity('session-1', 1))
      .rejects.toThrow('Invalid API response at activity.items[0].children[0].timestamp')
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

  it('rejects unexpected no-content responses from JSON endpoints', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(null, { status: 204 })))

    await expect(api.status()).rejects.toThrow('expected JSON, received 204 No Content')
  })

  it('rejects semantically invalid status timestamps', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse({
      state: 'idle',
      lastIngestAt: '2026-02-30T00:00:00Z',
      lastIngestAttemptAt: null,
      lastEventAt: null,
      filesScanned: 0,
      filesFailed: 0,
    })))

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
      pricing: { knownCostUsd: '1', unpricedTokens: 0, complete: true },
    })))

    await expect(api.settings()).resolves.toEqual(expect.objectContaining({ databaseBytes: 4096 }))
  })

  it('loads bounded filtered model IDs for alias suggestions', async () => {
    const fetchMock = vi.fn().mockResolvedValueOnce(jsonResponse({ items: ['gpt-a', 'gpt-b'] }))
    vi.stubGlobal('fetch', fetchMock)

    await expect(api.pricedModelIds({ q: 'gpt', limit: 100 })).resolves.toEqual(['gpt-a', 'gpt-b'])
    expect(fetchMock.mock.calls.map(([url]) => String(url))).toEqual([
      '/api/v1/prices/model-ids?q=gpt&limit=100',
    ])
  })

  it('loads non-paginated price metadata separately from the price ledger', async () => {
    const response = { aliases: [], aliasesTotal: 0, observedUnknown: [], observedUnknownTotal: 0 }
    const fetchMock = vi.fn().mockResolvedValueOnce(jsonResponse(response))
    vi.stubGlobal('fetch', fetchMock)

    await expect(api.priceMetadata()).resolves.toEqual(response)
    expect(fetchMock).toHaveBeenCalledWith('/api/v1/prices/metadata', expect.any(Object))
  })
})
