import { describe, expect, it } from 'vitest'
import {
  activityItemResponse,
  activityResponse,
  overviewResponse,
  overviewYearResponse,
  priceMetadataResponse,
  priceModelIdsResponse,
  pricesResponse,
  settingsResponse,
  sessionSummaryResponse,
  sessionsResponse,
  statsResponse,
  statusResponse,
} from './apiValidation'

const totals = {
  inputTokens: 0,
  cachedInputTokens: 0,
  outputTokens: 0,
  reasoningTokens: 0,
  blendedTokens: 0,
  totalTokens: 0,
  costUsd: null,
  unpricedTokens: 0,
  pricingComplete: true,
}

const session = {
  id: 'session-1',
  rootThreadId: 'session-1',
  startedAt: '2026-07-19T08:00:00Z',
  lastEventAt: '2026-07-19T08:01:00Z',
  title: 'Contract test',
  project: 'codex-usage',
  branch: null,
  messageCount: 1,
  turnCount: 1,
  agentCount: 0,
  toolCount: 0,
  totalTokens: 0,
  costUsd: null,
  unpricedTokens: 0,
  lifetimeCostUsd: null,
  lifetimeUnpricedTokens: 0,
}

const activityItem = {
  id: 'event-1',
  turnId: null,
  rolloutId: 'session-1',
  agentRunId: null,
  agentLabel: null,
  timestamp: '2026-07-19T08:00:00Z',
  kind: 'tool',
  role: null,
  label: null,
  body: null,
  status: null,
  toolName: 'exec',
  durationMs: null,
  model: null,
  effort: null,
  hasDetails: false,
  children: [],
  usage: null,
  counts: null,
}

const prices = {
  items: [],
  page: 1,
  pageSize: 25,
  total: 0,
  totalPages: 1,
  lastRefreshAt: null,
  lastRefreshErrorAt: null,
  refreshError: null,
  source: null,
}
const priceMetadata = { aliases: [], aliasesTotal: 0, observedUnknown: [], observedUnknownTotal: 0 }

const price = {
  modelId: 'gpt-test',
  effectiveFrom: '2026-07-19T00:00:00Z',
  effectiveTo: null,
  inputPerMillion: '1.000000',
  cachedInputPerMillion: null,
  outputPerMillion: '2.000000',
  currency: 'USD',
  source: 'manual',
}

function without(value: Record<string, unknown>, key: string) {
  const copy = { ...value }
  delete copy[key]
  return copy
}

describe('runtime API contracts', () => {
  it('accepts optional PeriodSummary fields when the API omits them', () => {
    const minimalPeriod = { sessionCount: 0, messageCount: 0, totals }

    expect(() => overviewResponse({
      updatedAt: null,
      periods: { today: minimalPeriod, week: minimalPeriod, month: minimalPeriod },
    })).not.toThrow()
  })

  it('still validates optional PeriodSummary fields when they are present', () => {
    const invalidPeriod = { sessionCount: 0, messageCount: 0, totals, label: null }

    expect(() => overviewResponse({
      updatedAt: null,
      periods: { today: invalidPeriod, week: invalidPeriod, month: invalidPeriod },
    })).toThrow('Invalid API response at overview.periods.today.label')
  })

  it('accepts canonical decimal strings without coercing dollar values to numbers', () => {
    const exactTotals = { ...totals, costUsd: '9007199254740993.000000000001' }
    const exactPeriod = { sessionCount: 1, messageCount: 1, totals: exactTotals }

    expect(overviewResponse({
      updatedAt: null,
      periods: { today: exactPeriod, week: exactPeriod, month: exactPeriod },
    }).periods.today.totals.costUsd).toBe('9007199254740993.000000000001')
  })

  it.each([
    ['overview.periods.today.totals.totalTokens', () => overviewResponse({
      updatedAt: null,
      periods: {
        today: { sessionCount: 0, messageCount: 0, totals: { ...totals, totalTokens: Number.MAX_SAFE_INTEGER + 1 } },
        week: { sessionCount: 0, messageCount: 0, totals },
        month: { sessionCount: 0, messageCount: 0, totals },
      },
    })],
    ['sessions.items[0].messageCount', () => sessionsResponse({
      items: [{ ...session, messageCount: -1 }],
      page: 1,
      pageSize: 50,
      total: 1,
      totalPages: 1,
      projects: [],
    })],
    ['sessions.page', () => sessionsResponse({
      items: [],
      page: 1.5,
      pageSize: 50,
      total: 0,
      totalPages: 1,
      projects: [],
    })],
    ['activityItem.durationMs', () => activityItemResponse({ ...activityItem, durationMs: 1.5 })],
    ['settings.databaseBytes', () => settingsResponse({
      databasePath: '/tmp/codex-usage.db',
      activeRoot: null,
      archiveRoot: null,
      timezone: 'UTC',
      lastIngestAt: null,
      sessionCount: 0,
      databaseBytes: -1,
      pricing: { knownCostUsd: '0', unpricedTokens: 0, complete: true },
    })],
  ])('rejects unsafe, fractional, or negative integer fields at %s', (path, validate) => {
    expect(validate).toThrow(`Invalid API response at ${path}: expected a non-negative safe integer`)
  })

  it.each([
    ['overview.periods.today.totals.costUsd', () => overviewResponse({
      updatedAt: null,
      periods: {
        today: { sessionCount: 0, messageCount: 0, totals: { ...totals, costUsd: 0 } },
        week: { sessionCount: 0, messageCount: 0, totals },
        month: { sessionCount: 0, messageCount: 0, totals },
      },
    })],
    ['sessions.items[0].costUsd', () => sessionsResponse({
      items: [{ ...session, costUsd: 0 }],
      page: 1,
      pageSize: 50,
      total: 1,
      totalPages: 1,
      projects: [],
    })],
    ['settings.pricing.knownCostUsd', () => settingsResponse({
      databasePath: '/tmp/codex-usage.db',
      activeRoot: null,
      archiveRoot: null,
      timezone: 'UTC',
      lastIngestAt: null,
      sessionCount: 0,
      databaseBytes: 0,
      pricing: { knownCostUsd: 0, unpricedTokens: 0, complete: true },
    })],
    ['stats.trend[0]', () => statsResponse({
      range: 'day',
      anchor: '2026-07-19',
      label: 'July 19',
      totals,
      rows: [],
      trend: [0],
    })],
  ])('rejects binary numbers at %s', (path, validate) => {
    expect(validate).toThrow(`Invalid API response at ${path}`)
  })

  it.each([
    ['rootThreadId', without(session, 'rootThreadId')],
    ['rootThreadId', { ...session, rootThreadId: null }],
    ['project', without(session, 'project')],
    ['project', { ...session, project: null }],
  ])('requires non-null SessionRow.%s', (field, candidate) => {
    expect(() => sessionsResponse({
      items: [candidate],
      page: 1,
      pageSize: 50,
      total: 1,
      totalPages: 1,
      projects: [],
    })).toThrow(`Invalid API response at sessions.items[0].${field}`)
  })

  it('requires SessionsResponse.projects', () => {
    expect(() => sessionsResponse({
      items: [],
      page: 1,
      pageSize: 50,
      total: 0,
      totalPages: 1,
    })).toThrow('Invalid API response at sessions.projects')
  })

  it('accepts omitted nullable SessionSummary extension fields', () => {
    expect(() => sessionSummaryResponse({
      session: { ...session, status: 'completed' },
      totals,
      models: [],
      agents: [],
      toolSummary: [],
    })).not.toThrow()
  })

  it('validates optional SessionSummary extension fields when present', () => {
    expect(() => sessionSummaryResponse({
      session: { ...session, status: 'completed', cwd: 42 },
      totals,
      models: [],
      agents: [],
      toolSummary: [],
    })).toThrow('Invalid API response at sessionSummary.session.cwd')
  })

  it.each(['agentLabel', 'counts'])('requires ActivityItem.%s while accepting null', field => {
    expect(() => activityItemResponse(activityItem)).not.toThrow()
    expect(() => activityItemResponse(without(activityItem, field)))
      .toThrow(`Invalid API response at activityItem.${field}`)
  })

  it.each(['lastRefreshAt', 'source'])('requires nullable PricesResponse.%s', field => {
    expect(() => pricesResponse(prices)).not.toThrow()
    expect(() => pricesResponse(without(prices, field)))
      .toThrow(`Invalid API response at prices.${field}`)
  })

  it('keeps refreshErrorKind optional and validates it when present', () => {
    expect(() => pricesResponse(prices)).not.toThrow()
    expect(() => pricesResponse({ ...prices, refreshErrorKind: 503 }))
      .toThrow('Invalid API response at prices.refreshErrorKind')
  })

  it.each([
    ['not-a-date', 'status.lastIngestAt'],
    ['2026-02-30T00:00:00Z', 'status.lastIngestAt'],
  ])('rejects invalid status timestamp %s', (value, path) => {
    expect(() => statusResponse({
      state: 'idle',
      lastIngestAt: value,
      lastIngestAttemptAt: null,
      lastEventAt: null,
      filesScanned: 0,
      filesFailed: 0,
    })).toThrow(`Invalid API response at ${path}`)
  })

  it('rejects invalid settings timestamps', () => {
    expect(() => settingsResponse({
      databasePath: '/tmp/codex-usage.db',
      activeRoot: '/tmp/sessions',
      archiveRoot: null,
      timezone: 'UTC',
      lastIngestAt: '2026-07-19T24:00:00Z',
      sessionCount: 0,
      databaseBytes: 0,
      pricing: { knownCostUsd: '0', unpricedTokens: 0, complete: true },
    })).toThrow('Invalid API response at settings.lastIngestAt')
  })

  it.each(['startedAt', 'lastEventAt'])('semantically validates SessionRow.%s', field => {
    expect(() => sessionsResponse({
      items: [{ ...session, [field]: '2026-02-30T00:00:00Z' }],
      page: 1,
      pageSize: 50,
      total: 1,
      totalPages: 1,
      projects: [],
    })).toThrow(`Invalid API response at sessions.items[0].${field}`)
  })

  it('semantically validates overview update and period timestamps', () => {
    const validPeriod = {
      label: 'Today',
      start: '2026-07-19T00:00:00Z',
      end: '2026-07-20T00:00:00Z',
      sessionCount: 0,
      messageCount: 0,
      totals,
    }
    expect(() => overviewResponse({
      updatedAt: '2026-02-30T00:00:00Z',
      periods: { today: validPeriod, week: validPeriod, month: validPeriod },
    })).toThrow('Invalid API response at overview.updatedAt')
    expect(() => overviewResponse({
      updatedAt: null,
      periods: {
        today: { ...validPeriod, end: '2026-07-20T24:00:00Z' },
        week: validPeriod,
        month: validPeriod,
      },
    })).toThrow('Invalid API response at overview.periods.today.end')
  })

  it('semantically validates the optional session completion timestamp', () => {
    expect(() => sessionSummaryResponse({
      session: { ...session, status: 'completed', completedAt: 'not-a-timestamp' },
      totals,
      models: [],
      agents: [],
      toolSummary: [],
    })).toThrow('Invalid API response at sessionSummary.session.completedAt')
  })

  it('semantically validates renderable date-only fields', () => {
    expect(() => overviewYearResponse({
      year: 2026,
      heatmap: [{ date: '2026-02-30', costUsd: '0', sessionCount: 0, totalTokens: 0 }],
      topProjects: [],
      topSessions: [],
    })).toThrow('Invalid API response at overviewYear.heatmap[0].date')
    expect(() => activityResponse({
      items: [],
      days: [{ date: '1969-12-31', durationMs: 0, totals }],
      page: 1,
      pageSize: 25,
      total: 0,
      totalPages: 1,
    })).toThrow('Invalid API response at activity.days[0].date')
    expect(() => statsResponse({
      range: 'month',
      anchor: '9999-01-01',
      label: 'January 9999',
      totals,
      rows: [],
      trend: [],
    })).toThrow('Invalid API response at stats.anchor')
  })

  it.each(['periodStart', 'periodEnd'])('semantically validates StatsRow.%s', field => {
    expect(() => statsResponse({
      range: 'day',
      anchor: '2026-07-19',
      label: 'July 19',
      totals,
      rows: [{
        ...totals,
        periodStart: '2026-07-19T00:00:00Z',
        periodEnd: '2026-07-20T00:00:00Z',
        [field]: '2026-07-19T24:00:00Z',
        label: 'MIDNIGHT',
        sessionCount: 0,
      }],
      trend: [],
    })).toThrow(`Invalid API response at stats.rows[0].${field}`)
  })

  it.each([
    ['prices.items[0].effectiveFrom', { ...prices, items: [{ ...price, effectiveFrom: '2026-02-30T00:00:00Z' }] }],
    ['prices.items[0].effectiveTo', { ...prices, items: [{ ...price, effectiveTo: 'not-a-timestamp' }] }],
    ['prices.lastRefreshAt', { ...prices, lastRefreshAt: '2026-02-30T00:00:00Z' }],
    ['prices.lastRefreshErrorAt', { ...prices, lastRefreshErrorAt: 'not-a-timestamp' }],
  ])('semantically validates %s', (path, candidate) => {
    expect(() => pricesResponse(candidate)).toThrow(`Invalid API response at ${path}`)
  })

  it('validates dedicated price metadata and model ID responses', () => {
    expect(() => priceMetadataResponse({
      ...priceMetadata,
      observedUnknown: [{ modelId: 'unknown', usageCount: 1, totalTokens: 1, lastSeenAt: '2026-07-19T24:00:00Z' }],
    })).toThrow('Invalid API response at priceMetadata.observedUnknown[0].lastSeenAt')
    expect(() => priceMetadataResponse({ ...priceMetadata, observedUnknownTotal: -1 }))
      .toThrow('Invalid API response at priceMetadata.observedUnknownTotal')
    expect(() => priceMetadataResponse({ ...priceMetadata, aliasesTotal: -1 }))
      .toThrow('Invalid API response at priceMetadata.aliasesTotal')
    expect(() => priceModelIdsResponse({ items: ['gpt-5', 7] }))
      .toThrow('Invalid API response at priceModelIds.items[1]')
  })
})
