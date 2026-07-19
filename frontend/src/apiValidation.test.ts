import { describe, expect, it } from 'vitest'
import {
  activityItemResponse,
  overviewResponse,
  pricesResponse,
  sessionSummaryResponse,
  sessionsResponse,
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
  aliases: [],
  observedUnknown: [],
  page: 1,
  pageSize: 25,
  total: 0,
  totalPages: 1,
  lastRefreshAt: null,
  lastRefreshErrorAt: null,
  refreshError: null,
  source: null,
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
})
