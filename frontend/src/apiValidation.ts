import type {
  ActivityItem,
  ActivityResponse,
  OverviewResponse,
  OverviewYearResponse,
  PricesResponse,
  SettingsResponse,
  SessionRow,
  SessionSummary,
  SessionUsageResponse,
  SessionsResponse,
  StatsResponse,
  StatusResponse,
  Totals,
} from './types'

type JsonObject = Record<string, unknown>

export class ResponseValidationError extends Error {
  constructor(path: string, expected: string) {
    super(`Invalid API response at ${path}: expected ${expected}`)
    this.name = 'ResponseValidationError'
  }
}

function object(value: unknown, path: string): JsonObject {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new ResponseValidationError(path, 'an object')
  }
  return value as JsonObject
}

function array(value: unknown, path: string): unknown[] {
  if (!Array.isArray(value)) throw new ResponseValidationError(path, 'an array')
  return value
}

function string(value: unknown, path: string) {
  if (typeof value !== 'string') throw new ResponseValidationError(path, 'a string')
}

function number(value: unknown, path: string) {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw new ResponseValidationError(path, 'a finite number')
  }
}

function boolean(value: unknown, path: string) {
  if (typeof value !== 'boolean') throw new ResponseValidationError(path, 'a boolean')
}

function nullable(value: unknown, path: string, validate: (value: unknown, path: string) => void) {
  if (value !== null) validate(value, path)
}

function optional(value: unknown, path: string, validate: (value: unknown, path: string) => void) {
  if (value !== undefined) validate(value, path)
}

function arrayOf(value: unknown, path: string, validate: (value: unknown, path: string) => void) {
  array(value, path).forEach((item, index) => validate(item, `${path}[${index}]`))
}

function totals(value: unknown, path: string): asserts value is Totals {
  const item = object(value, path)
  for (const key of [
    'inputTokens',
    'cachedInputTokens',
    'outputTokens',
    'reasoningTokens',
    'blendedTokens',
    'totalTokens',
    'unpricedTokens',
  ]) number(item[key], `${path}.${key}`)
  nullable(item.costUsd, `${path}.costUsd`, number)
  boolean(item.pricingComplete, `${path}.pricingComplete`)
}

function modelUsage(value: unknown, path: string) {
  const item = object(value, path)
  string(item.model, `${path}.model`)
  nullable(item.effort, `${path}.effort`, string)
  for (const key of [
    'inputTokens',
    'cachedInputTokens',
    'outputTokens',
    'reasoningTokens',
    'totalTokens',
    'unpricedTokens',
  ]) number(item[key], `${path}.${key}`)
  nullable(item.costUsd, `${path}.costUsd`, number)
}

function usageBreakdown(value: unknown, path: string) {
  const item = object(value, path)
  string(item.id, `${path}.id`)
  string(item.label, `${path}.label`)
  for (const key of ['model', 'agentRunId', 'turnId', 'effort']) nullable(item[key], `${path}.${key}`, string)
  totals(item.totals, `${path}.totals`)
}

function sessionRow(value: unknown, path: string): asserts value is SessionRow {
  const item = object(value, path)
  string(item.id, `${path}.id`)
  string(item.rootThreadId, `${path}.rootThreadId`)
  for (const key of ['startedAt', 'lastEventAt', 'title']) string(item[key], `${path}.${key}`)
  string(item.project, `${path}.project`)
  nullable(item.branch, `${path}.branch`, string)
  for (const key of [
    'messageCount',
    'turnCount',
    'agentCount',
    'toolCount',
    'totalTokens',
    'unpricedTokens',
    'lifetimeUnpricedTokens',
  ]) number(item[key], `${path}.${key}`)
  nullable(item.costUsd, `${path}.costUsd`, number)
  nullable(item.lifetimeCostUsd, `${path}.lifetimeCostUsd`, number)
}

function period(value: unknown, path: string) {
  const item = object(value, path)
  for (const key of ['label', 'start', 'end']) optional(item[key], `${path}.${key}`, string)
  number(item.sessionCount, `${path}.sessionCount`)
  number(item.messageCount, `${path}.messageCount`)
  for (const key of ['deltaPercent', 'deltaCostUsd']) {
    optional(item[key], `${path}.${key}`, value => nullable(value, `${path}.${key}`, number))
  }
  totals(item.totals, `${path}.totals`)
}

function paged(item: JsonObject, path: string) {
  for (const key of ['page', 'pageSize', 'total', 'totalPages']) number(item[key], `${path}.${key}`)
}

function activityItem(value: unknown, path: string): asserts value is ActivityItem {
  const item = object(value, path)
  for (const key of ['id', 'rolloutId', 'timestamp', 'kind']) string(item[key], `${path}.${key}`)
  for (const key of ['turnId', 'agentRunId', 'agentLabel', 'role', 'label', 'body', 'status', 'toolName', 'model', 'effort']) {
    nullable(item[key], `${path}.${key}`, string)
  }
  nullable(item.durationMs, `${path}.durationMs`, number)
  boolean(item.hasDetails, `${path}.hasDetails`)
  arrayOf(item.children, `${path}.children`, activityItem)
  for (const key of ['childPage', 'childPageSize', 'childTotal']) {
    optional(item[key], `${path}.${key}`, number)
  }
  optional(item.childHasMore, `${path}.childHasMore`, boolean)
  nullable(item.usage, `${path}.usage`, totals)
  nullable(item.counts, `${path}.counts`, value => {
    const counts = object(value, `${path}.counts`)
    for (const key of ['modelCalls', 'toolCalls', 'agentRuns', 'reviews', 'followUps']) number(counts[key], `${path}.counts.${key}`)
  })
}

export function statusResponse(value: unknown): StatusResponse {
  const item = object(value, 'status')
  string(item.state, 'status.state')
  for (const key of ['lastIngestAt', 'lastIngestAttemptAt', 'lastEventAt']) {
    nullable(item[key], `status.${key}`, string)
  }
  number(item.filesScanned, 'status.filesScanned')
  number(item.filesFailed, 'status.filesFailed')
  return item as unknown as StatusResponse
}

export function settingsResponse(value: unknown): SettingsResponse {
  const item = object(value, 'settings')
  string(item.databasePath, 'settings.databasePath')
  nullable(item.activeRoot, 'settings.activeRoot', string)
  nullable(item.archiveRoot, 'settings.archiveRoot', string)
  string(item.timezone, 'settings.timezone')
  nullable(item.lastIngestAt, 'settings.lastIngestAt', string)
  number(item.sessionCount, 'settings.sessionCount')
  number(item.databaseBytes, 'settings.databaseBytes')
  const pricing = object(item.pricing, 'settings.pricing')
  number(pricing.knownCostUsd, 'settings.pricing.knownCostUsd')
  number(pricing.unpricedTokens, 'settings.pricing.unpricedTokens')
  boolean(pricing.complete, 'settings.pricing.complete')
  return item as unknown as SettingsResponse
}

export function overviewResponse(value: unknown): OverviewResponse {
  const item = object(value, 'overview')
  nullable(item.updatedAt, 'overview.updatedAt', string)
  const periods = object(item.periods, 'overview.periods')
  for (const key of ['today', 'week', 'month']) period(periods[key], `overview.periods.${key}`)
  return item as unknown as OverviewResponse
}

export function overviewYearResponse(value: unknown): OverviewYearResponse {
  const item = object(value, 'overviewYear')
  number(item.year, 'overviewYear.year')
  arrayOf(item.heatmap, 'overviewYear.heatmap', (day, path) => {
    const candidate = object(day, path)
    string(candidate.date, `${path}.date`)
    nullable(candidate.costUsd, `${path}.costUsd`, number)
    for (const key of ['sessionCount', 'totalTokens']) number(candidate[key], `${path}.${key}`)
    optional(candidate.messageCount, `${path}.messageCount`, number)
    optional(candidate.future, `${path}.future`, boolean)
  })
  arrayOf(item.topProjects, 'overviewYear.topProjects', (project, path) => {
    const candidate = object(project, path)
    string(candidate.project, `${path}.project`)
    nullable(candidate.costUsd, `${path}.costUsd`, number)
    nullable(candidate.share, `${path}.share`, number)
  })
  arrayOf(item.topSessions, 'overviewYear.topSessions', sessionRow)
  return item as unknown as OverviewYearResponse
}

export function sessionsResponse(value: unknown): SessionsResponse {
  const item = object(value, 'sessions')
  arrayOf(item.items, 'sessions.items', sessionRow)
  paged(item, 'sessions')
  arrayOf(item.projects, 'sessions.projects', string)
  return item as unknown as SessionsResponse
}

export function sessionSummaryResponse(value: unknown): SessionSummary {
  const item = object(value, 'sessionSummary')
  sessionRow(item.session, 'sessionSummary.session')
  const session = object(item.session, 'sessionSummary.session')
  string(session.status, 'sessionSummary.session.status')
  for (const key of ['cwd', 'source', 'latestResult', 'firstPrompt', 'completedAt']) {
    optional(session[key], `sessionSummary.session.${key}`, value => {
      nullable(value, `sessionSummary.session.${key}`, string)
    })
  }
  totals(item.totals, 'sessionSummary.totals')
  arrayOf(item.models, 'sessionSummary.models', modelUsage)
  arrayOf(item.agents, 'sessionSummary.agents', (agent, path) => {
    const candidate = object(agent, path)
    string(candidate.id, `${path}.id`)
    string(candidate.label, `${path}.label`)
    nullable(candidate.path, `${path}.path`, string)
    nullable(candidate.nickname, `${path}.nickname`, string)
    string(candidate.status, `${path}.status`)
    for (const key of ['turnCount', 'toolCount', 'totalTokens', 'unpricedTokens']) number(candidate[key], `${path}.${key}`)
    nullable(candidate.costUsd, `${path}.costUsd`, number)
  })
  arrayOf(item.toolSummary, 'sessionSummary.toolSummary', (tool, path) => {
    const candidate = object(tool, path)
    string(candidate.tool, `${path}.tool`)
    for (const key of ['count', 'failedCount', 'totalDurationMs']) number(candidate[key], `${path}.${key}`)
  })
  return item as unknown as SessionSummary
}

export function activityItemResponse(value: unknown): ActivityItem {
  activityItem(value, 'activityItem')
  return value
}

export function activityResponse(value: unknown): ActivityResponse {
  const item = object(value, 'activity')
  arrayOf(item.items, 'activity.items', activityItem)
  arrayOf(item.days, 'activity.days', (day, path) => {
    const candidate = object(day, path)
    string(candidate.date, `${path}.date`)
    number(candidate.durationMs, `${path}.durationMs`)
    totals(candidate.totals, `${path}.totals`)
  })
  paged(item, 'activity')
  return item as unknown as ActivityResponse
}

export function sessionUsageResponse(value: unknown): SessionUsageResponse {
  const item = object(value, 'sessionUsage')
  totals(item.totals, 'sessionUsage.totals')
  arrayOf(item.byModel, 'sessionUsage.byModel', modelUsage)
  arrayOf(item.byAgent, 'sessionUsage.byAgent', usageBreakdown)
  arrayOf(item.byTurn, 'sessionUsage.byTurn', usageBreakdown)
  const pricing = object(item.pricing, 'sessionUsage.pricing')
  number(pricing.knownCostUsd, 'sessionUsage.pricing.knownCostUsd')
  number(pricing.unpricedTokens, 'sessionUsage.pricing.unpricedTokens')
  boolean(pricing.complete, 'sessionUsage.pricing.complete')
  return item as unknown as SessionUsageResponse
}

export function statsResponse(value: unknown): StatsResponse {
  const item = object(value, 'stats')
  string(item.range, 'stats.range')
  if (!['day', 'week', 'month', 'year', 'all'].includes(item.range as string)) {
    throw new ResponseValidationError('stats.range', 'day, week, month, year, or all')
  }
  string(item.anchor, 'stats.anchor')
  string(item.label, 'stats.label')
  totals(item.totals, 'stats.totals')
  arrayOf(item.rows, 'stats.rows', (row, path) => {
    totals(row, path)
    const candidate = object(row, path)
    string(candidate.periodStart, `${path}.periodStart`)
    string(candidate.periodEnd, `${path}.periodEnd`)
    string(candidate.label, `${path}.label`)
    number(candidate.sessionCount, `${path}.sessionCount`)
  })
  arrayOf(item.trend, 'stats.trend', (point, path) => nullable(point, path, number))
  return item as unknown as StatsResponse
}

export function pricesResponse(value: unknown): PricesResponse {
  const item = object(value, 'prices')
  arrayOf(item.items, 'prices.items', (price, path) => {
    const candidate = object(price, path)
    for (const key of ['modelId', 'effectiveFrom', 'inputPerMillion', 'outputPerMillion', 'currency', 'source']) {
      string(candidate[key], `${path}.${key}`)
    }
    nullable(candidate.effectiveTo, `${path}.effectiveTo`, string)
    nullable(candidate.cachedInputPerMillion, `${path}.cachedInputPerMillion`, string)
  })
  arrayOf(item.aliases, 'prices.aliases', (alias, path) => {
    const candidate = object(alias, path)
    string(candidate.observedModelId, `${path}.observedModelId`)
    string(candidate.canonicalModelId, `${path}.canonicalModelId`)
  })
  arrayOf(item.observedUnknown, 'prices.observedUnknown', (unknown, path) => {
    const candidate = object(unknown, path)
    string(candidate.modelId, `${path}.modelId`)
    number(candidate.usageCount, `${path}.usageCount`)
    number(candidate.totalTokens, `${path}.totalTokens`)
    string(candidate.lastSeenAt, `${path}.lastSeenAt`)
  })
  paged(item, 'prices')
  nullable(item.lastRefreshAt, 'prices.lastRefreshAt', string)
  nullable(item.lastRefreshErrorAt, 'prices.lastRefreshErrorAt', string)
  nullable(item.refreshError, 'prices.refreshError', string)
  optional(item.refreshErrorKind, 'prices.refreshErrorKind', value => nullable(value, 'prices.refreshErrorKind', string))
  nullable(item.source, 'prices.source', string)
  return item as unknown as PricesResponse
}
