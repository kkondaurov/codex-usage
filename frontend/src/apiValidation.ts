import type {
  ActivityItem,
  ActivityResponse,
  AliasesResponse,
  OverviewResponse,
  OverviewYearResponse,
  PricesResponse,
  SettingsResponse,
  SessionRow,
  SessionSummary,
  PriceMetadataResponse,
  PriceModelIdsResponse,
  SessionsResponse,
  StatsResponse,
  StatusResponse,
  Totals,
} from './types'
import { validDateOnly } from './calendar'
import { isDecimalString } from './decimal'

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

const RFC3339_TIMESTAMP = /^(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(?:Z|[+-](\d{2}):(\d{2}))$/

function timestamp(value: unknown, path: string) {
  string(value, path)
  const match = RFC3339_TIMESTAMP.exec(value as string)
  const date = match ? new Date(`${match[1]}T00:00:00Z`) : null
  const calendarDateIsValid = date != null
    && Number.isFinite(date.getTime())
    && date.toISOString().slice(0, 10) === match?.[1]
  const clockIsValid = match != null
    && Number(match[2]) <= 23
    && Number(match[3]) <= 59
    && Number(match[4]) <= 59
    && (match[5] === undefined || Number(match[5]) <= 23)
    && (match[6] === undefined || Number(match[6]) <= 59)
  if (!calendarDateIsValid || !clockIsValid || !Number.isFinite(Date.parse(value as string))) {
    throw new ResponseValidationError(path, 'an RFC 3339 timestamp')
  }
}

function dateOnly(value: unknown, path: string) {
  string(value, path)
  if (!validDateOnly(value as string)) {
    throw new ResponseValidationError(path, 'a YYYY-MM-DD date in the public year range')
  }
}

function number(value: unknown, path: string) {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw new ResponseValidationError(path, 'a finite number')
  }
}

function nonnegativeSafeInteger(value: unknown, path: string) {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) {
    throw new ResponseValidationError(path, 'a non-negative safe integer')
  }
}

function decimal(value: unknown, path: string) {
  if (!isDecimalString(value)) {
    throw new ResponseValidationError(path, 'a canonical decimal string')
  }
}

function nonnegativeDecimal(value: unknown, path: string) {
  if (!isDecimalString(value) || value.startsWith('-')) {
    throw new ResponseValidationError(path, 'a canonical non-negative decimal string')
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
  ]) nonnegativeSafeInteger(item[key], `${path}.${key}`)
  nullable(item.costUsd, `${path}.costUsd`, decimal)
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
  ]) nonnegativeSafeInteger(item[key], `${path}.${key}`)
  nullable(item.costUsd, `${path}.costUsd`, decimal)
}

function sessionRow(value: unknown, path: string): asserts value is SessionRow {
  const item = object(value, path)
  string(item.id, `${path}.id`)
  string(item.rootThreadId, `${path}.rootThreadId`)
  for (const key of ['startedAt', 'lastEventAt']) timestamp(item[key], `${path}.${key}`)
  string(item.title, `${path}.title`)
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
  ]) nonnegativeSafeInteger(item[key], `${path}.${key}`)
  nullable(item.costUsd, `${path}.costUsd`, decimal)
  nullable(item.lifetimeCostUsd, `${path}.lifetimeCostUsd`, decimal)
}

function period(value: unknown, path: string) {
  const item = object(value, path)
  optional(item.label, `${path}.label`, string)
  for (const key of ['start', 'end']) optional(item[key], `${path}.${key}`, timestamp)
  nonnegativeSafeInteger(item.sessionCount, `${path}.sessionCount`)
  nonnegativeSafeInteger(item.messageCount, `${path}.messageCount`)
  optional(item.deltaPercent, `${path}.deltaPercent`, value => nullable(value, `${path}.deltaPercent`, number))
  optional(item.deltaCostUsd, `${path}.deltaCostUsd`, value => nullable(value, `${path}.deltaCostUsd`, decimal))
  totals(item.totals, `${path}.totals`)
}

function paged(item: JsonObject, path: string) {
  for (const key of ['page', 'pageSize', 'total', 'totalPages']) nonnegativeSafeInteger(item[key], `${path}.${key}`)
}

function activityItem(value: unknown, path: string): asserts value is ActivityItem {
  const item = object(value, path)
  for (const key of ['id', 'rolloutId', 'kind']) string(item[key], `${path}.${key}`)
  timestamp(item.timestamp, `${path}.timestamp`)
  for (const key of ['turnId', 'agentRunId', 'agentLabel', 'role', 'label', 'body', 'status', 'toolName', 'model', 'effort']) {
    nullable(item[key], `${path}.${key}`, string)
  }
  nullable(item.durationMs, `${path}.durationMs`, nonnegativeSafeInteger)
  boolean(item.hasDetails, `${path}.hasDetails`)
  arrayOf(item.children, `${path}.children`, activityItem)
  for (const key of ['childPage', 'childPageSize', 'childTotal']) {
    optional(item[key], `${path}.${key}`, nonnegativeSafeInteger)
  }
  optional(item.childHasMore, `${path}.childHasMore`, boolean)
  optional(item.childNextCursor, `${path}.childNextCursor`, string)
  nullable(item.usage, `${path}.usage`, totals)
  nullable(item.counts, `${path}.counts`, value => {
    const counts = object(value, `${path}.counts`)
    for (const key of ['modelCalls', 'toolCalls', 'agentRuns', 'reviews', 'followUps']) nonnegativeSafeInteger(counts[key], `${path}.counts.${key}`)
  })
}

export function statusResponse(value: unknown): StatusResponse {
  const item = object(value, 'status')
  string(item.state, 'status.state')
  for (const key of ['lastIngestAt', 'lastIngestAttemptAt', 'lastEventAt']) {
    nullable(item[key], `status.${key}`, timestamp)
  }
  nonnegativeSafeInteger(item.filesScanned, 'status.filesScanned')
  nonnegativeSafeInteger(item.filesFailed, 'status.filesFailed')
  return item as unknown as StatusResponse
}

export function settingsResponse(value: unknown): SettingsResponse {
  const item = object(value, 'settings')
  string(item.databasePath, 'settings.databasePath')
  nullable(item.activeRoot, 'settings.activeRoot', string)
  nullable(item.archiveRoot, 'settings.archiveRoot', string)
  string(item.timezone, 'settings.timezone')
  nullable(item.lastIngestAt, 'settings.lastIngestAt', timestamp)
  nonnegativeSafeInteger(item.sessionCount, 'settings.sessionCount')
  nonnegativeSafeInteger(item.databaseBytes, 'settings.databaseBytes')
  const pricing = object(item.pricing, 'settings.pricing')
  decimal(pricing.knownCostUsd, 'settings.pricing.knownCostUsd')
  nonnegativeSafeInteger(pricing.unpricedTokens, 'settings.pricing.unpricedTokens')
  boolean(pricing.complete, 'settings.pricing.complete')
  return item as unknown as SettingsResponse
}

export function overviewResponse(value: unknown): OverviewResponse {
  const item = object(value, 'overview')
  nullable(item.updatedAt, 'overview.updatedAt', timestamp)
  const periods = object(item.periods, 'overview.periods')
  for (const key of ['today', 'week', 'month']) period(periods[key], `overview.periods.${key}`)
  return item as unknown as OverviewResponse
}

export function overviewYearResponse(value: unknown): OverviewYearResponse {
  const item = object(value, 'overviewYear')
  nonnegativeSafeInteger(item.year, 'overviewYear.year')
  arrayOf(item.heatmap, 'overviewYear.heatmap', (day, path) => {
    const candidate = object(day, path)
    dateOnly(candidate.date, `${path}.date`)
    nullable(candidate.costUsd, `${path}.costUsd`, decimal)
    for (const key of ['sessionCount', 'totalTokens']) nonnegativeSafeInteger(candidate[key], `${path}.${key}`)
    optional(candidate.messageCount, `${path}.messageCount`, nonnegativeSafeInteger)
    optional(candidate.future, `${path}.future`, boolean)
  })
  arrayOf(item.topProjects, 'overviewYear.topProjects', (project, path) => {
    const candidate = object(project, path)
    string(candidate.project, `${path}.project`)
    nullable(candidate.costUsd, `${path}.costUsd`, decimal)
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
  for (const key of ['cwd', 'source', 'latestResult', 'firstPrompt']) {
    optional(session[key], `sessionSummary.session.${key}`, value => {
      nullable(value, `sessionSummary.session.${key}`, string)
    })
  }
  optional(session.completedAt, 'sessionSummary.session.completedAt', value => {
    nullable(value, 'sessionSummary.session.completedAt', timestamp)
  })
  totals(item.totals, 'sessionSummary.totals')
  arrayOf(item.models, 'sessionSummary.models', modelUsage)
  arrayOf(item.agents, 'sessionSummary.agents', (agent, path) => {
    const candidate = object(agent, path)
    string(candidate.id, `${path}.id`)
    string(candidate.label, `${path}.label`)
    nullable(candidate.path, `${path}.path`, string)
    nullable(candidate.nickname, `${path}.nickname`, string)
    string(candidate.status, `${path}.status`)
    for (const key of ['turnCount', 'toolCount', 'totalTokens', 'unpricedTokens']) nonnegativeSafeInteger(candidate[key], `${path}.${key}`)
    nullable(candidate.costUsd, `${path}.costUsd`, decimal)
  })
  arrayOf(item.toolSummary, 'sessionSummary.toolSummary', (tool, path) => {
    const candidate = object(tool, path)
    string(candidate.tool, `${path}.tool`)
    for (const key of ['count', 'failedCount', 'totalDurationMs']) nonnegativeSafeInteger(candidate[key], `${path}.${key}`)
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
    dateOnly(candidate.date, `${path}.date`)
    nonnegativeSafeInteger(candidate.durationMs, `${path}.durationMs`)
    totals(candidate.totals, `${path}.totals`)
  })
  paged(item, 'activity')
  return item as unknown as ActivityResponse
}

export function statsResponse(value: unknown): StatsResponse {
  const item = object(value, 'stats')
  string(item.range, 'stats.range')
  if (!['day', 'week', 'month', 'year', 'all'].includes(item.range as string)) {
    throw new ResponseValidationError('stats.range', 'day, week, month, year, or all')
  }
  dateOnly(item.anchor, 'stats.anchor')
  string(item.label, 'stats.label')
  totals(item.totals, 'stats.totals')
  arrayOf(item.rows, 'stats.rows', (row, path) => {
    totals(row, path)
    const candidate = object(row, path)
    timestamp(candidate.periodStart, `${path}.periodStart`)
    timestamp(candidate.periodEnd, `${path}.periodEnd`)
    string(candidate.label, `${path}.label`)
    nonnegativeSafeInteger(candidate.sessionCount, `${path}.sessionCount`)
  })
  arrayOf(item.trend, 'stats.trend', (point, path) => nullable(point, path, decimal))
  return item as unknown as StatsResponse
}

export function pricesResponse(value: unknown): PricesResponse {
  const item = object(value, 'prices')
  arrayOf(item.items, 'prices.items', (price, path) => {
    const candidate = object(price, path)
    for (const key of ['modelId', 'source']) {
      string(candidate[key], `${path}.${key}`)
    }
    for (const key of ['inputPerMillion', 'outputPerMillion']) {
      nonnegativeDecimal(candidate[key], `${path}.${key}`)
    }
    if (candidate.currency !== 'USD') {
      throw new ResponseValidationError(`${path}.currency`, 'USD')
    }
    timestamp(candidate.effectiveFrom, `${path}.effectiveFrom`)
    nullable(candidate.effectiveTo, `${path}.effectiveTo`, timestamp)
    nullable(candidate.cachedInputPerMillion, `${path}.cachedInputPerMillion`, nonnegativeDecimal)
  })
  paged(item, 'prices')
  nullable(item.lastRefreshAt, 'prices.lastRefreshAt', timestamp)
  nullable(item.lastRefreshErrorAt, 'prices.lastRefreshErrorAt', timestamp)
  nullable(item.refreshError, 'prices.refreshError', string)
  optional(item.refreshErrorKind, 'prices.refreshErrorKind', value => nullable(value, 'prices.refreshErrorKind', string))
  nullable(item.source, 'prices.source', string)
  return item as unknown as PricesResponse
}

export function aliasesResponse(value: unknown): AliasesResponse {
  const item = object(value, 'aliases')
  arrayOf(item.items, 'aliases.items', (alias, path) => {
    const candidate = object(alias, path)
    string(candidate.observedModelId, `${path}.observedModelId`)
    string(candidate.canonicalModelId, `${path}.canonicalModelId`)
  })
  paged(item, 'aliases')
  return item as unknown as AliasesResponse
}

export function priceMetadataResponse(value: unknown): PriceMetadataResponse {
  const item = object(value, 'priceMetadata')
  arrayOf(item.observedUnknown, 'priceMetadata.observedUnknown', (unknown, path) => {
    const candidate = object(unknown, path)
    string(candidate.modelId, `${path}.modelId`)
    nonnegativeSafeInteger(candidate.usageCount, `${path}.usageCount`)
    nonnegativeSafeInteger(candidate.totalTokens, `${path}.totalTokens`)
    timestamp(candidate.lastSeenAt, `${path}.lastSeenAt`)
  })
  nonnegativeSafeInteger(item.observedUnknownTotal, 'priceMetadata.observedUnknownTotal')
  return item as unknown as PriceMetadataResponse
}

export function priceModelIdsResponse(value: unknown): PriceModelIdsResponse {
  const item = object(value, 'priceModelIds')
  arrayOf(item.items, 'priceModelIds.items', string)
  return item as unknown as PriceModelIdsResponse
}
