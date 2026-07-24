import type {
  ActivityResponse,
  AliasesResponse,
  OverviewResponse,
  OverviewYearResponse,
  PricesResponse,
  PriceMetadataResponse,
  PriceModelIdsResponse,
  SettingsResponse,
  SessionSort,
  SessionSummary,
  SessionsResponse,
  StatsRange,
  StatsResponse,
  StatusResponse,
} from './types'
import {
  activityItemResponse,
  activityResponse,
  aliasesResponse,
  overviewResponse,
  overviewYearResponse,
  pricesResponse,
  priceMetadataResponse,
  priceModelIdsResponse,
  sessionSummaryResponse,
  sessionsResponse,
  settingsResponse,
  statsResponse,
  statusResponse,
} from './apiValidation'

export class ApiError extends Error {
  status: number

  constructor(status: number, message: string) {
    super(message)
    this.name = 'ApiError'
    this.status = status
  }
}

async function checkedResponse(path: string, init?: RequestInit) {
  const response = await fetch(path, {
    headers: { Accept: 'application/json', 'Content-Type': 'application/json', ...init?.headers },
    ...init,
  })
  if (!response.ok) {
    const body = await response.json().catch(() => null) as { error?: string; message?: string } | null
    throw new ApiError(response.status, body?.error ?? body?.message ?? `Request failed (${response.status})`)
  }
  return response
}

async function requestJson<T>(path: string, init?: RequestInit, validate?: (value: unknown) => T): Promise<T> {
  const response = await checkedResponse(path, init)
  if (response.status === 204) {
    throw new Error(`Invalid API response from ${path}: expected JSON, received 204 No Content`)
  }
  const text = await response.text()
  if (!text) throw new Error(`Invalid API response from ${path}: expected a JSON body`)
  const value: unknown = JSON.parse(text)
  return validate ? validate(value) : value as T
}

async function requestNoContent(path: string, init?: RequestInit): Promise<void> {
  await checkedResponse(path, init)
}

function params(values: Record<string, string | number | null | undefined>) {
  const search = new URLSearchParams()
  for (const [key, value] of Object.entries(values)) {
    if (value !== null && value !== undefined && value !== '') search.set(key, String(value))
  }
  return search.toString()
}

function queryPath(path: string, values: Record<string, string | number | null | undefined>) {
  const query = params(values)
  return query ? `${path}?${query}` : path
}

const base = '/api/v1'

export const api = {
  status: (signal?: AbortSignal) => requestJson<StatusResponse>(`${base}/status`, { signal }, statusResponse),
  settings: (signal?: AbortSignal) => requestJson<SettingsResponse>(`${base}/settings`, { signal }, settingsResponse),
  overview: (signal?: AbortSignal) => requestJson<OverviewResponse>(`${base}/overview`, { signal }, overviewResponse),
  overviewYear: (year: number, signal?: AbortSignal) => requestJson<OverviewYearResponse>(`${base}/overview/year?${params({ year })}`, { signal }, overviewYearResponse),
  sessions: (options: { q?: string; date?: string; start?: string; end?: string; project?: string; sort?: SessionSort; page?: number; pageSize?: number }, signal?: AbortSignal) =>
    requestJson<SessionsResponse>(`${base}/sessions?${params({ ...options, pageSize: options.pageSize ?? 50 })}`, { signal }, sessionsResponse),
  sessionSummary: (id: string, signal?: AbortSignal) => requestJson<SessionSummary>(`${base}/sessions/${encodeURIComponent(id)}/summary`, { signal }, sessionSummaryResponse),
  sessionActivity: (id: string, page: number, signal?: AbortSignal) =>
    requestJson<ActivityResponse>(`${base}/sessions/${encodeURIComponent(id)}/activity?${params({ page, pageSize: 25 })}`, { signal }, activityResponse),
  sessionActivityDetail: (id: string, eventId: string, signal?: AbortSignal, childPage = 1, childPageSize = 250, childCursor?: string) =>
    requestJson<ActivityResponse['items'][number]>(`${base}/sessions/${encodeURIComponent(id)}/activity/${encodeURIComponent(eventId)}?${params({ childPage, childPageSize, childCursor })}`, { signal }, activityItemResponse),
  stats: (range: StatsRange, anchor?: string, signal?: AbortSignal) => requestJson<StatsResponse>(`${base}/stats?${params({ range, anchor })}`, { signal }, statsResponse),
  prices: (options: { q?: string; page?: number; pageSize?: number }, signal?: AbortSignal) => requestJson<PricesResponse>(`${base}/prices?${params({ ...options, pageSize: options.pageSize ?? 25 })}`, { signal }, pricesResponse),
  aliases: (options: { q?: string; page?: number; pageSize?: number }, signal?: AbortSignal) => requestJson<AliasesResponse>(queryPath(`${base}/aliases`, { ...options, pageSize: options.pageSize ?? 25 }), { signal }, aliasesResponse),
  priceMetadata: (signal?: AbortSignal, unknownLimit?: number) => requestJson<PriceMetadataResponse>(queryPath(`${base}/prices/metadata`, { unknownLimit }), { signal }, priceMetadataResponse),
  pricedModelIds: async (options: { q?: string; limit?: number } = {}, signal?: AbortSignal) => (
    await requestJson<PriceModelIdsResponse>(queryPath(`${base}/prices/model-ids`, options), { signal }, priceModelIdsResponse)
  ).items,
  savePrice: (modelId: string, value: { effectiveFrom: string; inputPerMillion: string; cachedInputPerMillion: string | null; outputPerMillion: string; currency?: string }) =>
    requestNoContent(`${base}/prices/${encodeURIComponent(modelId)}`, { method: 'PUT', body: JSON.stringify(value) }),
  deletePrice: (modelId: string, effectiveFrom: string) => requestNoContent(`${base}/prices/${encodeURIComponent(modelId)}?${params({ effectiveFrom })}`, { method: 'DELETE' }),
  saveAlias: (observedModelId: string, canonicalModelId: string) => requestNoContent(`${base}/aliases/${encodeURIComponent(observedModelId)}`, { method: 'PUT', body: JSON.stringify({ canonicalModelId }) }),
  deleteAlias: (observedModelId: string) => requestNoContent(`${base}/aliases/${encodeURIComponent(observedModelId)}`, { method: 'DELETE' }),
  refreshPrices: () => requestJson<{ updated?: number }>(`${base}/prices/refresh`, { method: 'POST' }),
}
