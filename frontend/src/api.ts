import type {
  ActivityResponse,
  OverviewResponse,
  OverviewYearResponse,
  PricesResponse,
  SettingsResponse,
  SessionSort,
  SessionSummary,
  SessionUsageResponse,
  SessionsResponse,
  StatsRange,
  StatsResponse,
  StatusResponse,
} from './types'
import {
  activityItemResponse,
  activityResponse,
  overviewResponse,
  overviewYearResponse,
  pricesResponse,
  sessionSummaryResponse,
  sessionUsageResponse,
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

async function request<T>(path: string, init?: RequestInit, validate?: (value: unknown) => T): Promise<T> {
  const response = await fetch(path, {
    headers: { Accept: 'application/json', 'Content-Type': 'application/json', ...init?.headers },
    ...init,
  })
  if (!response.ok) {
    const body = await response.json().catch(() => null) as { error?: string; message?: string } | null
    throw new ApiError(response.status, body?.error ?? body?.message ?? `Request failed (${response.status})`)
  }
  if (response.status === 204) return undefined as T
  const text = await response.text()
  const value: unknown = text ? JSON.parse(text) : undefined
  return validate ? validate(value) : value as T
}

function params(values: Record<string, string | number | null | undefined>) {
  const search = new URLSearchParams()
  for (const [key, value] of Object.entries(values)) {
    if (value !== null && value !== undefined && value !== '') search.set(key, String(value))
  }
  return search.toString()
}

const base = '/api/v1'

export const api = {
  status: (signal?: AbortSignal) => request<StatusResponse>(`${base}/status`, { signal }, statusResponse),
  settings: (signal?: AbortSignal) => request<SettingsResponse>(`${base}/settings`, { signal }, settingsResponse),
  overview: (signal?: AbortSignal) => request<OverviewResponse>(`${base}/overview`, { signal }, overviewResponse),
  overviewYear: (year: number, signal?: AbortSignal) => request<OverviewYearResponse>(`${base}/overview/year?${params({ year })}`, { signal }, overviewYearResponse),
  sessions: (options: { q?: string; date?: string; start?: string; end?: string; project?: string; sort?: SessionSort; page?: number; pageSize?: number }, signal?: AbortSignal) =>
    request<SessionsResponse>(`${base}/sessions?${params({ ...options, pageSize: options.pageSize ?? 50 })}`, { signal }, sessionsResponse),
  sessionSummary: (id: string, signal?: AbortSignal) => request<SessionSummary>(`${base}/sessions/${encodeURIComponent(id)}/summary`, { signal }, sessionSummaryResponse),
  sessionActivity: (id: string, page: number, signal?: AbortSignal) =>
    request<ActivityResponse>(`${base}/sessions/${encodeURIComponent(id)}/activity?${params({ page, pageSize: 25 })}`, { signal }, activityResponse),
  sessionActivityDetail: (id: string, eventId: string, signal?: AbortSignal, childPage = 1, childPageSize = 250) =>
    request<ActivityResponse['items'][number]>(`${base}/sessions/${encodeURIComponent(id)}/activity/${encodeURIComponent(eventId)}?${params({ childPage, childPageSize })}`, { signal }, activityItemResponse),
  sessionUsage: (id: string, signal?: AbortSignal) => request<SessionUsageResponse>(`${base}/sessions/${encodeURIComponent(id)}/usage`, { signal }, sessionUsageResponse),
  stats: (range: StatsRange, anchor?: string, signal?: AbortSignal) => request<StatsResponse>(`${base}/stats?${params({ range, anchor })}`, { signal }, statsResponse),
  prices: (options: { q?: string; page?: number; pageSize?: number }, signal?: AbortSignal) => request<PricesResponse>(`${base}/prices?${params({ ...options, pageSize: options.pageSize ?? 25 })}`, { signal }, pricesResponse),
  pricedModelIds: async (signal?: AbortSignal) => {
    const first = await request<PricesResponse>(`${base}/prices?${params({ page: 1, pageSize: 100 })}`, { signal }, pricesResponse)
    const remaining = await Promise.all(
      Array.from({ length: Math.max(0, first.totalPages - 1) }, (_, index) =>
        request<PricesResponse>(`${base}/prices?${params({ page: index + 2, pageSize: 100 })}`, { signal }, pricesResponse),
      ),
    )
    return [...new Set([first, ...remaining].flatMap(response => response.items.map(item => item.modelId)))].sort()
  },
  savePrice: (modelId: string, value: { effectiveFrom: string; inputPerMillion: string; cachedInputPerMillion: string | null; outputPerMillion: string; currency?: string }) =>
    request<void>(`${base}/prices/${encodeURIComponent(modelId)}`, { method: 'PUT', body: JSON.stringify(value) }),
  deletePrice: (modelId: string, effectiveFrom: string) => request<void>(`${base}/prices/${encodeURIComponent(modelId)}?${params({ effectiveFrom })}`, { method: 'DELETE' }),
  saveAlias: (observedModelId: string, canonicalModelId: string) => request<void>(`${base}/aliases/${encodeURIComponent(observedModelId)}`, { method: 'PUT', body: JSON.stringify({ canonicalModelId }) }),
  deleteAlias: (observedModelId: string) => request<void>(`${base}/aliases/${encodeURIComponent(observedModelId)}`, { method: 'DELETE' }),
  refreshPrices: () => request<{ updated?: number }>(`${base}/prices/refresh`, { method: 'POST' }),
}
