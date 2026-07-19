export interface Totals {
  inputTokens: number
  cachedInputTokens: number
  outputTokens: number
  reasoningTokens: number
  blendedTokens: number
  totalTokens: number
  costUsd: number | null
  unpricedTokens: number
  pricingComplete: boolean
}

export interface StatusResponse {
  state: 'idle' | 'scanning' | 'error' | string
  lastIngestAt: string | null
  lastIngestAttemptAt: string | null
  lastEventAt: string | null
  filesScanned: number
  filesFailed: number
}

export interface SettingsResponse {
  databasePath: string
  activeRoot: string | null
  archiveRoot: string | null
  timezone: string
  lastIngestAt: string | null
  sessionCount: number
  databaseBytes: number
  pricing: PricingSummary
}

export interface PeriodSummary {
  label?: string
  start?: string
  end?: string
  sessionCount: number
  messageCount: number
  deltaPercent?: number | null
  deltaCostUsd?: number | null
  totals: Totals
}

export interface SessionRow {
  id: string
  rootThreadId: string
  startedAt: string
  lastEventAt: string
  title: string
  project: string
  branch: string | null
  messageCount: number
  turnCount: number
  agentCount: number
  toolCount: number
  totalTokens: number
  costUsd: number | null
  unpricedTokens: number
  lifetimeCostUsd: number | null
  lifetimeUnpricedTokens: number
}

export interface HeatmapDay {
  date: string
  costUsd: number | null
  sessionCount: number
  messageCount?: number
  totalTokens: number
  future?: boolean
}

export interface ProjectDriver { project: string; costUsd: number | null; share: number | null }
export interface PricingSummary { knownCostUsd: number; unpricedTokens: number; complete: boolean }

export interface OverviewResponse {
  updatedAt: string | null
  periods: { today: PeriodSummary; week: PeriodSummary; month: PeriodSummary }
}

export interface OverviewYearResponse {
  year: number
  heatmap: HeatmapDay[]
  topProjects: ProjectDriver[]
  topSessions: SessionRow[]
}

export type SessionSort = 'recent' | 'cost'
export interface SessionsResponse {
  items: SessionRow[]
  page: number
  pageSize: number
  total: number
  totalPages: number
  projects: string[]
}

export interface ModelUsage {
  model: string
  effort: string | null
  inputTokens: number
  cachedInputTokens: number
  outputTokens: number
  reasoningTokens: number
  totalTokens: number
  costUsd: number | null
  unpricedTokens: number
}

export interface AgentSummary {
  id: string
  label: string
  path: string | null
  nickname: string | null
  status: string
  turnCount: number
  toolCount: number
  totalTokens: number
  costUsd: number | null
  unpricedTokens: number
}

export interface ToolSummary {
  tool: string
  count: number
  failedCount: number
  totalDurationMs: number
}

export interface SessionSummary {
  session: SessionRow & {
    cwd?: string | null
    source?: string | null
    latestResult?: string | null
    firstPrompt?: string | null
    completedAt?: string | null
    status: string
  }
  totals: Totals
  models: ModelUsage[]
  agents: AgentSummary[]
  toolSummary: ToolSummary[]
}

export type ActivityKind = 'user' | 'assistant' | 'update' | 'reasoning' | 'tool' | 'tool_result' | 'subagent' | 'goal' | 'plan' | 'compaction' | 'system' | 'final' | string
export interface ActivityItem {
  id: string
  turnId: string | null
  rolloutId: string
  agentRunId: string | null
  agentLabel: string | null
  timestamp: string
  kind: ActivityKind
  role: string | null
  label: string | null
  body: string | null
  status: string | null
  toolName: string | null
  durationMs: number | null
  model: string | null
  effort: string | null
  hasDetails: boolean
  children: ActivityItem[]
  childPage?: number
  childPageSize?: number
  childTotal?: number
  childHasMore?: boolean
  usage: Totals | null
  counts: {
    modelCalls: number
    toolCalls: number
    agentRuns: number
    reviews: number
    followUps: number
  } | null
}

export interface ActivityResponse {
  items: ActivityItem[]
  days: Array<{ date: string; durationMs: number; totals: Totals }>
  page: number
  pageSize: number
  total: number
  totalPages: number
}

export interface UsageBreakdown {
  id: string
  label: string
  model: string | null
  agentRunId: string | null
  turnId: string | null
  effort: string | null
  totals: Totals
}

export interface SessionUsageResponse {
  totals: Totals
  byModel: ModelUsage[]
  byAgent: UsageBreakdown[]
  byTurn: UsageBreakdown[]
  pricing: PricingSummary
}

export type StatsRange = 'day' | 'week' | 'month' | 'year' | 'all'
export interface StatsRow extends Totals { periodStart: string; periodEnd: string; label: string; sessionCount: number }
export interface StatsResponse {
  range: StatsRange
  anchor: string
  label: string
  totals: Totals
  rows: StatsRow[]
  trend: Array<number | null>
}

export interface PriceRow {
  modelId: string
  effectiveFrom: string
  effectiveTo: string | null
  inputPerMillion: string
  cachedInputPerMillion: string | null
  outputPerMillion: string
  currency: string
  source: string
}

export interface PriceAlias { observedModelId: string; canonicalModelId: string }
export interface UnknownModel {
  modelId: string
  usageCount: number
  totalTokens: number
  lastSeenAt: string
}
export interface PricesResponse {
  items: PriceRow[]
  aliases: PriceAlias[]
  observedUnknown: UnknownModel[]
  page: number
  pageSize: number
  total: number
  totalPages: number
  lastRefreshAt: string | null
  lastRefreshErrorAt: string | null
  refreshError: string | null
  refreshErrorKind?: string | null
  source: string | null
}
