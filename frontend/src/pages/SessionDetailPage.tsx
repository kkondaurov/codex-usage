import { ArrowsInSimple, CaretDown, ChatCircleText, Robot, ShieldCheck, UserCircle, Wrench } from '@phosphor-icons/react'
import { Fragment, useCallback, useEffect, useId, useRef, useState } from 'react'
import type { ReactNode } from 'react'
import { useParams, useSearchParams } from 'react-router-dom'
import { api } from '../api'
import { CompactMarkdown } from '../components/CompactMarkdown'
import { DegradedDataNotice, ErrorState, LoadingLedger, Pagination } from '../components/Common'
import { handleTabKeyDown } from '../components/tabKeyboard'
import { RichMarkdown } from '../components/RichMarkdown'
import { UserMessageContent } from '../components/UserMessageContent'
import { addDecimal } from '../decimal'
import { duration, ellipsis, estimatedMoney, shortDate, time, tokens } from '../format'
import { useAsync } from '../hooks'
import type { ActivityItem, SessionSummary, Totals } from '../types'

const ZERO_TOTALS: Totals = { inputTokens: 0, cachedInputTokens: 0, outputTokens: 0, reasoningTokens: 0, blendedTokens: 0, totalTokens: 0, costUsd: '0', unpricedTokens: 0, pricingComplete: true }
const SESSION_TABS = ['summary', 'activity'] as const
const COMPACT_MODEL_LIMIT = 6
const COMPACT_TOOL_LIMIT = 18

function Metric({ label, children, accent = false }: { label: string; children: ReactNode; accent?: boolean }) {
  return <div className={`detail-metric ${accent ? 'accent' : ''}`}><span>{label}</span><strong>{children}</strong></div>
}

function LongTextCard({ label, text, date, tone }: { label: string; text: string; date: string; tone: 'yellow' | 'blue' }) {
  const [expanded, setExpanded] = useState(false)
  const collapsedText = ellipsis(text, 270)
  const truncated = collapsedText !== text.replace(/\s+/g, ' ').trim()
  return (
    <article className={`long-text-card ${tone} ${truncated && expanded ? 'expanded' : ''}`}>
      <header><strong>{label}</strong>{truncated && <button type="button" aria-expanded={expanded} onClick={() => setExpanded(value => !value)}>{expanded ? 'COLLAPSE' : 'EXPAND'}</button>}</header>
      <p>{truncated && expanded ? text : collapsedText}</p>
      <time>{shortDate(date).toUpperCase()} · {time(date)}</time>
    </article>
  )
}

function SummaryTab({ detail }: { detail: SessionSummary }) {
  const [allModelsVisible, setAllModelsVisible] = useState(false)
  const [allToolsVisible, setAllToolsVisible] = useState(false)
  const modelListId = useId()
  const toolListId = useId()
  const maxToolCount = Math.max(1, ...detail.toolSummary.map(item => item.count))
  const totalModelTokens = Math.max(1, detail.models.reduce((sum, item) => sum + item.totalTokens, 0))
  const visibleModels = allModelsVisible ? detail.models : detail.models.slice(0, COMPACT_MODEL_LIMIT)
  const visibleTools = allToolsVisible ? detail.toolSummary : detail.toolSummary.slice(0, COMPACT_TOOL_LIMIT)
  const firstPrompt = detail.session.firstPrompt
  const latestResult = detail.session.latestResult
  return (
    <div className="session-overview-grid">
      <div className="session-highlights">
        {firstPrompt ? <LongTextCard label="FIRST PROMPT" text={firstPrompt} date={detail.session.startedAt} tone="yellow" /> : <div className="long-text-card yellow empty-highlight"><strong>FIRST PROMPT</strong><p>No user message was stored for this session.</p></div>}
        {latestResult ? <LongTextCard label="LATEST ASSISTANT RESULT" text={latestResult} date={detail.session.lastEventAt} tone="blue" /> : <div className="long-text-card blue empty-highlight"><strong>LATEST ASSISTANT RESULT</strong><p>No final assistant result was stored.</p></div>}
      </div>
      <aside className="session-insights">
        <section className="insight-card model-card">
          <h2>MODELS &amp; REASONING</h2>
          <div className="model-list" id={modelListId}>
            <div className="model-list-head"><span>MODEL</span><span>EFFORT</span><span>COST</span><span>API TOKENS</span><span>SHARE</span></div>
            {visibleModels.map((model, index) => {
              const share = model.totalTokens / totalModelTokens
              return <div className="model-row" key={`${model.model}-${model.effort}`}>
                <span className="model-name"><i className={`model-key model-${index % 3}`} /><strong>{model.model}</strong></span>
                <span className="model-effort">{model.effort ?? '—'}</span>
                <b>{estimatedMoney(model.costUsd, model.unpricedTokens)}</b>
                <b>{tokens(model.totalTokens)}</b>
                <b>{Math.round(share * 100)}%</b>
              </div>
            })}
          </div>
          <div className="model-bar">{visibleModels.map((model, index) => <i key={`${model.model}-${index}`} className={`model-${index % 3}`} style={{ width: `${model.totalTokens / totalModelTokens * 100}%` }} />)}</div>
          {detail.models.length > COMPACT_MODEL_LIMIT && <button type="button" className="insight-disclosure" aria-expanded={allModelsVisible} aria-controls={modelListId} onClick={() => setAllModelsVisible(value => !value)}>{allModelsVisible ? `SHOWING ALL ${detail.models.length} · SHOW TOP ${COMPACT_MODEL_LIMIT}` : `SHOWING ${COMPACT_MODEL_LIMIT} OF ${detail.models.length} · SHOW ALL`}</button>}
        </section>
        <section className="insight-card tools-card">
          <h2>TOOLS USED · {detail.toolSummary.reduce((sum, tool) => sum + tool.count, 0)}</h2>
          <div className="tool-list" id={toolListId}>{visibleTools.map(tool => <div key={tool.tool}><span><strong>{tool.tool}</strong><i style={{ width: `${Math.max(4, tool.count / maxToolCount * 100)}%` }} /></span><b>{tool.count}</b></div>)}</div>
          {detail.toolSummary.length > COMPACT_TOOL_LIMIT && <button type="button" className="insight-disclosure" aria-expanded={allToolsVisible} aria-controls={toolListId} onClick={() => setAllToolsVisible(value => !value)}>{allToolsVisible ? `SHOWING ALL ${detail.toolSummary.length} · SHOW TOP ${COMPACT_TOOL_LIMIT}` : `SHOWING ${COMPACT_TOOL_LIMIT} OF ${detail.toolSummary.length} · SHOW ALL`}</button>}
        </section>
      </aside>
    </div>
  )
}

function eventLabel(item: ActivityItem) {
  if (item.label === 'turn_aborted') return 'Turn interrupted'
  if (item.label === 'thread_rolled_back') return 'Thread rolled back'
  if ((item.kind === 'work_group' || item.kind === 'agent_group' || item.kind === 'review_group') && item.label) return item.label
  if (item.kind === 'final') return 'Final answer'
  if (item.kind === 'tool' && item.toolName) return item.toolName
  if (item.agentLabel && item.label && !item.label.includes(item.agentLabel)) return `${item.agentLabel} · ${item.label}`
  if (item.agentLabel) return item.agentLabel
  if (item.label) return item.label
  if (item.toolName) return item.toolName
  const labels: Record<string, string> = { user: 'User message', assistant: 'Assistant message', final: 'Final answer', update: 'Assistant update', reasoning: 'Reasoning summary', tool: 'Tool call', tool_result: 'Tool result', subagent: 'Subagent activity', goal: 'Goal update', plan: 'Plan update', compaction: 'Context compacted', system: 'System event', work_group: 'Work', agent_group: 'Agent work', review_group: 'Automated reviews', exchange: 'Conversation' }
  return labels[item.kind] ?? item.kind.replaceAll('_', ' ')
}

function statePayload(item: ActivityItem) {
  if (item.label !== 'turn_aborted' && item.label !== 'thread_rolled_back') return null
  try { return JSON.parse(item.body ?? '') as { reason?: string; duration_ms?: number; num_turns?: number } }
  catch { return null }
}

function eventBody(item: ActivityItem) {
  const payload = statePayload(item)
  if (item.label === 'thread_rolled_back' && payload?.num_turns != null) {
    return `${payload.num_turns} ${payload.num_turns === 1 ? 'turn' : 'turns'} removed from active history`
  }
  return payload?.reason ? `Reason: ${payload.reason}` : item.body?.trim() ?? ''
}

function eventDuration(item: ActivityItem) {
  if (item.label === 'thread_rolled_back') return null
  return item.durationMs ?? statePayload(item)?.duration_ms ?? null
}

function isStructuredText(value: string) {
  const trimmed = value.trim()
  if (trimmed.startsWith('{')) return true
  if (!trimmed.startsWith('[') || !/^\[\s*(?:\{|")/.test(trimmed)) return false
  try { return typeof JSON.parse(trimmed) === 'object' }
  catch { return false }
}

function isMarkdownActivity(item: ActivityItem, value: string) {
  if (['user', 'assistant', 'update', 'reasoning', 'final', 'goal', 'plan', 'compaction'].includes(item.kind)) return true
  return item.id === item.turnId && !isStructuredText(value)
}

function turnContext(item: ActivityItem) {
  const model = item.model?.trim()
  const effort = item.effort?.trim()
  if (model && effort) return `${model} · ${effort.toUpperCase()}`
  if (model) return model
  if (effort) return `Reasoning ${effort.toUpperCase()}`
  return null
}

function exchangeStatus(item: ActivityItem) {
  const status = item.status?.trim().toLowerCase()
  if (!status || ['completed', 'complete', 'success', 'allowed'].includes(status)) return null
  return status.replaceAll('_', ' ').toUpperCase()
}

function isGroup(item: ActivityItem) {
  return item.kind === 'work_group' || item.kind === 'agent_group' || item.kind === 'review_group'
}

function isMeta(item: ActivityItem) {
  return item.kind === 'compaction'
}

function isPlumbing(item: ActivityItem) {
  if (isGroup(item)) return false
  if (item.label === 'turn_aborted' || item.label === 'thread_rolled_back') return false
  if (item.kind === 'tool' || item.kind === 'tool_result' || item.kind === 'reasoning' || item.kind === 'system' || item.kind === 'subagent') return true
  return Boolean(item.agentLabel) && !['user', 'assistant', 'update', 'final', 'goal', 'plan', 'compaction'].includes(item.kind)
}

function workSummary(items: ActivityItem[]) {
  const tools = items.filter(item => item.kind === 'tool' || item.kind === 'tool_result').length
  const reasoning = items.filter(item => item.kind === 'reasoning').length
  const agents = items.filter(item => item.kind === 'subagent' || Boolean(item.agentLabel)).length
  const reviews = items.filter(item => item.kind === 'system' && (item.agentLabel?.toLowerCase().includes('guardian') || item.model?.includes('review'))).length
  const parts = [
    tools > 0 ? `${tools} ${tools === 1 ? 'tool event' : 'tool events'}` : null,
    reasoning > 0 ? `${reasoning} reasoning` : null,
    agents > 0 ? `${agents} ${agents === 1 ? 'agent event' : 'agent events'}` : null,
    reviews > 0 ? `${reviews} ${reviews === 1 ? 'review' : 'reviews'}` : null,
  ].filter(Boolean)
  return parts.length > 0 ? parts.join(' · ') : `${items.length} events`
}

function groupWork(children: ActivityItem[]) {
  const grouped: ActivityItem[] = []
  let run: ActivityItem[] = []

  function flush() {
    if (run.length === 0) return
    const first = run[0]
    grouped.push({
      id: `work-${first.id}-${run[run.length - 1].id}`,
      turnId: first.turnId,
      rolloutId: first.rolloutId,
      agentRunId: null,
      agentLabel: null,
      timestamp: first.timestamp,
      kind: 'work_group',
      role: null,
      label: `Work · ${run.length} ${run.length === 1 ? 'event' : 'events'}`,
      body: workSummary(run),
      status: null,
      toolName: null,
      durationMs: workDuration(run),
      model: null,
      effort: null,
      hasDetails: true,
      children: run,
      usage: workUsage(run),
      counts: null,
    })
    run = []
  }

  for (const child of children) {
    if (isPlumbing(child)) run.push(child)
    else { flush(); grouped.push(child) }
  }
  flush()
  return grouped
}

function workDuration(items: ActivityItem[]) {
  const points = items
    .map(item => ({ start: Date.parse(item.timestamp), duration: eventDuration(item) }))
    .filter(point => Number.isFinite(point.start))
  if (points.length === 0) return null
  if (points.length === 1) return points[0].duration
  const start = Math.min(...points.map(point => point.start))
  const end = Math.max(...points.map(point => point.start + Math.max(0, point.duration ?? 0)))
  return Math.max(0, end - start)
}

function workUsage(items: ActivityItem[]) {
  const attributed = items.flatMap(item => item.usage ? [{ ...ZERO_TOTALS, ...item.usage }] : [])
  if (attributed.length === 0) return null
  return attributed.reduce<Totals>((sum, usage) => ({
    inputTokens: sum.inputTokens + usage.inputTokens,
    cachedInputTokens: sum.cachedInputTokens + usage.cachedInputTokens,
    outputTokens: sum.outputTokens + usage.outputTokens,
    reasoningTokens: sum.reasoningTokens + usage.reasoningTokens,
    blendedTokens: sum.blendedTokens + usage.blendedTokens,
    totalTokens: sum.totalTokens + usage.totalTokens,
    costUsd: sum.costUsd == null || usage.costUsd == null ? null : addDecimal(sum.costUsd, usage.costUsd),
    unpricedTokens: sum.unpricedTokens + usage.unpricedTokens,
    pricingComplete: sum.pricingComplete && usage.pricingComplete,
  }), { ...ZERO_TOTALS })
}

function newestFirst(children: ActivityItem[]) {
  // Array sorting is stable. For equal timestamps, retain the backend's
  // source-line-aware page order instead of inventing a lexical ID order that
  // can reshuffle children when cursor pages are merged.
  return [...children].sort((left, right) => Date.parse(right.timestamp) - Date.parse(left.timestamp))
}

function mergeActivityDetail(current: ActivityItem, next: ActivityItem) {
  const children = new Map(current.children.map(child => [child.id, child]))
  for (const child of next.children) children.set(child.id, child)
  return { ...current, ...next, children: newestFirst([...children.values()]) }
}

function mergeActivityDetailPages(pages: Map<number, ActivityItem>) {
  const ordered = [...pages.entries()].sort(([left], [right]) => left - right)
  if (ordered.length === 0) return null
  const [, firstPage] = ordered[0]
  const [lastPageNumber, lastPage] = ordered[ordered.length - 1]
  const merged = ordered.slice(1).reduce(
    (current, [, page]) => mergeActivityDetail(current, page),
    firstPage,
  )
  return {
    ...merged,
    childPage: lastPage.childPage ?? lastPageNumber,
    childPageSize: firstPage.childPageSize ?? lastPage.childPageSize,
    childTotal: firstPage.childTotal ?? lastPage.childTotal,
    childHasMore: lastPage.childHasMore,
    childNextCursor: lastPage.childNextCursor,
  }
}

function loadedPagedChildren(item: ActivityItem) {
  if (!item.childTotal) return item.children.length
  if (item.kind === 'exchange') {
    return item.children.filter(child => child.kind !== 'agent_group' && child.kind !== 'review_group').length
  }
  return item.children.length
}

function activityDay(timestamp: string) {
  const date = new Date(timestamp)
  return [date.getFullYear(), String(date.getMonth() + 1).padStart(2, '0'), String(date.getDate()).padStart(2, '0')].join('-')
}

function ActivityIcon({ item }: { item: ActivityItem }) {
  if (item.kind === 'review_group' || item.kind === 'review') return <ShieldCheck weight="bold" />
  if (item.kind === 'agent_group' || item.kind === 'subagent' || Boolean(item.agentLabel)) return <Robot weight="bold" />
  if (item.kind === 'work_group' || item.kind === 'tool' || item.kind === 'tool_result') return <Wrench weight="bold" />
  if (item.kind === 'compaction') return <ArrowsInSimple weight="bold" />
  return null
}

function SenderIcon({ sender }: { sender: 'user' | 'assistant' }) {
  return sender === 'user'
    ? <UserCircle className="event-sender-icon" weight="bold" aria-hidden="true" />
    : <ChatCircleText className="event-sender-icon" weight="bold" aria-hidden="true" />
}

function EventRow({
  sessionId,
  item,
  depth = 0,
  exchange = false,
  refreshRevision = null,
}: {
  sessionId: string
  item: ActivityItem
  depth?: number
  exchange?: boolean
  refreshRevision?: number | null
}) {
  const detailId = useId()
  const [open, setOpen] = useState(false)
  const [detailPages, setDetailPages] = useState<Map<number, ActivityItem>>(() => (
    item.children.length > 0 ? new Map([[item.childPage ?? 1, item]]) : new Map()
  ))
  const [detailLoading, setDetailLoading] = useState(false)
  const [detailError, setDetailError] = useState<string | null>(null)
  const [failedDetailPages, setFailedDetailPages] = useState<number[]>([])
  const detailRequestSequence = useRef(0)
  const detailRequestController = useRef<AbortController | null>(null)
  const detailPagesRef = useRef(detailPages)
  detailPagesRef.current = detailPages
  const detail = mergeActivityDetailPages(detailPages)

  const isTool = item.kind === 'tool' || item.kind === 'tool_result'
  const preview = isTool ? '' : eventBody(item)
  const hasExchangeWork = exchange && Boolean(item.counts && (item.counts.modelCalls > 0 || item.counts.toolCalls > 0 || item.counts.agentRuns > 0 || item.counts.reviews > 0))
  const hasDetails = !isTool && (item.hasDetails || hasExchangeWork || preview.length > 160 || item.children.length > 0)
  const displayed = open && detail ? detail : item
  const usage = displayed.usage ? { ...ZERO_TOTALS, ...displayed.usage } : null
  const isReview = item.kind === 'review' || item.kind === 'review_group'
  const isAgent = !isReview && (item.kind === 'subagent' || item.kind === 'agent_group' || Boolean(item.agentLabel))
  const isCommunication = ['user', 'assistant', 'update', 'final'].includes(item.kind)
  const sender = item.kind === 'user' ? 'user' : 'assistant'
  const grouped = isGroup(item)
  const meta = isMeta(item)
  const modelContext = !exchange && (item.kind === 'subagent' || (open && item.id === item.turnId)) ? turnContext(detail ?? item) : null
  const status = exchange ? exchangeStatus(displayed) : null

  const loadDetail = useCallback(async ({
    page = 1,
    pages,
    pageSize = 250,
    append = false,
    replace = !append,
    quiet = false,
  }: { page?: number; pages?: number[]; pageSize?: number; append?: boolean; replace?: boolean; quiet?: boolean } = {}) => {
    const requestedPages = [...new Set(pages ?? [page])].sort((left, right) => left - right)
    const sequence = ++detailRequestSequence.current
    detailRequestController.current?.abort()
    const controller = new AbortController()
    detailRequestController.current = controller
    setDetailLoading(true)
    if (!quiet) setDetailError(null)
    try {
      // Polling may need to revalidate several explicitly loaded pages. Keep
      // those reads serialized so one long-open exchange cannot fan out an
      // unbounded burst of concurrent SQLite work every refresh interval.
      const responses: Array<{ requestedPage: number; detail: ActivityItem }> = []
      let nextCursor = requestedPages[0] > 1
        ? detailPagesRef.current.get(requestedPages[0] - 1)?.childNextCursor
        : undefined
      for (const requestedPage of requestedPages) {
        const detail = nextCursor == null
          ? await api.sessionActivityDetail(sessionId, item.id, controller.signal, requestedPage, pageSize)
          : await api.sessionActivityDetail(sessionId, item.id, controller.signal, requestedPage, pageSize, nextCursor)
        responses.push({
          requestedPage,
          detail,
        })
        nextCursor = detail.childNextCursor
      }
      if (sequence !== detailRequestSequence.current) return
      setDetailPages(current => {
        const next = replace ? new Map<number, ActivityItem>() : new Map(current)
        for (const response of responses) {
          next.set(response.detail.childPage ?? response.requestedPage, response.detail)
        }
        return next
      })
      setDetailError(null)
      setFailedDetailPages([])
    } catch (error) {
      if (sequence !== detailRequestSequence.current || (error instanceof Error && error.name === 'AbortError')) return
      setDetailError(error instanceof Error ? error.message : 'Could not load activity details')
      setFailedDetailPages(requestedPages)
    } finally {
      if (sequence === detailRequestSequence.current) {
        detailRequestController.current = null
        setDetailLoading(false)
      }
    }
  }, [item.id, sessionId])

  useEffect(() => () => {
    detailRequestSequence.current += 1
    detailRequestController.current?.abort()
    detailRequestController.current = null
  }, [item.id, sessionId])

  const itemRevision = JSON.stringify([
    item.timestamp,
    item.status,
    item.body,
    item.durationMs,
    item.usage,
    item.counts,
    item.hasDetails,
    item.children,
    item.childPage,
    item.childPageSize,
    item.childTotal,
    item.childHasMore,
    item.childNextCursor,
  ])

  useEffect(() => {
    const loadedPages = [...detailPagesRef.current.keys()].sort((left, right) => left - right)
    const pageSize = detailPagesRef.current.get(loadedPages[0])?.childPageSize ?? 250
    if (item.children.length > 0) {
      detailRequestSequence.current += 1
      detailRequestController.current?.abort()
      detailRequestController.current = null
      setDetailLoading(false)
      setDetailError(null)
      setFailedDetailPages([])
      setDetailPages(current => {
        const next = new Map(current)
        next.set(item.childPage ?? 1, item)
        return next
      })
      const remainingPages = loadedPages.filter(page => page !== (item.childPage ?? 1))
      if (open && remainingPages.length > 0) {
        void loadDetail({ pages: remainingPages, pageSize, replace: false, quiet: true })
      }
      return
    }
    if (open && hasDetails) {
      void loadDetail({
        pages: loadedPages.length > 0 ? loadedPages : [1],
        pageSize,
        replace: true,
        quiet: loadedPages.length > 0,
      })
      return
    }
    setDetailPages(current => {
      if (current.size === 0) return current
      const next = new Map(current)
      const firstPageNumber = Math.min(...next.keys())
      const firstPage = next.get(firstPageNumber)
      if (!firstPage || firstPage.id !== item.id) return new Map()
      next.set(firstPageNumber, { ...firstPage, ...item, children: firstPage.children })
      return next
    })
    // itemRevision reconciles parent-visible changes, while refreshRevision
    // also revalidates an expanded event after every successful parent poll.
    // Child-only changes are otherwise invisible when the parent row itself
    // remains byte-for-byte identical.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [itemRevision, refreshRevision, open, hasDetails, loadDetail])

  function toggle() {
    if (!hasDetails) return
    if (open) { setOpen(false); return }
    setOpen(true)
  }

  const body = detail ? eventBody(detail) : ''
  const durationMs = eventDuration(item)
  const children = detail
    ? (isGroup(detail) ? newestFirst(detail.children) : groupWork(newestFirst(detail.children)))
    : []
  const canLoadMore = Boolean(detail?.childHasMore)
  const loadedChildren = detail ? loadedPagedChildren(detail) : 0
  const communicationText = preview || item.label || eventLabel(item)
  const parentDay = activityDay(item.timestamp)
  const showChildDates = children.some(child => activityDay(child.timestamp) !== parentDay)
  const copyContent = exchange ? <>
    <span className="event-copy-title exchange-user-line from-user">
      {item.label && <span className="exchange-request"><CompactMarkdown maxLength={150}>{item.label}</CompactMarkdown></span>}
      {status && <span className={`event-status ${status === 'RUNNING' ? 'running' : 'attention'}`}>{status}</span>}
    </span>
    {preview && <small className="exchange-assistant-line from-assistant">{isMarkdownActivity(item, preview) ? <CompactMarkdown maxLength={190}>{preview}</CompactMarkdown> : ellipsis(preview, 190)}</small>}
  </> : isCommunication ? (
    <span className="communication-line"><SenderIcon sender={sender} /><CompactMarkdown className="event-message" maxLength={280}>{communicationText}</CompactMarkdown></span>
  ) : <>
    <span className="event-copy-title"><ActivityIcon item={item} /><strong>{eventLabel(item)}</strong></span>
    {modelContext && <span className="event-context"><span className="event-model" title={modelContext}>{modelContext}</span></span>}
    {preview && <small>{isMarkdownActivity(item, preview) ? <CompactMarkdown maxLength={170}>{preview}</CompactMarkdown> : ellipsis(preview, 170)}</small>}
  </>
  const rowClasses = `activity-event ${exchange ? 'exchange' : ''} ${isCommunication ? `communication from-${sender}` : ''} ${grouped ? 'group' : ''} ${meta ? 'meta' : ''} ${isTool ? 'tool' : ''} ${isAgent ? 'agent' : ''} ${isReview ? 'review' : ''} ${hasDetails ? 'expandable' : ''} ${open ? 'expanded' : ''}`
  const nested = depth > 0
  return (
    <div className={`activity-event-wrap kind-${item.kind}`} role={nested ? 'listitem' : 'presentation'} style={{ '--event-depth': depth } as React.CSSProperties}>
      <div className={rowClasses} role={nested ? undefined : 'row'} data-activity-depth={depth + 1}>
        <time role={nested ? undefined : 'cell'}>{time(item.timestamp)}</time>
        <span className="event-copy" role={nested ? undefined : 'cell'}>
          {hasDetails
            ? <button type="button" className="activity-event-trigger" aria-label={isCommunication ? `${eventLabel(item)}: ${ellipsis(communicationText, 180)}` : `Toggle ${eventLabel(item)} details`} aria-expanded={open} aria-controls={detailId} onClick={toggle}>{copyContent}<CaretDown className={open ? 'rotated' : ''} weight="bold" aria-hidden="true" /></button>
            : <span className="activity-event-static">{copyContent}</span>}
        </span>
        <span className="event-duration" role={nested ? undefined : 'cell'}>{durationMs != null ? duration(durationMs) : '—'}</span>
        <b className="event-cost" role={nested ? undefined : 'cell'}>{usage ? estimatedMoney(usage.costUsd ?? null, usage.unpricedTokens ?? 0) : '—'}</b>
        <b className="event-tokens" role={nested ? undefined : 'cell'}>{usage ? tokens(usage.totalTokens ?? 0) : '—'}</b>
        <span className="event-details-cell" role={nested ? undefined : 'cell'} aria-hidden="true" />
      </div>
      {open && (
        <div className="activity-detail-row" role={nested ? 'presentation' : 'row'}>
          <div className="activity-detail-cell" role={nested ? 'presentation' : 'cell'} aria-colspan={nested ? undefined : 6}>
            <div id={detailId} className="activity-event-details" role="region" aria-label={`${eventLabel(item)} details`}>
          {detailLoading && !detail && <div className="activity-detail-state">Loading details…</div>}
          {detailError && <div className="activity-detail-state failed" role="alert" aria-atomic="true"><span>{detailError}</span><button type="button" onClick={() => void loadDetail({
            pages: failedDetailPages.length > 0 ? failedDetailPages : [detail?.childHasMore ? (detail.childPage ?? 1) + 1 : 1],
            pageSize: detail?.childPageSize ?? 250,
            replace: detailPages.size === 0,
          })}>RETRY</button></div>}
          {detail && <>
            {!exchange && detail.kind === 'user' && body && <UserMessageContent raw={body} fallback={preview} />}
            {body && !exchange && detail.kind !== 'user' && !isGroup(detail) && (isMarkdownActivity(detail, body)
              ? <div className="activity-rich-text">{isCommunication && sender === 'assistant' ? <RichMarkdown>{body}</RichMarkdown> : <CompactMarkdown links="anchor">{body}</CompactMarkdown>}</div>
              : <pre>{body}</pre>)}
            {children.length > 0 && <div className="activity-child-list" role="list" aria-label={`${eventLabel(item)} events`}>{children.map((child, index) => {
              const day = activityDay(child.timestamp)
              const previousDay = index > 0 ? activityDay(children[index - 1].timestamp) : parentDay
              return <Fragment key={child.id}>{showChildDates && day !== previousDay && <div className="activity-child-date" role="listitem"><time dateTime={day}>{shortDate(child.timestamp).toUpperCase()}</time></div>}<EventRow sessionId={sessionId} item={child} depth={depth + 1} refreshRevision={refreshRevision} /></Fragment>
            })}</div>}
            {canLoadMore && <div className="activity-detail-pagination" role="status" aria-live="polite"><button type="button" disabled={detailLoading} onClick={() => void loadDetail({
              page: (detail.childPage ?? 1) + 1,
              pageSize: detail.childPageSize ?? 250,
              append: true,
            })}>{detailLoading ? 'LOADING…' : `LOAD MORE · ${loadedChildren} / ${detail.childTotal ?? loadedChildren}`}</button></div>}
          </>}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

function ActivityTab({ sessionId }: { sessionId: string }) {
  const [params, setParams] = useSearchParams()
  const rawPage = Number(params.get('page') ?? 1)
  const validRequestedPage = Number.isSafeInteger(rawPage) && rawPage > 0
  const page = validRequestedPage ? rawPage : 1
  const paginationRef = useRef<{ page: number; totalPages: number; total: number; pageSize: number } | null>(null)
  const { data, error, loading, lastSuccessfulAt, refresh } = useAsync(signal => api.sessionActivity(sessionId, page, signal), [sessionId, page], 30_000)
  if (data) {
    paginationRef.current = data.total > 0
      ? { page: data.page, totalPages: Math.max(1, data.totalPages), total: data.total, pageSize: data.pageSize }
      : null
  }
  const pagination = data?.total ? {
    page: data.page,
    totalPages: Math.max(1, data.totalPages),
    total: data.total,
    pageSize: data.pageSize,
  } : paginationRef.current
  const paginationUnavailable = loading || (!data && Boolean(error))
  useEffect(() => {
    if (!data) return
    const canonicalPage = Math.max(1, data.page)
    if (validRequestedPage && canonicalPage === page) return
    const next = new URLSearchParams(params)
    next.set('tab', 'activity')
    if (canonicalPage === 1) next.delete('page'); else next.set('page', String(canonicalPage))
    setParams(next, { replace: true })
  }, [data, page, params, setParams, validRequestedPage])
  return (
    <section className="page-ledger-frame activity-ledger" aria-busy={loading || undefined}>
      {error && data && <DegradedDataNotice error={error} lastSuccessfulAt={lastSuccessfulAt} onRetry={() => void refresh()} />}
      <div className="ledger-scroll activity-scroll" role="region" aria-label="Scrollable session activity ledger" tabIndex={0}>
        <div className="activity-table" role="table" aria-label="Session activity">
          <div role="rowgroup"><div className="activity-head" role="row"><span role="columnheader">TIME</span><span role="columnheader">ACTIVITY</span><span role="columnheader">DURATION</span><span role="columnheader">COST</span><span role="columnheader">API TOKENS</span><span role="columnheader" aria-label="Details" /></div></div>
          <div role="rowgroup">
            {loading && !data ? <div className="table-state-row" role="row"><div role="cell" aria-colspan={6}><LoadingLedger rows={10} /></div></div> : null}
            {error && !data ? <div className="table-state-row" role="row"><div role="cell" aria-colspan={6}><ErrorState error={error} onRetry={() => void refresh()} /></div></div> : null}
            {!data ? null : data.items.length === 0 ? <div className="no-results" role="row"><span role="cell"><strong>NO ACTIVITY STORED</strong><span>This session has no displayable events.</span></span></div> : data.items.map((item, index) => {
              const day = activityDay(item.timestamp)
              const previousDate = index > 0 ? new Date(data.items[index - 1].timestamp) : null
              const previousDay = previousDate ? activityDay(previousDate.toISOString()) : null
              const daySummary = data.days.find(summary => summary.date === day)
              return <Fragment key={item.id}>{day !== previousDay && <div className="activity-date-divider" role="row"><strong role="cell">{shortDate(item.timestamp).toUpperCase()}</strong><span role="cell" /><b role="cell">{daySummary ? duration(daySummary.durationMs) : '—'}</b><b role="cell">{daySummary ? estimatedMoney(daySummary.totals.costUsd, daySummary.totals.unpricedTokens) : '—'}</b><b role="cell">{daySummary ? tokens(daySummary.totals.totalTokens) : '—'}</b><span role="cell" /></div>}<EventRow sessionId={sessionId} item={item} exchange={item.kind === 'exchange'} refreshRevision={lastSuccessfulAt} /></Fragment>
            })}
          </div>
        </div>
      </div>
      {pagination && <Pagination {...pagination} busy={paginationUnavailable} onPage={value => { const next = new URLSearchParams(params); next.set('tab', 'activity'); next.set('page', String(value)); setParams(next) }} />}
    </section>
  )
}

function daysBetween(start: string, end: string) {
  const startDate = new Date(start)
  const endDate = new Date(end)
  const startDay = Date.UTC(startDate.getFullYear(), startDate.getMonth(), startDate.getDate())
  const endDay = Date.UTC(endDate.getFullYear(), endDate.getMonth(), endDate.getDate())
  return Math.max(1, Math.floor((endDay - startDay) / 86400000) + 1)
}

export function SessionDetailPage() {
  const { sessionId = '' } = useParams()
  const [params, setParams] = useSearchParams()
  const requestedTab = params.get('tab')
  const tab = requestedTab === 'activity' ? requestedTab : 'summary'
  const { data, error, loading, lastSuccessfulAt, refresh } = useAsync(signal => api.sessionSummary(sessionId, signal), [sessionId], 30_000)
  const session = data?.session
  const totals = data?.totals
  useEffect(() => {
    if (session) document.title = `${session.title || 'Untitled session'} · Codex usage`
  }, [session])

  if (error && !data) return <ErrorState error={error} onRetry={() => void refresh()} />
  if (loading && !data) return <LoadingLedger rows={12} />
  if (!data || !session || !totals) return null
  const dayCount = daysBetween(session.startedAt, session.lastEventAt)

  function selectTab(nextTab: 'summary' | 'activity') {
    if (nextTab === 'summary') setParams({})
    else setParams({ tab: nextTab })
  }

  return (
    <div className="session-detail-page">
      {error && <DegradedDataNotice error={error} lastSuccessfulAt={lastSuccessfulAt} onRetry={() => void refresh()} />}
      <section className="session-identity">
        <div className="session-id-line"><a className="session-id" href={`codex://threads/${encodeURIComponent(session.id)}`} aria-label={`Open session ${session.id} in Codex`} title="Open in Codex">{session.id}</a><span className={`session-outcome ${session.status}`}>{session.status.replaceAll('_', ' ').toUpperCase()}</span></div>
        <h1>{session.title || 'Untitled session'}</h1>
        <div className="identity-meta"><span><small>{dayCount} {dayCount === 1 ? 'DAY' : 'DAYS'}</small>{shortDate(session.startedAt)}–{shortDate(session.lastEventAt)}</span><span><small>PROJECT</small>{session.project || '—'}</span><span><small>BRANCH</small>{session.branch || '—'}</span><span><small>CWD</small>{session.cwd || '—'}</span></div>
      </section>
      <section className="detail-metrics">
        <Metric label="ESTIMATED COST" accent>{estimatedMoney(totals.costUsd, totals.unpricedTokens)}</Metric>
        <Metric label="MESSAGES">{session.messageCount.toLocaleString()}</Metric>
        <Metric label="API TOKENS">{tokens(totals.totalTokens)}</Metric>
        <Metric label="INPUT">{tokens(totals.inputTokens)}</Metric>
        <Metric label="CACHED INPUT">{tokens(totals.cachedInputTokens)}</Metric>
        <Metric label="OUTPUT">{tokens(totals.outputTokens)}</Metric>
      </section>
      <nav className="section-tabs" aria-label="Session sections" role="tablist">
        {SESSION_TABS.map(value => <button type="button" role="tab" aria-selected={tab === value} aria-controls="session-tab-panel" tabIndex={tab === value ? 0 : -1} className={tab === value ? 'active' : ''} onKeyDown={event => handleTabKeyDown(event, SESSION_TABS, tab, selectTab)} onClick={() => selectTab(value)} key={value}>{value.toUpperCase()}</button>)}
      </nav>
      <div id="session-tab-panel" role="tabpanel" aria-label={`${tab} session details`}>
        {tab === 'summary' ? <SummaryTab detail={data} /> : <ActivityTab sessionId={sessionId} />}
      </div>
    </div>
  )
}
