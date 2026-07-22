import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { MemoryRouter, Route, Routes, useLocation } from 'react-router-dom'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { api } from '../api'
import type { ActivityItem, SessionSummary, Totals } from '../types'
import { SessionDetailPage } from './SessionDetailPage'

const totals: Totals = {
  inputTokens: 10,
  cachedInputTokens: 0,
  outputTokens: 2,
  reasoningTokens: 1,
  blendedTokens: 12,
  totalTokens: 12,
  costUsd: '0.01',
  unpricedTokens: 0,
  pricingComplete: true,
}

function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (reason: Error) => void
  const promise = new Promise<T>((nextResolve, nextReject) => { resolve = nextResolve; reject = nextReject })
  return { promise, resolve, reject }
}

function LocationProbe() {
  const location = useLocation()
  return <output aria-label="Current location">{location.pathname}{location.search}</output>
}

const root: ActivityItem = {
  id: 'turn-1',
  turnId: 'turn-1',
  rolloutId: 'rollout-1',
  agentRunId: 'agent-1',
  agentLabel: null,
  timestamp: '2026-07-15T20:00:00Z',
  kind: 'exchange',
  role: 'user',
  label: 'Please fix the Activity hierarchy',
  body: 'The hierarchy is fixed and verified.',
  status: 'completed',
  toolName: null,
  durationMs: 100,
  model: 'gpt-5.6-sol',
  effort: 'high',
  hasDetails: true,
  children: [],
  usage: totals,
  counts: { modelCalls: 7, toolCalls: 4, agentRuns: 2, reviews: 3, followUps: 1 },
}

const child: ActivityItem = {
  ...root,
  id: 'event-1',
  agentLabel: null,
  kind: 'update',
  label: 'Assistant update',
  body: 'Child preview',
  hasDetails: false,
  usage: null,
  counts: null,
}

const tool: ActivityItem = {
  ...child,
  id: 'tool-1',
  kind: 'tool',
  label: 'exec',
  toolName: 'exec',
  body: 'cargo test',
  durationMs: 390,
  usage: { ...totals, totalTokens: 20, costUsd: '0.02' },
}

const reviewGroup: ActivityItem = {
  ...child,
  id: 'reviews-1',
  kind: 'review_group',
  label: 'Automated reviews · 2',
  body: '2 checks',
  hasDetails: true,
  children: [{ ...child, id: 'guardian-1', kind: 'review', agentLabel: 'guardian', label: 'guardian', body: '{"outcome":"allow"}', model: 'codex-auto-review', effort: 'low', usage: { ...totals, totalTokens: 3, costUsd: '0.03' } }],
  usage: { ...totals, totalTokens: 3, costUsd: '0.03' },
}

const agentGroup: ActivityItem = {
  ...child,
  id: 'agents-1',
  kind: 'agent_group',
  agentLabel: 'Kant',
  label: 'Kant · completed',
  body: '1 agent branch',
  hasDetails: true,
  children: [{ ...child, id: 'agent-update-1', kind: 'subagent', agentLabel: 'Kant', body: 'Agent found the rendering issue.', model: 'gpt-5.6-sol', effort: 'high', usage: { ...totals, totalTokens: 4, costUsd: '0.04' } }],
  usage: { ...totals, totalTokens: 4, costUsd: '0.04' },
}

const interrupted: ActivityItem = {
  ...child,
  id: 'event-2',
  kind: 'system',
  label: 'turn_aborted',
  body: '{"reason":"interrupted","duration_ms":3275}',
  status: 'interrupted',
}

const rolledBack: ActivityItem = {
  ...child,
  id: 'event-3',
  kind: 'system',
  label: 'thread_rolled_back',
  body: '{"type":"thread_rolled_back","num_turns":1}',
  status: 'rolled_back',
}

const summary: SessionSummary = {
  session: {
    id: 'session-1',
    rootThreadId: 'session-1',
    startedAt: '2026-07-15T20:00:00Z',
    lastEventAt: '2026-07-15T20:01:00Z',
    title: 'Lazy activity session',
    project: 'codex-usage',
    branch: 'main',
    messageCount: 1,
    turnCount: 1,
    agentCount: 1,
    toolCount: 0,
    totalTokens: 12,
    costUsd: '0.01',
    unpricedTokens: 0,
    lifetimeCostUsd: '0.01',
    lifetimeUnpricedTokens: 0,
    status: 'completed',
  },
  totals,
  models: [],
  agents: [],
  toolSummary: [],
}

afterEach(() => vi.restoreAllMocks())

describe('SessionDetailPage activity', () => {
  it('keeps agent details out of the summary sidebar', async () => {
    vi.spyOn(api, 'sessionSummary').mockResolvedValue({
      ...summary,
      agents: [{
        id: 'agent-1',
        label: 'Russell',
        path: null,
        nickname: 'Russell',
        status: 'completed',
        turnCount: 1,
        toolCount: 2,
        totalTokens: 12,
        costUsd: '0.01',
        unpricedTokens: 0,
      }],
    })

    render(
      <MemoryRouter initialEntries={['/sessions/session-1']}>
        <Routes>
          <Route path="/sessions/:sessionId" element={<SessionDetailPage />} />
        </Routes>
      </MemoryRouter>,
    )

    expect(await screen.findByText('Lazy activity session')).toBeVisible()
    expect(screen.getByRole('link', { name: 'Open session session-1 in Codex' })).toHaveAttribute('href', 'codex://threads/session-1')
    expect(screen.queryByRole('heading', { name: 'AGENTS · 1' })).not.toBeInTheDocument()
    expect(screen.queryByText('Russell')).not.toBeInTheDocument()
  })

  it('keeps useful model usage in Summary and removes the raw Usage tab', async () => {
    vi.spyOn(api, 'sessionSummary').mockResolvedValue({
      ...summary,
      models: [{
        model: 'gpt-5.6-sol',
        effort: 'ultra',
        inputTokens: 10,
        cachedInputTokens: 0,
        outputTokens: 2,
        reasoningTokens: 1,
        totalTokens: 12,
        costUsd: '0.01',
        unpricedTokens: 0,
      }],
    })

    const { container } = render(
      <MemoryRouter initialEntries={['/sessions/session-1?tab=usage']}>
        <Routes>
          <Route path="/sessions/:sessionId" element={<SessionDetailPage />} />
        </Routes>
      </MemoryRouter>,
    )

    expect(await screen.findByRole('heading', { name: 'MODELS & REASONING' })).toBeVisible()
    expect(screen.getByRole('tab', { name: 'SUMMARY' })).toHaveAttribute('aria-selected', 'true')
    expect(screen.queryByRole('tab', { name: 'USAGE' })).not.toBeInTheDocument()
    const modelCard = container.querySelector('.model-card') as HTMLElement
    expect(within(modelCard).getByText('COST')).toBeVisible()
    expect(within(modelCard).getByText('API TOKENS')).toBeVisible()
    expect(within(modelCard).getByText('$0.01')).toBeVisible()
    expect(within(modelCard).getByText('12')).toBeVisible()
    expect(within(modelCard).getByText('100%')).toBeVisible()
  })

  it('discloses compact summary limits and expands every model and tool category on request', async () => {
    vi.spyOn(api, 'sessionSummary').mockResolvedValue({
      ...summary,
      models: Array.from({ length: 8 }, (_, index) => ({
        model: `model-${index}`,
        effort: null,
        inputTokens: 10,
        cachedInputTokens: 0,
        outputTokens: 2,
        reasoningTokens: 1,
        totalTokens: 12,
        costUsd: '0.01' as const,
        unpricedTokens: 0,
      })),
      toolSummary: Array.from({ length: 20 }, (_, index) => ({
        tool: `tool-${index}`,
        count: 20 - index,
        failedCount: 0,
        totalDurationMs: index,
      })),
    })

    render(
      <MemoryRouter initialEntries={['/sessions/session-1']}>
        <Routes>
          <Route path="/sessions/:sessionId" element={<SessionDetailPage />} />
        </Routes>
      </MemoryRouter>,
    )

    expect(await screen.findByText('model-5')).toBeVisible()
    expect(screen.queryByText('model-6')).not.toBeInTheDocument()
    expect(screen.getByText('tool-17')).toBeVisible()
    expect(screen.queryByText('tool-18')).not.toBeInTheDocument()

    const models = screen.getByRole('button', { name: 'SHOWING 6 OF 8 · SHOW ALL' })
    const tools = screen.getByRole('button', { name: 'SHOWING 18 OF 20 · SHOW ALL' })
    expect(models).toHaveAttribute('aria-expanded', 'false')
    expect(tools).toHaveAttribute('aria-expanded', 'false')

    fireEvent.click(models)
    fireEvent.click(tools)
    expect(screen.getByText('model-7')).toBeVisible()
    expect(screen.getByText('tool-19')).toBeVisible()
    expect(models).toHaveAttribute('aria-expanded', 'true')
    expect(tools).toHaveAttribute('aria-expanded', 'true')
  })

  it('leads with the user request and progressively discloses the complete exchange hierarchy', async () => {
    vi.spyOn(api, 'sessionSummary').mockResolvedValue(summary)
    const markdownRoot = {
      ...root,
      status: 'running',
      label: 'Please **fix** the [Activity hierarchy](http://127.0.0.1:5610/sessions/session-1?tab=activity)',
      body: '😌 The hierarchy is live with **clean markup**',
    }
    vi.spyOn(api, 'sessionActivity').mockResolvedValue({
      items: [markdownRoot],
      days: [{ date: '2026-07-15', durationMs: 100, totals }],
      page: 1,
      pageSize: 25,
      total: 1,
      totalPages: 1,
    })
    const detail = vi.spyOn(api, 'sessionActivityDetail').mockResolvedValue({
      ...markdownRoot,
      children: [
        { ...child, id: 'user-1', timestamp: '2026-07-15T20:00:01Z', kind: 'user', label: 'User message', body: 'Could you make the Activity timeline easier to scan?' },
        { ...child, timestamp: '2026-07-15T20:00:02Z' },
        { ...tool, timestamp: '2026-07-15T20:00:03Z' },
        { ...child, id: 'compaction-1', timestamp: '2026-07-15T20:00:03.500Z', kind: 'compaction', label: 'Context compacted', body: 'Conversation context was compacted.' },
        { ...child, id: 'reasoning-1', timestamp: '2026-07-15T20:00:04Z', kind: 'reasoning', label: 'Reasoning summary', body: 'Checking event semantics' },
        { ...tool, id: 'tool-2', timestamp: '2026-07-15T20:00:05Z', label: 'apply_patch', toolName: 'apply_patch', body: 'Updated SessionDetailPage.tsx' },
        { ...reviewGroup, timestamp: '2026-07-15T20:00:06Z' },
        { ...agentGroup, timestamp: '2026-07-15T20:00:07Z' },
        { ...child, id: 'final-1', timestamp: '2026-07-15T20:00:08Z', kind: 'final', label: 'Final answer', body: '😌 [Activity view is live](http://127.0.0.1:5610/sessions/session-1?tab=activity) with **clean markup**' },
        { ...interrupted, timestamp: '2026-07-15T20:00:09Z' },
        { ...rolledBack, timestamp: '2026-07-16T00:00:10Z' },
      ].reverse(),
    })

    const { container } = render(
      <MemoryRouter initialEntries={['/sessions/session-1?tab=activity']}>
        <Routes>
          <Route path="/sessions/:sessionId" element={<SessionDetailPage />} />
        </Routes>
      </MemoryRouter>,
    )

    const prompt = await screen.findByText('fix')
    expect(await screen.findByText('1 DAY')).toBeVisible()
    expect(screen.getByRole('table', { name: 'Session activity' })).toBeInTheDocument()
    expect(screen.queryByRole('treegrid')).not.toBeInTheDocument()
    expect(container.querySelector('[aria-level]')).not.toBeInTheDocument()
    expect(screen.getAllByRole('columnheader')).toHaveLength(6)
    expect(detail).not.toHaveBeenCalled()
    const exchangeTrigger = prompt.closest('button')!
    const exchangeRow = prompt.closest('.activity-event') as HTMLElement
    expect(exchangeRow).toHaveAttribute('data-activity-depth', '1')
    expect(exchangeTrigger).toHaveAttribute('aria-expanded', 'false')
    expect(exchangeTrigger).toHaveAttribute('aria-controls')
    expect(exchangeRow).toHaveTextContent('Please fix the Activity hierarchy')
    expect(exchangeRow).toHaveTextContent('😌 The hierarchy is live with clean markup')
    expect(exchangeRow).toHaveTextContent('RUNNING')
    expect(exchangeRow).not.toHaveTextContent('LATEST')
    expect(exchangeRow).not.toHaveTextContent('gpt-5.6-sol · HIGH')
    expect(exchangeRow).not.toHaveTextContent('7 model calls')
    expect(exchangeRow).not.toHaveTextContent('4 tools')
    expect(exchangeRow).not.toHaveTextContent('2 agents')
    expect(exchangeRow).not.toHaveTextContent('3 reviews')
    expect(exchangeRow).not.toHaveTextContent('1 follow-up')
    expect(exchangeTrigger.querySelectorAll(':scope > .event-copy-title, :scope > .exchange-assistant-line')).toHaveLength(2)
    expect(exchangeRow.querySelector('.event-context')).not.toBeInTheDocument()
    expect(exchangeRow).not.toHaveTextContent('Turn')
    expect(exchangeRow).not.toHaveTextContent('http://127.0.0.1')
    expect(screen.queryByRole('link', { name: 'Activity hierarchy' })).not.toBeInTheDocument()

    fireEvent.click(exchangeTrigger)

    await waitFor(() => expect(detail).toHaveBeenCalledWith('session-1', 'turn-1', expect.any(AbortSignal), 1, 250))
    const userMessage = await screen.findByText('Could you make the Activity timeline easier to scan?')
    const assistantUpdate = screen.getByText('Child preview')
    const finalAnswer = screen.getByText('Activity view is live')
    expect(userMessage).toBeVisible()
    expect(exchangeTrigger).toHaveAttribute('aria-expanded', 'true')
    expect(document.getElementById(exchangeTrigger.getAttribute('aria-controls')!)).toHaveAttribute('role', 'region')
    expect(userMessage.closest('.activity-event')).toHaveAttribute('data-activity-depth', '2')
    expect(userMessage.closest('.activity-event')).not.toHaveAttribute('role')
    expect(userMessage.closest('[role="listitem"]')).toBeInTheDocument()
    expect(screen.getAllByRole('row')).toHaveLength(4)
    expect(assistantUpdate).toBeVisible()
    expect(finalAnswer).toBeVisible()
    expect(userMessage.closest('.activity-event')).toHaveClass('communication')
    expect(userMessage.closest('.activity-event')).toHaveClass('from-user')
    expect(assistantUpdate.closest('.activity-event')).toHaveClass('communication')
    expect(assistantUpdate.closest('.activity-event')).toHaveClass('from-assistant')
    expect(finalAnswer.closest('.activity-event')).toHaveClass('communication')
    expect(finalAnswer.closest('.activity-event')).toHaveClass('from-assistant')
    expect(userMessage.closest('.activity-event')?.querySelector('.event-sender-icon')).toBeInTheDocument()
    expect(assistantUpdate.closest('.activity-event')?.querySelector('.event-sender-icon')).toBeInTheDocument()
    expect(finalAnswer.closest('.activity-event')?.querySelector('.event-sender-icon')).toBeInTheDocument()
    expect(exchangeRow.querySelectorAll('.event-sender-icon')).toHaveLength(0)
    expect(screen.queryByText('User message')).not.toBeInTheDocument()
    expect(screen.queryByText('Assistant update')).not.toBeInTheDocument()
    expect(screen.queryByText('Final answer')).not.toBeInTheDocument()
    const workLabels = screen.queryAllByText(/^Work ·/).map(element => element.textContent)
    expect(workLabels).toEqual(['Work · 2 events', 'Work · 1 event'])
    expect(screen.queryByText('cargo test')).not.toBeInTheDocument()
    const compactionRow = screen.getByText('Context compacted').closest('.activity-event')!
    expect(compactionRow).toHaveClass('meta')
    expect(compactionRow).not.toHaveClass('communication')
    expect(compactionRow.querySelector('svg')).toBeInTheDocument()
    expect(screen.getByText('Automated reviews · 2')).toBeVisible()
    expect(screen.getByText('Kant · completed')).toBeVisible()
    expect(screen.getByText('Activity view is live').closest('.activity-event')).not.toHaveTextContent('http://127.0.0.1')
    expect(screen.getByText('Turn interrupted')).toBeVisible()
    expect(screen.getByText('Reason: interrupted')).toBeVisible()
    expect(screen.getByText('Thread rolled back')).toBeVisible()
    expect(screen.getByText('1 turn removed from active history')).toBeVisible()
    const july16Divider = screen.getByText('JUL 16')
    expect(july16Divider).toHaveClass('activity-child-date')
    expect(july16Divider).toHaveAttribute('role', 'presentation')
    expect(july16Divider).toHaveAttribute('aria-hidden', 'true')
    expect(screen.getAllByText('JUL 15').some(element => element.classList.contains('activity-child-date'))).toBe(true)
    const immediateRows = exchangeRow.parentElement!.querySelectorAll(':scope > .activity-detail-row > .activity-detail-cell > .activity-event-details > .activity-child-list > .activity-event-wrap > .activity-event')
    expect(immediateRows[0]).toHaveTextContent('Thread rolled back')
    expect(immediateRows[immediateRows.length - 1]).toHaveTextContent('Could you make the Activity timeline easier to scan?')
    expect(screen.queryByText('{"type":"thread_rolled_back","num_turns":1}')).not.toBeInTheDocument()
    expect(screen.queryByText(/ROLLOUT rollout-1/)).not.toBeInTheDocument()
    expect(screen.queryByText(/AGENT agent-1/)).not.toBeInTheDocument()
    expect(screen.queryByText('$0.00')).not.toBeInTheDocument()
    const narrativePreview = screen.getByText('Child preview')
    const narrativeRow = narrativePreview.closest('.activity-event')!
    expect(narrativePreview.closest('button')).toBeNull()
    expect(narrativeRow.querySelector('.event-cost')).toHaveTextContent('—')
    expect(narrativeRow.querySelector('.event-tokens')).toHaveTextContent('—')

    const workRow = screen.getByText('Work · 2 events').closest('.activity-event')!
    expect(workRow).toHaveClass('group')
    expect(workRow).not.toHaveClass('communication')
    expect(workRow.querySelector('.event-duration')).not.toHaveTextContent('—')
    expect(workRow.querySelector('.event-cost')).toHaveTextContent('$0.02')
    expect(workRow.querySelector('.event-tokens')).toHaveTextContent('20')
    fireEvent.click(within(workRow as HTMLElement).getByRole('button'))
    expect(screen.getByText('Checking event semantics')).toBeVisible()
    expect(workRow.querySelector('.event-cost')).toHaveTextContent('$0.02')
    expect(workRow.querySelector('.event-tokens')).toHaveTextContent('20')
    expect(screen.queryByText('Updated SessionDetailPage.tsx')).not.toBeInTheDocument()
    const groupedToolRow = screen.getByText('apply_patch').closest('.activity-event')!
    expect(groupedToolRow).toHaveAttribute('data-activity-depth', '3')
    expect(groupedToolRow.querySelector('button')).not.toBeInTheDocument()
    expect(groupedToolRow.querySelector('.event-cost')).toHaveTextContent('$0.02')
    expect(groupedToolRow.querySelector('.event-tokens')).toHaveTextContent('20')
    expect(detail).toHaveBeenCalledTimes(1)

    const loneWorkRow = screen.getByText('Work · 1 event').closest('.activity-event')!
    expect(loneWorkRow).toHaveClass('group')
    expect(loneWorkRow.querySelector('.event-duration')).toHaveTextContent('390ms')
    expect(loneWorkRow.querySelector('.event-cost')).toHaveTextContent('$0.02')
    expect(loneWorkRow.querySelector('.event-tokens')).toHaveTextContent('20')
    fireEvent.click(within(loneWorkRow as HTMLElement).getByRole('button'))
    expect(loneWorkRow.querySelector('.event-cost')).toHaveTextContent('$0.02')
    expect(loneWorkRow.querySelector('.event-tokens')).toHaveTextContent('20')
    expect(screen.queryByText('cargo test')).not.toBeInTheDocument()
    const loneToolRow = screen.getByText('exec').closest('.activity-event')!
    expect(loneToolRow.querySelector('button')).not.toBeInTheDocument()
    expect(loneToolRow.querySelector('.event-cost')).toHaveTextContent('$0.02')
    expect(loneToolRow.querySelector('.event-tokens')).toHaveTextContent('20')

    const reviewRow = screen.getByText('Automated reviews · 2').closest('.activity-event')!
    expect(reviewRow).toHaveClass('group')
    expect(reviewRow).not.toHaveClass('communication')
    fireEvent.click(within(reviewRow as HTMLElement).getByRole('button'))
    const individualReview = screen.getByText('{"outcome":"allow"}').closest('.activity-event')!
    expect(reviewRow.querySelector('.event-cost')).toHaveTextContent('$0.03')
    expect(reviewRow.querySelector('.event-tokens')).toHaveTextContent('3')
    expect(individualReview).toBeVisible()
    expect(individualReview).toHaveClass('review')
    expect(individualReview).not.toHaveClass('agent')
    expect(individualReview.querySelector('svg')).toBeInTheDocument()
    expect(individualReview.querySelector('.event-cost')).toHaveTextContent('$0.03')
    expect(individualReview.querySelector('.event-tokens')).toHaveTextContent('3')
    expect(detail).toHaveBeenCalledTimes(1)

    const agentRow = screen.getByText('Kant · completed').closest('.activity-event')!
    expect(agentRow).toHaveClass('group')
    expect(agentRow).not.toHaveClass('communication')
    fireEvent.click(within(agentRow as HTMLElement).getByRole('button'))
    const individualAgent = screen.getByText('Agent found the rendering issue.').closest('.activity-event')!
    expect(agentRow.querySelector('.event-cost')).toHaveTextContent('$0.04')
    expect(agentRow.querySelector('.event-tokens')).toHaveTextContent('4')
    expect(individualAgent).toHaveTextContent('gpt-5.6-sol · HIGH')
    expect(individualAgent).toHaveClass('agent')
    expect(individualAgent.querySelector('.event-cost')).toHaveTextContent('$0.04')
    expect(individualAgent.querySelector('.event-tokens')).toHaveTextContent('4')
  })

  it('opens preloaded exchange children locally without a detail request', async () => {
    vi.spyOn(api, 'sessionSummary').mockResolvedValue(summary)
    vi.spyOn(api, 'sessionActivity').mockResolvedValue({
      items: [{ ...root, children: [child] }],
      days: [{ date: '2026-07-15', durationMs: 100, totals }],
      page: 1,
      pageSize: 25,
      total: 1,
      totalPages: 1,
    })
    const detail = vi.spyOn(api, 'sessionActivityDetail')

    render(
      <MemoryRouter initialEntries={['/sessions/session-1?tab=activity']}>
        <Routes>
          <Route path="/sessions/:sessionId" element={<SessionDetailPage />} />
        </Routes>
      </MemoryRouter>,
    )

    fireEvent.click((await screen.findByText('Please fix the Activity hierarchy')).closest('button')!)
    expect(screen.getByText('Child preview')).toBeVisible()
    expect(detail).not.toHaveBeenCalled()
  })

  it('loads bounded Activity children page by page without duplicating lazy groups', async () => {
    vi.spyOn(api, 'sessionSummary').mockResolvedValue(summary)
    vi.spyOn(api, 'sessionActivity').mockResolvedValue({
      items: [root],
      days: [{ date: '2026-07-15', durationMs: 100, totals }],
      page: 1,
      pageSize: 25,
      total: 1,
      totalPages: 1,
    })
    const lazyAgents: ActivityItem = {
      ...child,
      id: 'group:agents:turn-1',
      kind: 'agent_group',
      label: 'Agents · 1',
      body: null,
      hasDetails: true,
      children: [],
      childPage: 1,
      childPageSize: 2,
      childTotal: 1,
      childHasMore: true,
    }
    const detail = vi.spyOn(api, 'sessionActivityDetail').mockImplementation((_sessionId, _eventId, _signal, page) => Promise.resolve(page === 1
      ? {
          ...root,
          children: [{ ...child, id: 'page-1', body: 'First bounded child' }, lazyAgents],
          childPage: 1,
          childPageSize: 2,
          childTotal: 2,
          childHasMore: true,
          childNextCursor: 'cursor-page-2',
        }
      : {
          ...root,
          children: [{ ...child, id: 'page-2', body: 'Second bounded child' }, lazyAgents],
          childPage: 2,
          childPageSize: 2,
          childTotal: 2,
          childHasMore: false,
        }))

    render(
      <MemoryRouter initialEntries={['/sessions/session-1?tab=activity']}>
        <Routes>
          <Route path="/sessions/:sessionId" element={<SessionDetailPage />} />
        </Routes>
      </MemoryRouter>,
    )

    fireEvent.click((await screen.findByText('Please fix the Activity hierarchy')).closest('button')!)
    expect(await screen.findByText('First bounded child')).toBeVisible()
    expect(screen.getByText('Agents · 1')).toBeVisible()
    const loadMore = screen.getByRole('button', { name: 'LOAD MORE · 1 / 2' })
    fireEvent.click(loadMore)

    await waitFor(() => expect(detail).toHaveBeenLastCalledWith(
      'session-1',
      'turn-1',
      expect.any(AbortSignal),
      2,
      2,
      'cursor-page-2',
    ))
    expect(await screen.findByText('Second bounded child')).toBeVisible()
    expect(screen.getByText('First bounded child')).toBeVisible()
    expect(screen.getAllByText('Agents · 1')).toHaveLength(1)
    expect(screen.queryByRole('button', { name: /LOAD MORE ·/ })).not.toBeInTheDocument()
  })

  it('revalidates every explicitly loaded Activity child page during polling', async () => {
    vi.useFakeTimers()
    try {
      vi.spyOn(api, 'sessionSummary').mockResolvedValue(summary)
      const running = { ...root, status: 'running', durationMs: 100 }
      const completed = { ...root, status: 'completed', durationMs: 900 }
      vi.spyOn(api, 'sessionActivity')
        .mockResolvedValueOnce({ items: [running], days: [{ date: '2026-07-15', durationMs: 100, totals }], page: 1, pageSize: 25, total: 1, totalPages: 1 })
        .mockResolvedValue({ items: [completed], days: [{ date: '2026-07-15', durationMs: 900, totals }], page: 1, pageSize: 25, total: 1, totalPages: 1 })
      const detail = vi.spyOn(api, 'sessionActivityDetail').mockImplementation((_sessionId, _eventId, _signal, page) => {
        const refreshed = detail.mock.calls.length > 2
        return Promise.resolve({
          ...(refreshed ? completed : running),
          children: [{
            ...child,
            id: `page-${page}`,
            body: `${refreshed ? 'Refreshed' : 'Initial'} child page ${page}`,
          }],
          childPage: page,
          childPageSize: 1,
          childTotal: 2,
          childHasMore: page === 1,
          childNextCursor: page === 1
            ? `${refreshed ? 'refreshed' : 'initial'}-page-2-cursor`
            : undefined,
        })
      })

      render(
        <MemoryRouter initialEntries={['/sessions/session-1?tab=activity']}>
          <Routes>
            <Route path="/sessions/:sessionId" element={<SessionDetailPage />} />
          </Routes>
        </MemoryRouter>,
      )
      await act(async () => { await Promise.resolve(); await Promise.resolve(); await Promise.resolve() })

      fireEvent.click(screen.getByText('Please fix the Activity hierarchy').closest('button')!)
      await act(async () => { await Promise.resolve(); await Promise.resolve(); await Promise.resolve() })
      expect(screen.getByText('Initial child page 1')).toBeVisible()
      fireEvent.click(screen.getByRole('button', { name: 'LOAD MORE · 1 / 2' }))
      await act(async () => { await Promise.resolve(); await Promise.resolve(); await Promise.resolve() })
      expect(screen.getByText('Initial child page 2')).toBeVisible()
      expect(detail.mock.calls[1]?.[5]).toBe('initial-page-2-cursor')

      await act(async () => { await vi.advanceTimersByTimeAsync(30_000) })

      expect(detail).toHaveBeenCalledTimes(4)
      expect(detail.mock.calls.slice(-2).map(call => call[3])).toEqual([1, 2])
      expect(detail.mock.calls[3]?.[5]).toBe('refreshed-page-2-cursor')
      expect(screen.getByText('Refreshed child page 1')).toBeVisible()
      expect(screen.getByText('Refreshed child page 2')).toBeVisible()
      expect(screen.queryByText('Initial child page 1')).not.toBeInTheDocument()
      expect(screen.queryByText('Initial child page 2')).not.toBeInTheDocument()
      expect(screen.queryByRole('button', { name: /LOAD MORE ·/ })).not.toBeInTheDocument()
    } finally {
      vi.useRealTimers()
    }
  })

  it('revalidates expanded Activity details when polling returns an unchanged parent row', async () => {
    vi.useFakeTimers()
    try {
      vi.spyOn(api, 'sessionSummary').mockResolvedValue(summary)
      const activityResponse = {
        items: [root],
        days: [{ date: '2026-07-15', durationMs: 100, totals }],
        page: 1,
        pageSize: 25,
        total: 1,
        totalPages: 1,
      }
      vi.spyOn(api, 'sessionActivity').mockResolvedValue(activityResponse)
      const detail = vi.spyOn(api, 'sessionActivityDetail')
        .mockResolvedValueOnce({ ...root, children: [{ ...child, body: 'Initial child-only state' }] })
        .mockResolvedValue({ ...root, children: [{ ...child, body: 'Refreshed child-only state' }] })

      render(
        <MemoryRouter initialEntries={['/sessions/session-1?tab=activity']}>
          <Routes>
            <Route path="/sessions/:sessionId" element={<SessionDetailPage />} />
          </Routes>
        </MemoryRouter>,
      )
      await act(async () => { await Promise.resolve(); await Promise.resolve(); await Promise.resolve() })

      fireEvent.click(screen.getByText('Please fix the Activity hierarchy').closest('button')!)
      await act(async () => { await Promise.resolve(); await Promise.resolve(); await Promise.resolve() })
      expect(screen.getByText('Initial child-only state')).toBeVisible()

      await act(async () => { await vi.advanceTimersByTimeAsync(30_000) })

      expect(detail).toHaveBeenCalledTimes(2)
      expect(screen.getByText('Refreshed child-only state')).toBeVisible()
      expect(screen.queryByText('Initial child-only state')).not.toBeInTheDocument()
    } finally {
      vi.useRealTimers()
    }
  })

  it('canonicalizes an out-of-range Activity page to the server-clamped page', async () => {
    vi.spyOn(api, 'sessionSummary').mockResolvedValue(summary)
    const activity = vi.spyOn(api, 'sessionActivity').mockImplementation((_sessionId, requestedPage) => Promise.resolve({
      items: [root],
      days: [{ date: '2026-07-15', durationMs: 100, totals }],
      page: Math.min(requestedPage, 3),
      pageSize: 25,
      total: 75,
      totalPages: 3,
    }))

    render(
      <MemoryRouter initialEntries={['/sessions/session-1?tab=activity&page=999']}>
        <Routes>
          <Route path="/sessions/:sessionId" element={<><SessionDetailPage /><LocationProbe /></>} />
        </Routes>
      </MemoryRouter>,
    )

    await waitFor(() => expect(activity).toHaveBeenLastCalledWith('session-1', 3, expect.any(AbortSignal)))
    expect(screen.getByLabelText('Current location')).toHaveTextContent('/sessions/session-1?tab=activity&page=3')
  })

  it('keeps the focused Activity paginator mounted without showing prior-page events', async () => {
    vi.spyOn(api, 'sessionSummary').mockResolvedValue(summary)
    let resolveSecond!: (value: Awaited<ReturnType<typeof api.sessionActivity>>) => void
    const second = new Promise<Awaited<ReturnType<typeof api.sessionActivity>>>(resolve => { resolveSecond = resolve })
    vi.spyOn(api, 'sessionActivity')
      .mockResolvedValueOnce({ items: [root], days: [{ date: '2026-07-15', durationMs: 100, totals }], page: 1, pageSize: 25, total: 50, totalPages: 2 })
      .mockReturnValueOnce(second)

    render(
      <MemoryRouter initialEntries={['/sessions/session-1?tab=activity']}>
        <Routes><Route path="/sessions/:sessionId" element={<SessionDetailPage />} /></Routes>
      </MemoryRouter>,
    )

    const nextPage = await screen.findByRole('button', { name: '02' })
    nextPage.focus()
    fireEvent.click(nextPage)
    await waitFor(() => expect(api.sessionActivity).toHaveBeenCalledTimes(2))

    expect(nextPage).toHaveFocus()
    expect(nextPage).not.toBeDisabled()
    expect(nextPage).toHaveAttribute('aria-disabled', 'true')
    expect(screen.getByRole('navigation', { name: 'Pagination' })).toHaveAttribute('aria-busy', 'true')
    expect(screen.queryByText('Please fix the Activity hierarchy')).not.toBeInTheDocument()
    expect(screen.getByRole('table', { name: 'Session activity' })).toBeVisible()

    const pageTwo = { ...root, id: 'turn-2', label: 'Page two activity' }
    resolveSecond({ items: [pageTwo], days: [{ date: '2026-07-15', durationMs: 100, totals }], page: 2, pageSize: 25, total: 50, totalPages: 2 })
    expect(await screen.findByText('Page two activity')).toBeVisible()
    expect(nextPage).toHaveFocus()
    expect(nextPage).not.toHaveAttribute('aria-disabled')
  })

  it('keeps retained Activity pagination inert after a replacement page fails', async () => {
    vi.spyOn(api, 'sessionSummary').mockResolvedValue(summary)
    const second = deferred<Awaited<ReturnType<typeof api.sessionActivity>>>()
    vi.spyOn(api, 'sessionActivity')
      .mockResolvedValueOnce({ items: [root], days: [{ date: '2026-07-15', durationMs: 100, totals }], page: 1, pageSize: 25, total: 50, totalPages: 2 })
      .mockReturnValueOnce(second.promise)

    render(
      <MemoryRouter initialEntries={['/sessions/session-1?tab=activity']}>
        <Routes><Route path="/sessions/:sessionId" element={<SessionDetailPage />} /></Routes>
      </MemoryRouter>,
    )

    const nextPage = await screen.findByRole('button', { name: '02' })
    nextPage.focus()
    fireEvent.click(nextPage)
    await waitFor(() => expect(api.sessionActivity).toHaveBeenCalledTimes(2))
    second.reject(new Error('activity page failed'))

    expect(await screen.findByText('activity page failed')).toBeVisible()
    expect(nextPage).toHaveFocus()
    expect(nextPage).not.toBeDisabled()
    expect(nextPage).toHaveAttribute('aria-disabled', 'true')
    expect(screen.getByRole('navigation', { name: 'Pagination' })).toHaveAttribute('aria-busy', 'true')
    expect(screen.queryByText('Please fix the Activity hierarchy')).not.toBeInTheDocument()

    fireEvent.click(nextPage)
    expect(api.sessionActivity).toHaveBeenCalledTimes(2)
  })

  it('shows a child date divider when every child falls on the day after its parent', async () => {
    vi.spyOn(api, 'sessionSummary').mockResolvedValue(summary)
    const midnightRoot = { ...root, timestamp: '2026-07-15T23:59:59+02:00', label: 'Cross-midnight exchange' }
    vi.spyOn(api, 'sessionActivity').mockResolvedValue({
      items: [midnightRoot],
      days: [{ date: '2026-07-15', durationMs: 100, totals }],
      page: 1,
      pageSize: 25,
      total: 1,
      totalPages: 1,
    })
    vi.spyOn(api, 'sessionActivityDetail').mockResolvedValue({
      ...midnightRoot,
      children: [{ ...child, timestamp: '2026-07-16T00:00:01+02:00', body: 'Only next-day child' }],
    })

    const { container } = render(
      <MemoryRouter initialEntries={['/sessions/session-1?tab=activity']}>
        <Routes><Route path="/sessions/:sessionId" element={<SessionDetailPage />} /></Routes>
      </MemoryRouter>,
    )
    fireEvent.click((await screen.findByText('Cross-midnight exchange')).closest('button')!)

    expect(await screen.findByText('Only next-day child')).toBeVisible()
    const divider = container.querySelector('.activity-child-date')
    expect(divider).toHaveTextContent('JUL 16')
  })

  it('clears a detail error after a successful retry', async () => {
    vi.spyOn(api, 'sessionSummary').mockResolvedValue(summary)
    vi.spyOn(api, 'sessionActivity').mockResolvedValue({
      items: [root],
      days: [{ date: '2026-07-15', durationMs: 100, totals }],
      page: 1,
      pageSize: 25,
      total: 1,
      totalPages: 1,
    })
    vi.spyOn(api, 'sessionActivityDetail')
      .mockRejectedValueOnce(new Error('detail request failed'))
      .mockResolvedValue({ ...root, children: [{ ...child, body: 'Recovered detail' }] })

    render(
      <MemoryRouter initialEntries={['/sessions/session-1?tab=activity']}>
        <Routes>
          <Route path="/sessions/:sessionId" element={<SessionDetailPage />} />
        </Routes>
      </MemoryRouter>,
    )

    fireEvent.click((await screen.findByText('Please fix the Activity hierarchy')).closest('button')!)
    const alert = await screen.findByRole('alert')
    expect(alert).toHaveTextContent('detail request failed')
    expect(within(alert).getByRole('button', { name: 'RETRY' })).toBeVisible()
    fireEvent.click(screen.getByRole('button', { name: 'RETRY' }))

    expect(await screen.findByText('Recovered detail')).toBeVisible()
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
  })

  it('aborts and ignores an older detail response when polling starts a replacement', async () => {
    vi.useFakeTimers()
    try {
      vi.spyOn(api, 'sessionSummary').mockResolvedValue(summary)
      const running = { ...root, status: 'running' }
      const completed = { ...root, status: 'completed', durationMs: 900 }
      vi.spyOn(api, 'sessionActivity')
        .mockResolvedValueOnce({ items: [running], days: [{ date: '2026-07-15', durationMs: 100, totals }], page: 1, pageSize: 25, total: 1, totalPages: 1 })
        .mockResolvedValue({ items: [completed], days: [{ date: '2026-07-15', durationMs: 900, totals }], page: 1, pageSize: 25, total: 1, totalPages: 1 })
      const firstDetail = deferred<ActivityItem>()
      let firstSignal: AbortSignal | undefined
      const detail = vi.spyOn(api, 'sessionActivityDetail').mockImplementation((_sessionId, _eventId, signal) => {
        if (detail.mock.calls.length === 1) {
          firstSignal = signal
          return firstDetail.promise
        }
        return Promise.resolve({ ...completed, children: [{ ...child, body: 'Newest detail' }] })
      })

      render(
        <MemoryRouter initialEntries={['/sessions/session-1?tab=activity']}>
          <Routes>
            <Route path="/sessions/:sessionId" element={<SessionDetailPage />} />
          </Routes>
        </MemoryRouter>,
      )
      await act(async () => { await Promise.resolve(); await Promise.resolve(); await Promise.resolve() })
      fireEvent.click(screen.getByText('Please fix the Activity hierarchy').closest('button')!)
      await act(async () => { await Promise.resolve() })

      await act(async () => { await vi.advanceTimersByTimeAsync(30_000) })
      expect(detail).toHaveBeenCalledTimes(2)
      expect(firstSignal?.aborted).toBe(true)
      expect(screen.getByText('Newest detail')).toBeVisible()

      await act(async () => { firstDetail.resolve({ ...running, children: [{ ...child, body: 'Stale detail' }] }); await Promise.resolve() })
      expect(screen.queryByText('Stale detail')).not.toBeInTheDocument()
      expect(screen.getByText('Newest detail')).toBeVisible()
    } finally {
      vi.useRealTimers()
    }
  })

  it('reconciles an open exchange when polling supplies refreshed children', async () => {
    vi.useFakeTimers()
    try {
      vi.spyOn(api, 'sessionSummary').mockResolvedValue(summary)
      const activity = vi.spyOn(api, 'sessionActivity')
        .mockResolvedValueOnce({
          items: [{ ...root, children: [{ ...child, body: 'Old child preview' }] }],
          days: [{ date: '2026-07-15', durationMs: 100, totals }],
          page: 1,
          pageSize: 25,
          total: 1,
          totalPages: 1,
        })
        .mockResolvedValue({
          items: [{ ...root, children: [{ ...child, body: 'Fresh child preview' }] }],
          days: [{ date: '2026-07-15', durationMs: 100, totals }],
          page: 1,
          pageSize: 25,
          total: 1,
          totalPages: 1,
        })

      render(
        <MemoryRouter initialEntries={['/sessions/session-1?tab=activity']}>
          <Routes>
            <Route path="/sessions/:sessionId" element={<SessionDetailPage />} />
          </Routes>
        </MemoryRouter>,
      )
      await act(async () => {
        await Promise.resolve()
        await Promise.resolve()
        await Promise.resolve()
      })

      fireEvent.click(screen.getByText('Please fix the Activity hierarchy').closest('button')!)
      expect(screen.getByText('Old child preview')).toBeVisible()

      await act(async () => { await vi.advanceTimersByTimeAsync(30_000) })
      expect(activity).toHaveBeenCalledTimes(2)
      expect(screen.getByText('Fresh child preview')).toBeVisible()
      expect(screen.queryByText('Old child preview')).not.toBeInTheDocument()
    } finally {
      vi.useRealTimers()
    }
  })

  it('refreshes separately loaded details when the open parent exchange changes', async () => {
    vi.useFakeTimers()
    try {
      vi.spyOn(api, 'sessionSummary').mockResolvedValue(summary)
      const running = { ...root, status: 'running', durationMs: 100, usage: { ...totals, totalTokens: 12, costUsd: '0.01' } }
      const completed = { ...root, status: 'completed', durationMs: 900, usage: { ...totals, totalTokens: 24, costUsd: '0.02' } }
      vi.spyOn(api, 'sessionActivity')
        .mockResolvedValueOnce({
          items: [running],
          days: [{ date: '2026-07-15', durationMs: 100, totals }],
          page: 1,
          pageSize: 25,
          total: 1,
          totalPages: 1,
        })
        .mockResolvedValue({
          items: [completed],
          days: [{ date: '2026-07-15', durationMs: 900, totals: { ...totals, totalTokens: 24, costUsd: '0.02' } }],
          page: 1,
          pageSize: 25,
          total: 1,
          totalPages: 1,
        })
      const detail = vi.spyOn(api, 'sessionActivityDetail')
        .mockResolvedValueOnce({ ...running, children: [{ ...child, body: 'Initial fetched child' }] })
        .mockResolvedValue({ ...completed, children: [{ ...child, body: 'Refreshed fetched child' }] })

      render(
        <MemoryRouter initialEntries={['/sessions/session-1?tab=activity']}>
          <Routes>
            <Route path="/sessions/:sessionId" element={<SessionDetailPage />} />
          </Routes>
        </MemoryRouter>,
      )
      await act(async () => {
        await Promise.resolve()
        await Promise.resolve()
        await Promise.resolve()
      })

      const exchangeTrigger = screen.getByText('Please fix the Activity hierarchy').closest('button')!
      const exchangeRow = exchangeTrigger.closest('.activity-event')!
      fireEvent.click(exchangeTrigger)
      await act(async () => {
        await Promise.resolve()
        await Promise.resolve()
      })
      expect(detail).toHaveBeenCalledTimes(1)
      expect(screen.getByText('Initial fetched child')).toBeVisible()
      expect(exchangeRow).toHaveTextContent('RUNNING')

      await act(async () => { await vi.advanceTimersByTimeAsync(30_000) })

      expect(detail).toHaveBeenCalledTimes(2)
      expect(screen.getByText('Refreshed fetched child')).toBeVisible()
      expect(screen.queryByText('Initial fetched child')).not.toBeInTheDocument()
      expect(exchangeRow).not.toHaveTextContent('RUNNING')
      expect(exchangeRow.querySelector('.event-cost')).toHaveTextContent('$0.02')
      expect(exchangeRow.querySelector('.event-tokens')).toHaveTextContent('24')
    } finally {
      vi.useRealTimers()
    }
  })

  it('renders an expanded assistant message as structured Markdown', async () => {
    vi.spyOn(api, 'sessionSummary').mockResolvedValue(summary)
    const richFinal: ActivityItem = {
      ...child,
      id: 'rich-final',
      kind: 'final',
      label: 'Final answer',
      body: 'Opening paragraph.\n\n- First item\n- Second item',
      hasDetails: true,
      children: [{ ...tool, id: 'nested-tool' }],
    }
    vi.spyOn(api, 'sessionActivity').mockResolvedValue({
      items: [{ ...root, children: [richFinal] }],
      days: [{ date: '2026-07-15', durationMs: 100, totals }],
      page: 1,
      pageSize: 25,
      total: 1,
      totalPages: 1,
    })

    render(
      <MemoryRouter initialEntries={['/sessions/session-1?tab=activity']}>
        <Routes>
          <Route path="/sessions/:sessionId" element={<SessionDetailPage />} />
        </Routes>
      </MemoryRouter>,
    )

    fireEvent.click((await screen.findByText('Please fix the Activity hierarchy')).closest('button')!)
    const finalPreview = screen.getByText(/Opening paragraph\. First item Second item/)
    fireEvent.click(finalPreview.closest('button')!)

    const richMarkdown = screen.getByText('Opening paragraph.', { selector: '.activity-rich-markdown p span' }).closest('.activity-rich-markdown') as HTMLElement
    expect(richMarkdown).toBeVisible()
    expect(within(richMarkdown).getAllByRole('listitem').map(item => item.textContent)).toEqual(['First item', 'Second item'])
  })

  it('separates the authored user request from captured textual context', async () => {
    vi.spyOn(api, 'sessionSummary').mockResolvedValue(summary)
    const userPreview: ActivityItem = {
      ...child,
      id: 'captured-user',
      kind: 'user',
      label: 'User message',
      body: 'Please make the authored request prominent.',
      hasDetails: true,
    }
    vi.spyOn(api, 'sessionActivity').mockResolvedValue({
      items: [{ ...root, children: [userPreview] }],
      days: [{ date: '2026-07-15', durationMs: 100, totals }],
      page: 1,
      pageSize: 25,
      total: 1,
      totalPages: 1,
    })
    const detail = vi.spyOn(api, 'sessionActivityDetail').mockResolvedValue({
      ...userPreview,
      body: `# Applications mentioned by the user:

<appshot app="Google Chrome" window-title="Codex usage">large capture</appshot>

## My request for Codex:
Please make the authored request prominent.`,
    })

    render(
      <MemoryRouter initialEntries={['/sessions/session-1?tab=activity']}>
        <Routes>
          <Route path="/sessions/:sessionId" element={<SessionDetailPage />} />
        </Routes>
      </MemoryRouter>,
    )

    fireEvent.click((await screen.findByText('Please fix the Activity hierarchy')).closest('button')!)
    fireEvent.click(screen.getByText('Please make the authored request prominent.').closest('button')!)

    await waitFor(() => expect(detail).toHaveBeenCalledWith('session-1', 'captured-user', expect.any(AbortSignal), 1, 250))
    expect(screen.getByText('Please make the authored request prominent.', { selector: '.user-message-primary p span' })).toBeVisible()
    expect(screen.getByText('SUPPORTING MATERIAL · 1')).toBeVisible()
    expect(screen.getByText('APP CAPTURE')).toBeVisible()
    expect(screen.queryByText('large capture')).not.toBeInTheDocument()
  })

  it('groups reverse-ordered turns by local day and keeps metric headings aligned', async () => {
    vi.spyOn(api, 'sessionSummary').mockResolvedValue(summary)
    vi.spyOn(api, 'sessionActivity').mockResolvedValue({
      items: [
        { ...root, id: 'latest', turnId: 'latest', label: 'Latest request', body: '{"risk_level":"low","user_authorization":"high"', timestamp: '2026-07-15T20:00:00Z', hasDetails: false },
        { ...root, id: 'same-day', turnId: 'same-day', label: 'Earlier same day request', timestamp: '2026-07-15T18:00:00Z', hasDetails: false },
        { ...root, id: 'previous-day', turnId: 'previous-day', label: 'Previous day request', timestamp: '2026-07-14T18:00:00Z', hasDetails: false },
      ],
      days: [
        { date: '2026-07-15', durationMs: 200, totals: { ...totals, totalTokens: 24, costUsd: '0.02' } },
        { date: '2026-07-14', durationMs: 100, totals },
      ],
      page: 1,
      pageSize: 25,
      total: 3,
      totalPages: 1,
    })

    const { container } = render(
      <MemoryRouter initialEntries={['/sessions/session-1?tab=activity']}>
        <Routes>
          <Route path="/sessions/:sessionId" element={<SessionDetailPage />} />
        </Routes>
      </MemoryRouter>,
    )

    expect(await screen.findByText('Latest request')).toBeVisible()
    expect(screen.getByText('{"risk_level":"low","user_authorization":"high"')).toBeVisible()
    expect(screen.getAllByText('JUL 15')).toHaveLength(1)
    expect(screen.getAllByText('JUL 14')).toHaveLength(1)
    expect(screen.queryByText('STATUS')).not.toBeInTheDocument()
    expect(screen.queryByText('TYPE')).not.toBeInTheDocument()
    expect(container.querySelector('.activity-ledger')).toHaveClass('page-ledger-frame')
    expect(container.querySelector('.activity-head')).toHaveTextContent('DURATIONCOSTAPI TOKENS')
    expect(container.querySelector('.activity-date-divider')).toHaveTextContent('200ms$0.0224')
    expect(Array.from(container.querySelectorAll('.activity-event .exchange-request')).map(element => element.textContent)).toEqual([
      'Latest request',
      'Earlier same day request',
      'Previous day request',
    ])
  })
})
