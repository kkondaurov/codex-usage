import { useEffect } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'
import { api } from '../api'
import { canonicalStatsAnchor, dateOnly, shiftAnchor, validDateOnly } from '../calendar'
import { DegradedDataNotice, ErrorState, LoadingLedger, PageTitle } from '../components/Common'
import { handleTabKeyDown } from '../components/tabKeyboard'
import { estimatedMoney, tokens } from '../format'
import { useCachedAsync } from '../hooks'
import type { StatsRange, StatsRow } from '../types'

const RANGES: Array<{ value: StatsRange; label: string }> = [
  { value: 'day', label: 'DAY' },
  { value: 'week', label: 'WEEK' },
  { value: 'month', label: 'MONTH' },
  { value: 'year', label: 'YEAR' },
  { value: 'all', label: 'ALL' },
]
const RANGE_VALUES = RANGES.map(item => item.value)

function inferredEnd(row: StatsRow, range: StatsRange) {
  if (row.periodEnd) return row.periodEnd
  const date = new Date(row.periodStart)
  if (range === 'day') date.setHours(date.getHours() + 1)
  else if (range === 'month') date.setDate(date.getDate() + 1)
  else if (range === 'year') date.setMonth(date.getMonth() + 1)
  else date.setFullYear(date.getFullYear() + 100)
  return date.toISOString()
}

export function StatsPage() {
  const navigate = useNavigate()
  const [params, setParams] = useSearchParams()
  const requestedRange = params.get('range') ?? params.get('period')
  const range = (RANGES.some(item => item.value === requestedRange) ? requestedRange : 'month') as StatsRange
  const rawAnchor = params.get('anchor')
  const anchor = canonicalStatsAnchor(rawAnchor, range)
    ?? canonicalStatsAnchor(dateOnly(new Date()), range)!

  useEffect(() => {
    const next = new URLSearchParams(params)
    let changed = false
    if (requestedRange !== range || params.has('period')) {
      next.set('range', range)
      next.delete('period')
      changed = true
    }
    if (range === 'all') {
      if (next.has('anchor')) {
        next.delete('anchor')
        changed = true
      }
    } else if (rawAnchor !== anchor) {
      next.set('anchor', anchor)
      changed = true
    }
    if (changed) setParams(next, { replace: true })
  }, [anchor, params, range, rawAnchor, requestedRange, setParams])

  const requestKey = `stats:${range}:${range === 'all' ? 'all' : anchor}`
  const { data, error, loading, lastSuccessfulAt, refresh } = useCachedAsync(requestKey, signal => api.stats(range, range === 'all' ? undefined : anchor, signal), [range, anchor], 30_000, 30_000)
  const today = dateOnly(new Date())
  const currentPeriodAnchor = canonicalStatsAnchor(today, range) ?? today
  const previousAnchor = shiftAnchor(anchor, range, -1)
  const nextAnchor = shiftAnchor(anchor, range, 1)
  const canPrevious = range !== 'all' && validDateOnly(previousAnchor)
  const canNext = range !== 'all' && validDateOnly(nextAnchor) && nextAnchor <= currentPeriodAnchor
  const visibleRows = data?.rows.filter(row => (
    new Date(row.periodStart).getTime() <= Date.now()
    || row.sessionCount > 0
    || row.totalTokens > 0
  )) ?? []

  function selectRange(value: StatsRange) {
    const next = new URLSearchParams()
    next.set('range', value)
    if (value !== 'all') next.set('anchor', canonicalStatsAnchor(today, value) ?? today)
    setParams(next)
  }

  function navigateRange(amount: number) {
    const next = shiftAnchor(anchor, range, amount)
    if (!validDateOnly(next) || (amount > 0 && next > currentPeriodAnchor)) return
    setParams({ range, anchor: next })
  }

  return (
    <div className="stats-page">
      <PageTitle>Stats</PageTitle>
      <div className="stats-controls">
        <div className="period-tabs" role="tablist" aria-label="Stats period">{RANGES.map(item => <button type="button" role="tab" aria-selected={range === item.value} aria-controls="stats-range-panel" tabIndex={range === item.value ? 0 : -1} key={item.value} className={range === item.value ? 'active' : ''} onKeyDown={event => handleTabKeyDown(event, RANGE_VALUES, range, selectRange)} onClick={() => selectRange(item.value)}>{item.label}</button>)}</div>
        {range !== 'all' && <div className="stats-navigator"><button type="button" disabled={!canPrevious} onClick={() => navigateRange(-1)}>PREVIOUS</button><strong>{data?.label ?? '…'}</strong><button type="button" disabled={!canNext} onClick={() => navigateRange(1)}>NEXT</button></div>}
      </div>
      <section id="stats-range-panel" className="stats-ledger" role="tabpanel" aria-label={`${range} statistics`} aria-busy={loading || undefined}>
        {error && !data ? <ErrorState error={error} onRetry={() => void refresh()} /> : null}
        {error && data ? <DegradedDataNotice error={error} lastSuccessfulAt={lastSuccessfulAt} onRetry={() => void refresh()} /> : null}
        {!data && loading ? <LoadingLedger rows={12} /> : null}
        {data && <div className="ledger-scroll stats-scroll" role="region" aria-label="Scrollable statistics ledger" tabIndex={0}>
          <div className="stats-table" role="table" aria-label={`${range} usage statistics`}>
            <div className="stats-head" role="row"><span role="columnheader">PERIOD</span><span role="columnheader">SESSIONS</span><span role="columnheader">COST</span><span role="columnheader">INPUT</span><span role="columnheader">CACHED</span><span role="columnheader">OUTPUT</span><span role="columnheader">REASONING</span><span role="columnheader">BLENDED</span><span role="columnheader">API</span></div>
            {[...visibleRows].reverse().map(row => {
              const empty = row.sessionCount === 0 && row.totalTokens === 0
              const end = inferredEnd(row, range)
              return <div className={`stats-row ${empty ? 'empty' : ''}`} role="row" key={`${row.periodStart}-${row.label}`}><span role="cell"><button type="button" className="stats-row-hit" aria-label={`View ${row.label} sessions`} onClick={() => navigate(`/sessions?${new URLSearchParams({ start: row.periodStart, end })}`)} />{row.label}</span><b role="cell">{row.sessionCount}</b><b role="cell">{estimatedMoney(row.costUsd, row.unpricedTokens)}</b><b role="cell">{tokens(row.inputTokens)}</b><b role="cell">{tokens(row.cachedInputTokens)}</b><b role="cell">{tokens(row.outputTokens)}</b><b role="cell">{tokens(row.reasoningTokens)}</b><b role="cell">{tokens(row.blendedTokens)}</b><b role="cell">{tokens(row.totalTokens)}</b></div>
            })}
          </div>
        </div>}
      </section>
    </div>
  )
}
