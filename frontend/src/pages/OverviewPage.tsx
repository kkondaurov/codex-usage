import { ArrowRight, CaretLeft, CaretRight } from '@phosphor-icons/react'
import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import type { CSSProperties, FocusEvent, KeyboardEvent as ReactKeyboardEvent, MouseEvent as ReactMouseEvent, PointerEvent as ReactPointerEvent } from 'react'
import { createPortal } from 'react-dom'
import { Link } from 'react-router-dom'
import { api } from '../api'
import { MIN_PUBLIC_YEAR } from '../calendar'
import { DegradedDataNotice } from '../components/Common'
import { compareDecimal, decimalRatioGreaterThan, decimalSign } from '../decimal'
import { estimatedMoney, money, tokens } from '../format'
import { useCachedAsync } from '../hooks'
import type { HeatmapDay, OverviewYearResponse, PeriodSummary } from '../types'

const DAY_NAMES = ['MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN']
const MONTH_NAMES = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
const DAY_MS = 86_400_000
const HOVERCARD_GAP = 10
const VIEWPORT_MARGIN = 8

function dateOnly(date: Date) {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`
}

function calendarDay(date: Date) {
  return Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()) / DAY_MS
}

export interface HeatmapCardAnchor { left: number; right: number; top: number; bottom: number }
export interface HeatmapCardSize { width: number; height: number }
export interface HeatmapViewport { width: number; height: number }

// Pure geometry helpers live beside the component so their DOM contract stays
// reviewable with the heatmap implementation.
// eslint-disable-next-line react-refresh/only-export-components
export function placeHeatmapCard(
  anchor: HeatmapCardAnchor,
  card: HeatmapCardSize,
  viewport: HeatmapViewport,
  gap = HOVERCARD_GAP,
  margin = VIEWPORT_MARGIN,
) {
  const maxLeft = Math.max(margin, viewport.width - card.width - margin)
  const maxTop = Math.max(margin, viewport.height - card.height - margin)
  let left = anchor.right + gap
  if (left + card.width > viewport.width - margin) left = anchor.left - gap - card.width
  let top = anchor.bottom + gap
  if (top + card.height > viewport.height - margin) top = anchor.top - gap - card.height
  return {
    left: Math.min(Math.max(left, margin), maxLeft),
    top: Math.min(Math.max(top, margin), maxTop),
  }
}

// eslint-disable-next-line react-refresh/only-export-components
export function buildAnnualHeatmapLayout(year: number, days: HeatmapDay[], now = new Date(), includeCells = true) {
  const first = new Date(year, 0, 1)
  const mondayOffset = (first.getDay() + 6) % 7
  const gridStart = new Date(year, 0, 1 - mondayOffset)
  const byDate = new Map(days.map(day => [day.date, day]))
  const last = new Date(year, 11, 31)
  const weekCount = Math.floor((calendarDay(last) - calendarDay(gridStart)) / 7) + 1
  const cells: Array<{ day: HeatmapDay; week: number; row: number }> = []
  if (includeCells) {
    for (let week = 0; week < weekCount; week += 1) {
      for (let row = 0; row < 7; row += 1) {
        const value = new Date(gridStart)
        value.setDate(gridStart.getDate() + week * 7 + row)
        if (value.getFullYear() !== year) continue
        const iso = dateOnly(value)
        const source = byDate.get(iso)
        const entry = {
          date: iso,
          costUsd: source ? source.costUsd : '0',
          sessionCount: source?.sessionCount ?? 0,
          messageCount: source?.messageCount ?? 0,
          totalTokens: source?.totalTokens ?? 0,
          future: source?.future ?? calendarDay(value) > calendarDay(now),
        }
        cells.push({ day: entry, week, row })
      }
    }
  }
  const months = MONTH_NAMES.map((name, month) => {
    const value = new Date(year, month, 1)
    return { name, month, week: Math.floor((calendarDay(value) - calendarDay(gridStart)) / 7) }
  })
  return { cells, months, weekCount }
}

function periodDelta(period: PeriodSummary) {
  if (period.totals.unpricedTokens > 0) return '—'
  if (period.deltaPercent === null || period.deltaPercent === undefined) return '—'
  return `${period.deltaPercent >= 0 ? '+' : ''}${period.deltaPercent.toFixed(1)}%`
}

function PeriodPanel({ period, label, className = '' }: { period: PeriodSummary; label: string; className?: string }) {
  return (
    <section className={`period-panel ${className}`}>
      <span className="eyebrow coral-text">{label}</span>
      <strong className="period-cost">{estimatedMoney(period.totals.costUsd, period.totals.unpricedTokens)}</strong>
      <span className="period-delta">{periodDelta(period)}</span>
      <span className="period-counts">{period.sessionCount} SESSIONS <i>·</i> {period.messageCount} MESSAGES</span>
    </section>
  )
}

function TodayPanel({ period }: { period: PeriodSummary }) {
  const hasActivity = period.sessionCount > 0
  // Period boundaries are UTC instants for the local midnight. Slicing the
  // serialized start produces yesterday east of UTC, so navigation must use
  // the browser's local calendar date instead.
  const date = dateOnly(new Date())
  return (
    <section className="today-panel">
      <span className="eyebrow coral-text">TODAY</span>
      <strong className="today-cost">{estimatedMoney(period.totals.costUsd ?? (hasActivity ? null : '0'), period.totals.unpricedTokens)}</strong>
      <div className="today-rule" />
      <div className="today-stats">
        <div><strong>{period.sessionCount} sessions</strong><strong>{period.messageCount} messages</strong></div>
        <div><strong>{tokens(period.totals.totalTokens)} API tokens</strong><span>{hasActivity && period.totals.unpricedTokens === 0 && period.deltaCostUsd != null ? `${decimalSign(period.deltaCostUsd) >= 0 ? '+' : ''}${money(period.deltaCostUsd)}` : hasActivity ? 'Price pending' : 'No activity yet today'}</span></div>
      </div>
      {hasActivity
        ? <Link to={`/sessions?${new URLSearchParams({ date })}`} className="text-link">VIEW TODAY’S SESSIONS <ArrowRight weight="bold" /></Link>
        : <span className="text-link disabled">NO SESSIONS TODAY</span>}
    </section>
  )
}

function SummarySkeleton() {
  return (
    <div className="overview-hero overview-summary-skeleton" aria-label="Loading overview summary" aria-busy="true">
      <section className="today-panel overview-skeleton-section">
        <span className="eyebrow coral-text">TODAY</span>
        <i className="skeleton-block skeleton-today-cost" />
        <div className="today-rule" />
        <i className="skeleton-block skeleton-copy" />
        <i className="skeleton-block skeleton-copy short" />
      </section>
      <div className="overview-summary">
        <div className="overview-title-row"><h1>Overview</h1><span>LOADING SUMMARY</span></div>
        <div className="overview-periods">
          <section className="period-panel"><span className="eyebrow coral-text">THIS WEEK</span><i className="skeleton-block skeleton-period-cost" /><i className="skeleton-block skeleton-copy" /></section>
          <section className="period-panel with-divider"><span className="eyebrow coral-text">THIS MONTH</span><i className="skeleton-block skeleton-period-cost" /><i className="skeleton-block skeleton-copy" /></section>
        </div>
      </div>
    </div>
  )
}

function LocalError({ label, error, onRetry, className = '' }: { label: string; error: Error; onRetry: () => void; className?: string }) {
  return (
    <div className={`overview-local-error ${className}`} role="alert">
      <span className="eyebrow coral-text">{label}</span>
      <strong>{error.message}</strong>
      <button type="button" onClick={onRetry}>TRY AGAIN</button>
    </div>
  )
}

function SummaryError({ error, onRetry }: { error: Error; onRetry: () => void }) {
  return (
    <div className="overview-hero overview-summary-error">
      <section className="today-panel"><LocalError label="COULDN’T LOAD SUMMARY" error={error} onRetry={onRetry} className="summary-local-error" /></section>
      <div className="overview-summary">
        <div className="overview-title-row"><h1>Overview</h1><span>SUMMARY UNAVAILABLE</span></div>
        <div className="overview-periods overview-error-periods" aria-hidden="true" />
      </div>
    </div>
  )
}

function formatHeatmapDate(value: string) {
  return new Intl.DateTimeFormat('en-US', { weekday: 'short', month: 'short', day: 'numeric', year: 'numeric' }).format(new Date(`${value}T12:00:00`)).toUpperCase()
}

interface HeatmapCardState {
  day: HeatmapDay
  anchor: HTMLButtonElement
  pinned: boolean
}

function AnnualHeatmap({
  year,
  days,
  loading,
  error,
  onRetry,
  onPrevious,
  onNext,
}: {
  year: number
  days: HeatmapDay[] | null
  loading: boolean
  error: Error | null
  onRetry: () => void
  onPrevious: () => void
  onNext: () => void
}) {
  const layout = useMemo(() => buildAnnualHeatmapLayout(year, days ?? [], new Date(), !error), [days, error, year])
  const maxCost = (days ?? []).reduce(
    (maximum, day) => day.costUsd != null && compareDecimal(day.costUsd, maximum) > 0 ? day.costUsd : maximum,
    '1',
  )
  const [card, setCard] = useState<HeatmapCardState | null>(null)
  const [cardPosition, setCardPosition] = useState<{ left: number; top: number } | null>(null)
  const [focusDate, setFocusDate] = useState<string | null>(null)
  const cardRef = useRef<HTMLDivElement>(null)
  const cardActionRef = useRef<HTMLAnchorElement>(null)
  const closeTimer = useRef<number | null>(null)
  const gridRef = useRef<HTMLDivElement>(null)
  const focusCardAction = useRef(false)
  const focusableCells = layout.cells.filter(cell => !cell.day.future)
  const todayDate = dateOnly(new Date())
  const activeCells = focusableCells.filter(cell => cell.day.sessionCount > 0 || cell.day.totalTokens > 0 || (cell.day.costUsd != null && decimalSign(cell.day.costUsd) > 0))
  const defaultFocusDate = year === new Date().getFullYear() && focusableCells.some(cell => cell.day.date === todayDate)
    ? todayDate
    : activeCells.at(-1)?.day.date ?? focusableCells.at(-1)?.day.date ?? null
  const rovingDate = focusableCells.some(cell => cell.day.date === focusDate) ? focusDate : defaultFocusDate

  const clearCloseTimer = () => {
    if (closeTimer.current !== null) window.clearTimeout(closeTimer.current)
    closeTimer.current = null
  }
  const scheduleClose = () => {
    clearCloseTimer()
    closeTimer.current = window.setTimeout(() => {
      setCard(current => current?.pinned ? current : null)
      closeTimer.current = null
    }, 120)
  }
  const openTransientCard = (day: HeatmapDay, anchor: HTMLButtonElement) => {
    clearCloseTimer()
    focusCardAction.current = false
    setCard(current => current?.day.date === day.date && current.pinned ? current : { day, anchor, pinned: false })
  }
  const pinCard = (day: HeatmapDay, anchor: HTMLButtonElement, focusAction: boolean) => {
    clearCloseTimer()
    if (card?.day.date === day.date && card.pinned) {
      focusCardAction.current = false
      setCard(null)
      return
    }
    focusCardAction.current = focusAction
    setCard({ day, anchor, pinned: true })
  }
  const moveRovingFocus = (date: string, key: string) => {
    const current = layout.cells.find(cell => cell.day.date === date)
    if (!current) return
    const target = key === 'ArrowLeft'
      ? { week: current.week - 1, row: current.row }
      : key === 'ArrowRight'
        ? { week: current.week + 1, row: current.row }
        : key === 'ArrowUp'
          ? { week: current.week, row: current.row - 1 }
          : { week: current.week, row: current.row + 1 }
    const next = layout.cells.find(cell => cell.week === target.week && cell.row === target.row)
    if (!next || next.day.future || loading || error) return
    setFocusDate(next.day.date)
    const nextTile = gridRef.current
      ?.querySelector<HTMLButtonElement>(`.heatmap-tile[data-date="${next.day.date}"]`)
    nextTile?.focus({ preventScroll: true })
    nextTile?.scrollIntoView?.({ block: 'nearest', inline: 'nearest' })
  }

  const tileFromTarget = (target: EventTarget | null) => {
    if (!(target instanceof Element)) return null
    const tile = target.closest<HTMLButtonElement>('.heatmap-tile[data-date]')
    return tile && gridRef.current?.contains(tile) ? tile : null
  }
  const dayForTile = (tile: HTMLButtonElement) => layout.cells.find(cell => cell.day.date === tile.dataset.date)?.day
  const pointerEnteredTile = (event: ReactPointerEvent<HTMLDivElement>) => {
    const tile = tileFromTarget(event.target)
    if (!tile || tile.disabled || event.pointerType === 'touch') return
    if (event.relatedTarget instanceof Node && tile.contains(event.relatedTarget)) return
    const day = dayForTile(tile)
    if (day) openTransientCard(day, tile)
  }
  const pointerLeftTile = (event: ReactPointerEvent<HTMLDivElement>) => {
    const tile = tileFromTarget(event.target)
    if (!tile || tile.disabled) return
    if (event.relatedTarget instanceof Node && tile.contains(event.relatedTarget)) return
    scheduleClose()
  }
  const focusedTile = (event: FocusEvent<HTMLDivElement>) => {
    const tile = tileFromTarget(event.target)
    if (!tile || tile.disabled) return
    const day = dayForTile(tile)
    if (!day) return
    setFocusDate(day.date)
    openTransientCard(day, tile)
  }
  const blurredTile = (event: FocusEvent<HTMLDivElement>) => {
    const tile = tileFromTarget(event.target)
    if (!tile || tile.disabled) return
    if (event.relatedTarget && cardRef.current?.contains(event.relatedTarget as Node)) return
    scheduleClose()
  }
  const pressedTileKey = (event: ReactKeyboardEvent<HTMLDivElement>) => {
    const tile = tileFromTarget(event.target)
    if (!tile || tile.disabled) return
    const day = dayForTile(tile)
    if (!day) return
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault()
      pinCard(day, tile, true)
      return
    }
    if (!['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown'].includes(event.key)) return
    event.preventDefault()
    moveRovingFocus(day.date, event.key)
  }
  const clickedTile = (event: ReactMouseEvent<HTMLDivElement>) => {
    const tile = tileFromTarget(event.target)
    if (!tile || tile.disabled) return
    const day = dayForTile(tile)
    if (day) pinCard(day, tile, event.detail === 0)
  }

  useEffect(() => {
    setCard(null)
    setCardPosition(null)
    focusCardAction.current = false
  }, [year])

  useEffect(() => setFocusDate(defaultFocusDate), [defaultFocusDate])

  useEffect(() => () => clearCloseTimer(), [])

  useEffect(() => {
    if (!card) return
    const closeOutside = (event: PointerEvent) => {
      const target = event.target as Node | null
      if (target && (card.anchor.contains(target) || cardRef.current?.contains(target))) return
      setCard(null)
    }
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key !== 'Escape') return
      if (cardRef.current?.contains(document.activeElement)) card.anchor.focus({ preventScroll: true })
      focusCardAction.current = false
      setCard(null)
    }
    document.addEventListener('pointerdown', closeOutside)
    document.addEventListener('keydown', closeOnEscape)
    return () => {
      document.removeEventListener('pointerdown', closeOutside)
      document.removeEventListener('keydown', closeOnEscape)
    }
  }, [card])

  useLayoutEffect(() => {
    if (!card?.pinned || !focusCardAction.current || !cardActionRef.current) return
    focusCardAction.current = false
    cardActionRef.current.focus({ preventScroll: true })
  }, [card])

  useLayoutEffect(() => {
    if (!card) return
    const updatePosition = () => {
      if (!card.anchor.isConnected || !cardRef.current) return
      const anchor = card.anchor.getBoundingClientRect()
      const popover = cardRef.current.getBoundingClientRect()
      setCardPosition(placeHeatmapCard(anchor, popover, { width: window.innerWidth, height: window.innerHeight }))
    }
    setCardPosition(null)
    updatePosition()
    window.addEventListener('resize', updatePosition)
    window.addEventListener('scroll', updatePosition, true)
    return () => {
      window.removeEventListener('resize', updatePosition)
      window.removeEventListener('scroll', updatePosition, true)
    }
  }, [card])

  const cardId = card ? `heatmap-card-${card.day.date}` : undefined
  return (
    <section className="annual-section" role="region" aria-label={`${year} yearly usage ledger`} tabIndex={0}>
      <div className="heatmap-header" style={{ '--heatmap-weeks': layout.weekCount } as CSSProperties}>
        <div className="year-control"><button type="button" aria-label="Previous year" aria-disabled={year <= MIN_PUBLIC_YEAR || undefined} onClick={() => { if (year <= MIN_PUBLIC_YEAR) return; setCard(null); setCardPosition(null); onPrevious() }}><CaretLeft weight="bold" /></button><strong>{year}</strong><button type="button" aria-label="Next year" aria-disabled={year >= new Date().getFullYear() || undefined} onClick={() => { if (year >= new Date().getFullYear()) return; setCard(null); setCardPosition(null); onNext() }}><CaretRight weight="bold" /></button></div>
        {layout.months.map(month => <span key={month.name} style={{ gridColumn: month.week + 2 }}>{month.name}</span>)}
      </div>
      <div className="heatmap-layout">
        <div className="weekday-labels" aria-hidden="true">{DAY_NAMES.map(day => <span key={day}>{day}</span>)}</div>
        <div
          ref={gridRef}
          className={`heatmap-grid ${loading ? 'loading' : ''}`}
          role="group"
          aria-label={loading ? `Loading ${year} yearly usage` : `${year} usage by day`}
          aria-busy={loading}
          style={{ '--heatmap-weeks': layout.weekCount } as CSSProperties}
          onPointerOver={pointerEnteredTile}
          onPointerOut={pointerLeftTile}
          onFocus={focusedTile}
          onBlur={blurredTile}
          onKeyDown={pressedTileKey}
          onClick={clickedTile}
        >
          {!loading && !error && layout.cells.map(({ day, week, row }) => {
            const intensity = day.costUsd === null
              ? 'unknown'
              : decimalSign(day.costUsd) === 0
                ? 'zero'
                : decimalRatioGreaterThan(day.costUsd, maxCost, 55n, 100n)
                  ? 'high'
                  : decimalRatioGreaterThan(day.costUsd, maxCost, 20n, 100n) ? 'medium' : 'low'
            const isToday = day.date === todayDate
            const expanded = card?.day.date === day.date
            const disabled = loading || Boolean(error) || Boolean(day.future)
            return (
              <button
                key={day.date}
                data-date={day.date}
                type="button"
                className={`heatmap-tile ${intensity} ${isToday ? 'today' : ''} ${expanded ? 'selected' : ''}`}
                style={{ gridColumn: week + 1, gridRow: row + 1 }}
                disabled={disabled}
                tabIndex={!disabled && day.date === rovingDate ? 0 : -1}
                aria-label={`${day.date}: ${money(day.costUsd)}, ${day.sessionCount} sessions, ${day.messageCount ?? 0} messages, ${tokens(day.totalTokens)} API tokens`}
                aria-current={isToday ? 'date' : undefined}
                aria-haspopup="dialog"
                aria-expanded={expanded}
                aria-controls={expanded ? cardId : undefined}
              />
            )
          })}
          {error && <LocalError label={`COULDN’T LOAD ${year}`} error={error} onRetry={onRetry} className="heatmap-local-error" />}
        </div>
      </div>
      {card && createPortal(
        <div
          ref={cardRef}
          id={cardId}
          className={`heatmap-popover ${card.pinned ? 'pinned' : ''}`}
          role="dialog"
          aria-modal="false"
          aria-label={`${card.day.date} usage details`}
          style={{ left: cardPosition?.left ?? 0, top: cardPosition?.top ?? 0, visibility: cardPosition ? 'visible' : 'hidden' }}
          onPointerEnter={clearCloseTimer}
          onPointerLeave={scheduleClose}
          onFocus={clearCloseTimer}
          onBlur={(event: FocusEvent<HTMLDivElement>) => {
            if (event.relatedTarget && (cardRef.current?.contains(event.relatedTarget as Node) || card.anchor.contains(event.relatedTarget as Node))) return
            scheduleClose()
          }}
        >
          <span className="eyebrow coral-text">{formatHeatmapDate(card.day.date)}</span>
          <strong>{money(card.day.costUsd)}</strong>
          <div />
          <span>{card.day.sessionCount} sessions · {card.day.messageCount ?? 0} messages</span>
          <span>{tokens(card.day.totalTokens)} API tokens</span>
          <Link ref={cardActionRef} to={`/sessions?${new URLSearchParams({ date: card.day.date })}`}>VIEW SESSIONS <ArrowRight weight="bold" /></Link>
        </div>,
        document.body,
      )}
    </section>
  )
}

function yearSessionsUrl(year: number, project?: string) {
  const values: Record<string, string> = {
    start: `${year}-01-01`,
    end: `${year}-12-31`,
  }
  if (project) values.project = project
  values.sort = 'cost'
  return `/sessions?${new URLSearchParams(values)}`
}

function sessionDate(value: string) {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return '—'
  return new Intl.DateTimeFormat('en-US', { month: 'short', day: 'numeric' }).format(date).toUpperCase()
}

function BottomSkeleton({ year }: { year: number }) {
  return (
    <div className="overview-bottom" aria-label={`Loading ${year} overview details`} aria-busy="true">
      <section className="drivers-card overview-card-loading">
        <header><h2>TOP PROJECTS · {year}</h2></header>
        {Array.from({ length: 3 }, (_, index) => <div className="bottom-skeleton-row driver-skeleton-row" key={index}><i /></div>)}
      </section>
      <section className="recent-card overview-card-loading">
        <header><h2>TOP SESSIONS · {year}</h2></header>
        <div className="recent-head"><span>#</span><span>SESSION</span><span>PROJECT</span><span>DATE</span><span>COST</span><span>MESSAGES</span><span>API TOKENS</span></div>
        {Array.from({ length: 3 }, (_, index) => <div className="bottom-skeleton-row recent-skeleton-row" key={index}><i /></div>)}
      </section>
    </div>
  )
}

function BottomError({ year, error, onRetry }: { year: number; error: Error; onRetry: () => void }) {
  return (
    <div className="overview-bottom">
      <section className="drivers-card">
        <header><h2>TOP PROJECTS · {year}</h2></header>
        <LocalError label="PROJECTS UNAVAILABLE" error={error} onRetry={onRetry} className="bottom-local-error" />
      </section>
      <section className="recent-card">
        <header><h2>TOP SESSIONS · {year}</h2></header>
        <LocalError label="SESSIONS UNAVAILABLE" error={error} onRetry={onRetry} className="bottom-local-error" />
      </section>
    </div>
  )
}

function YearBottom({ year, data }: { year: number; data: OverviewYearResponse }) {
  const allSessionsUrl = yearSessionsUrl(year)
  return (
    <div className="overview-bottom">
      <section className="drivers-card">
        <header><h2>TOP PROJECTS · {year}</h2><Link to={allSessionsUrl}>VIEW YEAR <ArrowRight weight="bold" /></Link></header>
        {data.topProjects.length === 0
          ? <div className="overview-empty-state"><strong>No project usage in {year}.</strong></div>
          : data.topProjects.slice(0, 3).map((driver, index) => (
            <Link className="driver-row" to={yearSessionsUrl(year, driver.project)} key={driver.project}><span>{String(index + 1).padStart(2, '0')}</span><strong title={driver.project}>{driver.project.split('/').filter(Boolean).at(-1)}</strong><b>{money(driver.costUsd)}</b><em>{driver.share === null ? '—' : `${Math.round(driver.share * 100)}%`}</em></Link>
          ))}
      </section>
      <section className="recent-card">
        <header><h2>TOP SESSIONS · {year}</h2><Link to={allSessionsUrl}>VIEW YEAR SESSIONS <ArrowRight weight="bold" /></Link></header>
        <div className="recent-head"><span>#</span><span>SESSION</span><span>PROJECT</span><span>DATE</span><span>COST</span><span>MESSAGES</span><span>API TOKENS</span></div>
        {data.topSessions.length === 0 && <div className="recent-empty">No sessions recorded in {year}.</div>}
        {data.topSessions.slice(0, 3).map((session, index) => (
          <Link to={`/sessions/${session.id}`} className="recent-row" key={session.id}><span>{String(index + 1).padStart(2, '0')}</span><strong>{session.title}</strong><i>{session.project ?? '—'}</i><time dateTime={session.lastEventAt}>{sessionDate(session.lastEventAt)}</time><b>{estimatedMoney(session.costUsd, session.unpricedTokens)}</b><b>{session.messageCount}</b><b>{tokens(session.totalTokens)}</b></Link>
        ))}
      </section>
    </div>
  )
}

export function OverviewPage() {
  const [year, setYear] = useState(new Date().getFullYear())
  const summary = useCachedAsync('overview', signal => api.overview(signal), [], 30_000, 30_000)

  return (
    <div className="overview-page">
      {summary.data && summary.error && <DegradedDataNotice error={summary.error} lastSuccessfulAt={summary.lastSuccessfulAt} onRetry={() => void summary.refresh()} />}
      {summary.data
        ? (
          <div className="overview-hero">
            <TodayPanel period={summary.data.periods.today} />
            <div className="overview-summary">
              <div className="overview-title-row"><h1>Overview</h1></div>
              <div className="overview-periods"><PeriodPanel period={summary.data.periods.week} label="THIS WEEK" /><PeriodPanel period={summary.data.periods.month} label="THIS MONTH" className="with-divider" /></div>
            </div>
          </div>
        )
        : summary.error && !summary.loading
          ? <SummaryError error={summary.error} onRetry={() => void summary.refresh()} />
          : <SummarySkeleton />}
      <YearOverviewSections
        year={year}
        onPrevious={() => setYear(value => Math.max(MIN_PUBLIC_YEAR, value - 1))}
        onNext={() => setYear(value => value + 1)}
      />
    </div>
  )
}

function YearOverviewSections({
  year,
  onPrevious,
  onNext,
}: {
  year: number
  onPrevious: () => void
  onNext: () => void
}) {
  const yearly = useCachedAsync(`overview-year:${year}`, signal => api.overviewYear(year, signal), [year], 30_000, 30_000)
  const yearData = yearly.data?.year === year ? yearly.data : null
  const yearError = !yearData && !yearly.loading ? yearly.error : null

  return (
    <>
      {yearData && yearly.error && <DegradedDataNotice error={yearly.error} lastSuccessfulAt={yearly.lastSuccessfulAt} onRetry={() => void yearly.refresh()} />}
      <AnnualHeatmap
        year={year}
        days={yearData?.heatmap ?? null}
        loading={!yearData && !yearError}
        error={yearError}
        onRetry={() => void yearly.refresh()}
        onPrevious={onPrevious}
        onNext={onNext}
      />
      {yearData
        ? <YearBottom year={year} data={yearData} />
        : yearError
          ? <BottomError year={year} error={yearError} onRetry={() => void yearly.refresh()} />
          : <BottomSkeleton year={year} />}
    </>
  )
}
