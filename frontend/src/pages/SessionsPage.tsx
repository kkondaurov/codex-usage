import { CaretDown } from '@phosphor-icons/react'
import { useEffect, useMemo, useRef, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { api } from '../api'
import { validDateOnly } from '../calendar'
import { DegradedDataNotice, ErrorState, LoadingLedger, PageTitle, Pagination } from '../components/Common'
import { RangeCalendar } from '../components/RangeCalendar'
import { estimatedMoney, shortDateTime, tokens } from '../format'
import { useAsync } from '../hooks'
import type { SessionSort } from '../types'

const rfc3339Pattern = /^(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(?:Z|[+-](\d{2}):(\d{2}))$/

function validBoundary(value: string | null) {
  if (!value) return false
  if (validDateOnly(value)) return true
  const match = rfc3339Pattern.exec(value)
  if (!match || !validDateOnly(match[1])) return false
  const [, , hour, minute, second, offsetHour, offsetMinute] = match
  return Number(hour) <= 23
    && Number(minute) <= 59
    && Number(second) <= 59
    && (offsetHour === undefined || Number(offsetHour) <= 23)
    && (offsetMinute === undefined || Number(offsetMinute) <= 59)
    && Number.isFinite(Date.parse(value))
}

function displayBoundary(value: string, exclusiveEnd = false) {
  if (!value.includes('T')) return new Date(`${value.slice(0, 10)}T12:00:00`)
  const boundary = new Date(value)
  if (exclusiveEnd) boundary.setTime(boundary.getTime() - 1)
  return boundary
}

function humanRange(start: string | null, end: string | null) {
  if (!start) return 'ALL DATES'
  const startDate = displayBoundary(start)
  const endDate = end ? displayBoundary(end, true) : startDate
  const month = (value: Date) => new Intl.DateTimeFormat('en-US', { month: 'short' }).format(value).toUpperCase()
  if (startDate.getFullYear() === endDate.getFullYear() && startDate.getMonth() === endDate.getMonth()) {
    return startDate.getDate() === endDate.getDate() ? `${month(startDate)} ${startDate.getDate()}` : `${month(startDate)} ${startDate.getDate()}–${endDate.getDate()}`
  }
  return `${month(startDate)} ${startDate.getDate()}–${month(endDate)} ${endDate.getDate()}`
}

function SortHeader({ label, value, sort, onSort }: { label: string; value: SessionSort; sort: SessionSort; onSort: (value: SessionSort) => void }) {
  return <span className="sessions-sort-cell" role="columnheader"><button type="button" className={sort === value ? 'sort-active' : ''} aria-pressed={sort === value} onClick={() => onSort(value)}>{sort === value && <CaretDown weight="fill" />}{label}</button></span>
}

function SessionCost({ costUsd, unpricedTokens, lifetimeCostUsd, lifetimeUnpricedTokens }: {
  costUsd: number | null
  unpricedTokens: number
  lifetimeCostUsd: number | null
  lifetimeUnpricedTokens: number
}) {
  const periodCost = estimatedMoney(costUsd, unpricedTokens)
  const lifetimeCost = estimatedMoney(lifetimeCostUsd, lifetimeUnpricedTokens)

  return (
    <b className="session-cost" role="cell">
      <span>{periodCost}</span>
      {periodCost !== lifetimeCost && <span className="lifetime-cost">{lifetimeCost}</span>}
      {unpricedTokens > 0 && <small>Unknown price</small>}
    </b>
  )
}

export function SessionsPage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const [search, setSearch] = useState(searchParams.get('q') ?? '')
  const [calendarOpen, setCalendarOpen] = useState(false)
  const [projectOpen, setProjectOpen] = useState(false)
  const [projectActiveIndex, setProjectActiveIndex] = useState(0)
  const dateFilterRef = useRef<HTMLButtonElement>(null)
  const projectFilterRef = useRef<HTMLDivElement>(null)
  const projectButtonRef = useRef<HTMLButtonElement>(null)
  const projectMenuRef = useRef<HTMLDivElement>(null)
  const projectOpenIndexRef = useRef<number | null>(null)
  const querySearch = searchParams.get('q') ?? ''
  const rawDate = searchParams.get('date')
  const rawStart = searchParams.get('start')
  const rawEnd = searchParams.get('end')
  const directDate = validDateOnly(rawDate) ? rawDate : null
  const rangeStart = validBoundary(rawStart) ? rawStart : null
  const rangeEnd = validBoundary(rawEnd) ? rawEnd : null
  const start = directDate ?? rangeStart
  const end = directDate ?? (rangeStart ? rangeEnd : null)
  const rawProject = searchParams.get('project')
  const project = rawProject && rawProject !== 'all' ? rawProject : null
  const sort: SessionSort = searchParams.get('sort') === 'cost' ? 'cost' : 'recent'
  const rawPage = Number(searchParams.get('page') ?? 1)
  const page = Number.isSafeInteger(rawPage) && rawPage > 0 ? rawPage : 1

  useEffect(() => { setSearch(current => current === querySearch ? current : querySearch) }, [querySearch])
  useEffect(() => {
    const next = new URLSearchParams(searchParams)
    let changed = false
    if (rawDate && !directDate) { next.delete('date'); changed = true }
    if (rawStart && !rangeStart) { next.delete('start'); changed = true }
    if (rawEnd && (!rangeEnd || (!directDate && !rangeStart))) { next.delete('end'); changed = true }
    if (rawProject && !project) { next.delete('project'); changed = true }
    if (changed) setSearchParams(next, { replace: true })
  }, [directDate, project, rangeEnd, rangeStart, rawDate, rawEnd, rawProject, rawStart, searchParams, setSearchParams])
  useEffect(() => {
    const normalizedSearch = search.trim()
    if (normalizedSearch === querySearch) return
    const timer = window.setTimeout(() => {
      const next = new URLSearchParams(searchParams)
      if (normalizedSearch) next.set('q', normalizedSearch); else next.delete('q')
      next.delete('page')
      setSearchParams(next, { replace: true })
    }, 220)
    return () => window.clearTimeout(timer)
  }, [querySearch, search, searchParams, setSearchParams])

  const queryKey = JSON.stringify([querySearch, directDate, start, end, project, sort, page])
  const { data, error, loading, lastSuccessfulAt, refresh } = useAsync(signal => api.sessions({
    q: querySearch || undefined,
    date: directDate ?? undefined,
    start: directDate ? undefined : start ?? undefined,
    end: directDate ? undefined : end ?? undefined,
    project: project ?? undefined,
    sort,
    page,
  }, signal), [queryKey], 30_000)

  const projects = useMemo(() => {
    const values = data?.projects?.length ? [...data.projects] : data?.items.map(item => item.project).filter((value): value is string => Boolean(value)) ?? []
    if (project) values.push(project)
    return [...new Set(values)].sort((a, b) => a.localeCompare(b))
  }, [data, project])

  useEffect(() => {
    if (!data || page <= Math.max(1, data.totalPages)) return
    const next = new URLSearchParams(searchParams)
    next.set('page', String(Math.max(1, data.totalPages)))
    setSearchParams(next, { replace: true })
  }, [data, page, searchParams, setSearchParams])
  useEffect(() => {
    if (!projectOpen) return
    const options = projectMenuRef.current?.querySelectorAll<HTMLElement>('[role="menuitemradio"]')
    const requestedIndex = projectOpenIndexRef.current
    const selectedIndex = project ? projects.indexOf(project) + 1 : 0
    const targetIndex = requestedIndex ?? Math.max(0, selectedIndex)
    const boundedTargetIndex = Math.min(targetIndex, Math.max(0, options?.length ? options.length - 1 : 0))
    projectOpenIndexRef.current = null
    setProjectActiveIndex(boundedTargetIndex)
    window.requestAnimationFrame(() => options?.[boundedTargetIndex]?.focus())
    const closeOutside = (event: MouseEvent) => {
      if (!projectFilterRef.current?.contains(event.target as Node)) setProjectOpen(false)
    }
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        event.preventDefault()
        setProjectOpen(false)
        window.requestAnimationFrame(() => projectButtonRef.current?.focus())
      }
    }
    document.addEventListener('mousedown', closeOutside)
    document.addEventListener('keydown', closeOnEscape)
    return () => {
      document.removeEventListener('mousedown', closeOutside)
      document.removeEventListener('keydown', closeOnEscape)
    }
  }, [project, projectOpen, projects])

  function update(values: Record<string, string | null>) {
    const next = new URLSearchParams(searchParams)
    for (const [key, value] of Object.entries(values)) { if (value) next.set(key, value); else next.delete(key) }
    if (!('page' in values)) next.delete('page')
    setSearchParams(next)
  }

  function closeCalendar() {
    setCalendarOpen(false)
    window.requestAnimationFrame(() => dateFilterRef.current?.focus())
  }

  function selectProject(value: string | null) {
    update({ project: value })
    setProjectOpen(false)
    window.requestAnimationFrame(() => projectButtonRef.current?.focus())
  }

  function clearDateFilter() {
    setCalendarOpen(false)
    update({ date: null, start: null, end: null })
    window.requestAnimationFrame(() => dateFilterRef.current?.focus())
  }

  function clearProjectFilter() {
    setProjectOpen(false)
    update({ project: null })
    window.requestAnimationFrame(() => projectButtonRef.current?.focus())
  }

  function openProjectMenu(index: number | null = null) {
    projectOpenIndexRef.current = index
    setCalendarOpen(false)
    setProjectOpen(true)
  }

  function handleProjectButtonKeyDown(event: React.KeyboardEvent<HTMLButtonElement>) {
    if (event.key === 'ArrowDown') {
      event.preventDefault()
      openProjectMenu(0)
    } else if (event.key === 'ArrowUp') {
      event.preventDefault()
      openProjectMenu(projects.length)
    }
  }

  function handleProjectMenuKeyDown(event: React.KeyboardEvent<HTMLDivElement>) {
    const options = [...(projectMenuRef.current?.querySelectorAll<HTMLElement>('[role="menuitemradio"]') ?? [])]
    const index = options.indexOf(document.activeElement as HTMLElement)
    if (event.key === 'Escape') {
      event.preventDefault()
      setProjectOpen(false)
      window.requestAnimationFrame(() => projectButtonRef.current?.focus())
    } else if (event.key === 'Tab') {
      setProjectOpen(false)
    } else if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
      event.preventDefault()
      const step = event.key === 'ArrowDown' ? 1 : -1
      const nextIndex = (Math.max(0, index) + step + options.length) % options.length
      setProjectActiveIndex(nextIndex)
      options[nextIndex]?.focus()
    } else if (event.key === 'Home' || event.key === 'End') {
      event.preventDefault()
      const nextIndex = event.key === 'Home' ? 0 : options.length - 1
      setProjectActiveIndex(nextIndex)
      options[nextIndex]?.focus()
    }
  }

  return (
    <div className="sessions-page">
      <PageTitle>Sessions</PageTitle>
      <div className="session-filters">
        <label className="search-field"><input value={search} onChange={event => setSearch(event.target.value)} placeholder="Search title, ID, project or branch" aria-label="Search sessions" /></label>
        <div className="filter-row">
          <div className="filter-wrap">
            <button ref={dateFilterRef} type="button" className={`filter-button ${start ? 'selected' : ''}`} aria-haspopup="dialog" aria-controls="session-date-range-dialog" aria-expanded={calendarOpen} onClick={() => { setProjectOpen(false); setCalendarOpen(value => !value) }}><span>{humanRange(start, end)}</span>{!start && <b>SELECT</b>}</button>
            {start && <button className="clear-filter-button" type="button" aria-label="Clear date range" onClick={clearDateFilter}>CLEAR</button>}
            {calendarOpen && <RangeCalendar id="session-date-range-dialog" initialStart={start} initialEnd={end} onCancel={closeCalendar} onApply={(nextStart, nextEnd) => { update({ date: null, start: nextStart, end: nextEnd }); closeCalendar() }} />}
          </div>
          <div ref={projectFilterRef} className="filter-wrap project-filter">
            <button ref={projectButtonRef} type="button" className={`filter-button ${project ? 'selected' : ''}`} aria-haspopup="menu" aria-controls="project-menu" aria-expanded={projectOpen} onKeyDown={handleProjectButtonKeyDown} onClick={() => { if (projectOpen) setProjectOpen(false); else openProjectMenu() }}><span>{project ?? 'ALL PROJECTS'}</span>{!project && <b>SELECT</b>}</button>
            {project && <button className="clear-filter-button project-clear-button" type="button" aria-label="Clear project filter" onClick={clearProjectFilter}>CLEAR</button>}
            {projectOpen && <div ref={projectMenuRef} id="project-menu" className="project-menu" role="menu" aria-label="Projects" onKeyDown={handleProjectMenuKeyDown}><button type="button" role="menuitemradio" aria-checked={!project} tabIndex={projectActiveIndex === 0 ? 0 : -1} onClick={() => selectProject(null)}>ALL PROJECTS</button>{projects.map((option, index) => <button type="button" role="menuitemradio" aria-checked={project === option} tabIndex={projectActiveIndex === index + 1 ? 0 : -1} key={option} onClick={() => selectProject(option)}>{option}</button>)}</div>}
          </div>
        </div>
      </div>
      {error && !data ? <ErrorState error={error} onRetry={() => void refresh()} /> : null}
      {error && data ? <DegradedDataNotice error={error} lastSuccessfulAt={lastSuccessfulAt} onRetry={() => void refresh()} /> : null}
      {!data && loading ? <LoadingLedger rows={12} /> : null}
      {data && (
        <section className="page-ledger-frame sessions-ledger">
          <div className="ledger-banner">{data.total.toLocaleString()} RESULTS</div>
          <div className="sessions-table" role="table" aria-label="Sessions">
            <div className="sessions-head" role="row">
              <span role="columnheader">SESSION</span><span role="columnheader">PROJECT / BRANCH</span>
              <SortHeader label="LAST ACTIVITY" value="recent" sort={sort} onSort={value => update({ sort: value })} />
              <SortHeader label="COST" value="cost" sort={sort} onSort={value => update({ sort: value })} />
              <span role="columnheader">AGENTS / TOOLS</span><span role="columnheader">API TOKENS</span><span role="columnheader">MESSAGES</span>
            </div>
            {data.items.length === 0 ? (
            <div className="no-results"><strong>NO SESSIONS FOUND</strong><span>{querySearch ? `No sessions match “${querySearch}”.` : 'No sessions match the selected filters.'}</span><button type="button" className="clear-results" onClick={() => { setSearch(''); setSearchParams({}) }}>CLEAR FILTERS</button></div>
          ) : data.items.map(session => (
            <div className={`session-row ${session.unpricedTokens > 0 ? 'unknown-price' : ''}`} role="row" key={session.id}>
              <span role="cell"><Link className="session-row-link" aria-label={`Open session ${session.title || 'Untitled session'}`} to={`/sessions/${session.id}`} /><strong>{session.title || 'Untitled session'}</strong><small>{session.id.slice(0, 8)}</small></span>
              <span role="cell"><strong>{session.project || '—'}</strong><small>{session.branch || '—'}</small></span>
              <time role="cell">{shortDateTime(session.lastEventAt)}</time>
              <SessionCost costUsd={session.costUsd} unpricedTokens={session.unpricedTokens} lifetimeCostUsd={session.lifetimeCostUsd} lifetimeUnpricedTokens={session.lifetimeUnpricedTokens} />
              <b role="cell">{session.agentCount} / {session.toolCount}</b>
              <b role="cell">{tokens(session.totalTokens)}</b>
              <b role="cell">{session.messageCount.toLocaleString()}</b>
            </div>
          ))}
          </div>
          {data.total > 0 && <Pagination page={data.page} totalPages={Math.max(1, data.totalPages)} total={data.total} pageSize={data.pageSize} onPage={value => update({ page: String(value) })} />}
        </section>
      )}
    </div>
  )
}
