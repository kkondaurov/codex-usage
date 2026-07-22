import { CaretDown } from '@phosphor-icons/react'
import { useEffect, useMemo, useRef, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { api } from '../api'
import { validDateOnly } from '../calendar'
import { DegradedDataNotice, ErrorState, LoadingLedger, PageTitle, Pagination } from '../components/Common'
import { RangeCalendar } from '../components/RangeCalendar'
import type { DecimalString } from '../decimal'
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

function boundaryTime(value: string, inclusiveDateEnd: boolean) {
  if (!validDateOnly(value)) return Date.parse(value)
  const boundary = new Date(`${value}T00:00:00`)
  if (inclusiveDateEnd) boundary.setDate(boundary.getDate() + 1)
  return boundary.getTime()
}

function increasingRange(start: string, end: string) {
  return boundaryTime(end, true) > boundaryTime(start, false)
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
  const month = (value: Date) => new Intl.DateTimeFormat('en-US', { month: 'short' }).format(value).toUpperCase()
  if (!end) return `SINCE ${month(startDate)} ${startDate.getDate()}`
  const endDate = displayBoundary(end, true)
  if (startDate.getFullYear() === endDate.getFullYear() && startDate.getMonth() === endDate.getMonth()) {
    return startDate.getDate() === endDate.getDate() ? `${month(startDate)} ${startDate.getDate()}` : `${month(startDate)} ${startDate.getDate()}–${endDate.getDate()}`
  }
  return `${month(startDate)} ${startDate.getDate()}–${month(endDate)} ${endDate.getDate()}`
}

function SortHeader({ label, value, sort, onSort }: { label: string; value: SessionSort; sort: SessionSort; onSort: (value: SessionSort) => void }) {
  return <span className="sessions-sort-cell" role="columnheader"><button type="button" className={sort === value ? 'sort-active' : ''} aria-pressed={sort === value} onClick={() => onSort(value)}>{sort === value && <CaretDown weight="fill" />}{label}</button></span>
}

function SessionCost({ costUsd, unpricedTokens, lifetimeCostUsd, lifetimeUnpricedTokens }: {
  costUsd: DecimalString | null
  unpricedTokens: number
  lifetimeCostUsd: DecimalString | null
  lifetimeUnpricedTokens: number
}) {
  const periodCost = estimatedMoney(costUsd, unpricedTokens)
  const lifetimeCost = estimatedMoney(lifetimeCostUsd, lifetimeUnpricedTokens)
  const costsDiffer = periodCost !== lifetimeCost

  return (
    <b className="session-cost" role="cell">
      {costsDiffer
        ? <><span className="scoped-cost"><small>PERIOD</small>{periodCost}</span><span className="scoped-cost lifetime-cost"><small>ALL TIME</small>{lifetimeCost}</span></>
        : <span>{periodCost}</span>}
      {unpricedTokens > 0 && <small>Unknown price</small>}
    </b>
  )
}

export function SessionsPage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const [search, setSearch] = useState(searchParams.get('q') ?? '')
  const [calendarOpen, setCalendarOpen] = useState(false)
  const [projectOpen, setProjectOpen] = useState(false)
  const [projectSearch, setProjectSearch] = useState('')
  const [projectActiveValue, setProjectActiveValue] = useState<string | null>(null)
  const dateFilterRef = useRef<HTMLButtonElement>(null)
  const projectFilterRef = useRef<HTMLDivElement>(null)
  const projectButtonRef = useRef<HTMLButtonElement>(null)
  const projectMenuRef = useRef<HTMLDivElement>(null)
  const projectSearchRef = useRef<HTMLInputElement>(null)
  const projectOpenIndexRef = useRef<number | null>(null)
  const paginationRef = useRef<{ page: number; totalPages: number; total: number; pageSize: number } | null>(null)
  const projectChoicesRef = useRef<string[]>([])
  const querySearch = searchParams.get('q') ?? ''
  const rawDate = searchParams.get('date')
  const rawStart = searchParams.get('start')
  const rawEnd = searchParams.get('end')
  const directDate = validDateOnly(rawDate) ? rawDate : null
  const rangeStart = validBoundary(rawStart) ? rawStart : null
  const rangeEnd = validBoundary(rawEnd) ? rawEnd : null
  const rangeEndIsUsable = Boolean(rangeStart && rangeEnd && increasingRange(rangeStart, rangeEnd))
  const canonicalRangeEnd = rangeEndIsUsable ? rangeEnd : null
  const start = directDate ?? rangeStart
  const end = directDate ?? (rangeStart ? canonicalRangeEnd : null)
  const rawProject = searchParams.get('project')
  const project = rawProject || null
  const rawSort = searchParams.get('sort')
  const sort: SessionSort = rawSort === 'cost' ? 'cost' : 'recent'
  const rawPageValue = searchParams.get('page')
  const rawPage = Number(rawPageValue ?? 1)
  const validPageParameter = rawPageValue === null
    || (/^[1-9]\d*$/.test(rawPageValue) && Number.isSafeInteger(rawPage))
  const page = Number.isSafeInteger(rawPage) && rawPage > 0 ? rawPage : 1

  useEffect(() => { setSearch(current => current === querySearch ? current : querySearch) }, [querySearch])
  useEffect(() => {
    const next = new URLSearchParams(searchParams)
    let changed = false
    if (rawDate && !directDate) { next.delete('date'); changed = true }
    if (directDate) {
      if (rawStart) { next.delete('start'); changed = true }
      if (rawEnd) { next.delete('end'); changed = true }
    } else {
      if (rawStart && !rangeStart) { next.delete('start'); changed = true }
      if (rawEnd && (!rangeEnd || !rangeStart || !rangeEndIsUsable)) { next.delete('end'); changed = true }
    }
    if (rawProject !== null && !project) { next.delete('project'); changed = true }
    if (rawSort && rawSort !== 'recent' && rawSort !== 'cost') { next.delete('sort'); changed = true }
    if (!validPageParameter) { next.delete('page'); changed = true }
    if (changed) setSearchParams(next, { replace: true })
  }, [directDate, project, rangeEnd, rangeEndIsUsable, rangeStart, rawDate, rawEnd, rawPageValue, rawProject, rawSort, rawStart, searchParams, setSearchParams, validPageParameter])
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
  if (data) {
    paginationRef.current = data.total > 0
      ? { page: data.page, totalPages: Math.max(1, data.totalPages), total: data.total, pageSize: data.pageSize }
      : null
    projectChoicesRef.current = data.projects.length > 0
      ? [...data.projects]
      : data.items.map(item => item.project).filter((value): value is string => Boolean(value))
  }
  const pagination = data?.total ? {
    page: data.page,
    totalPages: Math.max(1, data.totalPages),
    total: data.total,
    pageSize: data.pageSize,
  } : paginationRef.current
  const paginationUnavailable = loading || (!data && Boolean(error))

  const projectValues = [...projectChoicesRef.current]
  if (project) projectValues.push(project)
  const projects = [...new Set(projectValues)].sort((a, b) => a.localeCompare(b))

  const projectOptions = useMemo(() => {
    const needle = projectSearch.trim().toLocaleLowerCase()
    const options: Array<{ value: string | null; label: string }> = []
    if (!needle || 'all projects'.includes(needle)) options.push({ value: null, label: 'ALL PROJECTS' })
    options.push(...projects
      .filter(option => option.toLocaleLowerCase().includes(needle))
      .map(option => ({ value: option, label: option })))
    return options
  }, [projectSearch, projects])
  const matchingProjectActiveIndex = projectOptions.findIndex(option => option.value === projectActiveValue)
  const projectActiveIndex = matchingProjectActiveIndex >= 0 ? matchingProjectActiveIndex : 0

  useEffect(() => {
    if (!data || page <= Math.max(1, data.totalPages)) return
    const next = new URLSearchParams(searchParams)
    next.set('page', String(Math.max(1, data.totalPages)))
    setSearchParams(next, { replace: true })
  }, [data, page, searchParams, setSearchParams])
  useEffect(() => {
    if (!projectOpen) return
    const targetIndex = projectOpenIndexRef.current ?? 0
    projectOpenIndexRef.current = null
    window.requestAnimationFrame(() => {
      projectSearchRef.current?.focus()
      document.getElementById(`project-option-${targetIndex}`)?.scrollIntoView?.({ block: 'nearest' })
    })
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
  }, [projectOpen])
  useEffect(() => {
    if (projectOptions.length > 0 && matchingProjectActiveIndex < 0) {
      setProjectActiveValue(projectOptions[0].value)
    }
  }, [matchingProjectActiveIndex, projectOptions])

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
    setProjectSearch('')
    setProjectOpen(false)
    window.requestAnimationFrame(() => projectButtonRef.current?.focus())
  }

  function clearDateFilter() {
    setCalendarOpen(false)
    update({ date: null, start: null, end: null })
    window.requestAnimationFrame(() => dateFilterRef.current?.focus())
  }

  function clearProjectFilter() {
    setProjectSearch('')
    setProjectOpen(false)
    update({ project: null })
    window.requestAnimationFrame(() => projectButtonRef.current?.focus())
  }

  function openProjectMenu(index: number | null = null) {
    const selectedIndex = project ? projects.indexOf(project) + 1 : 0
    const targetIndex = Math.min(index ?? Math.max(0, selectedIndex), projects.length)
    projectOpenIndexRef.current = targetIndex
    setProjectActiveValue(targetIndex === 0 ? null : projects[targetIndex - 1])
    setProjectSearch('')
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

  function handleProjectSearchKeyDown(event: React.KeyboardEvent<HTMLInputElement>) {
    const optionCount = projectOptions.length
    if (event.key === 'Escape') {
      event.preventDefault()
      event.stopPropagation()
      setProjectOpen(false)
      window.requestAnimationFrame(() => projectButtonRef.current?.focus())
    } else if (event.key === 'Tab') {
      setProjectOpen(false)
    } else if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
      event.preventDefault()
      if (optionCount === 0) return
      const step = event.key === 'ArrowDown' ? 1 : -1
      const nextIndex = (projectActiveIndex + step + optionCount) % optionCount
      setProjectActiveValue(projectOptions[nextIndex].value)
      window.requestAnimationFrame(() => document.getElementById(`project-option-${nextIndex}`)?.scrollIntoView?.({ block: 'nearest' }))
    } else if (event.key === 'Home' || event.key === 'End') {
      event.preventDefault()
      if (optionCount === 0) return
      const nextIndex = event.key === 'Home' ? 0 : optionCount - 1
      setProjectActiveValue(projectOptions[nextIndex].value)
      window.requestAnimationFrame(() => document.getElementById(`project-option-${nextIndex}`)?.scrollIntoView?.({ block: 'nearest' }))
    } else if (event.key === 'Enter' && optionCount > 0) {
      event.preventDefault()
      selectProject(projectOptions[projectActiveIndex]?.value ?? null)
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
            <button ref={projectButtonRef} type="button" className={`filter-button ${project ? 'selected' : ''}`} aria-haspopup="listbox" aria-controls="project-options" aria-expanded={projectOpen} onKeyDown={handleProjectButtonKeyDown} onClick={() => { if (projectOpen) setProjectOpen(false); else openProjectMenu() }}><span>{project ?? 'ALL PROJECTS'}</span>{!project && <b>SELECT</b>}</button>
            {project && <button className="clear-filter-button project-clear-button" type="button" aria-label="Clear project filter" onClick={clearProjectFilter}>CLEAR</button>}
            {projectOpen && <div ref={projectMenuRef} className="project-menu">
              <label className="project-menu-search"><span>SEARCH PROJECTS</span><input ref={projectSearchRef} type="search" role="combobox" aria-label="Search projects" aria-autocomplete="list" aria-controls="project-options" aria-expanded="true" aria-activedescendant={projectOptions.length > 0 ? `project-option-${projectActiveIndex}` : undefined} value={projectSearch} onChange={event => { setProjectSearch(event.target.value); setProjectActiveValue(null) }} onKeyDown={handleProjectSearchKeyDown} /></label>
              <div id="project-options" className="project-menu-options" role="listbox" aria-label="Projects">
                {projectOptions.map((option, index) => <button id={`project-option-${index}`} type="button" role="option" aria-selected={project === option.value} className={projectActiveIndex === index ? 'active' : ''} tabIndex={-1} key={option.value === null ? 'all-projects-sentinel' : `project:${option.value}`} onMouseEnter={() => setProjectActiveValue(option.value)} onClick={() => selectProject(option.value)}>{option.label}</button>)}
                {projectOptions.length === 0 && <span className="project-menu-empty">NO PROJECTS FOUND</span>}
              </div>
            </div>}
          </div>
        </div>
      </div>
      <span className="sr-only" aria-live="polite">{loading ? 'Loading sessions' : data ? `${data.total} sessions loaded` : ''}</span>
      {error && data ? <DegradedDataNotice error={error} lastSuccessfulAt={lastSuccessfulAt} onRetry={() => void refresh()} /> : null}
      <section className="page-ledger-frame sessions-ledger" aria-busy={loading || undefined}>
          <div className="ledger-scroll sessions-scroll" role="region" aria-label="Scrollable sessions ledger" tabIndex={0}>
            <div className="ledger-banner">{data ? `${data.total.toLocaleString()} RESULTS` : loading ? 'LOADING RESULTS' : 'RESULTS UNAVAILABLE'}</div>
            <div className="sessions-table" role="table" aria-label="Sessions" aria-rowcount={data ? data.total + 1 : undefined}>
              <div className="sessions-head" role="row">
                <span role="columnheader">SESSION</span><span role="columnheader">PROJECT / BRANCH</span>
                <SortHeader label="LAST ACTIVITY" value="recent" sort={sort} onSort={value => update({ sort: value })} />
                <SortHeader label="COST" value="cost" sort={sort} onSort={value => update({ sort: value })} />
                <span role="columnheader">AGENTS / TOOLS</span><span role="columnheader">API TOKENS</span><span role="columnheader">MESSAGES</span>
              </div>
              {!data && loading ? <div className="table-state-row" role="row"><div role="cell" aria-colspan={7}><LoadingLedger rows={8} /></div></div> : null}
              {error && !data ? <div className="table-state-row" role="row"><div role="cell" aria-colspan={7}><ErrorState error={error} onRetry={() => void refresh()} /></div></div> : null}
              {data?.items.length === 0 ? (
              <div className="table-state-row" role="row"><div className="no-results" role="cell" aria-colspan={7}><strong>NO SESSIONS FOUND</strong><span>{querySearch ? `No sessions match “${querySearch}”.` : 'No sessions match the selected filters.'}</span><button type="button" className="clear-results" onClick={() => { setSearch(''); setSearchParams({}) }}>CLEAR FILTERS</button></div></div>
            ) : data?.items.map(session => (
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
          </div>
          {pagination && <Pagination {...pagination} busy={paginationUnavailable} onPage={value => update({ page: String(value) })} />}
        </section>
    </div>
  )
}
