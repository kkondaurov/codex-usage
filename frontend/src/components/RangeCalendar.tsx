import { useEffect, useMemo, useRef, useState } from 'react'
import type { KeyboardEvent as ReactKeyboardEvent } from 'react'
import { shiftAnchor } from '../calendar'

const FOCUSABLE_SELECTOR = [
  'button:not([disabled]):not([tabindex="-1"])',
  'a[href]',
  'input:not([disabled])',
  'select:not([disabled])',
  'textarea:not([disabled])',
  '[tabindex]:not([tabindex="-1"])',
].join(',')

function isoDate(date: Date) {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`
}

function normalizeBoundary(value: string | null, isEnd: boolean) {
  if (!value) return null
  if (!value.includes('T')) return value.slice(0, 10)
  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) return null
  if (isEnd) parsed.setTime(parsed.getTime() - 1)
  return isoDate(parsed)
}

function monthGrid(month: Date) {
  const first = new Date(month.getFullYear(), month.getMonth(), 1)
  const start = new Date(first)
  start.setDate(first.getDate() - ((first.getDay() + 6) % 7))
  return Array.from({ length: 42 }, (_, index) => {
    const date = new Date(start)
    date.setDate(start.getDate() + index)
    return { date, outside: date.getMonth() !== month.getMonth() }
  })
}

function CalendarMonth({
  month,
  start,
  end,
  focusedDate,
  onSelect,
  onMoveFocus,
}: {
  month: Date
  start: string | null
  end: string | null
  focusedDate: string
  onSelect: (date: string) => void
  onMoveFocus: (date: Date) => void
}) {
  const days = useMemo(() => monthGrid(month), [month])
  const monthLabel = new Intl.DateTimeFormat('en-US', { month: 'long', year: 'numeric' }).format(month)

  function moveFrom(date: Date, event: ReactKeyboardEvent<HTMLButtonElement>) {
    const next = new Date(date)
    if (event.key === 'ArrowLeft') next.setDate(next.getDate() - 1)
    else if (event.key === 'ArrowRight') next.setDate(next.getDate() + 1)
    else if (event.key === 'ArrowUp') next.setDate(next.getDate() - 7)
    else if (event.key === 'ArrowDown') next.setDate(next.getDate() + 7)
    else if (event.key === 'Home') next.setDate(next.getDate() - ((next.getDay() + 6) % 7))
    else if (event.key === 'End') next.setDate(next.getDate() + (6 - ((next.getDay() + 6) % 7)))
    else if (event.key === 'PageUp') onMoveFocus(new Date(`${shiftAnchor(isoDate(next), 'month', -1)}T12:00:00`))
    else if (event.key === 'PageDown') onMoveFocus(new Date(`${shiftAnchor(isoDate(next), 'month', 1)}T12:00:00`))
    else return
    event.preventDefault()
    if (event.key !== 'PageUp' && event.key !== 'PageDown') onMoveFocus(next)
  }

  return (
    <div className="calendar-month">
      <h3 id={`calendar-month-${month.getFullYear()}-${month.getMonth()}`}>{monthLabel.toUpperCase()}</h3>
      <div className="calendar-grid" role="grid" aria-labelledby={`calendar-month-${month.getFullYear()}-${month.getMonth()}`}>
        <div className="calendar-weekdays" role="row">{['MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU'].map(day => <span role="columnheader" key={day}>{day}</span>)}</div>
        <div className="calendar-days">
          {Array.from({ length: 6 }, (_, week) => <div role="row" key={week}>{days.slice(week * 7, week * 7 + 7).map(({ date, outside }) => {
          const iso = isoDate(date)
          const inRange = Boolean(start && end && iso >= start && iso <= end)
          return (
            <span role="gridcell" aria-selected={inRange || iso === start || iso === end} key={iso}>
              <button
                type="button"
                data-calendar-date={iso}
                tabIndex={!outside && iso === focusedDate ? 0 : -1}
                aria-label={new Intl.DateTimeFormat('en-US', { dateStyle: 'long' }).format(date)}
                className={`${outside ? 'outside' : ''} ${inRange ? 'in-range' : ''} ${iso === start ? 'range-start' : ''} ${iso === end ? 'range-end' : ''}`}
                onFocus={() => { if (!outside) onMoveFocus(date) }}
                onKeyDown={event => moveFrom(date, event)}
                onClick={() => onSelect(iso)}
              >{String(date.getDate()).padStart(2, '0')}</button>
            </span>
          )
        })}</div>)}
        </div>
      </div>
    </div>
  )
}

export function RangeCalendar({
  id,
  initialStart,
  initialEnd,
  onCancel,
  onApply,
}: {
  id?: string
  initialStart: string | null
  initialEnd: string | null
  onCancel: () => void
  onApply: (start: string, end: string) => void
}) {
  const normalizedStart = normalizeBoundary(initialStart, false)
  const normalizedEnd = normalizeBoundary(initialEnd, true)
  const base = normalizedStart ? new Date(`${normalizedStart}T12:00:00`) : new Date()
  const [month, setMonth] = useState(new Date(base.getFullYear(), base.getMonth(), 1))
  const [start, setStart] = useState<string | null>(normalizedStart)
  const [end, setEnd] = useState<string | null>(normalizedEnd)
  const [focusedDate, setFocusedDate] = useState(normalizedStart ?? isoDate(base))
  const dialogRef = useRef<HTMLDivElement>(null)
  const onCancelRef = useRef(onCancel)
  const nextMonth = new Date(month.getFullYear(), month.getMonth() + 1, 1)

  useEffect(() => {
    onCancelRef.current = onCancel
  }, [onCancel])

  useEffect(() => {
    const previouslyFocused = document.activeElement instanceof HTMLElement ? document.activeElement : null
    const dialog = dialogRef.current
    const focusableElements = () => Array.from(dialog?.querySelectorAll<HTMLElement>(FOCUSABLE_SELECTOR) ?? [])

    const [first] = focusableElements()
    const selectedDay = dialog?.querySelector<HTMLElement>('[data-calendar-date][tabindex="0"]')
    ;(selectedDay ?? first ?? dialog)?.focus()

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        event.preventDefault()
        onCancelRef.current()
        return
      }
      if (event.key !== 'Tab') return

      const focusable = focusableElements()
      if (focusable.length === 0) {
        event.preventDefault()
        dialog?.focus()
        return
      }

      const firstElement = focusable[0]
      const lastElement = focusable[focusable.length - 1]
      if (event.shiftKey && document.activeElement === firstElement) {
        event.preventDefault()
        lastElement.focus()
      } else if (!event.shiftKey && document.activeElement === lastElement) {
        event.preventDefault()
        firstElement.focus()
      } else if (dialog && !dialog.contains(document.activeElement)) {
        event.preventDefault()
        firstElement.focus()
      }
    }
    document.addEventListener('keydown', handleKeyDown)
    return () => {
      document.removeEventListener('keydown', handleKeyDown)
      if (previouslyFocused?.isConnected) previouslyFocused.focus()
    }
  }, [])

  function moveFocus(date: Date) {
    const nextDate = new Date(date.getFullYear(), date.getMonth(), date.getDate(), 12)
    const shownStart = new Date(month.getFullYear(), month.getMonth(), 1, 12)
    const shownEnd = new Date(month.getFullYear(), month.getMonth() + 2, 0, 12)
    if (nextDate < shownStart) {
      setMonth(new Date(nextDate.getFullYear(), nextDate.getMonth(), 1))
    } else if (nextDate > shownEnd) {
      setMonth(new Date(nextDate.getFullYear(), nextDate.getMonth() - 1, 1))
    }
    const iso = isoDate(nextDate)
    setFocusedDate(iso)
    window.requestAnimationFrame(() => {
      dialogRef.current?.querySelector<HTMLButtonElement>(`[data-calendar-date="${iso}"]:not(.outside)`)?.focus()
    })
  }

  function showMonth(amount: number) {
    const next = new Date(month.getFullYear(), month.getMonth() + amount, 1)
    setMonth(next)
    const iso = isoDate(next)
    setFocusedDate(iso)
    window.requestAnimationFrame(() => {
      dialogRef.current?.querySelector<HTMLButtonElement>(`[data-calendar-date="${iso}"]:not(.outside)`)?.focus()
    })
  }

  function select(date: string) {
    setFocusedDate(date)
    if (!start || end) {
      setStart(date)
      setEnd(null)
    } else if (date < start) {
      setEnd(start)
      setStart(date)
    } else {
      setEnd(date)
    }
  }

  return (
    <div ref={dialogRef} id={id} className="range-calendar" role="dialog" aria-modal="true" aria-label="Choose date range" tabIndex={-1}>
      <div className="calendar-nav">
        <button type="button" onClick={() => showMonth(-1)}>PREVIOUS</button>
        <button type="button" onClick={() => showMonth(1)}>NEXT</button>
      </div>
      <div className="calendar-months">
        <CalendarMonth month={month} start={start} end={end} focusedDate={focusedDate} onSelect={select} onMoveFocus={moveFocus} />
        <CalendarMonth month={nextMonth} start={start} end={end} focusedDate={focusedDate} onSelect={select} onMoveFocus={moveFocus} />
      </div>
      <div className="calendar-actions">
        <button type="button" className="button" onClick={onCancel}>CANCEL</button>
        <button type="button" className="button button-coral" disabled={!start} onClick={() => start && onApply(start, end ?? start)}>APPLY RANGE</button>
      </div>
    </div>
  )
}
