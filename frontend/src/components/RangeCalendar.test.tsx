import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { useState } from 'react'
import { describe, expect, it, vi } from 'vitest'
import { RangeCalendar } from './RangeCalendar'

function CalendarHarness() {
  const [open, setOpen] = useState(false)
  return (
    <>
      <button type="button" onClick={() => setOpen(true)}>OPEN CALENDAR</button>
      {open && <RangeCalendar initialStart={null} initialEnd={null} onApply={vi.fn()} onCancel={() => setOpen(false)} />}
    </>
  )
}

describe('RangeCalendar', () => {
  it('commits a selected range only when Apply is pressed', () => {
    const onApply = vi.fn()
    const onCancel = vi.fn()
    render(<RangeCalendar initialStart="2026-07-01" initialEnd="2026-07-09" onApply={onApply} onCancel={onCancel} />)

    expect(screen.getByRole('dialog', { name: 'Choose date range' })).toBeVisible()
    fireEvent.click(screen.getByRole('button', { name: 'APPLY RANGE' }))
    expect(onApply).toHaveBeenCalledWith('2026-07-01', '2026-07-09')
    expect(onCancel).not.toHaveBeenCalled()
  })

  it('cancels without changing the committed filter', () => {
    const onApply = vi.fn()
    const onCancel = vi.fn()
    render(<RangeCalendar initialStart={null} initialEnd={null} onApply={onApply} onCancel={onCancel} />)
    fireEvent.click(screen.getByRole('button', { name: 'CANCEL' }))
    expect(onCancel).toHaveBeenCalledOnce()
    expect(onApply).not.toHaveBeenCalled()
  })

  it('normalizes exact RFC3339 day boundaries before opening', () => {
    const onApply = vi.fn()
    render(
      <RangeCalendar
        initialStart="2026-07-13T22:00:00Z"
        initialEnd="2026-07-14T22:00:00Z"
        onApply={onApply}
        onCancel={vi.fn()}
      />,
    )

    expect(screen.getByRole('button', { name: 'July 14, 2026' })).toHaveClass('range-start', 'range-end')
    fireEvent.click(screen.getByRole('button', { name: 'APPLY RANGE' }))
    expect(onApply).toHaveBeenCalledWith('2026-07-14', '2026-07-14')
  })

  it('dismisses on Escape without applying', () => {
    const onApply = vi.fn()
    const onCancel = vi.fn()
    render(<RangeCalendar initialStart={null} initialEnd={null} onApply={onApply} onCancel={onCancel} />)

    fireEvent.keyDown(document, { key: 'Escape' })
    expect(onCancel).toHaveBeenCalledOnce()
    expect(onApply).not.toHaveBeenCalled()
  })

  it('moves focus into the modal and contains keyboard focus', () => {
    render(<CalendarHarness />)
    fireEvent.click(screen.getByRole('button', { name: 'OPEN CALENDAR' }))

    const dialog = screen.getByRole('dialog', { name: 'Choose date range' })
    const previous = screen.getByRole('button', { name: 'PREVIOUS' })
    const cancel = screen.getByRole('button', { name: 'CANCEL' })
    const focusedDay = dialog.querySelector<HTMLButtonElement>('.calendar-days button[tabindex="0"]')!

    expect(dialog).toHaveAttribute('aria-modal', 'true')
    expect(focusedDay).toHaveFocus()

    previous.focus()
    fireEvent.keyDown(document, { key: 'Tab', shiftKey: true })
    expect(cancel).toHaveFocus()

    cancel.focus()
    fireEvent.keyDown(document, { key: 'Tab' })
    expect(previous).toHaveFocus()
  })

  it('uses one roving day tab stop and supports spatial arrow navigation', async () => {
    render(<RangeCalendar initialStart={null} initialEnd={null} onApply={vi.fn()} onCancel={vi.fn()} />)

    const today = document.querySelector<HTMLButtonElement>('.calendar-days button[tabindex="0"]')!
    const current = new Date(`${today.dataset.calendarDate}T12:00:00`)
    current.setDate(current.getDate() + 1)
    const tomorrowIso = `${current.getFullYear()}-${String(current.getMonth() + 1).padStart(2, '0')}-${String(current.getDate()).padStart(2, '0')}`
    expect(screen.getAllByRole('grid')).toHaveLength(2)
    expect(document.querySelectorAll('.calendar-days button[tabindex="0"]')).toHaveLength(1)

    fireEvent.keyDown(today, { key: 'ArrowRight' })

    const tomorrow = document.querySelector<HTMLButtonElement>(`.calendar-days button[data-calendar-date="${tomorrowIso}"]:not(.outside)`)!
    await waitFor(() => expect(tomorrow).toHaveFocus())
    expect(tomorrow).toHaveAttribute('tabindex', '0')
    expect(today).toHaveAttribute('tabindex', '-1')
  })

  it('clamps PageUp and PageDown to the last valid day of the destination month', async () => {
    const first = render(<RangeCalendar initialStart="2025-01-31" initialEnd={null} onApply={vi.fn()} onCancel={vi.fn()} />)
    const january31 = document.querySelector<HTMLButtonElement>('[data-calendar-date="2025-01-31"]:not(.outside)')!

    fireEvent.keyDown(january31, { key: 'PageDown' })
    await waitFor(() => expect(document.querySelector('[data-calendar-date="2025-02-28"]:not(.outside)')).toHaveFocus())

    first.unmount()
    render(<RangeCalendar initialStart="2025-03-31" initialEnd={null} onApply={vi.fn()} onCancel={vi.fn()} />)
    const march31 = document.querySelector<HTMLButtonElement>('[data-calendar-date="2025-03-31"]:not(.outside)')!
    march31.focus()
    fireEvent.keyDown(march31, { key: 'PageUp' })
    await waitFor(() => expect(document.querySelector('[data-calendar-date="2025-02-28"]:not(.outside)')).toHaveFocus())
  })

  it('restores focus to the trigger after Escape closes the modal', () => {
    render(<CalendarHarness />)
    const trigger = screen.getByRole('button', { name: 'OPEN CALENDAR' })
    trigger.focus()
    fireEvent.click(trigger)

    expect(document.querySelector('.calendar-days button[tabindex="0"]')).toHaveFocus()
    fireEvent.keyDown(document, { key: 'Escape' })

    expect(screen.queryByRole('dialog', { name: 'Choose date range' })).not.toBeInTheDocument()
    expect(trigger).toHaveFocus()
  })
})
