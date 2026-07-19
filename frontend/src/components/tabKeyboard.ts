import type { KeyboardEvent } from 'react'

export function handleTabKeyDown<T extends string>(
  event: KeyboardEvent<HTMLButtonElement>,
  values: readonly T[],
  active: T,
  onSelect: (value: T) => void,
) {
  if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return
  event.preventDefault()
  const current = Math.max(0, values.indexOf(active))
  const next = event.key === 'Home'
    ? 0
    : event.key === 'End'
      ? values.length - 1
      : (current + (event.key === 'ArrowRight' ? 1 : -1) + values.length) % values.length
  const tabs = event.currentTarget.closest('[role="tablist"]')?.querySelectorAll<HTMLElement>('[role="tab"]')
  onSelect(values[next])
  tabs?.[next]?.focus()
}
