import type { StatsRange } from './types'

const dateOnlyPattern = /^(\d{4})-(\d{2})-(\d{2})$/
export const MIN_PUBLIC_YEAR = 1970
const MAX_PUBLIC_YEAR = 9998

export function dateOnly(date: Date) {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`
}

export function validDateOnly(value: string | null) {
  if (!value) return false
  const match = dateOnlyPattern.exec(value)
  if (!match) return false
  const [, year, month, day] = match
  const numericYear = Number(year)
  if (numericYear < MIN_PUBLIC_YEAR || numericYear > MAX_PUBLIC_YEAR) return false
  const date = new Date(Date.UTC(numericYear, Number(month) - 1, Number(day)))
  return date.getUTCFullYear() === numericYear
    && date.getUTCMonth() === Number(month) - 1
    && date.getUTCDate() === Number(day)
}

export function canonicalStatsAnchor(value: string | null, range: StatsRange) {
  if (!value || !validDateOnly(value)) return null
  if (range === 'month') return `${value.slice(0, 7)}-01`
  if (range === 'year') return `${value.slice(0, 4)}-01-01`
  if (range !== 'week') return value

  const date = new Date(`${value}T12:00:00`)
  const daysSinceMonday = (date.getDay() + 6) % 7
  date.setDate(date.getDate() - daysSinceMonday)
  const monday = dateOnly(date)
  // 1970 began on a Thursday. The first complete public-domain week starts
  // on Jan 5; earlier anchors would canonicalize to a forbidden 1969 date.
  return validDateOnly(monday) ? monday : '1970-01-05'
}

export function shiftAnchor(anchor: string, range: StatsRange, amount: number) {
  const canonicalAnchor = canonicalStatsAnchor(anchor, range) ?? anchor
  const date = new Date(`${canonicalAnchor.slice(0, 10)}T12:00:00`)
  if (range === 'day') date.setDate(date.getDate() + amount)
  if (range === 'week') date.setDate(date.getDate() + amount * 7)
  if (range === 'month') date.setMonth(date.getMonth() + amount, 1)
  if (range === 'year') date.setFullYear(date.getFullYear() + amount, 0, 1)
  return canonicalStatsAnchor(dateOnly(date), range) ?? dateOnly(date)
}
