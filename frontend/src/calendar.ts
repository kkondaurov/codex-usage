import type { StatsRange } from './types'

const dateOnlyPattern = /^(\d{4})-(\d{2})-(\d{2})$/

export function dateOnly(date: Date) {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`
}

export function validDateOnly(value: string | null) {
  if (!value) return false
  const match = dateOnlyPattern.exec(value)
  if (!match) return false
  const [, year, month, day] = match
  const date = new Date(Date.UTC(Number(year), Number(month) - 1, Number(day)))
  return date.getUTCFullYear() === Number(year)
    && date.getUTCMonth() === Number(month) - 1
    && date.getUTCDate() === Number(day)
}

export function shiftAnchor(anchor: string, range: StatsRange, amount: number) {
  const date = new Date(`${anchor.slice(0, 10)}T12:00:00`)
  if (range === 'day') date.setDate(date.getDate() + amount)
  if (range === 'week') date.setDate(date.getDate() + amount * 7)
  if (range === 'month' || range === 'year') {
    const day = date.getDate()
    const targetYear = date.getFullYear() + (range === 'year' ? amount : 0)
    const targetMonth = date.getMonth() + (range === 'month' ? amount : 0)
    const lastDay = new Date(targetYear, targetMonth + 1, 0, 12).getDate()
    date.setDate(1)
    date.setFullYear(targetYear, targetMonth, Math.min(day, lastDay))
  }
  return dateOnly(date)
}
