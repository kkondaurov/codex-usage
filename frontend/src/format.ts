import type { DecimalString } from './decimal'
import { formatUsd } from './decimal'

const compactNumber = new Intl.NumberFormat('en-US', {
  notation: 'compact',
  maximumFractionDigits: 1,
})

function parsedDate(value: string) {
  const date = new Date(value)
  return Number.isFinite(date.getTime()) ? date : null
}

export function money(value: DecimalString | null, digits = 2) {
  if (value === null) return '—'
  return formatUsd(value, digits)
}

export function estimatedMoney(value: DecimalString | null, unpricedTokens = 0, digits = 2) {
  return money(unpricedTokens > 0 ? null : value, digits)
}

export function tokens(value: number) {
  if (value === 0) return '0'
  return compactNumber.format(value).toUpperCase()
}

export function integer(value: number) {
  return new Intl.NumberFormat('en-US').format(value)
}

export function bytes(value: number) {
  if (value < 1024) return `${value} B`
  if (value < 1024 ** 2) return `${(value / 1024).toFixed(1)} KB`
  if (value < 1024 ** 3) return `${Math.round(value / 1024 ** 2)} MB`
  return `${(value / 1024 ** 3).toFixed(1)} GB`
}

export function shortDate(value: string) {
  const date = parsedDate(value)
  return date ? new Intl.DateTimeFormat('en-US', { month: 'short', day: 'numeric' }).format(date) : '—'
}

export function shortDateTime(value: string) {
  const date = parsedDate(value)
  if (!date) return '—'
  return new Intl.DateTimeFormat('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).format(date).replace(',', ' ·')
}

export function time(value: string) {
  const date = parsedDate(value)
  if (!date) return '—'
  return new Intl.DateTimeFormat('en-US', {
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).format(date)
}

export function relativeTime(value: string) {
  const date = parsedDate(value)
  if (!date) return 'at an unknown time'
  const seconds = Math.max(0, Math.round((Date.now() - date.getTime()) / 1000))
  if (seconds < 60) return `${seconds}s ago`
  const minutes = Math.round(seconds / 60)
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.round(minutes / 60)
  if (hours < 24) return `${hours}h ago`
  return shortDate(value)
}

export function ellipsis(value: string, length = 80) {
  const clean = value.replace(/\s+/g, ' ').trim()
  return clean.length > length ? `${clean.slice(0, length - 1)}…` : clean
}

export function duration(value: number) {
  if (value < 1000) return `${Math.round(value)}ms`
  if (value < 60_000) return `${(value / 1000).toFixed(value < 10_000 ? 1 : 0)}s`
  const totalSeconds = Math.round(value / 1000)
  if (totalSeconds >= 3600) {
    const totalMinutes = Math.round(totalSeconds / 60)
    const hours = Math.floor(totalMinutes / 60)
    const minutes = totalMinutes % 60
    return `${hours}h ${minutes}m`
  }
  const minutes = Math.floor(totalSeconds / 60)
  const seconds = totalSeconds % 60
  return `${minutes}m ${seconds}s`
}
