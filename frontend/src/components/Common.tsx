import type { ReactNode } from 'react'

export function PageTitle({ children }: { children: ReactNode }) {
  return (
    <div className="page-heading">
      <h1>{children}</h1>
    </div>
  )
}

export function LoadingLedger({ rows = 8 }: { rows?: number }) {
  return (
    <div className="loading-ledger" aria-label="Loading" aria-busy="true">
      {Array.from({ length: rows }).map((_, index) => <div key={index} />)}
    </div>
  )
}

export function ErrorState({ error, onRetry }: { error: Error; onRetry: () => void }) {
  return (
    <div className="error-state" role="alert">
      <span className="eyebrow">COULDN’T LOAD DATA</span>
      <strong>{error.message}</strong>
      <button className="button button-coral" type="button" onClick={onRetry}>TRY AGAIN</button>
    </div>
  )
}

function successfulUpdateLabel(value: number | null) {
  if (value === null) return 'an earlier successful request'
  return new Intl.DateTimeFormat(undefined, {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  }).format(new Date(value))
}

export function DegradedDataNotice({ error, lastSuccessfulAt, onRetry }: {
  error: Error
  lastSuccessfulAt: number | null
  onRetry: () => void
}) {
  return (
    <div className="degraded-data-notice" role="alert">
      <span><strong>SHOWING STALE DATA</strong> · Last successful update {successfulUpdateLabel(lastSuccessfulAt)}. Refresh failed: {error.message}</span>
      <button type="button" onClick={onRetry}>TRY AGAIN</button>
    </div>
  )
}

function pageNumbers(page: number, totalPages: number) {
  if (totalPages <= 5) return Array.from({ length: totalPages }, (_, i) => i + 1)
  const values: Array<number | 'ellipsis'> = [1]
  const start = Math.max(2, page - 1)
  const end = Math.min(totalPages - 1, page + 1)
  if (start > 2) values.push('ellipsis')
  for (let value = start; value <= end; value += 1) values.push(value)
  if (end < totalPages - 1) values.push('ellipsis')
  values.push(totalPages)
  return values
}

export function Pagination({
  page,
  totalPages,
  total,
  pageSize,
  onPage,
  busy = false,
}: {
  page: number
  totalPages: number
  total: number
  pageSize: number
  onPage: (page: number) => void
  busy?: boolean
}) {
  const first = total === 0 ? 0 : (page - 1) * pageSize + 1
  const last = Math.min(page * pageSize, total)
  const previousAtBoundary = page <= 1
  const nextAtBoundary = page >= totalPages
  return (
    <nav className="pagination" aria-label="Pagination" aria-busy={busy || undefined}>
      <span>{first}–{last} / {total.toLocaleString()} · {pageSize} PER PAGE</span>
      <div className="pagination-controls">
        <button type="button" disabled={previousAtBoundary} aria-disabled={busy && !previousAtBoundary ? true : undefined} onClick={() => { if (!busy && !previousAtBoundary) onPage(page - 1) }}>
          PREVIOUS
        </button>
        {pageNumbers(page, totalPages).map((value, index) => value === 'ellipsis'
          ? <span key={`e-${index}`}>…</span>
          : <button key={value} type="button" aria-disabled={busy || undefined} aria-current={value === page ? 'page' : undefined} className={value === page ? 'active' : ''} onClick={() => { if (!busy) onPage(value) }}>{String(value).padStart(2, '0')}</button>
        )}
        <button type="button" disabled={nextAtBoundary} aria-disabled={busy && !nextAtBoundary ? true : undefined} onClick={() => { if (!busy && !nextAtBoundary) onPage(page + 1) }}>
          NEXT
        </button>
      </div>
    </nav>
  )
}
