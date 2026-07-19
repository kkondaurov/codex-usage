import { NavLink, useLocation } from 'react-router-dom'
import { api } from '../api'
import { relativeTime } from '../format'
import { useAsync } from '../hooks'

const links = [
  { to: '/', label: 'Overview', match: (path: string) => path === '/' },
  { to: '/sessions', label: 'Sessions', match: (path: string) => path.startsWith('/sessions') },
  { to: '/stats', label: 'Stats', match: (path: string) => path.startsWith('/stats') },
]

export function AppHeader() {
  const location = useLocation()
  const { data, error, loading } = useAsync(signal => api.status(signal), [], 5000)
  const lastSuccess = data?.lastIngestAt ? `Updated ${relativeTime(data.lastIngestAt)}` : null
  let statusLabel = loading && !data ? 'Checking status…' : 'Waiting for first update'
  let statusTitle: string | undefined
  let statusTone = ''

  if (error) {
    statusLabel = data && lastSuccess ? `Status stale · ${lastSuccess}` : 'Status unavailable'
    statusTitle = error.message
    statusTone = 'attention'
  } else if (data?.state === 'scanning' || data?.state === 'busy') {
    statusLabel = 'Updating…'
  } else if (data?.state === 'error') {
    const attempted = data.lastIngestAttemptAt ?? data.lastIngestAt
    statusLabel = attempted ? `Update failed ${relativeTime(attempted)}` : 'Update failed'
    statusTitle = lastSuccess ? `${lastSuccess}. ${data.filesFailed.toLocaleString()} source files failed.` : `${data.filesFailed.toLocaleString()} source files failed.`
    statusTone = 'attention'
  } else if (data && data.filesFailed > 0) {
    statusLabel = `${data.filesFailed.toLocaleString()} ingest ${data.filesFailed === 1 ? 'error' : 'errors'}`
    statusTitle = lastSuccess ?? undefined
    statusTone = 'attention'
  } else if (lastSuccess) {
    statusLabel = lastSuccess
  }

  return (
    <header className="app-header">
      <NavLink to="/" className="brand" aria-label="Codex usage overview">
        <img src="/app-icon.png" width="28" height="28" alt="" />
        <span>Codex usage</span>
      </NavLink>
      <nav className="primary-nav" aria-label="Primary navigation">
        {links.map((link) => (
          <NavLink key={link.to} to={link.to} className={link.match(location.pathname) ? 'active' : ''}>
            {link.label}
          </NavLink>
        ))}
      </nav>
      <div className="header-status" aria-live="polite">
        <span className={`updated-label ${statusTone}`} title={statusTitle}>{statusLabel}</span>
        <span className="header-divider" />
        <NavLink to="/settings" className={location.pathname.startsWith('/settings') ? 'active' : ''}>Settings</NavLink>
      </div>
    </header>
  )
}
