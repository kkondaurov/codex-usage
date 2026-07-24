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
  let statusAnnouncement = loading && !data ? 'Checking usage update status' : 'Waiting for the first usage update'
  let statusTitle: string | undefined
  let statusTone = ''

  if (error) {
    statusLabel = data && lastSuccess ? `Status stale · ${lastSuccess}` : 'Status unavailable'
    statusAnnouncement = `Usage update status unavailable: ${error.message}`
    statusTitle = error.message
    statusTone = 'attention'
  } else if (data?.state === 'scanning' || data?.state === 'busy') {
    statusLabel = 'Updating…'
    statusAnnouncement = 'Usage data update in progress'
  } else if (data?.state === 'error') {
    const attempted = data.lastIngestAttemptAt ?? data.lastIngestAt
    statusLabel = attempted ? `Update failed ${relativeTime(attempted)}` : 'Update failed'
    statusAnnouncement = `Usage data update failed with ${data.filesFailed.toLocaleString()} ${data.filesFailed === 1 ? 'source file error' : 'source file errors'}`
    statusTitle = lastSuccess ? `${lastSuccess}. ${data.filesFailed.toLocaleString()} source files failed.` : `${data.filesFailed.toLocaleString()} source files failed.`
    statusTone = 'attention'
  } else if (data && data.filesFailed > 0) {
    statusLabel = `${data.filesFailed.toLocaleString()} ingest ${data.filesFailed === 1 ? 'error' : 'errors'}`
    statusAnnouncement = `Usage data has ${data.filesFailed.toLocaleString()} ingest ${data.filesFailed === 1 ? 'error' : 'errors'}`
    statusTitle = lastSuccess ?? undefined
    statusTone = 'attention'
  } else if (lastSuccess) {
    statusLabel = lastSuccess
    statusAnnouncement = 'Usage data is up to date'
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
      <div className="header-status">
        <span className={`updated-label ${statusTone}`} title={statusTitle}>{statusLabel}</span>
        <span className="sr-only" aria-live="polite" aria-atomic="true">{statusAnnouncement}</span>
        <span className="header-divider" />
        <NavLink to="/settings" className={location.pathname.startsWith('/settings') ? 'active' : ''}>Settings</NavLink>
      </div>
    </header>
  )
}
