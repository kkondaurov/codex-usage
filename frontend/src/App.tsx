import { useEffect, useRef } from 'react'
import { Navigate, Route, Routes, useLocation } from 'react-router-dom'
import { AppErrorBoundary } from './components/AppErrorBoundary'
import { AppHeader } from './components/AppHeader'
import { OverviewPage } from './pages/OverviewPage'
import { SessionDetailPage } from './pages/SessionDetailPage'
import { SessionsPage } from './pages/SessionsPage'
import { SettingsPage } from './pages/SettingsPage'
import { StatsPage } from './pages/StatsPage'

export default function App() {
  const location = useLocation()
  const mainRef = useRef<HTMLElement>(null)
  const previousPath = useRef(location.pathname)
  const resetKey = `${location.pathname}${location.search}`
  useEffect(() => {
    const routeTitle = location.pathname === '/'
      ? 'Overview'
      : location.pathname === '/sessions'
        ? 'Sessions'
        : location.pathname.startsWith('/sessions/')
          ? `Session ${location.pathname.slice('/sessions/'.length).slice(0, 8)}`
          : location.pathname === '/stats'
            ? 'Stats'
            : location.pathname === '/settings'
              ? 'Settings'
              : 'Overview'
    document.title = `${routeTitle} · Codex usage`
  }, [location.pathname])
  useEffect(() => {
    if (previousPath.current !== location.pathname) {
      previousPath.current = location.pathname
      window.scrollTo({ top: 0, left: 0, behavior: 'auto' })
      mainRef.current?.focus({ preventScroll: true })
    }
  }, [location.pathname])
  return (
    <AppErrorBoundary resetKey={`shell:${resetKey}`}>
      <div className="app-shell">
        <a className="skip-link" href="#main-content">Skip to main content</a>
        <AppHeader />
        <main ref={mainRef} id="main-content" tabIndex={-1}>
          <AppErrorBoundary resetKey={resetKey}>
            <Routes>
              <Route path="/" element={<OverviewPage />} />
              <Route path="/sessions" element={<SessionsPage />} />
              <Route path="/sessions/:sessionId" element={<SessionDetailPage />} />
              <Route path="/stats" element={<StatsPage />} />
              <Route path="/settings" element={<SettingsPage />} />
              <Route path="*" element={<Navigate to="/" replace />} />
            </Routes>
          </AppErrorBoundary>
        </main>
      </div>
    </AppErrorBoundary>
  )
}
