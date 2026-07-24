import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { afterEach, describe, expect, it, vi } from 'vitest'
import App from './App'

const headerState = vi.hoisted(() => ({ throws: false }))

vi.mock('./components/AppHeader', () => ({
  AppHeader() {
    if (headerState.throws) throw new Error('Broken header')
    return <header>Application header</header>
  },
}))

vi.mock('./pages/OverviewPage', () => ({
  OverviewPage() {
    return <h1>Overview</h1>
  },
}))

vi.mock('./pages/SessionsPage', () => ({ SessionsPage: () => <h1>Sessions</h1> }))
vi.mock('./pages/SessionDetailPage', () => ({ SessionDetailPage: () => <h1>Session detail</h1> }))
vi.mock('./pages/StatsPage', () => ({ StatsPage: () => <h1>Stats</h1> }))
vi.mock('./pages/SettingsPage', () => ({ SettingsPage: () => <h1>Settings</h1> }))

afterEach(() => {
  headerState.throws = false
  vi.restoreAllMocks()
})

describe('application shell recovery', () => {
  it('contains header failures and offers a full-app reload', () => {
    vi.spyOn(console, 'error').mockImplementation(() => {})
    headerState.throws = true

    render(
      <MemoryRouter initialEntries={['/']}>
        <App />
      </MemoryRouter>,
    )

    expect(screen.getByRole('alert')).toHaveTextContent('Broken header')
    expect(screen.getByRole('button', { name: 'RELOAD APP' })).toBeVisible()
  })

  it('offers a keyboard bypass to a stable focusable main target', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <App />
      </MemoryRouter>,
    )

    expect(screen.getByRole('link', { name: 'Skip to main content' })).toHaveAttribute('href', '#main-content')
    const main = screen.getByRole('main')
    expect(main).toHaveAttribute('id', 'main-content')
    expect(main).toHaveAttribute('tabindex', '-1')
  })

  it.each([
    ['/', 'Overview · Codex usage'],
    ['/sessions', 'Sessions · Codex usage'],
    ['/sessions/019f6768-ef84-74d3-ab05-e4b5fb717fa8', 'Session 019f6768 · Codex usage'],
    ['/stats', 'Stats · Codex usage'],
    ['/settings', 'Settings · Codex usage'],
  ])('sets a route-specific document title for %s', (entry, title) => {
    render(<MemoryRouter initialEntries={[entry]}><App /></MemoryRouter>)
    expect(document.title).toBe(title)
  })
})
