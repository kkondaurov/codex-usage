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
})
