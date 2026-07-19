import { render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { AppErrorBoundary } from './AppErrorBoundary'

function BrokenPage(): never {
  throw new Error('Broken route')
}

afterEach(() => vi.restoreAllMocks())

describe('AppErrorBoundary', () => {
  it('releases a render failure when navigation changes its reset key', () => {
    vi.spyOn(console, 'error').mockImplementation(() => undefined)
    const { rerender } = render(
      <AppErrorBoundary resetKey="/broken"><BrokenPage /></AppErrorBoundary>,
    )
    expect(screen.getByRole('alert')).toHaveTextContent('Broken route')

    rerender(
      <AppErrorBoundary resetKey="/safe"><p>Safe route</p></AppErrorBoundary>,
    )
    expect(screen.getByText('Safe route')).toBeVisible()
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
  })
})
