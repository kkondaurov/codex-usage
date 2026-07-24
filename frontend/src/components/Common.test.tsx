import { fireEvent, render, screen } from '@testing-library/react'
import { useState } from 'react'
import { describe, expect, it, vi } from 'vitest'
import { Pagination } from './Common'

function FinalPageHarness({ onPage }: { onPage: (page: number) => void }) {
  const [page, setPage] = useState(1)
  return <Pagination page={page} totalPages={2} total={100} pageSize={50} onPage={(next) => { onPage(next); setPage(next) }} />
}

describe('Pagination', () => {
  it('keeps NEXT focused and inert after it reaches the final page', () => {
    const onPage = vi.fn()
    render(<FinalPageHarness onPage={onPage} />)

    const next = screen.getByRole('button', { name: 'NEXT' })
    next.focus()
    fireEvent.click(next)

    expect(onPage).toHaveBeenCalledOnce()
    expect(onPage).toHaveBeenCalledWith(2)
    expect(next).toHaveFocus()
    expect(next).not.toBeDisabled()
    expect(next).toHaveAttribute('aria-disabled', 'true')

    fireEvent.click(next)
    expect(onPage).toHaveBeenCalledOnce()
  })
})
