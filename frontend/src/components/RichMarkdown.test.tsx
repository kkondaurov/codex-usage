import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { RichMarkdown } from './RichMarkdown'

const content = `A short **introduction** with a [safe link](https://example.com).

- First point
- Second point with \`inline code\`

3. Third step
4. Fourth step

## Details

> A useful aside.

\`\`\`ts
const answer = 42
\`\`\`

<oai-mem-citation>
private citation plumbing
</oai-mem-citation>`

describe('RichMarkdown', () => {
  it('preserves assistant-message block structure and hides internal memory markup', () => {
    render(<RichMarkdown>{content}</RichMarkdown>)

    expect(screen.getByText('introduction')).toBeVisible()
    expect(screen.getByRole('link', { name: 'safe link' })).toHaveAttribute('href', 'https://example.com')
    const lists = screen.getAllByRole('list')
    expect(lists).toHaveLength(2)
    expect(screen.getAllByRole('listitem')).toHaveLength(4)
    expect(lists[0].tagName).toBe('UL')
    expect(lists[1]).toHaveAttribute('start', '3')
    expect(screen.getByRole('heading', { name: 'Details' })).toBeVisible()
    expect(screen.getByText('A useful aside.').closest('blockquote')).toBeInTheDocument()
    expect(screen.getByText('const answer = 42').closest('pre')).toBeInTheDocument()
    expect(screen.queryByText('private citation plumbing')).not.toBeInTheDocument()
  })

  it('bounds adversarial unmatched inline markup', () => {
    const value = '['.repeat(32_000)
    const startedAt = performance.now()

    const { container } = render(<RichMarkdown>{value}</RichMarkdown>)

    expect(container).toHaveTextContent(value)
    expect(performance.now() - startedAt).toBeLessThan(500)
  })

  it('preserves source heading depth while capping it below the page hierarchy', () => {
    render(<RichMarkdown>{'# One\n## Two\n### Three\n#### Four\n###### Six'}</RichMarkdown>)

    expect(screen.getByRole('heading', { name: 'One', level: 3 })).toBeVisible()
    expect(screen.getByRole('heading', { name: 'Two', level: 4 })).toBeVisible()
    expect(screen.getByRole('heading', { name: 'Three', level: 5 })).toBeVisible()
    expect(screen.getByRole('heading', { name: 'Four', level: 6 })).toBeVisible()
    expect(screen.getByRole('heading', { name: 'Six', level: 6 })).toBeVisible()
  })
})
