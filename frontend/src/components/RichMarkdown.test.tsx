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
})
