import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { CompactMarkdown } from './CompactMarkdown'
import { compactMarkdownText, safeMarkdownHref } from './markdownExcerpt'

describe('CompactMarkdown', () => {
  it('flattens blocks and renders inline markup without leaking markers', () => {
    const { container } = render(<CompactMarkdown>{`# Result

> **Ready** with \`cargo test\`.

- First item
- _Second_ item`}</CompactMarkdown>)

    expect(container.firstElementChild).toHaveTextContent('Result Ready with cargo test. First item Second item')
    expect(screen.getByText('Ready').tagName).toBe('STRONG')
    expect(screen.getByText('cargo test').tagName).toBe('CODE')
    expect(screen.getByText('Second').tagName).toBe('EM')
    expect(document.body).not.toHaveTextContent(/[#>`*_]/)
  })

  it('shows link labels and only creates anchors for safe destinations when requested', () => {
    const { rerender } = render(<CompactMarkdown links="anchor">[Activity view is live](http://127.0.0.1:5610/sessions/1?tab=activity)</CompactMarkdown>)

    expect(screen.getByRole('link', { name: 'Activity view is live' })).toHaveAttribute('href', 'http://127.0.0.1:5610/sessions/1?tab=activity')
    expect(document.body).not.toHaveTextContent('[Activity view is live]')

    rerender(<CompactMarkdown links="anchor">[Do not run](javascript:alert(1))</CompactMarkdown>)
    expect(screen.queryByRole('link')).not.toBeInTheDocument()
    expect(screen.getByText('Do not run')).toBeVisible()
  })

  it('never executes raw HTML and keeps its readable content', () => {
    render(<CompactMarkdown links="anchor">{'Before <script>alert("nope")</script> after <img src=x onerror=alert(1)>'}</CompactMarkdown>)

    expect(document.querySelector('script')).not.toBeInTheDocument()
    expect(document.querySelector('img')).not.toBeInTheDocument()
    expect(screen.getByText('Before alert("nope") after')).toBeVisible()
  })

  it('uses image alt text and produces a bounded plain excerpt', () => {
    const value = '![diagram](https://example.com/image.png) **Alpha** and [Beta](https://example.com) continue'

    expect(compactMarkdownText(value)).toBe('diagram Alpha and Beta continue')
    expect(compactMarkdownText(value, 18)).toBe('diagram Alpha and…')
  })

  it('accepts local and web links but rejects executable or protocol-relative URLs', () => {
    expect(safeMarkdownHref('/sessions/one')).toBe('/sessions/one')
    expect(safeMarkdownHref('https://example.com')).toBe('https://example.com')
    expect(safeMarkdownHref('mailto:hello@example.com')).toBe('mailto:hello@example.com')
    expect(safeMarkdownHref('//evil.example')).toBeNull()
    expect(safeMarkdownHref('data:text/html,bad')).toBeNull()
    expect(safeMarkdownHref('javascript:alert(1)')).toBeNull()
  })
})
