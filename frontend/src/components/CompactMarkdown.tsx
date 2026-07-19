import type { ReactNode } from 'react'
import { markdownExcerptNodes } from './markdownExcerpt'
import type { MarkdownExcerptNode } from './markdownExcerpt'

export type CompactMarkdownProps = {
  children: string
  className?: string
  links?: 'anchor' | 'text'
  maxLength?: number
}

function render(nodes: MarkdownExcerptNode[], links: CompactMarkdownProps['links'], prefix = 'md'): ReactNode[] {
  return nodes.map((node, index) => {
    const key = `${prefix}-${index}`
    if (node.type === 'text') return node.value
    if (node.type === 'code') return <code key={key}>{node.value}</code>
    if (node.type === 'strong') return <strong key={key}>{render(node.children, links, key)}</strong>
    if (node.type === 'emphasis') return <em key={key}>{render(node.children, links, key)}</em>
    if (node.type === 'delete') return <del key={key}>{render(node.children, links, key)}</del>
    if (node.type === 'link' && links === 'anchor' && node.href) return <a href={node.href} key={key}>{render(node.children, links, key)}</a>
    return <span key={key}>{render(node.children, links, key)}</span>
  })
}

export function CompactMarkdown({ children, className, links = 'text', maxLength }: CompactMarkdownProps) {
  const nodes = markdownExcerptNodes(children, maxLength)
  return <span className={className}>{render(nodes, links)}</span>
}
