export type MarkdownExcerptNode =
  | { type: 'text'; value: string }
  | { type: 'code'; value: string }
  | { type: 'strong' | 'emphasis' | 'delete'; children: MarkdownExcerptNode[] }
  | { type: 'link'; href: string | null; children: MarkdownExcerptNode[] }

const HTML_COMMENT = /<!--[\s\S]*?-->/g
const HTML_TAG = /<\/?[A-Za-z][A-Za-z0-9_:-]*(?:\s[^<>]*?)?\s*\/?>/g
const FENCE_LINE = /^\s{0,3}(```+|~~~+)(?:[^\n]*)$/gm
const HEADING = /^\s{0,3}#{1,6}\s+/gm
const BLOCKQUOTE = /^\s{0,3}>\s?/gm
const LIST_MARKER = /^\s{0,3}(?:[-+*]|\d+[.)])\s+(?:\[[ xX]\]\s+)?/gm
const SETEXT_UNDERLINE = /^\s{0,3}(?:=+|-+)\s*$/gm
const TABLE_DIVIDER = /^\s*\|?(?:\s*:?-{3,}:?\s*\|)+\s*:?-{3,}:?\s*\|?\s*$/gm

function flattenBlocks(value: string) {
  return value
    .replace(/\r\n?/g, '\n')
    .replace(HTML_COMMENT, ' ')
    .replace(FENCE_LINE, ' ')
    .replace(TABLE_DIVIDER, ' ')
    .replace(SETEXT_UNDERLINE, ' ')
    .replace(HEADING, '')
    .replace(BLOCKQUOTE, '')
    .replace(LIST_MARKER, '')
    .replace(HTML_TAG, '')
    .replace(/\s*\|\s*/g, ' · ')
    .replace(/\s+/g, ' ')
    .trim()
}

function markerAt(value: string, index: number) {
  for (const marker of ['**', '__', '~~', '*', '_']) {
    if (value.startsWith(marker, index)) return marker
  }
  return null
}

function closingDelimiter(value: string, marker: string, start: number) {
  let cursor = start
  while (cursor < value.length) {
    const next = value.indexOf(marker, cursor)
    if (next < 0) return -1
    if (next === 0 || value[next - 1] !== '\\') return next
    cursor = next + marker.length
  }
  return -1
}

function closingBracket(value: string, start: number) {
  let depth = 1
  for (let index = start; index < value.length; index += 1) {
    if (value[index] === '\\') { index += 1; continue }
    if (value[index] === '[') depth += 1
    if (value[index] === ']') depth -= 1
    if (depth === 0) return index
  }
  return -1
}

function closingParenthesis(value: string, start: number) {
  let depth = 1
  let angleWrapped = false
  for (let index = start; index < value.length; index += 1) {
    if (value[index] === '\\') { index += 1; continue }
    if (value[index] === '<' && index === start) angleWrapped = true
    if (value[index] === '>' && angleWrapped) angleWrapped = false
    if (angleWrapped) continue
    if (value[index] === '(') depth += 1
    if (value[index] === ')') depth -= 1
    if (depth === 0) return index
  }
  return -1
}

function linkDestination(value: string) {
  const trimmed = value.trim()
  if (!trimmed) return null
  if (trimmed.startsWith('<')) {
    const close = trimmed.indexOf('>')
    return close > 0 ? trimmed.slice(1, close) : null
  }
  return trimmed.match(/^(?:\\.|[^\s])+/)?.[0] ?? null
}

export function safeMarkdownHref(value: string | null) {
  if (!value) return null
  const href = value.replace(/\\([\\()])/g, '$1')
  if (/^(?:https?:|mailto:)/i.test(href)) return href
  if (/^(?:\/|\.\/|\.\.\/|\?|#)/.test(href) && !href.startsWith('//')) return href
  return null
}

function parseInline(value: string): MarkdownExcerptNode[] {
  const nodes: MarkdownExcerptNode[] = []
  let text = ''

  function flushText() {
    if (!text) return
    nodes.push({ type: 'text', value: text })
    text = ''
  }

  for (let index = 0; index < value.length;) {
    if (value[index] === '\\' && index + 1 < value.length && /[\\`*_[\]{}()#+.!|>~-]/.test(value[index + 1])) {
      text += value[index + 1]
      index += 2
      continue
    }

    const image = value.startsWith('![', index)
    const link = image || value[index] === '['
    if (link) {
      const labelStart = index + (image ? 2 : 1)
      const labelEnd = closingBracket(value, labelStart)
      if (labelEnd >= 0 && value[labelEnd + 1] === '(') {
        const destinationEnd = closingParenthesis(value, labelEnd + 2)
        if (destinationEnd >= 0) {
          flushText()
          const label = parseInline(value.slice(labelStart, labelEnd))
          if (image) nodes.push(...label)
          else nodes.push({ type: 'link', href: safeMarkdownHref(linkDestination(value.slice(labelEnd + 2, destinationEnd))), children: label })
          index = destinationEnd + 1
          continue
        }
      }
    }

    if (value[index] === '`') {
      let ticks = 1
      while (value[index + ticks] === '`') ticks += 1
      const marker = '`'.repeat(ticks)
      const end = closingDelimiter(value, marker, index + ticks)
      if (end >= 0) {
        flushText()
        nodes.push({ type: 'code', value: value.slice(index + ticks, end).replace(/^ | $/g, '') })
        index = end + ticks
        continue
      }
    }

    const marker = markerAt(value, index)
    if (marker) {
      const end = closingDelimiter(value, marker, index + marker.length)
      if (end > index + marker.length) {
        flushText()
        const type = marker === '**' || marker === '__' ? 'strong' : marker === '~~' ? 'delete' : 'emphasis'
        nodes.push({ type, children: parseInline(value.slice(index + marker.length, end)) })
        index = end + marker.length
        continue
      }
    }

    text += value[index]
    index += 1
  }

  flushText()
  return nodes
}

function plainText(nodes: MarkdownExcerptNode[]): string {
  return nodes.map(node => node.type === 'text' || node.type === 'code' ? node.value : plainText(node.children)).join('')
}

function truncate(nodes: MarkdownExcerptNode[], maxLength?: number) {
  if (maxLength == null || maxLength < 1 || plainText(nodes).length <= maxLength) return nodes
  let remaining = Math.max(0, maxLength - 1)

  function visit(items: MarkdownExcerptNode[]): MarkdownExcerptNode[] {
    const result: MarkdownExcerptNode[] = []
    for (const node of items) {
      if (remaining <= 0) break
      if (node.type === 'text' || node.type === 'code') {
        const value = node.value.slice(0, remaining)
        if (value) result.push({ ...node, value })
        remaining -= value.length
        continue
      }
      const children = visit(node.children)
      if (children.length) result.push({ ...node, children })
    }
    return result
  }

  return [...visit(nodes), { type: 'text', value: '…' } satisfies MarkdownExcerptNode]
}

export function markdownExcerptNodes(value: string, maxLength?: number) {
  return truncate(parseInline(flattenBlocks(value)), maxLength)
}

export function compactMarkdownText(value: string, maxLength?: number) {
  return plainText(markdownExcerptNodes(value, maxLength))
}
