export type MarkdownExcerptNode =
  | { type: 'text'; value: string }
  | { type: 'code'; value: string }
  | { type: 'strong' | 'emphasis' | 'delete'; children: MarkdownExcerptNode[] }
  | { type: 'link'; href: string | null; children: MarkdownExcerptNode[] }

interface ParseBudget {
  remaining: number
}

const INLINE_PARSE_BUDGET_MULTIPLIER = 16
const MIN_INLINE_PARSE_BUDGET = 256
const INTERNAL_MEMORY_CITATION = /<oai-mem-citation(?:\s[^>]*)?>[\s\S]*?<\/oai-mem-citation\s*>/gi

export function stripInternalMarkdownMetadata(value: string) {
  return value.replace(INTERNAL_MEMORY_CITATION, '')
}

type FlattenedSegment =
  | { type: 'markdown'; value: string }
  | { type: 'code'; value: string }

interface Fence {
  marker: '`' | '~'
  length: number
}

function fenceAt(line: string): Fence | null {
  let index = 0
  while (index < Math.min(3, line.length) && line[index] === ' ') index += 1
  const marker = line[index]
  if (marker !== '`' && marker !== '~') return null
  let end = index
  while (line[end] === marker) end += 1
  if (end - index < 3) return null
  return { marker, length: end - index }
}

function closesFence(line: string, fence: Fence) {
  let index = 0
  while (index < Math.min(3, line.length) && line[index] === ' ') index += 1
  let end = index
  while (line[end] === fence.marker) end += 1
  return end - index >= fence.length && line.slice(end).trim().length === 0
}

function isSetextUnderline(line: string) {
  const value = line.trim()
  return /^=+$/.test(value) || /^-+$/.test(value)
}

function isTableDivider(line: string) {
  let value = line.trim()
  if (value.startsWith('|')) value = value.slice(1)
  if (value.endsWith('|')) value = value.slice(0, -1)
  const cells = value.split('|')
  if (cells.length < 2) return false
  return cells.every(cell => {
    const divider = cell.trim()
    const body = divider.startsWith(':') ? divider.slice(1) : divider
    const hyphens = body.endsWith(':') ? body.slice(0, -1) : body
    return /^-{3,}$/.test(hyphens)
  })
}

function stripBlockPrefix(line: string) {
  let value = line
  let previous = ''
  while (value !== previous) {
    previous = value
    value = value
      .replace(/^\s{0,3}>\s?/, '')
      .replace(/^\s{0,3}#{1,6}(?:\s+|$)/, '')
      .replace(/^\s{0,3}(?:[-+*]|\d+[.)])\s+(?:\[[ xX]\]\s+)?/, '')
  }
  return value
}

function closingBackticks(value: string, start: number, length: number) {
  for (let index = start; index < value.length;) {
    if (value[index] !== '`') { index += 1; continue }
    let end = index
    while (value[end] === '`') end += 1
    if (end - index === length) return index
    index = end
  }
  return -1
}

function closingLinkDestination(value: string, start: number) {
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

function closingHtmlTag(value: string, start: number) {
  let quote: '"' | "'" | null = null
  for (let index = start; index < value.length; index += 1) {
    const character = value[index]
    if (quote) {
      if (character === quote) quote = null
      continue
    }
    if (character === '"' || character === "'") { quote = character; continue }
    if (character === '>') return index
  }
  return -1
}

function looksLikeHtmlTag(value: string, index: number) {
  const nameStart = value[index + 1] === '/' ? index + 2 : index + 1
  return /[A-Za-z]/.test(value[nameStart] ?? '')
}

function sanitizeMarkdown(value: string) {
  const result: string[] = []
  let pendingSpace = false
  let hasOutput = false
  let endsWithSpace = false

  function append(valueToAppend: string) {
    if (!valueToAppend) return
    if (pendingSpace && hasOutput && !endsWithSpace) result.push(' ')
    pendingSpace = false
    result.push(valueToAppend)
    hasOutput = true
    endsWithSpace = valueToAppend.endsWith(' ')
  }

  for (let index = 0; index < value.length;) {
    if (/\s/.test(value[index])) {
      pendingSpace = true
      index += 1
      continue
    }

    if (value.startsWith('<!--', index)) {
      const end = value.indexOf('-->', index + 4)
      if (end < 0) {
        append(value.slice(index))
        break
      }
      pendingSpace = hasOutput
      index = end + 3
      continue
    }

    if (value[index] === '`') {
      let end = index
      while (value[end] === '`') end += 1
      const close = closingBackticks(value, end, end - index)
      if (close < 0) {
        append(value.slice(index))
        break
      }
      append(value.slice(index, close + end - index))
      index = close + end - index
      continue
    }

    if (value[index] === ']' && value[index + 1] === '(') {
      const close = closingLinkDestination(value, index + 2)
      if (close < 0) {
        append(value.slice(index))
        break
      }
      append(value.slice(index, close + 1))
      index = close + 1
      continue
    }

    if (value[index] === '<' && looksLikeHtmlTag(value, index)) {
      const close = closingHtmlTag(value, index + 1)
      if (close < 0) {
        append(value.slice(index))
        break
      }
      index = close + 1
      continue
    }

    let end = index + 1
    while (end < value.length
      && !/\s/.test(value[end])
      && value[end] !== '`'
      && value[end] !== '<'
      && !(value[end] === ']' && value[end + 1] === '(')) end += 1
    append(value.slice(index, end))
    index = end
  }

  return result.join('').trim()
}

function flattenBlocks(value: string): FlattenedSegment[] {
  const segments: FlattenedSegment[] = []
  const prose: string[] = []
  const code: string[] = []
  let fence: Fence | null = null

  function flushProse() {
    const flattened = sanitizeMarkdown(prose.join('\n'))
    if (flattened) segments.push({ type: 'markdown', value: flattened })
    prose.length = 0
  }

  function flushCode() {
    if (code.length > 0) segments.push({ type: 'code', value: code.join('\n') })
    code.length = 0
  }

  for (const line of value.replace(/\r\n?/g, '\n').split('\n')) {
    if (fence) {
      if (closesFence(line, fence)) {
        flushCode()
        fence = null
      } else {
        code.push(line)
      }
      continue
    }

    const openingFence = fenceAt(line)
    if (openingFence) {
      flushProse()
      fence = openingFence
      continue
    }
    if (isSetextUnderline(line) || isTableDivider(line)) continue
    prose.push(stripBlockPrefix(line))
  }

  if (fence) flushCode()
  flushProse()
  return segments
}

function markerAt(value: string, index: number) {
  for (const marker of ['**', '__', '~~', '*', '_']) {
    if (value.startsWith(marker, index)) return marker
  }
  return null
}

function consumeBudget(budget: ParseBudget, amount = 1) {
  if (budget.remaining < amount) {
    budget.remaining = 0
    return false
  }
  budget.remaining -= amount
  return true
}

function closingDelimiter(value: string, marker: string, start: number, budget: ParseBudget) {
  let cursor = start
  while (cursor < value.length) {
    const next = value.indexOf(marker, cursor)
    const scanned = next < 0 ? value.length - cursor : next - cursor + marker.length
    if (!consumeBudget(budget, scanned)) return -1
    if (next < 0) return -1
    if (next === 0 || value[next - 1] !== '\\') return next
    cursor = next + marker.length
  }
  return -1
}

function closingBracket(value: string, start: number, budget: ParseBudget) {
  let depth = 1
  for (let index = start; index < value.length; index += 1) {
    if (!consumeBudget(budget)) return -1
    if (value[index] === '\\') { index += 1; continue }
    if (value[index] === '[') depth += 1
    if (value[index] === ']') depth -= 1
    if (depth === 0) return index
  }
  return -1
}

function closingParenthesis(value: string, start: number, budget: ParseBudget) {
  let depth = 1
  let angleWrapped = false
  for (let index = start; index < value.length; index += 1) {
    if (!consumeBudget(budget)) return -1
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

function parseInline(value: string, budget: ParseBudget = {
  remaining: Math.max(MIN_INLINE_PARSE_BUDGET, value.length * INLINE_PARSE_BUDGET_MULTIPLIER),
}): MarkdownExcerptNode[] {
  const nodes: MarkdownExcerptNode[] = []
  let text = ''

  function flushText() {
    if (!text) return
    nodes.push({ type: 'text', value: text })
    text = ''
  }

  for (let index = 0; index < value.length;) {
    if (!consumeBudget(budget)) {
      text += value.slice(index)
      break
    }
    if (value[index] === '\\' && index + 1 < value.length && /[\\`*_[\]{}()#+.!|>~-]/.test(value[index + 1])) {
      text += value[index + 1]
      index += 2
      continue
    }

    const image = value.startsWith('![', index)
    const link = image || value[index] === '['
    if (link) {
      const labelStart = index + (image ? 2 : 1)
      const labelEnd = closingBracket(value, labelStart, budget)
      if (labelEnd >= 0 && value[labelEnd + 1] === '(') {
        const destinationEnd = closingParenthesis(value, labelEnd + 2, budget)
        if (destinationEnd >= 0) {
          flushText()
          const label = parseInline(value.slice(labelStart, labelEnd), budget)
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
      const end = closingDelimiter(value, marker, index + ticks, budget)
      if (end >= 0) {
        flushText()
        nodes.push({ type: 'code', value: value.slice(index + ticks, end).replace(/^ | $/g, '') })
        index = end + ticks
        continue
      }
    }

    const marker = markerAt(value, index)
    if (marker) {
      const end = closingDelimiter(value, marker, index + marker.length, budget)
      if (end > index + marker.length) {
        flushText()
        const type = marker === '**' || marker === '__' ? 'strong' : marker === '~~' ? 'delete' : 'emphasis'
        nodes.push({ type, children: parseInline(value.slice(index + marker.length, end), budget) })
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
  const nodes: MarkdownExcerptNode[] = []
  for (const segment of flattenBlocks(stripInternalMarkdownMetadata(value))) {
    if (nodes.length > 0) nodes.push({ type: 'text', value: ' ' })
    if (segment.type === 'code') nodes.push({ type: 'code', value: segment.value })
    else nodes.push(...parseInline(segment.value))
  }
  return truncate(nodes, maxLength)
}

export function compactMarkdownText(value: string, maxLength?: number) {
  return plainText(markdownExcerptNodes(value, maxLength))
}
