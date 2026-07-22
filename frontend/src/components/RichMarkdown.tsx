import { CompactMarkdown } from './CompactMarkdown'
import { stripInternalMarkdownMetadata } from './markdownExcerpt'

type RichMarkdownBlock =
  | { type: 'paragraph'; text: string }
  | { type: 'heading'; level: number; text: string }
  | { type: 'list'; ordered: boolean; start?: number; items: string[] }
  | { type: 'quote'; text: string }
  | { type: 'code'; language?: string; text: string }
  | { type: 'rule' }

const FENCE = /^\s{0,3}(```+|~~~+)\s*([A-Za-z0-9_+-]+)?\s*$/
const HEADING = /^\s{0,3}(#{1,6})\s+(.+?)\s*#*\s*$/
const LIST_ITEM = /^\s{0,3}([-+*]|\d+[.)])\s+(?:\[[ xX]\]\s+)?(.+)$/
const QUOTE = /^\s{0,3}>\s?(.*)$/
const RULE = /^\s{0,3}((?:\*\s*){3,}|(?:-\s*){3,}|(?:_\s*){3,})$/

function startsBlock(line: string) {
  return FENCE.test(line) || HEADING.test(line) || LIST_ITEM.test(line) || QUOTE.test(line) || RULE.test(line)
}

function richMarkdownBlocks(value: string): RichMarkdownBlock[] {
  const lines = stripInternalMarkdownMetadata(value).replace(/\r\n?/g, '\n').trim().split('\n')
  const blocks: RichMarkdownBlock[] = []

  for (let index = 0; index < lines.length;) {
    const line = lines[index]
    if (!line.trim()) { index += 1; continue }

    const fence = line.match(FENCE)
    if (fence) {
      const marker = fence[1]
      const body: string[] = []
      index += 1
      while (index < lines.length && !new RegExp(`^\\s{0,3}${marker[0]}{${marker.length},}\\s*$`).test(lines[index])) {
        body.push(lines[index])
        index += 1
      }
      if (index < lines.length) index += 1
      blocks.push({ type: 'code', language: fence[2], text: body.join('\n') })
      continue
    }

    const heading = line.match(HEADING)
    if (heading) {
      blocks.push({ type: 'heading', level: heading[1].length, text: heading[2] })
      index += 1
      continue
    }

    const list = line.match(LIST_ITEM)
    if (list) {
      const ordered = /^\d/.test(list[1])
      const start = ordered ? Number.parseInt(list[1], 10) : undefined
      const items: string[] = []
      while (index < lines.length) {
        const item = lines[index].match(LIST_ITEM)
        if (!item || /^\d/.test(item[1]) !== ordered) break
        let text = item[2]
        index += 1
        while (index < lines.length && /^\s{2,}\S/.test(lines[index]) && !LIST_ITEM.test(lines[index])) {
          text += ` ${lines[index].trim()}`
          index += 1
        }
        items.push(text)
      }
      blocks.push({ type: 'list', ordered, start, items })
      continue
    }

    const quote = line.match(QUOTE)
    if (quote) {
      const quoted = [quote[1]]
      index += 1
      while (index < lines.length) {
        const next = lines[index].match(QUOTE)
        if (!next) break
        quoted.push(next[1])
        index += 1
      }
      blocks.push({ type: 'quote', text: quoted.join(' ') })
      continue
    }

    if (RULE.test(line)) {
      blocks.push({ type: 'rule' })
      index += 1
      continue
    }

    const paragraph = [line.trim()]
    index += 1
    while (index < lines.length && lines[index].trim() && !startsBlock(lines[index])) {
      paragraph.push(lines[index].trim())
      index += 1
    }
    blocks.push({ type: 'paragraph', text: paragraph.join(' ') })
  }

  return blocks
}

function Inline({ children }: { children: string }) {
  return <CompactMarkdown links="anchor">{children}</CompactMarkdown>
}

function MarkdownHeading({ level, children }: { level: number; children: string }) {
  const content = <Inline>{children}</Inline>
  if (level <= 1) return <h3 className="level-1">{content}</h3>
  if (level === 2) return <h4 className="level-2">{content}</h4>
  if (level === 3) return <h5 className="level-3">{content}</h5>
  return <h6 className={`level-${level}`}>{content}</h6>
}

export function RichMarkdown({ children }: { children: string }) {
  return (
    <div className="activity-rich-markdown">
      {richMarkdownBlocks(children).map((block, index) => {
        const key = `${block.type}-${index}`
        if (block.type === 'heading') return <MarkdownHeading level={block.level} key={key}>{block.text}</MarkdownHeading>
        if (block.type === 'list') {
          const items = block.items.map((item, itemIndex) => <li key={`${key}-${itemIndex}`}><Inline>{item}</Inline></li>)
          return block.ordered ? <ol key={key} start={block.start}>{items}</ol> : <ul key={key}>{items}</ul>
        }
        if (block.type === 'quote') return <blockquote key={key}><Inline>{block.text}</Inline></blockquote>
        if (block.type === 'code') return <pre key={key}><code data-language={block.language}>{block.text}</code></pre>
        if (block.type === 'rule') return <hr key={key} />
        return <p key={key}><Inline>{block.text}</Inline></p>
      })}
    </div>
  )
}
