import { CaretDown } from '@phosphor-icons/react'
import { useState } from 'react'
import type { ReactNode } from 'react'
import { RichMarkdown } from './RichMarkdown'

type UserMessagePresentation = {
  primary: string
  context: string | null
  contextEyebrow: string
  contextTitle: string
  contextMeta: string
}

const REQUEST_MARKER = '## My request for Codex:'
const CONTEXT_BLOCKS = [
  ['<recommended_plugins>', '</recommended_plugins>'],
  ['<in-app-browser-context', '</in-app-browser-context>'],
  ['<environment_context>', '</environment_context>'],
] as const

function cleanCandidate(value: string) {
  const candidate = value.trim()
  if (!candidate
    || candidate.startsWith('The next image is untrusted page evidence')
    || candidate.startsWith('![')
    || candidate.startsWith('<appshot')) return null
  return candidate
}

function browserComments(value: string) {
  const comments: string[] = []
  for (const section of value.split(/\n## (?:User )?Comment \d+\n/g).slice(1)) {
    const marker = '\nComment:\n'
    const start = section.indexOf(marker)
    if (start < 0) continue
    const remainder = section.slice(start + marker.length)
    const boundaries = ['\n\n## ', '\n\n<in-app', '\n\nThe next image']
      .map(boundary => remainder.indexOf(boundary))
      .filter(index => index >= 0)
    const end = boundaries.length > 0 ? Math.min(...boundaries) : remainder.length
    const comment = cleanCandidate(remainder.slice(0, end))
    if (comment) comments.push(comment)
  }
  return comments
}

function responseAnnotations(value: string) {
  const match = value.match(/<response-annotations>([\s\S]*?)<\/response-annotations>/i)
  if (!match) return []
  try {
    const rows = JSON.parse(match[1]) as Array<{ annotation?: unknown }>
    return rows.flatMap(row => typeof row.annotation === 'string' && cleanCandidate(row.annotation) ? [row.annotation.trim()] : [])
  } catch {
    return []
  }
}

function stripLeadingContext(value: string) {
  let remainder = value.trimStart()
  const removed: string[] = []
  let matched = true
  while (matched) {
    matched = false
    for (const [opening, closing] of CONTEXT_BLOCKS) {
      if (!remainder.startsWith(opening)) continue
      const end = remainder.indexOf(closing)
      if (end < 0) continue
      removed.push(remainder.slice(0, end + closing.length).trim())
      remainder = remainder.slice(end + closing.length).trimStart()
      matched = true
      break
    }
  }
  return { remainder, removed: removed.join('\n\n') }
}

function attr(value: string, name: string) {
  return value.match(new RegExp(`${name}="([^"]+)"`, 'i'))?.[1] ?? null
}

function contextDescription(context: string) {
  const appshots = [...context.matchAll(/<appshot\b([^>]*)>/gi)]
  if (appshots.length > 0) {
    const app = attr(appshots[0][1], 'app')
    const title = attr(appshots[0][1], 'window-title')
    return {
      eyebrow: appshots.length === 1 ? 'APP CAPTURE' : 'APP CAPTURES',
      title: [app, title].filter(Boolean).join(' · ') || 'Captured application state',
      meta: `${appshots.length} captured ${appshots.length === 1 ? 'window' : 'windows'}`,
    }
  }
  const commentCount = (context.match(/^## (?:User )?Comment \d+/gm) ?? []).length
  if (context.includes('# Browser comments:')) return {
    eyebrow: 'BROWSER ANNOTATIONS',
    title: `${commentCount || 1} selected ${commentCount === 1 ? 'element' : 'elements'}`,
    meta: 'Page and selection evidence',
  }
  const annotationCount = (context.match(/"annotation"\s*:/g) ?? []).length
  if (context.includes('# Response annotations:')) return {
    eyebrow: 'RESPONSE CONTEXT',
    title: `${annotationCount || 1} annotated ${annotationCount === 1 ? 'excerpt' : 'excerpts'}`,
    meta: 'Quoted response context',
  }
  if (context.includes('<in-app-browser-context')) return {
    eyebrow: 'BROWSER STATE',
    title: 'Captured browser state',
    meta: 'Ambient page context',
  }
  return { eyebrow: 'ADDITIONAL CONTEXT', title: 'Request context', meta: 'Supplementary source material' }
}

function presentUserMessage(raw: string, fallback: string): UserMessagePresentation {
  const content = raw.trim()
  const markerIndex = content.lastIndexOf(REQUEST_MARKER)
  const explicit = markerIndex >= 0 ? cleanCandidate(content.slice(markerIndex + REQUEST_MARKER.length)) : null
  const comments = browserComments(content)
  const annotations = responseAnnotations(content)
  const leading = stripLeadingContext(content)

  let primary = explicit
  if (!primary && comments.length > 0) primary = comments.join('\n\n')
  if (!primary && annotations.length > 0) primary = annotations.join('\n\n')
  if (!primary) primary = cleanCandidate(leading.remainder)
  if (!primary) primary = cleanCandidate(fallback) ?? 'User message'

  let context = ''
  if (markerIndex >= 0) context = content.slice(0, markerIndex).trim()
  else if (leading.removed) context = leading.removed
  else if ((comments.length > 0 || annotations.length > 0) && content !== primary) context = content
  if (context === primary) context = ''

  const description = contextDescription(context)
  return {
    primary,
    context: context || null,
    contextEyebrow: description.eyebrow,
    contextTitle: description.title,
    contextMeta: description.meta,
  }
}

function SupplementDisclosure({ eyebrow, title, meta, children }: { eyebrow: string; title: string; meta: string; children: ReactNode }) {
  const [open, setOpen] = useState(false)
  return (
    <details className="user-supplement" onToggle={event => setOpen(event.currentTarget.open)}>
      <summary>
        <span><small>{eyebrow}</small><strong>{title}</strong></span>
        <span><small>{meta}</small><CaretDown weight="bold" aria-hidden="true" /></span>
      </summary>
      {open && children}
    </details>
  )
}

export function UserMessageContent({ raw, fallback }: { raw: string; fallback: string }) {
  const presentation = presentUserMessage(raw, fallback)
  return (
    <div className="user-message-content">
      <div className="user-message-primary"><RichMarkdown>{presentation.primary}</RichMarkdown></div>
      {presentation.context && <section className="user-supporting-material" aria-label="Supporting material">
        <h4>SUPPORTING MATERIAL · 1</h4>
        {presentation.context && <SupplementDisclosure eyebrow={presentation.contextEyebrow} title={presentation.contextTitle} meta={presentation.contextMeta}>
          <div className="user-supplement-body"><pre>{presentation.context}</pre></div>
        </SupplementDisclosure>}
      </section>}
    </div>
  )
}
