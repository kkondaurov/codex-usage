import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { describe, expect, it } from 'vitest'

const styles = readFileSync(resolve(process.cwd(), 'src/styles.css'), 'utf8')

function colorVariable(name: string) {
  const match = styles.match(new RegExp(`--${name}:\\s*(#[0-9a-f]{6})`, 'i'))
  if (!match) throw new Error(`Missing --${name} color token`)
  return match[1]
}

function luminance(hex: string) {
  const channels = hex.slice(1).match(/.{2}/g)?.map(value => Number.parseInt(value, 16) / 255) ?? []
  const linear = channels.map(value => value <= 0.04045 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4)
  return 0.2126 * linear[0] + 0.7152 * linear[1] + 0.0722 * linear[2]
}

function contrast(foreground: string, background: string) {
  const lighter = Math.max(luminance(foreground), luminance(background))
  const darker = Math.min(luminance(foreground), luminance(background))
  return (lighter + 0.05) / (darker + 0.05)
}

describe('core accessibility tokens', () => {
  it('keeps warning and accent copy at WCAG AA text contrast', () => {
    expect(contrast(colorVariable('accent-text'), colorVariable('yellow'))).toBeGreaterThanOrEqual(4.5)
    expect(contrast(colorVariable('coral-dark'), '#fce0d8')).toBeGreaterThanOrEqual(4.5)
  })

  it('uses a high-contrast keyboard focus indicator on every interactive surface', () => {
    expect(styles).toMatch(/button:focus-visible[^{]*\{[^}]*outline:\s*3px solid var\(--text\)/)
    expect(styles).toMatch(/\.search-field:focus-within\s*\{[^}]*outline:\s*3px solid var\(--text\)/)
    expect(contrast(colorVariable('text'), colorVariable('yellow'))).toBeGreaterThanOrEqual(3)
  })

  it('keeps selected-filter clear controls visibly styled', () => {
    const clearControl = styles.match(/\.clear-filter-button\s*\{([^}]*)\}/)?.[1] ?? ''
    expect(clearControl).toMatch(/background:\s*var\(--coral\)/)
    expect(clearControl).not.toMatch(/opacity:\s*0/)
  })
})
