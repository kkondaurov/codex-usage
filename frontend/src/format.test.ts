import { describe, expect, it } from 'vitest'
import {
  bytes,
  duration,
  ellipsis,
  estimatedMoney,
  money,
  relativeTime,
  shortDate,
  shortDateTime,
  time,
  tokens,
} from './format'

describe('display formatting', () => {
  it('keeps unknown prices distinct from zero', () => {
    expect(money(null)).toBe('—')
    expect(money('0')).toBe('$0.00')
    expect(estimatedMoney('0', 25_607)).toBe('—')
    expect(estimatedMoney('0', 0)).toBe('$0.00')
  })

  it('formats token and byte magnitudes for the ledgers', () => {
    expect(tokens(12_800_000)).toBe('12.8M')
    expect(bytes(128 * 1024 * 1024)).toBe('128 MB')
  })

  it('collapses whitespace before truncating copy', () => {
    expect(ellipsis(' one\n two   three ', 13)).toBe('one two three')
    expect(ellipsis('one two three four', 12)).toBe('one two thr…')
  })

  it('keeps tool durations compact', () => {
    expect(duration(420)).toBe('420ms')
    expect(duration(2_400)).toBe('2.4s')
    expect(duration(125_000)).toBe('2m 5s')
    expect(duration(899_999)).toBe('15m 0s')
    expect(duration(3_599_499)).toBe('59m 59s')
    expect(duration(3_599_500)).toBe('1h 0m')
    expect(duration(4_425_000)).toBe('1h 14m')
    expect(duration(16_107_000)).toBe('4h 28m')
  })

  it('renders invalid dates as unknown instead of throwing', () => {
    expect(shortDate('not-a-date')).toBe('—')
    expect(shortDateTime('not-a-date')).toBe('—')
    expect(time('not-a-date')).toBe('—')
    expect(relativeTime('not-a-date')).toBe('at an unknown time')
  })
})
