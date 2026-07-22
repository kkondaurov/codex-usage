import { describe, expect, it } from 'vitest'
import {
  addDecimal,
  compareDecimal,
  decimalRatioGreaterThan,
  decimalSign,
  formatUsd,
  isDecimalString,
} from './decimal'

describe('decimal money helpers', () => {
  it('validates plain decimal strings without accepting exponent notation or binary numbers', () => {
    expect(isDecimalString('0')).toBe(true)
    expect(isDecimalString('-12.3400')).toBe(true)
    expect(isDecimalString('01.2')).toBe(false)
    expect(isDecimalString('1e3')).toBe(false)
    expect(isDecimalString(1.2)).toBe(false)
  })

  it('adds and compares arbitrary-precision decimals exactly', () => {
    expect(addDecimal('9007199254740992.000000000001', '0.000000000009')).toBe('9007199254740992.00000000001')
    expect(addDecimal('-1.25', '0.25')).toBe('-1')
    expect(compareDecimal('9007199254740993', '9007199254740992.999999999999')).toBe(1)
    expect(decimalSign('-0.000000000001')).toBe(-1)
  })

  it('formats and rounds USD without converting through Number', () => {
    expect(formatUsd('2')).toBe('$2.00')
    expect(formatUsd('2.5')).toBe('$2.50')
    expect(formatUsd('12345678901234567890.125')).toBe('$12,345,678,901,234,567,890.13')
    expect(formatUsd('-0.004')).toBe('$0.00')
    expect(formatUsd('-0.005')).toBe('-$0.01')
    expect(formatUsd('12.5', 0)).toBe('$13')
  })

  it('compares heatmap ratios by integer cross multiplication', () => {
    expect(decimalRatioGreaterThan('5.500000000001', '10', 55n, 100n)).toBe(true)
    expect(decimalRatioGreaterThan('5.5', '10', 55n, 100n)).toBe(false)
  })
})
