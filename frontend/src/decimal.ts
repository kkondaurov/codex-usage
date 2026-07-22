export type DecimalString = string

const DECIMAL_PATTERN = /^-?(?:0|[1-9]\d*)(?:\.\d+)?$/

interface DecimalParts {
  negative: boolean
  coefficient: bigint
  scale: number
}

export function isDecimalString(value: unknown): value is DecimalString {
  return typeof value === 'string' && DECIMAL_PATTERN.test(value)
}

function parts(value: DecimalString): DecimalParts {
  if (!isDecimalString(value)) throw new Error(`Invalid decimal string: ${value}`)
  const negative = value.startsWith('-')
  const unsigned = negative ? value.slice(1) : value
  const [whole, fraction = ''] = unsigned.split('.')
  return {
    negative,
    coefficient: BigInt(`${whole}${fraction}`),
    scale: fraction.length,
  }
}

function powerOfTen(exponent: number) {
  return 10n ** BigInt(exponent)
}

function signedCoefficient(value: DecimalParts, scale: number) {
  const coefficient = value.coefficient * powerOfTen(scale - value.scale)
  return value.negative ? -coefficient : coefficient
}

function decimalFromScaled(coefficient: bigint, scale: number): DecimalString {
  const negative = coefficient < 0n
  let digits = (negative ? -coefficient : coefficient).toString().padStart(scale + 1, '0')
  if (scale === 0) return `${negative ? '-' : ''}${digits}`
  const split = digits.length - scale
  const fraction = digits.slice(split).replace(/0+$/, '')
  digits = digits.slice(0, split)
  if (!fraction) return `${negative ? '-' : ''}${digits}`
  return `${negative ? '-' : ''}${digits}.${fraction}`
}

export function addDecimal(left: DecimalString, right: DecimalString): DecimalString {
  const leftParts = parts(left)
  const rightParts = parts(right)
  const scale = Math.max(leftParts.scale, rightParts.scale)
  return decimalFromScaled(
    signedCoefficient(leftParts, scale) + signedCoefficient(rightParts, scale),
    scale,
  )
}

export function compareDecimal(left: DecimalString, right: DecimalString) {
  const leftParts = parts(left)
  const rightParts = parts(right)
  const scale = Math.max(leftParts.scale, rightParts.scale)
  const difference = signedCoefficient(leftParts, scale) - signedCoefficient(rightParts, scale)
  return difference < 0n ? -1 : difference > 0n ? 1 : 0
}

export function decimalSign(value: DecimalString) {
  return compareDecimal(value, '0')
}

export function decimalRatioGreaterThan(
  value: DecimalString,
  maximum: DecimalString,
  numerator: bigint,
  denominator: bigint,
) {
  const valueParts = parts(value)
  const maximumParts = parts(maximum)
  const scale = Math.max(valueParts.scale, maximumParts.scale)
  return signedCoefficient(valueParts, scale) * denominator
    > signedCoefficient(maximumParts, scale) * numerator
}

export function formatUsd(value: DecimalString, digits = 2) {
  const valueParts = parts(value)
  const drop = valueParts.scale - digits
  let rounded: bigint
  if (drop <= 0) {
    rounded = valueParts.coefficient * powerOfTen(-drop)
  } else {
    const divisor = powerOfTen(drop)
    rounded = valueParts.coefficient / divisor
    const remainder = valueParts.coefficient % divisor
    if (remainder * 2n >= divisor) rounded += 1n
  }

  const padded = rounded.toString().padStart(digits + 1, '0')
  const wholeDigits = digits === 0 ? padded : padded.slice(0, -digits)
  const fraction = digits === 0 ? '' : padded.slice(-digits)
  const groupedWhole = wholeDigits.replace(/\B(?=(\d{3})+(?!\d))/g, ',')
  const negative = valueParts.negative && rounded !== 0n
  return `${negative ? '-' : ''}$${groupedWhole}${digits === 0 ? '' : `.${fraction}`}`
}
