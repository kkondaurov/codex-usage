import { act, fireEvent, render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { invalidateAsyncCache, useAsync, useCachedAsync } from './hooks'

const STALE_MS = 30_000

function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (reason: Error) => void
  const promise = new Promise<T>((nextResolve, nextReject) => {
    resolve = nextResolve
    reject = nextReject
  })
  return { promise, resolve, reject }
}

async function flush() {
  await act(async () => {
    await Promise.resolve()
    await Promise.resolve()
    await Promise.resolve()
  })
}

function CacheProbe({ cacheKey, loader }: { cacheKey: string; loader: (signal: AbortSignal) => Promise<string> }) {
  const { data, loading } = useCachedAsync(cacheKey, loader, [cacheKey], STALE_MS, STALE_MS)
  return <span>{loading && !data ? 'loading' : data}</span>
}

function AsyncProbe({ requestKey, loader }: { requestKey: string; loader: (signal: AbortSignal) => Promise<string> }) {
  const { data, error, loading } = useAsync(loader, [requestKey])
  if (error && !data) return <span>{error.message}</span>
  if (loading && !data) return <span>loading</span>
  return <span>{data ?? 'empty'}</span>
}

function AsyncHealthProbe({ loader }: { loader: (signal: AbortSignal) => Promise<string> }) {
  const { data, error, lastSuccessfulAt, refresh } = useAsync(loader, ['health'])
  return <><span>{data ?? 'loading'}</span><output aria-label="Last success">{lastSuccessfulAt ?? 'none'}</output>{error && <span>{error.message}</span>}<button type="button" onClick={() => void refresh(true)}>REFRESH</button></>
}

beforeEach(() => {
  vi.useFakeTimers()
  vi.setSystemTime(new Date('2026-07-17T12:00:00+02:00'))
})

afterEach(() => {
  vi.useRealTimers()
  vi.restoreAllMocks()
})

describe('useCachedAsync', () => {
  it('renders cached data synchronously without refetching when remounted within the freshness window', async () => {
    const loader = vi.fn().mockResolvedValue('cached overview')
    const first = render(<CacheProbe cacheKey="overview" loader={loader} />)
    await flush()
    expect(screen.getByText('cached overview')).toBeInTheDocument()
    expect(loader).toHaveBeenCalledTimes(1)

    first.unmount()
    act(() => vi.advanceTimersByTime(10_000))
    render(<CacheProbe cacheKey="overview" loader={loader} />)

    expect(screen.getByText('cached overview')).toBeInTheDocument()
    expect(loader).toHaveBeenCalledTimes(1)
  })

  it('keeps stale data visible while performing one quiet background refresh', async () => {
    const revalidation = deferred<string>()
    const loader = vi.fn()
      .mockResolvedValueOnce('stale overview')
      .mockReturnValueOnce(revalidation.promise)
    const first = render(<CacheProbe cacheKey="overview" loader={loader} />)
    await flush()
    first.unmount()
    act(() => vi.advanceTimersByTime(STALE_MS + 1))

    render(<CacheProbe cacheKey="overview" loader={loader} />)
    expect(screen.getByText('stale overview')).toBeInTheDocument()
    await flush()
    expect(loader).toHaveBeenCalledTimes(2)
    expect(screen.getByText('stale overview')).toBeInTheDocument()

    await act(async () => {
      revalidation.resolve('fresh overview')
      await Promise.resolve()
      await Promise.resolve()
    })
    expect(screen.getByText('fresh overview')).toBeInTheDocument()
    expect(loader).toHaveBeenCalledTimes(2)
  })

  it('preserves stale data when a background refresh fails', async () => {
    const loader = vi.fn()
      .mockResolvedValueOnce('last good overview')
      .mockRejectedValueOnce(new Error('refresh failed'))
    const first = render(<CacheProbe cacheKey="overview" loader={loader} />)
    await flush()
    first.unmount()
    act(() => vi.advanceTimersByTime(STALE_MS + 1))

    render(<CacheProbe cacheKey="overview" loader={loader} />)
    await flush()

    expect(screen.getByText('last good overview')).toBeInTheDocument()
    expect(loader).toHaveBeenCalledTimes(2)
  })

  it('deduplicates an in-flight request across unmount and remount', async () => {
    const request = deferred<string>()
    let sharedSignal: AbortSignal | undefined
    const loader = vi.fn((signal: AbortSignal) => {
      sharedSignal = signal
      return request.promise
    })
    const first = render(<CacheProbe cacheKey="overview" loader={loader} />)
    await flush()
    expect(loader).toHaveBeenCalledTimes(1)

    first.unmount()
    expect(sharedSignal?.aborted).toBe(false)
    act(() => vi.advanceTimersByTime(50))
    render(<CacheProbe cacheKey="overview" loader={loader} />)
    await flush()
    expect(loader).toHaveBeenCalledTimes(1)
    act(() => vi.advanceTimersByTime(100))
    expect(sharedSignal?.aborted).toBe(false)

    await act(async () => {
      request.resolve('shared result')
      await Promise.resolve()
      await Promise.resolve()
    })
    expect(screen.getByText('shared result')).toBeInTheDocument()
  })

  it('aborts an in-flight request after its last subscriber stays unmounted past the grace period', async () => {
    const request = deferred<string>()
    const signals: AbortSignal[] = []
    const loader = vi.fn((signal: AbortSignal) => {
      signals.push(signal)
      return request.promise
    })
    const view = render(<CacheProbe cacheKey="overview" loader={loader} />)
    await flush()

    view.unmount()
    act(() => vi.advanceTimersByTime(99))
    expect(signals[0]?.aborted).toBe(false)

    act(() => vi.advanceTimersByTime(1))
    expect(signals[0]?.aborted).toBe(true)

    render(<CacheProbe cacheKey="overview" loader={loader} />)
    await flush()
    expect(loader).toHaveBeenCalledTimes(2)
    expect(signals[1]?.aborted).toBe(false)

    await act(async () => {
      request.resolve('fresh request')
      await Promise.resolve()
      await Promise.resolve()
    })
    expect(screen.getByText('fresh request')).toBeInTheDocument()
  })

  it('aborts shared work when its cache entry is explicitly invalidated', async () => {
    const request = deferred<string>()
    let sharedSignal: AbortSignal | undefined
    const loader = vi.fn((signal: AbortSignal) => {
      sharedSignal = signal
      return request.promise
    })
    render(<CacheProbe cacheKey="stats:month" loader={loader} />)
    await flush()

    invalidateAsyncCache('stats:')

    expect(sharedSignal?.aborted).toBe(true)
  })

  it('isolates cached values by key and restores a previously viewed year without refetching', async () => {
    const currentYear = vi.fn().mockResolvedValue('2026 usage')
    const previousYear = vi.fn().mockResolvedValue('2025 usage')
    const { rerender } = render(<CacheProbe cacheKey="overview-year:2026" loader={currentYear} />)
    await flush()
    expect(screen.getByText('2026 usage')).toBeInTheDocument()

    rerender(<CacheProbe cacheKey="overview-year:2025" loader={previousYear} />)
    expect(screen.getByText('loading')).toBeInTheDocument()
    expect(screen.queryByText('2026 usage')).not.toBeInTheDocument()
    await flush()
    expect(screen.getByText('2025 usage')).toBeInTheDocument()

    rerender(<CacheProbe cacheKey="overview-year:2026" loader={currentYear} />)
    expect(screen.getByText('2026 usage')).toBeInTheDocument()
    expect(currentYear).toHaveBeenCalledTimes(1)
    expect(previousYear).toHaveBeenCalledTimes(1)
  })
})

describe('useAsync', () => {
  it('aborts superseded identity requests and the active request on unmount', async () => {
    const first = deferred<string>()
    const replacement = deferred<string>()
    let firstSignal: AbortSignal | undefined
    let replacementSignal: AbortSignal | undefined
    const view = render(
      <AsyncProbe requestKey="session-a" loader={(signal) => { firstSignal = signal; return first.promise }} />,
    )
    await flush()

    view.rerender(
      <AsyncProbe requestKey="session-b" loader={(signal) => { replacementSignal = signal; return replacement.promise }} />,
    )
    await flush()

    expect(firstSignal?.aborted).toBe(true)
    expect(replacementSignal?.aborted).toBe(false)

    view.unmount()
    expect(replacementSignal?.aborted).toBe(true)
  })

  it('retains data and its last-success timestamp when a quiet refresh fails', async () => {
    const loader = vi.fn()
      .mockResolvedValueOnce('last good data')
      .mockRejectedValueOnce(new Error('background refresh failed'))
    render(<AsyncHealthProbe loader={loader} />)
    await flush()

    const successfulAt = Date.now()
    expect(screen.getByText('last good data')).toBeInTheDocument()
    expect(screen.getByLabelText('Last success')).toHaveTextContent(String(successfulAt))

    act(() => vi.advanceTimersByTime(5_000))
    fireEvent.click(screen.getByRole('button', { name: 'REFRESH' }))
    await flush()

    expect(screen.getByText('last good data')).toBeInTheDocument()
    expect(screen.getByText('background refresh failed')).toBeInTheDocument()
    expect(screen.getByLabelText('Last success')).toHaveTextContent(String(successfulAt))
  })

  it('never exposes one request identity under another and surfaces the replacement error', async () => {
    const first = deferred<string>()
    const replacement = deferred<string>()
    const { rerender } = render(
      <AsyncProbe requestKey="session-a" loader={() => first.promise} />,
    )

    await act(async () => {
      first.resolve('Session A')
      await Promise.resolve()
      await Promise.resolve()
    })
    expect(screen.getByText('Session A')).toBeInTheDocument()

    rerender(<AsyncProbe requestKey="session-b" loader={() => replacement.promise} />)
    expect(screen.getByText('loading')).toBeInTheDocument()
    expect(screen.queryByText('Session A')).not.toBeInTheDocument()

    await act(async () => {
      replacement.reject(new Error('Session B was not found'))
      await Promise.resolve()
      await Promise.resolve()
    })
    expect(screen.getByText('Session B was not found')).toBeInTheDocument()
    expect(screen.queryByText('Session A')).not.toBeInTheDocument()
  })
})
