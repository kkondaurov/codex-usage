import { act, fireEvent, render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { clearAsyncCache, invalidateAsyncCache, useAsync, useCachedAsync } from './hooks'

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

function CachedRetryProbe({ loader }: { loader: (signal: AbortSignal) => Promise<string> }) {
  const { data, error, loading, refresh } = useCachedAsync('retry-probe', loader, ['retry-probe'], STALE_MS)
  return <>
    <output aria-label="Cached data">{data ?? 'empty'}</output>
    <output aria-label="Cached state">{loading ? 'loading' : 'idle'}</output>
    {error && <span>{error.message}</span>}
    <button type="button" onClick={() => void refresh(true)}>QUIET REFRESH</button>
    <button type="button" onClick={() => void refresh()}>RETRY</button>
  </>
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

function AsyncRetryProbe({ loader }: { loader: (signal: AbortSignal) => Promise<string> }) {
  const { data, error, loading, refresh } = useAsync(loader, ['retry-probe'])
  return <>
    <output aria-label="Async data">{data ?? 'empty'}</output>
    <output aria-label="Async state">{loading ? 'loading' : 'idle'}</output>
    {error && <span>{error.message}</span>}
    <button type="button" onClick={() => void refresh(true)}>QUIET REFRESH</button>
    <button type="button" onClick={() => void refresh()}>RETRY</button>
  </>
}

beforeEach(() => {
  vi.useFakeTimers()
  vi.setSystemTime(new Date('2026-07-17T12:00:00+02:00'))
})

afterEach(() => {
  clearAsyncCache()
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

  it('clears a stale background error when an explicit retry starts', async () => {
    const retry = deferred<string>()
    const loader = vi.fn()
      .mockResolvedValueOnce('last good overview')
      .mockRejectedValueOnce(new Error('background refresh failed'))
      .mockReturnValueOnce(retry.promise)
    render(<CachedRetryProbe loader={loader} />)
    await flush()

    fireEvent.click(screen.getByRole('button', { name: 'QUIET REFRESH' }))
    await flush()
    expect(screen.getByText('background refresh failed')).toBeInTheDocument()
    expect(screen.getByLabelText('Cached data')).toHaveTextContent('last good overview')

    fireEvent.click(screen.getByRole('button', { name: 'RETRY' }))
    expect(screen.getByLabelText('Cached state')).toHaveTextContent('loading')
    expect(screen.queryByText('background refresh failed')).not.toBeInTheDocument()
    expect(screen.getByLabelText('Cached data')).toHaveTextContent('last good overview')

    await act(async () => {
      retry.resolve('recovered overview')
      await Promise.resolve()
      await Promise.resolve()
    })
    expect(screen.getByLabelText('Cached state')).toHaveTextContent('idle')
    expect(screen.getByLabelText('Cached data')).toHaveTextContent('recovered overview')
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

  it('evicts the least recently used idle result when the bounded cache is full', async () => {
    const firstLoader = vi.fn().mockResolvedValue('value 0')
    const secondLoader = vi.fn().mockResolvedValue('value 1')
    const view = render(<CacheProbe cacheKey="cache-key-0" loader={firstLoader} />)
    await flush()

    for (let index = 1; index < 64; index += 1) {
      vi.advanceTimersByTime(1)
      const loader = index === 1 ? secondLoader : vi.fn().mockResolvedValue(`value ${index}`)
      view.rerender(<CacheProbe cacheKey={`cache-key-${index}`} loader={loader} />)
      await flush()
    }

    // Touch the oldest entry so key 1, not key 0, becomes the LRU victim.
    vi.advanceTimersByTime(1)
    view.rerender(<CacheProbe cacheKey="cache-key-0" loader={firstLoader} />)
    await flush()
    expect(firstLoader).toHaveBeenCalledTimes(1)

    vi.advanceTimersByTime(1)
    view.rerender(<CacheProbe cacheKey="cache-key-64" loader={() => Promise.resolve('value 64')} />)
    await flush()

    view.rerender(<CacheProbe cacheKey="cache-key-1" loader={secondLoader} />)
    await flush()

    expect(secondLoader).toHaveBeenCalledTimes(2)
    expect(screen.getByText('value 1')).toBeInTheDocument()
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

  it('clears a stale background error when an explicit retry starts', async () => {
    const retry = deferred<string>()
    const loader = vi.fn()
      .mockResolvedValueOnce('last good data')
      .mockRejectedValueOnce(new Error('background refresh failed'))
      .mockReturnValueOnce(retry.promise)
    render(<AsyncRetryProbe loader={loader} />)
    await flush()

    fireEvent.click(screen.getByRole('button', { name: 'QUIET REFRESH' }))
    await flush()
    expect(screen.getByText('background refresh failed')).toBeInTheDocument()
    expect(screen.getByLabelText('Async data')).toHaveTextContent('last good data')

    fireEvent.click(screen.getByRole('button', { name: 'RETRY' }))
    expect(screen.getByLabelText('Async state')).toHaveTextContent('loading')
    expect(screen.queryByText('background refresh failed')).not.toBeInTheDocument()
    expect(screen.getByLabelText('Async data')).toHaveTextContent('last good data')

    await act(async () => {
      retry.resolve('recovered data')
      await Promise.resolve()
      await Promise.resolve()
    })
    expect(screen.getByLabelText('Async state')).toHaveTextContent('idle')
    expect(screen.getByLabelText('Async data')).toHaveTextContent('recovered data')
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
