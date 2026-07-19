import { useCallback, useEffect, useRef, useState } from 'react'

interface AsyncCacheEntry {
  data?: unknown
  fetchedAt: number
  hasData: boolean
  inFlight?: Promise<unknown>
  controller?: AbortController
  subscribers: number
  orphanTimer?: number
}

interface AsyncCacheSnapshot<T> {
  data: T
  fetchedAt: number
}

const asyncCache = new Map<string, AsyncCacheEntry>()
const ORPHANED_REQUEST_GRACE_MS = 100

interface AsyncState<T> {
  identity: object
  data: T | null
  error: Error | null
  loading: boolean
  lastSuccessfulAt: number | null
}

function dependenciesMatch(left: unknown[], right: unknown[]) {
  return left.length === right.length && left.every((value, index) => Object.is(value, right[index]))
}

function cachedSnapshot<T>(key: string): AsyncCacheSnapshot<T> | null {
  const entry = asyncCache.get(key)
  if (!entry?.hasData) return null
  return { data: entry.data as T, fetchedAt: entry.fetchedAt }
}

type AsyncLoader<T> = (signal: AbortSignal) => Promise<T>

function isAbortError(error: unknown) {
  return error instanceof Error && error.name === 'AbortError'
}

function ensureAsyncCacheEntry(key: string) {
  let entry = asyncCache.get(key)
  if (!entry) {
    entry = { fetchedAt: 0, hasData: false, subscribers: 0 }
    asyncCache.set(key, entry)
  }
  return entry
}

function retainCached(key: string) {
  const entry = ensureAsyncCacheEntry(key)
  entry.subscribers += 1
  if (entry.orphanTimer !== undefined) {
    window.clearTimeout(entry.orphanTimer)
    entry.orphanTimer = undefined
  }
}

function releaseCached(key: string) {
  const entry = asyncCache.get(key)
  if (!entry) return

  entry.subscribers = Math.max(0, entry.subscribers - 1)
  if (entry.subscribers > 0 || !entry.inFlight || entry.orphanTimer !== undefined) return

  entry.orphanTimer = window.setTimeout(() => {
    const current = asyncCache.get(key)
    if (current !== entry) return
    current.orphanTimer = undefined
    if (current.subscribers > 0 || !current.inFlight) return

    const controller = current.controller
    current.inFlight = undefined
    current.controller = undefined
    controller?.abort()
    if (!current.hasData) asyncCache.delete(key)
  }, ORPHANED_REQUEST_GRACE_MS)
}

function loadCached<T>(key: string, loader: AsyncLoader<T>): Promise<T> {
  const entry = ensureAsyncCacheEntry(key)
  if (entry?.inFlight) return entry.inFlight as Promise<T>

  const controller = new AbortController()
  let source: Promise<T>
  try {
    source = loader(controller.signal)
  } catch (error) {
    source = Promise.reject(error)
  }

  const promise = source
    .then((next) => {
      const current = asyncCache.get(key)
      if (current?.inFlight === promise) {
        current.data = next
        current.fetchedAt = Date.now()
        current.hasData = true
      }
      return next
    })
    .finally(() => {
      const current = asyncCache.get(key)
      if (current?.inFlight === promise) {
        current.inFlight = undefined
        current.controller = undefined
        if (current.orphanTimer !== undefined) {
          window.clearTimeout(current.orphanTimer)
          current.orphanTimer = undefined
        }
      }
    })

  entry.inFlight = promise
  entry.controller = controller
  return promise
}

export function clearAsyncCache() {
  for (const entry of asyncCache.values()) {
    if (entry.orphanTimer !== undefined) window.clearTimeout(entry.orphanTimer)
    entry.controller?.abort()
  }
  asyncCache.clear()
}

export function invalidateAsyncCache(...prefixes: string[]) {
  for (const [key, entry] of asyncCache.entries()) {
    if (prefixes.some(prefix => key.startsWith(prefix))) {
      if (entry.orphanTimer !== undefined) window.clearTimeout(entry.orphanTimer)
      entry.controller?.abort()
      asyncCache.delete(key)
    }
  }
}

export function useAsync<T>(loader: AsyncLoader<T>, dependencies: unknown[], refreshMs?: number) {
  const identityRef = useRef<{ dependencies: unknown[]; identity: object }>({
    dependencies: [...dependencies],
    identity: {},
  })
  if (!dependenciesMatch(identityRef.current.dependencies, dependencies)) {
    identityRef.current = { dependencies: [...dependencies], identity: {} }
  }
  const identity = identityRef.current.identity
  const [state, setState] = useState<AsyncState<T>>({ identity, data: null, error: null, loading: true, lastSuccessfulAt: null })
  const loaderRef = useRef(loader)
  const requestSequence = useRef(0)
  const inFlight = useRef(false)
  const controllerRef = useRef<AbortController | null>(null)
  loaderRef.current = loader

  const runRefresh = useCallback(async (quiet: boolean, propagateError: boolean) => {
    const sequence = ++requestSequence.current
    controllerRef.current?.abort()
    const controller = new AbortController()
    controllerRef.current = controller
    inFlight.current = true
    if (!quiet) {
      setState(current => current.identity === identity
        ? { ...current, loading: true }
        : { identity, data: null, error: null, loading: true, lastSuccessfulAt: null })
    }
    try {
      const next = await loaderRef.current(controller.signal)
      if (sequence === requestSequence.current) {
        setState({ identity, data: next, error: null, loading: false, lastSuccessfulAt: Date.now() })
      }
    } catch (nextError) {
      if (controller.signal.aborted || isAbortError(nextError)) return
      const error = nextError instanceof Error ? nextError : new Error('Something went wrong')
      if (sequence === requestSequence.current) {
        setState(current => current.identity === identity
          ? { ...current, error, loading: false }
          : { identity, data: null, error, loading: false, lastSuccessfulAt: null })
      }
      if (propagateError) throw error
    } finally {
      if (sequence === requestSequence.current) {
        inFlight.current = false
        if (controllerRef.current === controller) controllerRef.current = null
      }
    }
  }, [identity])

  const refresh = useCallback(
    async (quiet = false) => runRefresh(quiet, false),
    [runRefresh],
  )
  const refreshOrThrow = useCallback(
    async (quiet = false) => runRefresh(quiet, true),
    [runRefresh],
  )

  useEffect(() => {
    requestSequence.current += 1
    controllerRef.current?.abort()
    controllerRef.current = null
    inFlight.current = false
    setState({ identity, data: null, error: null, loading: true, lastSuccessfulAt: null })
    void refresh()
    const timer = refreshMs
      ? window.setInterval(() => { if (!inFlight.current) void refresh(true) }, refreshMs)
      : undefined
    return () => {
      requestSequence.current += 1
      controllerRef.current?.abort()
      controllerRef.current = null
      inFlight.current = false
      if (timer !== undefined) window.clearInterval(timer)
    }
  }, [identity, refresh, refreshMs])

  return state.identity === identity
    ? { data: state.data, error: state.error, loading: state.loading, lastSuccessfulAt: state.lastSuccessfulAt, refresh, refreshOrThrow }
    : { data: null, error: null, loading: true, lastSuccessfulAt: null, refresh, refreshOrThrow }
}

export function useCachedAsync<T>(
  cacheKey: string,
  loader: AsyncLoader<T>,
  dependencies: unknown[],
  staleMs: number,
  refreshMs?: number,
) {
  const initial = cachedSnapshot<T>(cacheKey)
  const [stateKey, setStateKey] = useState(cacheKey)
  const [data, setData] = useState<T | null>(initial?.data ?? null)
  const [error, setError] = useState<Error | null>(null)
  const [loading, setLoading] = useState(!initial)
  const [lastSuccessfulAt, setLastSuccessfulAt] = useState<number | null>(initial?.fetchedAt ?? null)
  const loaderRef = useRef(loader)
  const requestSequence = useRef(0)
  const mounted = useRef(false)
  loaderRef.current = loader

  const refresh = useCallback(async (quiet = false) => {
    const sequence = ++requestSequence.current
    if (!quiet) setLoading(true)
    try {
      const next = await loadCached(cacheKey, signal => loaderRef.current(signal))
      if (mounted.current && sequence === requestSequence.current) {
        setData(next)
        setError(null)
        setLastSuccessfulAt(cachedSnapshot<T>(cacheKey)?.fetchedAt ?? Date.now())
      }
    } catch (nextError) {
      if (mounted.current && sequence === requestSequence.current && !isAbortError(nextError)) {
        setError(nextError instanceof Error ? nextError : new Error('Something went wrong'))
      }
    } finally {
      if (mounted.current && sequence === requestSequence.current) setLoading(false)
    }
  }, [cacheKey])

  useEffect(() => {
    mounted.current = true
    retainCached(cacheKey)
    const snapshot = cachedSnapshot<T>(cacheKey)
    const age = snapshot ? Date.now() - snapshot.fetchedAt : null
    setStateKey(cacheKey)
    setData(snapshot?.data ?? null)
    setError(null)
    setLoading(!snapshot)
    setLastSuccessfulAt(snapshot?.fetchedAt ?? null)

    if (!snapshot) void refresh()
    else if (age !== null && age >= staleMs) void refresh(true)

    let firstPoll: number | undefined
    let poll: number | undefined
    if (refreshMs) {
      const firstDelay = snapshot && age !== null && age < staleMs
        ? Math.max(1, Math.min(refreshMs, staleMs - age))
        : refreshMs
      firstPoll = window.setTimeout(() => {
        void refresh(true)
        poll = window.setInterval(() => void refresh(true), refreshMs)
      }, firstDelay)
    }

    return () => {
      mounted.current = false
      requestSequence.current += 1
      if (firstPoll !== undefined) window.clearTimeout(firstPoll)
      if (poll !== undefined) window.clearInterval(poll)
      releaseCached(cacheKey)
    }
    // The caller controls the dependency contract.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...dependencies, cacheKey, refresh, refreshMs, staleMs])

  const current = stateKey === cacheKey ? null : cachedSnapshot<T>(cacheKey)
  return {
    data: stateKey === cacheKey ? data : current?.data ?? null,
    error: stateKey === cacheKey ? error : null,
    loading: stateKey === cacheKey ? loading : !current,
    lastSuccessfulAt: stateKey === cacheKey ? lastSuccessfulAt : current?.fetchedAt ?? null,
    refresh,
  }
}
