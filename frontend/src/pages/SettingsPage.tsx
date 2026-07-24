import { PencilSimple, Plus, Trash, X } from '@phosphor-icons/react'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import { api } from '../api'
import { dateOnly } from '../calendar'
import { DegradedDataNotice, ErrorState, LoadingLedger, PageTitle, Pagination } from '../components/Common'
import { shortDateTime } from '../format'
import { invalidateAsyncCache, useAsync } from '../hooks'
import type { PriceAlias, PriceRow, UnknownModel } from '../types'

interface PriceForm { modelId: string; effectiveFrom: string; input: string; cached: string; output: string }

const decimalRate = /^(?:0|[1-9]\d*)(?:\.\d{1,6})?$/
const modalFocusableSelector = [
  'button:not([disabled])',
  'a[href]',
  'input:not([disabled])',
  'select:not([disabled])',
  'textarea:not([disabled])',
  '[tabindex]:not([tabindex="-1"])',
].join(',')

function priceMoney(value: string) {
  const [whole, fraction = '00'] = value.split('.')
  return `$${whole.replace(/\B(?=(\d{3})+(?!\d))/g, ',')}.${fraction}`
}

function storageSize(bytes: number) {
  if (bytes < 1024) return `${bytes} B`
  const units = ['KB', 'MB', 'GB', 'TB']
  let value = bytes / 1024
  let unit = units[0]
  for (let index = 1; index < units.length && value >= 1024; index += 1) {
    value /= 1024
    unit = units[index]
  }
  return `${value >= 10 ? value.toFixed(1) : value.toFixed(2)} ${unit}`
}

function useModalFocus(onClose: () => void) {
  const modalRef = useRef<HTMLFormElement>(null)
  const onCloseRef = useRef(onClose)

  useEffect(() => {
    onCloseRef.current = onClose
  }, [onClose])

  useEffect(() => {
    const previouslyFocused = document.activeElement instanceof HTMLElement ? document.activeElement : null
    const modal = modalRef.current
    const focusableElements = () => Array.from(modal?.querySelectorAll<HTMLElement>(modalFocusableSelector) ?? [])
    const initial = modal?.querySelector<HTMLElement>('[data-modal-initial-focus]') ?? focusableElements()[0] ?? modal
    initial?.focus()

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        event.preventDefault()
        onCloseRef.current()
        return
      }
      if (event.key !== 'Tab') return

      const focusable = focusableElements()
      if (focusable.length === 0) {
        event.preventDefault()
        modal?.focus()
        return
      }

      const first = focusable[0]
      const last = focusable[focusable.length - 1]
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault()
        last.focus()
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault()
        first.focus()
      } else if (modal && !modal.contains(document.activeElement)) {
        event.preventDefault()
        first.focus()
      }
    }

    document.addEventListener('keydown', handleKeyDown)
    return () => {
      document.removeEventListener('keydown', handleKeyDown)
      if (previouslyFocused?.isConnected) previouslyFocused.focus()
    }
  }, [])

  return modalRef
}

function usePendingModal(onClose: () => void) {
  const pendingRef = useRef(false)
  const [pending, setPending] = useState(false)
  const requestClose = useCallback(() => {
    if (!pendingRef.current) onClose()
  }, [onClose])
  const modalRef = useModalFocus(requestClose)

  const beginPending = useCallback(() => {
    if (pendingRef.current) return false
    pendingRef.current = true
    setPending(true)
    return true
  }, [])
  const failPending = useCallback(() => {
    pendingRef.current = false
    setPending(false)
  }, [])
  const completePending = useCallback(() => {
    pendingRef.current = false
    onClose()
  }, [onClose])

  return { modalRef, pending, requestClose, beginPending, failPending, completePending }
}

function PriceEditor({ initial, onClose, onSaved }: { initial?: PriceRow; onClose: () => void; onSaved: (message: string) => Promise<void> }) {
  const { modalRef, pending: saving, requestClose, beginPending, failPending, completePending } = usePendingModal(onClose)
  const [form, setForm] = useState<PriceForm>({
    modelId: initial?.modelId ?? '',
    effectiveFrom: initial?.effectiveFrom?.slice(0, 10) ?? dateOnly(new Date()),
    input: initial?.inputPerMillion ?? '',
    cached: initial?.cachedInputPerMillion ?? '',
    output: initial?.outputPerMillion ?? '',
  })
  const [error, setError] = useState<string | null>(null)

  async function save(event: React.FormEvent) {
    event.preventDefault()
    const inputText = form.input.trim()
    const outputText = form.output.trim()
    const cachedText = form.cached.trim()
    if (!form.modelId.trim() || !form.effectiveFrom) {
      setError('Enter a model ID and effective date.')
      return
    }
    if (!decimalRate.test(inputText) || !decimalRate.test(outputText)) {
      setError('Input and output prices are required, non-negative decimals with up to 6 places.')
      return
    }
    if (cachedText !== '' && !decimalRate.test(cachedText)) {
      setError('Cached price must be a non-negative decimal with up to 6 places, or left blank.')
      return
    }
    if (!beginPending()) return
    setError(null)
    try {
      await api.savePrice(form.modelId.trim(), { effectiveFrom: `${form.effectiveFrom}T00:00:00Z`, inputPerMillion: inputText, cachedInputPerMillion: cachedText || null, outputPerMillion: outputText, currency: 'USD' })
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : 'Could not save price')
      failPending()
      return
    }
    completePending()
    await onSaved('Price saved')
  }

  return (
    <div className="editor-scrim" role="presentation" onMouseDown={event => { if (event.target === event.currentTarget) requestClose() }}>
      <form ref={modalRef} className="data-editor" role="dialog" aria-modal="true" aria-label={initial ? 'Edit model price' : 'Add model price'} tabIndex={-1} onSubmit={save}>
        <header><div><span className="eyebrow coral-text">PRICE DATA</span><h2>{initial ? 'Edit model price' : 'Add model price'}</h2></div><button type="button" aria-label="Close" disabled={saving} onClick={requestClose}><X weight="bold" /></button></header>
        <label>MODEL ID<input data-modal-initial-focus={!initial ? 'true' : undefined} value={form.modelId} readOnly={Boolean(initial)} onChange={event => setForm(value => ({ ...value, modelId: event.target.value }))} /></label>
        <label>EFFECTIVE FROM<input type="date" value={form.effectiveFrom} readOnly={Boolean(initial)} onChange={event => setForm(value => ({ ...value, effectiveFrom: event.target.value }))} /></label>
        <div className="editor-price-grid">
          <label>INPUT / 1M<input data-modal-initial-focus={initial ? 'true' : undefined} inputMode="decimal" value={form.input} onChange={event => setForm(value => ({ ...value, input: event.target.value }))} /></label>
          <label>CACHED / 1M<input inputMode="decimal" placeholder="Optional" value={form.cached} onChange={event => setForm(value => ({ ...value, cached: event.target.value }))} /></label>
          <label>OUTPUT / 1M<input inputMode="decimal" value={form.output} onChange={event => setForm(value => ({ ...value, output: event.target.value }))} /></label>
        </div>
        {error && <div className="inline-error" role="alert">{error}</div>}
        <footer><button type="button" className="button" disabled={saving} onClick={requestClose}>CANCEL</button><button type="submit" className="button button-coral" disabled={saving}>{saving ? 'SAVING' : 'SAVE PRICE'}</button></footer>
      </form>
    </div>
  )
}

function AliasEditor({ initial, observedModelId, models, onClose, onSaved }: { initial?: PriceAlias; observedModelId?: string; models: string[]; onClose: () => void; onSaved: (message: string) => Promise<void> }) {
  const { modalRef, pending: saving, requestClose, beginPending, failPending, completePending } = usePendingModal(onClose)
  const observedReadOnly = Boolean(observedModelId || initial)
  const [observed, setObserved] = useState(initial?.observedModelId ?? observedModelId ?? '')
  const [canonical, setCanonical] = useState(initial?.canonicalModelId ?? '')
  const [availableModels, setAvailableModels] = useState(models)
  const [suggestionError, setSuggestionError] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const controller = new AbortController()
    const timer = window.setTimeout(() => {
      void api.pricedModelIds({ q: canonical.trim() || undefined, limit: 100 }, controller.signal)
        .then(next => {
          if (!controller.signal.aborted) {
            setAvailableModels([...new Set([...models, ...next])].sort())
            setSuggestionError(null)
          }
        })
        .catch(error => {
          if (!controller.signal.aborted && !(error instanceof Error && error.name === 'AbortError')) {
            setSuggestionError('Could not load priced model suggestions. You can still type a model ID.')
          }
        })
    }, 180)
    return () => {
      window.clearTimeout(timer)
      controller.abort()
    }
  }, [canonical, models])

  async function save(event: React.FormEvent) {
    event.preventDefault()
    if (!observed.trim() || !canonical.trim()) { setError('Enter both model IDs.'); return }
    if (!beginPending()) return
    setError(null)
    try { await api.saveAlias(observed.trim(), canonical.trim()) }
    catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : 'Could not save alias')
      failPending()
      return
    }
    completePending()
    await onSaved('Mapping saved')
  }

  return (
    <div className="editor-scrim" role="presentation" onMouseDown={event => { if (event.target === event.currentTarget) requestClose() }}>
      <form ref={modalRef} className="data-editor alias-editor" role="dialog" aria-modal="true" aria-label="Map observed model ID" tabIndex={-1} onSubmit={save}>
        <header><div><span className="eyebrow coral-text">MODEL ALIAS</span><h2>Map observed model</h2></div><button type="button" aria-label="Close" disabled={saving} onClick={requestClose}><X weight="bold" /></button></header>
        <p>This model ID will use the selected canonical model’s price for past and future usage.</p>
        <label>OBSERVED MODEL ID<input data-modal-initial-focus={!observedReadOnly ? 'true' : undefined} value={observed} readOnly={observedReadOnly} onChange={event => setObserved(event.target.value)} /></label>
        <label>CANONICAL MODEL ID<input data-modal-initial-focus={observedReadOnly ? 'true' : undefined} list="priced-models" value={canonical} onChange={event => setCanonical(event.target.value)} placeholder="Choose or type a priced model" /><datalist id="priced-models">{availableModels.map(model => <option value={model} key={model} />)}</datalist></label>
        {suggestionError && <p className="editor-note">{suggestionError}</p>}
        {error && <div className="inline-error" role="alert">{error}</div>}
        <footer><button type="button" className="button" disabled={saving} onClick={requestClose}>CANCEL</button><button type="submit" className="button button-coral" disabled={saving}>{saving ? 'SAVING' : 'SAVE MAPPING'}</button></footer>
      </form>
    </div>
  )
}

type Editor = { kind: 'price'; initial?: PriceRow } | { kind: 'alias'; initial?: PriceAlias; observed?: string }

function unknownId(item: UnknownModel) { return item.modelId }

function priceSourceLabel(source: string) {
  const displaySource = source.startsWith('remote:') ? source.slice('remote:'.length) : source
  if (displaySource.includes('BerriAI/litellm')) return 'LITELLM'
  try { return new URL(displaySource).hostname.toUpperCase() }
  catch { return displaySource }
}

function PriceData() {
  const [params, setParams] = useSearchParams()
  const rawPage = Number(params.get('page') ?? 1)
  const page = Number.isSafeInteger(rawPage) && rawPage > 0 ? rawPage : 1
  const query = params.get('q') ?? ''
  const rawAliasQuery = params.get('aliasQ') ?? ''
  const aliasQuery = rawAliasQuery.trim()
  const rawAliasPage = params.get('aliasPage')
  const parsedAliasPage = Number(rawAliasPage ?? 1)
  const aliasPage = Number.isSafeInteger(parsedAliasPage) && parsedAliasPage > 0 ? parsedAliasPage : 1
  const [search, setSearch] = useState(query)
  const [aliasSearch, setAliasSearch] = useState(aliasQuery)
  const [refreshing, setRefreshing] = useState(false)
  const [mutationError, setMutationError] = useState<string | null>(null)
  const [editor, setEditor] = useState<Editor | null>(null)
  const settings = useAsync(signal => api.settings(signal), [])
  const metadata = useAsync(signal => api.priceMetadata(signal), [])
  const { data, error, loading, lastSuccessfulAt, refresh, refreshOrThrow } = useAsync(signal => api.prices({ q: query || undefined, page }, signal), [query, page])
  const aliases = useAsync(signal => api.aliases({ q: aliasQuery || undefined, page: aliasPage }, signal), [aliasQuery, aliasPage])
  const models = useMemo(() => [...new Set(data?.items.map(item => item.modelId) ?? [])].sort(), [data?.items])
  const commitSearch = useCallback((value: string) => {
    const next = new URLSearchParams(params)
    next.set('tab', 'price-data')
    if (value.trim()) next.set('q', value.trim()); else next.delete('q')
    next.delete('page')
    setParams(next, { replace: true })
  }, [params, setParams])
  const commitAliasSearch = useCallback((value: string) => {
    const next = new URLSearchParams(params)
    next.set('tab', 'price-data')
    if (value.trim()) next.set('aliasQ', value.trim()); else next.delete('aliasQ')
    next.delete('aliasPage')
    setParams(next, { replace: true })
  }, [params, setParams])

  useEffect(() => {
    setSearch(current => current === query ? current : query)
  }, [query])

  useEffect(() => {
    setAliasSearch(current => current === aliasQuery ? current : aliasQuery)
  }, [aliasQuery])

  useEffect(() => {
    const normalized = search.trim()
    if (normalized === query) return
    const timer = window.setTimeout(() => commitSearch(normalized), 220)
    return () => window.clearTimeout(timer)
  }, [commitSearch, query, search])

  useEffect(() => {
    const normalized = aliasSearch.trim()
    if (normalized === aliasQuery) return
    const timer = window.setTimeout(() => commitAliasSearch(normalized), 220)
    return () => window.clearTimeout(timer)
  }, [aliasQuery, aliasSearch, commitAliasSearch])

  useEffect(() => {
    const canonicalAliasPage = aliasPage > 1 ? String(aliasPage) : null
    const canonicalAliasQuery = aliasQuery || null
    if (rawAliasPage === canonicalAliasPage && (params.get('aliasQ') || null) === canonicalAliasQuery) return
    const next = new URLSearchParams(params)
    next.set('tab', 'price-data')
    if (canonicalAliasPage) next.set('aliasPage', canonicalAliasPage); else next.delete('aliasPage')
    if (canonicalAliasQuery) next.set('aliasQ', canonicalAliasQuery); else next.delete('aliasQ')
    setParams(next, { replace: true })
  }, [aliasPage, aliasQuery, params, rawAliasPage, setParams])

  useEffect(() => {
    if (!data) return
    const lastPage = Math.max(1, data.totalPages)
    if (page <= lastPage) return
    const next = new URLSearchParams(params)
    next.set('tab', 'price-data')
    next.set('page', String(lastPage))
    setParams(next, { replace: true })
  }, [data, page, params, setParams])

  useEffect(() => {
    if (!aliases.data) return
    const lastPage = Math.max(1, aliases.data.totalPages)
    if (aliasPage <= lastPage) return
    const next = new URLSearchParams(params)
    next.set('tab', 'price-data')
    if (lastPage > 1) next.set('aliasPage', String(lastPage)); else next.delete('aliasPage')
    setParams(next, { replace: true })
  }, [aliasPage, aliases.data, params, setParams])

  async function refreshAfterMutation(successMessage: string) {
    setMutationError(null)
    invalidateAsyncCache('overview', 'stats:')
    const results = await Promise.allSettled([
      refreshOrThrow(),
      metadata.refreshOrThrow(),
      aliases.refreshOrThrow(),
    ])
    const labels = ['price list', 'unknown-model data', 'alias list']
    const failures = results.flatMap((result, index) => result.status === 'rejected'
      ? [{ label: labels[index], detail: result.reason instanceof Error ? result.reason.message : 'Reload failed' }]
      : [])
    if (failures.length === 0) return
    const failedSurfaces = failures.length === 1
      ? `the ${failures[0].label}`
      : failures.map(failure => failure.label).join(', ')
    setMutationError(`${successMessage}, but ${failedSurfaces} could not be reloaded: ${failures.map(failure => failure.detail).join('; ')}`)
  }

  async function refreshPrices() {
    setRefreshing(true); setMutationError(null)
    try { await api.refreshPrices() }
    catch (nextError) {
      const refreshFailure = nextError instanceof Error ? nextError.message : 'Price refresh failed'
      setMutationError(refreshFailure)
      try {
        await refreshOrThrow(true)
        setMutationError(null)
      } catch (reloadError) {
        const detail = reloadError instanceof Error ? reloadError.message : 'Reload failed'
        setMutationError(`${refreshFailure}. The persisted refresh status could not be reloaded: ${detail}`)
      }
      setRefreshing(false)
      return
    }
    await refreshAfterMutation('Prices refreshed')
    setRefreshing(false)
  }

  async function removePrice(price: PriceRow) {
    if (!window.confirm(`Delete the ${price.modelId} price effective ${price.effectiveFrom.slice(0, 10)}?`)) return
    try { await api.deletePrice(price.modelId, price.effectiveFrom) }
    catch (nextError) { setMutationError(nextError instanceof Error ? nextError.message : 'Could not delete price'); return }
    await refreshAfterMutation('Price deleted')
  }

  async function removeAlias(alias: PriceAlias) {
    if (!window.confirm(`Remove the mapping for ${alias.observedModelId}?`)) return
    try { await api.deleteAlias(alias.observedModelId) }
    catch (nextError) { setMutationError(nextError instanceof Error ? nextError.message : 'Could not delete mapping'); return }
    await refreshAfterMutation('Mapping deleted')
  }

  function submitSearch(event: React.FormEvent) {
    event.preventDefault()
    commitSearch(search)
  }

  function submitAliasSearch(event: React.FormEvent) {
    event.preventDefault()
    commitAliasSearch(aliasSearch)
  }

  const pageOutOfRange = Boolean(data && page > Math.max(1, data.totalPages))
  const visibleData = pageOutOfRange ? null : data
  const resultsLoading = loading || pageOutOfRange
  const paginationRef = useRef<{ page: number; totalPages: number; total: number; pageSize: number } | null>(null)
  if (visibleData) {
    paginationRef.current = visibleData.total > 0
      ? { page: visibleData.page, totalPages: Math.max(1, visibleData.totalPages), total: visibleData.total, pageSize: visibleData.pageSize }
      : null
  }
  const pagination = visibleData?.total ? {
    page: visibleData.page,
    totalPages: Math.max(1, visibleData.totalPages),
    total: visibleData.total,
    pageSize: visibleData.pageSize,
  } : paginationRef.current
  const paginationUnavailable = resultsLoading || (!visibleData && Boolean(error))
  const aliasPageOutOfRange = Boolean(aliases.data && aliasPage > Math.max(1, aliases.data.totalPages))
  const visibleAliases = aliasPageOutOfRange ? null : aliases.data
  const aliasesLoading = aliases.loading || aliasPageOutOfRange
  const aliasPaginationRef = useRef<{ page: number; totalPages: number; total: number; pageSize: number } | null>(null)
  if (visibleAliases) {
    aliasPaginationRef.current = visibleAliases.total > 0
      ? { page: visibleAliases.page, totalPages: Math.max(1, visibleAliases.totalPages), total: visibleAliases.total, pageSize: visibleAliases.pageSize }
      : null
  }
  const aliasPagination = visibleAliases?.total ? {
    page: visibleAliases.page,
    totalPages: Math.max(1, visibleAliases.totalPages),
    total: visibleAliases.total,
    pageSize: visibleAliases.pageSize,
  } : aliasPaginationRef.current
  const aliasPaginationUnavailable = aliasesLoading || (!visibleAliases && Boolean(aliases.error))
  return (
    <div className="price-settings">
      <span className="sr-only" aria-live="polite">{resultsLoading ? 'Loading model prices' : visibleData ? `${visibleData.total} model prices loaded` : ''}</span>
      {error && visibleData && <DegradedDataNotice error={error} lastSuccessfulAt={lastSuccessfulAt} onRetry={() => void refresh()} />}
      <div className="price-toolbar">
        <span>LAST SYNC {visibleData?.lastRefreshAt ? shortDateTime(visibleData.lastRefreshAt).toUpperCase() : 'PENDING'}{visibleData?.source ? <> · SOURCE <span title={visibleData.source}>{priceSourceLabel(visibleData.source)}</span></> : ''}</span>
        <div><button type="button" className="button" onClick={() => setEditor({ kind: 'alias' })}><Plus weight="bold" /> ADD ALIAS</button><button type="button" className="button" onClick={() => setEditor({ kind: 'price' })}><Plus weight="bold" /> ADD PRICE</button><button type="button" className="button button-coral" disabled={refreshing} onClick={() => void refreshPrices()}>{refreshing ? 'REFRESHING' : 'REFRESH PRICES'}</button></div>
      </div>
      {settings.data && <div className="storage-notice" role="status"><span><strong>LOCAL DATABASE · {storageSize(settings.data.databaseBytes)}</strong><small>Usage history is retained locally and the database can continue growing as sessions are ingested.</small></span><code title={settings.data.databasePath}>{settings.data.databasePath}</code></div>}
      {settings.error && <div className="settings-metadata-warning pricing-refresh-warning" role="alert"><span><strong>LOCAL DATABASE DETAILS UNAVAILABLE</strong><small>{settings.error.message}</small></span><button type="button" onClick={() => void settings.refresh()}>TRY AGAIN</button></div>}
      {visibleData?.refreshError && <div className="pricing-refresh-warning" role="alert"><span><strong>PRICE REFRESH FAILED{visibleData.refreshErrorKind ? ` · ${visibleData.refreshErrorKind.toUpperCase()}` : ''}</strong>{visibleData.lastRefreshErrorAt ? <> · {shortDateTime(visibleData.lastRefreshErrorAt).toUpperCase()}</> : null}<small>{visibleData.refreshError}</small></span><button type="button" disabled={refreshing} onClick={() => void refreshPrices()}>TRY AGAIN</button></div>}
      {mutationError && <div className="inline-error" role="alert">{mutationError}</div>}
      {metadata.error && <div className="settings-metadata-warning pricing-refresh-warning" role="alert"><span><strong>UNKNOWN MODEL DATA UNAVAILABLE</strong><small>{metadata.error.message}</small></span><button type="button" onClick={() => void metadata.refresh()}>TRY AGAIN</button></div>}
      {metadata.data && metadata.data.observedUnknown.length > 0 && (
        <div className="missing-price-banner expanded-warning"><span><strong>MISSING PRICE DATA</strong><small>{metadata.data.observedUnknown.map(item => unknownId(item)).join(', ')}</small></span><div>{metadata.data.observedUnknown.slice(0, 3).map(item => <button key={unknownId(item)} type="button" onClick={() => setEditor({ kind: 'alias', observed: unknownId(item) })}>MAP {unknownId(item)}</button>)}</div><b>{metadata.data.observedUnknownTotal} MODEL {metadata.data.observedUnknownTotal === 1 ? 'ID' : 'IDS'}</b></div>
      )}
      <form className="price-search" onSubmit={submitSearch}><label className="search-field"><input value={search} onChange={event => setSearch(event.target.value)} placeholder="Search model IDs" aria-label="Search model prices" /></label></form>
      <div className="ledger-scroll price-scroll" role="region" aria-label="Scrollable model price ledger" aria-busy={resultsLoading || undefined} tabIndex={0}>
        <section className="price-ledger" role="table" aria-label="Model prices" aria-rowcount={visibleData && visibleData.total > 0 ? visibleData.total + 1 : undefined}>
          <div className="price-head" role="row" aria-rowindex={visibleData && visibleData.total > 0 ? 1 : undefined}><span role="columnheader">MODEL</span><span role="columnheader">SOURCE</span><span role="columnheader">INPUT / 1M</span><span role="columnheader">CACHED / 1M</span><span role="columnheader">OUTPUT / 1M</span><span role="columnheader" aria-label="Actions" /></div>
          {visibleData?.items.map((price, index) => {
            const manual = price.source === 'manual'
            const editLabel = manual ? `Edit ${price.modelId}` : `Override ${price.modelId} price`
            return <div className="price-row" role="row" aria-rowindex={(visibleData.page - 1) * visibleData.pageSize + index + 2} key={`${price.modelId}-${price.effectiveFrom}-${price.source}`}><span role="cell">{price.modelId}</span><span role="cell" className="price-source" title={price.source}>{priceSourceLabel(price.source)}</span><b role="cell">{priceMoney(price.inputPerMillion)}</b><b role="cell">{price.cachedInputPerMillion === null ? '—' : priceMoney(price.cachedInputPerMillion)}</b><b role="cell">{priceMoney(price.outputPerMillion)}</b><span role="cell" className="row-actions"><button type="button" aria-label={editLabel} title={manual ? 'Edit manual price' : 'Create a manual override'} onClick={() => setEditor({ kind: 'price', initial: price })}><PencilSimple /></button>{manual && <button type="button" aria-label={`Delete ${price.modelId}`} title="Delete manual price" onClick={() => void removePrice(price)}><Trash /></button>}</span></div>
          })}
          {!visibleData && resultsLoading ? <div className="table-state-row" role="row"><div role="cell" aria-colspan={6}><LoadingLedger rows={8} /></div></div> : null}
          {error && !visibleData && !resultsLoading ? <div className="table-state-row" role="row"><div role="cell" aria-colspan={6}><ErrorState error={error} onRetry={() => void refresh()} /></div></div> : null}
          {visibleData?.items.length === 0 && <div className="table-state-row" role="row"><div className="usage-empty" role="cell" aria-colspan={6}>No prices match this search.</div></div>}
        </section>
        {pagination && <Pagination {...pagination} ariaLabel="Model price pagination" busy={paginationUnavailable} onPage={value => { const next = new URLSearchParams(params); next.set('tab', 'price-data'); next.set('page', String(value)); setParams(next) }} />}
      </div>
      <div className="ledger-scroll alias-scroll" role="region" aria-label="Scrollable model alias ledger" aria-busy={aliasesLoading || undefined} tabIndex={0}>
        <section className="alias-ledger">
          <h2>MODEL ALIASES</h2>
          <form className="alias-search" onSubmit={submitAliasSearch}><label className="search-field"><input value={aliasSearch} onChange={event => setAliasSearch(event.target.value)} placeholder="Search observed or canonical model IDs" aria-label="Search model aliases" /></label></form>
          {aliases.error && visibleAliases && <DegradedDataNotice error={aliases.error} lastSuccessfulAt={aliases.lastSuccessfulAt} onRetry={() => void aliases.refresh()} />}
          {!visibleAliases && aliasesLoading && <LoadingLedger rows={4} />}
          {aliases.error && !visibleAliases && !aliasesLoading && <ErrorState error={aliases.error} onRetry={() => void aliases.refresh()} />}
          {visibleAliases?.items.length === 0 && <div className="usage-empty">{aliasQuery ? 'No model aliases match this search.' : 'No model aliases configured.'}</div>}
          {visibleAliases?.items.map(alias => <div className="alias-row" key={alias.observedModelId}><span>{alias.observedModelId}</span><b>USES</b><strong>{alias.canonicalModelId}</strong><span className="row-actions"><button type="button" aria-label={`Edit alias ${alias.observedModelId}`} onClick={() => setEditor({ kind: 'alias', initial: alias })}><PencilSimple /></button><button type="button" aria-label={`Delete alias ${alias.observedModelId}`} onClick={() => void removeAlias(alias)}><Trash /></button></span></div>)}
        </section>
        {aliasPagination && <Pagination {...aliasPagination} ariaLabel="Model alias pagination" busy={aliasPaginationUnavailable} onPage={value => { const next = new URLSearchParams(params); next.set('tab', 'price-data'); if (value > 1) next.set('aliasPage', String(value)); else next.delete('aliasPage'); setParams(next) }} />}
      </div>
      {editor?.kind === 'price' && <PriceEditor initial={editor.initial} onClose={() => setEditor(current => current === editor ? null : current)} onSaved={refreshAfterMutation} />}
      {editor?.kind === 'alias' && <AliasEditor initial={editor.initial} observedModelId={editor.observed} models={models} onClose={() => setEditor(current => current === editor ? null : current)} onSaved={refreshAfterMutation} />}
    </div>
  )
}

export function SettingsPage() {
  return (
    <div className="settings-page">
      <PageTitle>Settings</PageTitle>
      <nav className="section-tabs settings-tabs" aria-label="Settings sections" role="tablist">
        <button type="button" role="tab" aria-selected="true" aria-controls="settings-price-panel" tabIndex={0} className="active">PRICE DATA</button>
      </nav>
      <div id="settings-price-panel" role="tabpanel" aria-label="Price data">
        <PriceData />
      </div>
    </div>
  )
}
