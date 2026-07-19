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

function PriceEditor({ initial, onClose, onSaved }: { initial?: PriceRow; onClose: () => void; onSaved: (message: string) => Promise<void> }) {
  const modalRef = useModalFocus(onClose)
  const [form, setForm] = useState<PriceForm>({
    modelId: initial?.modelId ?? '',
    effectiveFrom: initial?.effectiveFrom?.slice(0, 10) ?? dateOnly(new Date()),
    input: initial?.inputPerMillion ?? '',
    cached: initial?.cachedInputPerMillion ?? '',
    output: initial?.outputPerMillion ?? '',
  })
  const [saving, setSaving] = useState(false)
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
    setSaving(true); setError(null)
    try {
      await api.savePrice(form.modelId.trim(), { effectiveFrom: `${form.effectiveFrom}T00:00:00Z`, inputPerMillion: inputText, cachedInputPerMillion: cachedText || null, outputPerMillion: outputText, currency: 'USD' })
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : 'Could not save price')
      setSaving(false)
      return
    }
    onClose()
    await onSaved('Price saved')
  }

  return (
    <div className="editor-scrim" role="presentation" onMouseDown={event => { if (event.target === event.currentTarget) onClose() }}>
      <form ref={modalRef} className="data-editor" role="dialog" aria-modal="true" aria-label={initial ? 'Edit model price' : 'Add model price'} tabIndex={-1} onSubmit={save}>
        <header><div><span className="eyebrow coral-text">PRICE DATA</span><h2>{initial ? 'Edit model price' : 'Add model price'}</h2></div><button type="button" aria-label="Close" onClick={onClose}><X weight="bold" /></button></header>
        <label>MODEL ID<input data-modal-initial-focus={!initial ? 'true' : undefined} value={form.modelId} readOnly={Boolean(initial)} onChange={event => setForm(value => ({ ...value, modelId: event.target.value }))} /></label>
        <label>EFFECTIVE FROM<input type="date" value={form.effectiveFrom} readOnly={Boolean(initial)} onChange={event => setForm(value => ({ ...value, effectiveFrom: event.target.value }))} /></label>
        <div className="editor-price-grid">
          <label>INPUT / 1M<input data-modal-initial-focus={initial ? 'true' : undefined} inputMode="decimal" value={form.input} onChange={event => setForm(value => ({ ...value, input: event.target.value }))} /></label>
          <label>CACHED / 1M<input inputMode="decimal" placeholder="Optional" value={form.cached} onChange={event => setForm(value => ({ ...value, cached: event.target.value }))} /></label>
          <label>OUTPUT / 1M<input inputMode="decimal" value={form.output} onChange={event => setForm(value => ({ ...value, output: event.target.value }))} /></label>
        </div>
        {error && <div className="inline-error" role="alert">{error}</div>}
        <footer><button type="button" className="button" onClick={onClose}>CANCEL</button><button type="submit" className="button button-coral" disabled={saving}>{saving ? 'SAVING' : 'SAVE PRICE'}</button></footer>
      </form>
    </div>
  )
}

function AliasEditor({ initial, observedModelId, models, onClose, onSaved }: { initial?: PriceAlias; observedModelId?: string; models: string[]; onClose: () => void; onSaved: (message: string) => Promise<void> }) {
  const modalRef = useModalFocus(onClose)
  const observedReadOnly = Boolean(observedModelId || initial)
  const [observed, setObserved] = useState(initial?.observedModelId ?? observedModelId ?? '')
  const [canonical, setCanonical] = useState(initial?.canonicalModelId ?? '')
  const [availableModels, setAvailableModels] = useState(models)
  const [suggestionError, setSuggestionError] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const controller = new AbortController()
    void api.pricedModelIds(controller.signal)
      .then(next => {
        if (!controller.signal.aborted) setAvailableModels([...new Set([...models, ...next])].sort())
      })
      .catch(error => {
        if (!controller.signal.aborted && !(error instanceof Error && error.name === 'AbortError')) {
          setSuggestionError('Could not load every priced model suggestion. You can still type a model ID.')
        }
      })
    return () => controller.abort()
  }, [models])

  async function save(event: React.FormEvent) {
    event.preventDefault()
    if (!observed.trim() || !canonical.trim()) { setError('Enter both model IDs.'); return }
    setSaving(true); setError(null)
    try { await api.saveAlias(observed.trim(), canonical.trim()) }
    catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : 'Could not save alias')
      setSaving(false)
      return
    }
    onClose()
    await onSaved('Mapping saved')
  }

  return (
    <div className="editor-scrim" role="presentation" onMouseDown={event => { if (event.target === event.currentTarget) onClose() }}>
      <form ref={modalRef} className="data-editor alias-editor" role="dialog" aria-modal="true" aria-label="Map observed model ID" tabIndex={-1} onSubmit={save}>
        <header><div><span className="eyebrow coral-text">MODEL ALIAS</span><h2>Map observed model</h2></div><button type="button" aria-label="Close" onClick={onClose}><X weight="bold" /></button></header>
        <p>This model ID will use the selected canonical model’s price for past and future usage.</p>
        <label>OBSERVED MODEL ID<input data-modal-initial-focus={!observedReadOnly ? 'true' : undefined} value={observed} readOnly={observedReadOnly} onChange={event => setObserved(event.target.value)} /></label>
        <label>CANONICAL MODEL ID<input data-modal-initial-focus={observedReadOnly ? 'true' : undefined} list="priced-models" value={canonical} onChange={event => setCanonical(event.target.value)} placeholder="Choose or type a priced model" /><datalist id="priced-models">{availableModels.map(model => <option value={model} key={model} />)}</datalist></label>
        {suggestionError && <p className="editor-note">{suggestionError}</p>}
        {error && <div className="inline-error" role="alert">{error}</div>}
        <footer><button type="button" className="button" onClick={onClose}>CANCEL</button><button type="submit" className="button button-coral" disabled={saving}>{saving ? 'SAVING' : 'SAVE MAPPING'}</button></footer>
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
  const [search, setSearch] = useState(query)
  const [refreshing, setRefreshing] = useState(false)
  const [mutationError, setMutationError] = useState<string | null>(null)
  const [editor, setEditor] = useState<Editor | null>(null)
  const settings = useAsync(signal => api.settings(signal), [])
  const { data, error, loading, lastSuccessfulAt, refresh, refreshOrThrow } = useAsync(signal => api.prices({ q: query || undefined, page }, signal), [query, page])
  const models = useMemo(() => [...new Set(data?.items.map(item => item.modelId) ?? [])].sort(), [data?.items])
  const commitSearch = useCallback((value: string) => {
    const next = new URLSearchParams(params)
    next.set('tab', 'price-data')
    if (value.trim()) next.set('q', value.trim()); else next.delete('q')
    next.delete('page')
    setParams(next, { replace: true })
  }, [params, setParams])

  useEffect(() => {
    setSearch(current => current === query ? current : query)
  }, [query])

  useEffect(() => {
    const normalized = search.trim()
    if (normalized === query) return
    const timer = window.setTimeout(() => commitSearch(normalized), 220)
    return () => window.clearTimeout(timer)
  }, [commitSearch, query, search])

  useEffect(() => {
    if (!data) return
    const lastPage = Math.max(1, data.totalPages)
    if (page <= lastPage) return
    const next = new URLSearchParams(params)
    next.set('tab', 'price-data')
    next.set('page', String(lastPage))
    setParams(next, { replace: true })
  }, [data, page, params, setParams])

  async function refreshAfterMutation(successMessage: string) {
    setMutationError(null)
    invalidateAsyncCache('overview', 'stats:')
    try {
      await refreshOrThrow()
    } catch (nextError) {
      const detail = nextError instanceof Error ? nextError.message : 'Reload failed'
      setMutationError(`${successMessage}, but the price list could not be reloaded: ${detail}`)
    }
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

  if (loading && !data) return <LoadingLedger rows={16} />
  if (error && !data) return <ErrorState error={error} onRetry={() => void refresh()} />
  if (!data) return null
  if (page > Math.max(1, data.totalPages)) return <LoadingLedger rows={16} />
  return (
    <div className="price-settings">
      {error && <DegradedDataNotice error={error} lastSuccessfulAt={lastSuccessfulAt} onRetry={() => void refresh()} />}
      <div className="price-toolbar">
        <span>LAST SYNC {data.lastRefreshAt ? shortDateTime(data.lastRefreshAt).toUpperCase() : 'PENDING'}{data.source ? <> · SOURCE <span title={data.source}>{priceSourceLabel(data.source)}</span></> : ''}</span>
        <div><button type="button" className="button" onClick={() => setEditor({ kind: 'alias' })}><Plus weight="bold" /> ADD ALIAS</button><button type="button" className="button" onClick={() => setEditor({ kind: 'price' })}><Plus weight="bold" /> ADD PRICE</button><button type="button" className="button button-coral" disabled={refreshing} onClick={() => void refreshPrices()}>{refreshing ? 'REFRESHING' : 'REFRESH PRICES'}</button></div>
      </div>
      {settings.data && <div className="storage-notice" role="status"><span><strong>LOCAL DATABASE · {storageSize(settings.data.databaseBytes)}</strong><small>Usage history is retained locally and the database can continue growing as sessions are ingested.</small></span><code title={settings.data.databasePath}>{settings.data.databasePath}</code></div>}
      {settings.error && <div className="settings-metadata-warning pricing-refresh-warning" role="alert"><span><strong>LOCAL DATABASE DETAILS UNAVAILABLE</strong><small>{settings.error.message}</small></span><button type="button" onClick={() => void settings.refresh()}>TRY AGAIN</button></div>}
      {data.refreshError && <div className="pricing-refresh-warning" role="alert"><span><strong>PRICE REFRESH FAILED{data.refreshErrorKind ? ` · ${data.refreshErrorKind.toUpperCase()}` : ''}</strong>{data.lastRefreshErrorAt ? <> · {shortDateTime(data.lastRefreshErrorAt).toUpperCase()}</> : null}<small>{data.refreshError}</small></span><button type="button" disabled={refreshing} onClick={() => void refreshPrices()}>TRY AGAIN</button></div>}
      {mutationError && <div className="inline-error" role="alert">{mutationError}</div>}
      {data.observedUnknown.length > 0 && (
        <div className="missing-price-banner expanded-warning"><span><strong>MISSING PRICE DATA</strong><small>{data.observedUnknown.map(item => unknownId(item)).join(', ')}</small></span><div>{data.observedUnknown.slice(0, 3).map(item => <button key={unknownId(item)} type="button" onClick={() => setEditor({ kind: 'alias', observed: unknownId(item) })}>MAP {unknownId(item)}</button>)}</div><b>{data.observedUnknown.length} MODEL {data.observedUnknown.length === 1 ? 'ID' : 'IDS'}</b></div>
      )}
      <form className="price-search" onSubmit={submitSearch}><label className="search-field"><input value={search} onChange={event => setSearch(event.target.value)} placeholder="Search model IDs" aria-label="Search model prices" /></label></form>
      <section className="price-ledger" role="table" aria-label="Model prices">
        <div className="price-head" role="row"><span role="columnheader">MODEL</span><span role="columnheader">SOURCE</span><span role="columnheader">INPUT / 1M</span><span role="columnheader">CACHED / 1M</span><span role="columnheader">OUTPUT / 1M</span><span role="columnheader" aria-label="Actions" /></div>
        {data.items.map(price => {
          const manual = price.source === 'manual'
          const editLabel = manual ? `Edit ${price.modelId}` : `Override ${price.modelId} price`
          return <div className="price-row" role="row" key={`${price.modelId}-${price.effectiveFrom}-${price.source}`}><span role="cell">{price.modelId}</span><span role="cell" className="price-source" title={price.source}>{priceSourceLabel(price.source)}</span><b role="cell">{priceMoney(price.inputPerMillion)}</b><b role="cell">{price.cachedInputPerMillion === null ? '—' : priceMoney(price.cachedInputPerMillion)}</b><b role="cell">{priceMoney(price.outputPerMillion)}</b><span role="cell" className="row-actions"><button type="button" aria-label={editLabel} title={manual ? 'Edit manual price' : 'Create a manual override'} onClick={() => setEditor({ kind: 'price', initial: price })}><PencilSimple /></button>{manual && <button type="button" aria-label={`Delete ${price.modelId}`} title="Delete manual price" onClick={() => void removePrice(price)}><Trash /></button>}</span></div>
        })}
        {data.items.length === 0 && <div className="usage-empty">No prices match this search.</div>}
        {data.total > 0 && <Pagination page={data.page} totalPages={Math.max(1, data.totalPages)} total={data.total} pageSize={data.pageSize} onPage={value => { const next = new URLSearchParams(params); next.set('tab', 'price-data'); next.set('page', String(value)); setParams(next) }} />}
      </section>
      <section className="alias-ledger">
        <h2>MODEL ALIASES</h2>
        {data.aliases.length === 0 ? <div className="usage-empty">No model aliases configured.</div> : data.aliases.map(alias => <div className="alias-row" key={alias.observedModelId}><span>{alias.observedModelId}</span><b>USES</b><strong>{alias.canonicalModelId}</strong><span className="row-actions"><button type="button" aria-label={`Edit alias ${alias.observedModelId}`} onClick={() => setEditor({ kind: 'alias', initial: alias })}><PencilSimple /></button><button type="button" aria-label={`Delete alias ${alias.observedModelId}`} onClick={() => void removeAlias(alias)}><Trash /></button></span></div>)}
      </section>
      {editor?.kind === 'price' && <PriceEditor initial={editor.initial} onClose={() => setEditor(null)} onSaved={refreshAfterMutation} />}
      {editor?.kind === 'alias' && <AliasEditor initial={editor.initial} observedModelId={editor.observed} models={models} onClose={() => setEditor(null)} onSaved={refreshAfterMutation} />}
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
