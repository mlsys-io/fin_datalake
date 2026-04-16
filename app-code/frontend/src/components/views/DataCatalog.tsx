import React, { useCallback, useEffect, useMemo, useState } from 'react'
import {
  Columns3,
  Database,
  Eye,
  FileSearch,
  RefreshCw,
  Search,
  Table2,
} from 'lucide-react'
import {
  getDataSchema,
  listCatalogSources,
  listDataTables,
  previewDataTable,
  queryStream,
  type CatalogSource,
  type CatalogSourcesResponse,
  type DataCatalogResponse,
  type DataQueryResponse,
  type DataSchemaResponse,
  type DataTableSummary,
} from '../../api/client'
import { EmptyState, ErrorState, LoadingState, ResourceMeta } from '../shared/AsyncState'
import { usePollingResource } from '../../hooks/usePollingResource'
import { getRisingWaveSchema } from '../../lib/risingwave'

type TableDetails = {
  schema: DataSchemaResponse | null
  preview: DataQueryResponse | null
  errors: string[]
}

type LiveDataDetails = {
  signals: DataQueryResponse | null
  prices: DataQueryResponse | null
  errors: string[]
}

type StorageGroup = {
  label: string
  detail: string
  count: number
  tables: DataTableSummary[]
}

type Row = Record<string, unknown>

function rowsToObjects(result: DataQueryResponse | null | undefined): Row[] {
  if (!result?.columns?.length || !result.rows?.length) return []
  return result.rows.map(row => Object.fromEntries(result.columns!.map((column, index) => [column, row[index]])))
}

function asString(value: unknown): string {
  if (value == null) return 'n/a'
  if (typeof value === 'string') return value
  if (typeof value === 'number' && Number.isFinite(value)) return String(value)
  if (typeof value === 'boolean') return value ? 'true' : 'false'
  return String(value)
}

function titleCase(value: string): string {
  return value
    .split(/[_\-. ]+/)
    .filter(Boolean)
    .map(part => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ')
}

function storageFamilyLabel(family: string): string {
  switch (family) {
    case 'postgres':
      return 'PostgreSQL / SQL'
    case 'streaming_sql':
      return 'RisingWave'
    case 'lakehouse':
      return 'Delta Lake / Object Storage'
    case 'object_storage':
      return 'Object Storage'
    case 'local_file':
      return 'Local File Store'
    default:
      return titleCase(family || 'Other')
  }
}

function storageDetail(label: string): string {
  switch (label) {
    case 'PostgreSQL / SQL': return 'Relational tables and metadata-backed sources'
    case 'RisingWave': return 'Live streaming rows surfaced through the gateway'
    case 'Delta Lake / Object Storage': return 'Catalogued lakehouse tables and file-backed datasets'
    case 'Object Storage': return 'Blob/object-backed datasets and exports'
    case 'Local File Store': return 'Local or embedded storage sources'
    default: return 'Unclassified storage source'
  }
}

function toneForSource(status: CatalogSource['status']): string {
  if (status === 'available') return 'border-emerald-200 bg-emerald-50 text-emerald-700'
  if (status === 'partial') return 'border-amber-200 bg-amber-50 text-amber-700'
  if (status === 'pending') return 'border-stone-200 bg-stone-100 text-stone-700'
  return 'border-stone-200 bg-stone-50 text-stone-600'
}

function SourceCard({ source }: { source: CatalogSource }) {
  return (
    <div className="rounded-lg border border-stone-200 bg-stone-50 p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">{source.label}</p>
          <p className="mt-2 text-sm text-stone-500">{source.detail}</p>
        </div>
        <span className={`rounded-md border px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.16em] ${toneForSource(source.status)}`}>
          {source.status}
        </span>
      </div>
      <div className="mt-3 flex flex-wrap gap-2">
        {source.tables.slice(0, 4).map(table => (
          <span key={`${source.id}-${table.qualified_name ?? table.path ?? table.name}`} className="rounded-md border border-stone-200 bg-white px-2 py-1 text-xs text-stone-600">
            {table.name}
          </span>
        ))}
      </div>
    </div>
  )
}

function groupTablesByStorage(tables: DataTableSummary[]): StorageGroup[] {
  const map = new Map<string, DataTableSummary[]>()
  for (const table of tables) {
    const label = storageFamilyLabel(table.family ?? 'other')
    const current = map.get(label) ?? []
    current.push(table)
    map.set(label, current)
  }

  return Array.from(map.entries())
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([label, grouped]) => ({
      label,
      detail: storageDetail(label),
      count: grouped.length,
      tables: grouped,
    }))
}

function TableRowPreview({ row }: { row: Row }) {
  const entries = Object.entries(row).slice(0, 6)
  return (
    <tr className="border-t border-stone-100 align-top">
      {entries.map(([key, value]) => (
        <td key={key} className="px-3 py-2">
          <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-stone-400">{key}</p>
          <p className="mt-1 break-words text-sm text-stone-700">{asString(value)}</p>
        </td>
      ))}
    </tr>
  )
}

function QuerySnapshotCard({
  title,
  description,
  storage,
  response,
  kind,
}: {
  title: string
  description: string
  storage: string
  response: DataQueryResponse | null
  kind: 'signals' | 'prices'
}) {
  const rows = rowsToObjects(response)
  const latest = rows[0] ?? null
  const count = response?.row_count ?? rows.length

  const summaryRows = kind === 'signals'
    ? [
        { label: 'Symbol', value: asString(latest?.symbol) },
        { label: 'Action', value: asString(latest?.action) },
        { label: 'Confidence', value: asString(latest?.confidence) },
      ]
    : [
        { label: 'Symbol', value: asString(latest?.symbol) },
        { label: 'Close', value: asString(latest?.close) },
        { label: 'Return', value: asString(latest?.price_return_pct) },
      ]

  return (
    <div className="rounded-lg border border-stone-200 bg-white p-4 shadow-sm">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">{title}</p>
          <p className="mt-1 text-sm text-stone-500">{description}</p>
          <p className="mt-1 text-xs uppercase tracking-[0.16em] text-stone-400">{storage}</p>
        </div>
        <span className="rounded-md border border-stone-200 bg-stone-50 px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.16em] text-stone-500">
          {count} rows
        </span>
      </div>

      {latest ? (
        <div className="mt-4 grid gap-2 sm:grid-cols-3">
          {summaryRows.map(item => (
            <div key={item.label} className="rounded-md border border-stone-200 bg-stone-50 px-3 py-2">
              <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-stone-400">{item.label}</p>
              <p className="mt-1 break-words text-sm text-stone-700">{item.value}</p>
            </div>
          ))}
        </div>
      ) : (
        <div className="mt-4">
          <EmptyState title="No rows loaded yet" detail="Refresh the live snapshot when the RisingWave tables are available." />
        </div>
      )}

      {rows.length ? (
        <div className="mt-4 overflow-x-auto rounded-lg border border-stone-200 bg-white">
          <table className="min-w-full text-left text-sm">
            <thead className="bg-stone-50 text-[11px] uppercase tracking-[0.16em] text-stone-400">
              <tr>
                {(response?.columns ?? []).slice(0, 6).map(column => (
                  <th key={column} className="px-3 py-3">{column}</th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-stone-100">
              {rows.slice(0, 5).map((row, index) => (
                <TableRowPreview key={`${title}-${index}`} row={row} />
              ))}
            </tbody>
          </table>
        </div>
      ) : null}
    </div>
  )
}

export const DataCatalog: React.FC = () => {
  const [search, setSearch] = useState('')
  const [selectedTable, setSelectedTable] = useState<DataTableSummary | null>(null)
  const risingwaveSchema = getRisingWaveSchema()

  const loadTables = useCallback(async (): Promise<DataCatalogResponse> => {
    return listDataTables()
  }, [])

  const {
    data: catalog,
    loading,
    refreshing,
    error,
    lastUpdated,
    stale,
    refresh,
  } = usePollingResource(loadTables, { pollIntervalMs: 60_000 })

  const loadLiveData = useCallback(async (): Promise<LiveDataDetails> => {
    const errors: string[] = []
    const signalSql = `SELECT symbol, action, confidence, sentiment_label, sentiment_score, analyst_summary, last_price, sma_5, sma_20, vwap, price_return_pct, volatility_estimate, timestamp_ms FROM ${risingwaveSchema}.market_pulse_signals ORDER BY timestamp_ms DESC LIMIT 5`
    const priceSql = `SELECT symbol, close, vwap, sma_5, sma_20, price_return_pct, volatility_estimate, timestamp_ms FROM ${risingwaveSchema}.market_pulse_prices ORDER BY timestamp_ms DESC LIMIT 5`

    const [signalsRes, pricesRes] = await Promise.allSettled([
      queryStream(signalSql),
      queryStream(priceSql),
    ])

    const signals = signalsRes.status === 'fulfilled' ? signalsRes.value : null
    const prices = pricesRes.status === 'fulfilled' ? pricesRes.value : null

    if (signalsRes.status === 'rejected') {
      errors.push(`Signals: ${signalsRes.reason instanceof Error ? signalsRes.reason.message : 'request failed'}`)
    }
    if (pricesRes.status === 'rejected') {
      errors.push(`Prices: ${pricesRes.reason instanceof Error ? pricesRes.reason.message : 'request failed'}`)
    }

    return { signals, prices, errors }
  }, [risingwaveSchema])

  const {
    data: liveData,
    loading: liveLoading,
    refreshing: liveRefreshing,
    error: liveError,
    lastUpdated: liveLastUpdated,
    stale: liveStale,
    refresh: refreshLiveData,
  } = usePollingResource(loadLiveData, { pollIntervalMs: 45_000 })

  const loadCatalogSources = useCallback(async (): Promise<CatalogSourcesResponse> => {
    return listCatalogSources()
  }, [])

  const {
    data: catalogSources,
    loading: sourcesLoading,
    refreshing: sourcesRefreshing,
    error: sourcesError,
    lastUpdated: sourcesLastUpdated,
    stale: sourcesStale,
    refresh: refreshCatalogSources,
  } = usePollingResource(loadCatalogSources, { pollIntervalMs: 60_000 })

  const tables = useMemo(() => catalog?.tables ?? [], [catalog?.tables])
  const filteredTables = useMemo(() => tables.filter(table => {
    const haystack = `${table.name} ${table.path}`.toLowerCase()
    return haystack.includes(search.trim().toLowerCase())
  }), [search, tables])

  const effectiveSelectedTable = selectedTable ?? tables[0] ?? null
  const selectedTablePath = effectiveSelectedTable?.path ?? null

  const loadTableDetails = useCallback(async (): Promise<TableDetails> => {
    if (!effectiveSelectedTable) {
      return { schema: null, preview: null, errors: [] }
    }

    const [schemaRes, previewRes] = await Promise.allSettled([
      getDataSchema(effectiveSelectedTable.path),
      previewDataTable(effectiveSelectedTable.path, 10),
    ])

    const errors: string[] = []
    const schema = schemaRes.status === 'fulfilled' ? schemaRes.value : null
    const preview = previewRes.status === 'fulfilled' ? previewRes.value : null

    if (schemaRes.status === 'rejected') {
      errors.push(`Schema: ${schemaRes.reason instanceof Error ? schemaRes.reason.message : 'request failed'}`)
    }
    if (previewRes.status === 'rejected') {
      errors.push(`Preview: ${previewRes.reason instanceof Error ? previewRes.reason.message : 'request failed'}`)
    }

    return { schema, preview, errors }
  }, [effectiveSelectedTable])

  const {
    data: tableDetails,
    loading: detailsLoading,
    refreshing: detailsRefreshing,
    error: detailsError,
    refresh: refreshDetails,
  } = usePollingResource(loadTableDetails, { pollIntervalMs: effectiveSelectedTable ? 60_000 : undefined })

  useEffect(() => {
    if (selectedTablePath) {
      void refreshDetails()
    }
  }, [refreshDetails, selectedTablePath])

  const selectedRows = useMemo(() => rowsToObjects(tableDetails?.preview), [tableDetails?.preview])
  const schemaFields = tableDetails?.schema?.fields ?? []
  const totalTables = tables.length
  const selectedLabel = effectiveSelectedTable?.name ?? 'None'
  const sourceLabel = catalog?.source ?? 'n/a'
  const previewCount = tableDetails?.preview?.row_count ?? selectedRows.length
  const storageGroups = useMemo(() => groupTablesByStorage(tables), [tables])
  const liveSources = catalogSources?.live_sources ?? []
  const staticSources = catalogSources?.static_sources ?? []
  const sourceSummary = catalogSources?.summary ?? null
  const hasPartialState = Boolean(
    error
    || tableDetails?.errors.length
    || detailsError
    || liveError
    || liveData?.errors.length
    || sourcesError
  )

  return (
    <div className="mx-auto max-w-7xl space-y-6">
      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <div className="flex items-center gap-4 rounded-lg border border-stone-200 bg-white p-4 shadow-sm">
          <div className="rounded-lg bg-stone-100 p-3 text-stone-700">
            <Database size={24} />
          </div>
          <div>
            <p className="text-sm text-stone-500">Active tables</p>
            <p className="text-2xl font-bold text-stone-900">{totalTables}</p>
          </div>
        </div>
        <div className="rounded-lg border border-stone-200 bg-white p-4 shadow-sm">
          <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Source</p>
          <p className="mt-2 text-2xl font-bold text-stone-900">{sourceLabel}</p>
          <p className="mt-2 text-sm text-stone-500">{catalog?.error ?? 'Catalog source reported by the gateway'}</p>
        </div>
        <div className="rounded-lg border border-stone-200 bg-white p-4 shadow-sm">
          <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Selected table</p>
          <p className="mt-2 text-2xl font-bold text-stone-900">{selectedLabel}</p>
          <p className="mt-2 text-sm text-stone-500">{effectiveSelectedTable?.path ?? 'Pick a table to inspect schema and preview rows.'}</p>
        </div>
        <div className="rounded-lg border border-stone-200 bg-white p-4 shadow-sm">
          <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Preview rows</p>
          <p className="mt-2 text-2xl font-bold text-stone-900">{previewCount}</p>
          <p className="mt-2 text-sm text-stone-500">Selected table preview</p>
        </div>
      </div>

      <section className="rounded-lg border border-stone-200 bg-white p-5 shadow-sm">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="min-w-0">
            <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Source-aware catalog</p>
            <p className="mt-2 text-sm text-stone-500">
              Live sources are discovered from the gateway first, and static sources remain visible when live discovery is incomplete.
            </p>
          </div>
          <div className="flex items-center gap-3">
            <ResourceMeta lastUpdated={sourcesLastUpdated} refreshing={sourcesRefreshing} stale={sourcesStale} />
            <button
              type="button"
              onClick={() => void refreshCatalogSources()}
              className="inline-flex items-center gap-2 rounded-lg border border-stone-200 bg-white px-3 py-2 text-sm text-stone-600 transition hover:bg-stone-100"
            >
              <RefreshCw size={14} className={sourcesRefreshing ? 'animate-spin' : ''} />
              Refresh sources
            </button>
          </div>
        </div>

        {sourcesLoading && !catalogSources ? (
          <LoadingState label="Loading catalog sources..." />
        ) : sourcesError && !catalogSources ? (
          <ErrorState title="Catalog sources unavailable" detail={sourcesError} onRetry={() => void refreshCatalogSources()} />
        ) : (
          <>
            <div className="mt-4 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
              <div className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Sources</p>
                <p className="mt-2 text-2xl font-bold text-stone-900">{sourceSummary?.total_sources ?? liveSources.length + staticSources.length}</p>
                <p className="mt-1 text-sm text-stone-500">Live and fallback sources</p>
              </div>
              <div className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Available</p>
                <p className="mt-2 text-2xl font-bold text-stone-900">{sourceSummary?.available_sources ?? liveSources.filter(source => source.status === 'available').length}</p>
                <p className="mt-1 text-sm text-stone-500">Sources returning rows</p>
              </div>
              <div className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Tables</p>
                <p className="mt-2 text-2xl font-bold text-stone-900">{sourceSummary?.total_tables ?? liveSources.reduce((count, source) => count + source.tables.length, 0)}</p>
                <p className="mt-1 text-sm text-stone-500">Across all live sources</p>
              </div>
              <div className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Static sources</p>
                <p className="mt-2 text-2xl font-bold text-stone-900">{staticSources.length}</p>
                <p className="mt-1 text-sm text-stone-500">Fallback catalog groups</p>
              </div>
            </div>

            <div className="mt-4 grid gap-4 md:grid-cols-2 xl:grid-cols-3">
              {liveSources.map(source => (
                <SourceCard key={source.id} source={source} />
              ))}
              {staticSources.map(source => (
                <SourceCard key={source.id} source={source} />
              ))}
            </div>
          </>
        )}
      </section>

      <section className="rounded-lg border border-stone-200 bg-white p-5 shadow-sm">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="min-w-0">
            <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Live data catalog</p>
            <p className="mt-2 text-sm text-stone-500">
              The live catalog surfaces current gateway-backed rows first, while the static catalog below keeps the storage inventory grouped by backend family.
            </p>
          </div>
          <div className="flex items-center gap-3">
            <ResourceMeta lastUpdated={liveLastUpdated} refreshing={liveRefreshing} stale={liveStale} />
            <button
              type="button"
              onClick={() => void refreshLiveData()}
              className="inline-flex items-center gap-2 rounded-lg border border-stone-200 bg-white px-3 py-2 text-sm text-stone-600 transition hover:bg-stone-100"
            >
              <RefreshCw size={14} className={liveRefreshing ? 'animate-spin' : ''} />
              Refresh live data
            </button>
          </div>
        </div>

        {liveLoading && !liveData ? (
          <LoadingState label="Loading live data rows..." />
        ) : liveError && !liveData ? (
          <ErrorState title="Live data catalog unavailable" detail={liveError} onRetry={() => void refreshLiveData()} />
        ) : (
          <div className="mt-4 grid gap-4 xl:grid-cols-2">
            <QuerySnapshotCard
              title="Live signal stream"
              description={`Schema ${risingwaveSchema}.market_pulse_signals`}
              storage="RisingWave"
              response={liveData?.signals ?? null}
              kind="signals"
            />
            <QuerySnapshotCard
              title="Live price stream"
              description={`Schema ${risingwaveSchema}.market_pulse_prices`}
              storage="RisingWave"
              response={liveData?.prices ?? null}
              kind="prices"
            />
          </div>
        )}
      </section>

      {loading && !catalog ? (
        <LoadingState label="Loading catalog..." />
      ) : error && tables.length === 0 ? (
        <ErrorState title="Data catalog unavailable" detail={error} onRetry={() => void refresh()} />
      ) : (
        <div className="grid gap-6 xl:grid-cols-[minmax(0,0.95fr)_minmax(0,1.05fr)]">
          <section className="rounded-lg border border-stone-200 bg-white p-5 shadow-sm">
            <div className="flex flex-wrap items-center justify-between gap-3 border-b border-stone-200 pb-4">
              <h3 className="flex items-center gap-2 text-lg font-semibold text-stone-900">
                <Table2 size={18} className="text-stone-500" />
                Static data catalog
              </h3>
              <div className="flex items-center gap-3">
                <ResourceMeta lastUpdated={lastUpdated} refreshing={refreshing} stale={stale} />
                <button
                  type="button"
                  onClick={() => void refresh()}
                  className="inline-flex items-center gap-2 rounded-lg border border-stone-200 bg-white px-3 py-2 text-sm text-stone-600 transition hover:bg-stone-100"
                >
                  <RefreshCw size={14} className={refreshing ? 'animate-spin' : ''} />
                  Refresh
                </button>
              </div>
            </div>

            <div className="mt-4 grid gap-3 md:grid-cols-2 xl:grid-cols-3">
              {storageGroups.length ? storageGroups.map(group => (
                <div key={group.label} className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                  <div className="flex flex-wrap items-start justify-between gap-3">
                    <div>
                      <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">{group.label}</p>
                      <p className="mt-2 text-2xl font-bold text-stone-900">{group.count}</p>
                      <p className="mt-1 text-sm text-stone-500">{group.detail}</p>
                    </div>
                    <span className="rounded-md border border-stone-200 bg-white px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.16em] text-stone-500">
                      {group.label}
                    </span>
                  </div>
                  <div className="mt-3 flex flex-wrap gap-2">
                    {group.tables.slice(0, 3).map(table => (
                      <span key={table.path} className="rounded-md border border-stone-200 bg-white px-2 py-1 text-xs text-stone-600">
                        {table.name}
                      </span>
                    ))}
                  </div>
                </div>
              )) : (
                <div className="md:col-span-2 xl:col-span-3">
                  <EmptyState title="No storage groups yet" detail="The catalog will populate storage families once tables are discovered." />
                </div>
              )}
            </div>

            <label className="mt-4 flex items-center gap-2 rounded-lg border border-stone-200 bg-white px-3 py-2">
              <Search size={16} className="shrink-0 text-stone-400" />
              <input
                type="text"
                placeholder="Search by table name or path"
                className="w-full bg-transparent text-sm text-stone-900 outline-none placeholder:text-stone-400"
                value={search}
                onChange={e => setSearch(e.target.value)}
              />
            </label>

            <div className="mt-4 space-y-3">
              {filteredTables.length === 0 ? (
                <EmptyState
                  title={search ? 'No tables match your search' : 'No tables found'}
                  detail={search ? 'Try a broader search term.' : 'The catalog is currently empty.'}
                />
              ) : (
                filteredTables.map(table => {
                  const active = selectedTable?.path === table.path
                  return (
                    <button
                      key={table.path}
                      type="button"
                      onClick={() => setSelectedTable(table)}
                      className={`w-full rounded-lg border p-4 text-left transition ${
                        active
                          ? 'border-stone-900 bg-stone-50 shadow-sm'
                          : 'border-stone-200 bg-stone-50 hover:border-stone-400 hover:bg-white'
                      }`}
                    >
                      <div className="flex flex-wrap items-start justify-between gap-3">
                        <div className="min-w-0">
                          <p className="font-medium text-stone-900">{table.name}</p>
                          <p className="mt-1 break-words text-sm text-stone-500">
                            Path: <span className="font-mono text-xs text-stone-700">{table.path}</span>
                          </p>
                        </div>
                        {active ? (
                          <span className="rounded-md border border-stone-200 bg-white px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.16em] text-stone-500">
                            Selected
                          </span>
                        ) : null}
                      </div>
                    </button>
                  )
                })
              )}
            </div>
          </section>

          <section className="space-y-6">
            <div className="rounded-lg border border-stone-200 bg-white p-5 shadow-sm">
              <div className="flex flex-wrap items-center justify-between gap-3">
                <div className="flex items-center gap-2">
                  <Columns3 size={18} className="text-stone-500" />
                  <h3 className="text-lg font-semibold text-stone-900">Table Details</h3>
                </div>
                <button
                  type="button"
                  onClick={() => void refreshDetails()}
                  disabled={!selectedTable}
                  className="inline-flex items-center gap-2 rounded-lg border border-stone-200 bg-white px-3 py-2 text-sm text-stone-600 transition hover:bg-stone-100 disabled:cursor-not-allowed disabled:opacity-50"
                >
                  <RefreshCw size={14} className={detailsRefreshing ? 'animate-spin' : ''} />
                  Refresh selection
                </button>
              </div>

              {selectedTable ? (
                <div className="mt-4 space-y-4">
                  <div className="grid gap-3 md:grid-cols-2">
                    <div className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                      <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Table name</p>
                      <p className="mt-2 text-sm font-semibold text-stone-900">{selectedTable.name}</p>
                    </div>
                    <div className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                      <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Source</p>
                      <p className="mt-2 text-sm font-semibold text-stone-900">{sourceLabel}</p>
                      <p className="mt-1 text-xs uppercase tracking-[0.16em] text-stone-500">{storageFamilyLabel(selectedTable.family ?? 'other')}</p>
                    </div>
                  </div>

                  <div className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                    <div className="flex items-center gap-2">
                      <FileSearch size={16} className="text-stone-500" />
                      <p className="text-sm font-semibold text-stone-900">Schema</p>
                    </div>
                    {detailsLoading && !tableDetails ? (
                      <LoadingState label="Loading schema..." />
                    ) : schemaFields.length ? (
                      <div className="mt-4 overflow-hidden rounded-lg border border-stone-200 bg-white">
                        <table className="w-full text-left text-sm">
                          <thead className="bg-stone-50 text-[11px] uppercase tracking-[0.16em] text-stone-400">
                            <tr>
                              <th className="px-4 py-3">Field</th>
                              <th className="px-4 py-3">Type</th>
                            </tr>
                          </thead>
                          <tbody className="divide-y divide-stone-100">
                            {schemaFields.map(field => (
                              <tr key={field.name}>
                                <td className="px-4 py-3 font-medium text-stone-900">{field.name}</td>
                                <td className="px-4 py-3 text-stone-600">{field.type}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    ) : tableDetails?.errors?.length ? (
                      <div className="mt-4 space-y-2">
                        <EmptyState title="Schema unavailable" detail={tableDetails.errors.join(' - ')} />
                      </div>
                    ) : (
                      <EmptyState title="No schema loaded yet" detail="Refresh the selection to fetch the current table schema." />
                    )}
                  </div>

                  <div className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                    <div className="flex items-center gap-2">
                      <Eye size={16} className="text-stone-500" />
                      <p className="text-sm font-semibold text-stone-900">Preview</p>
                    </div>
                    {detailsLoading && !tableDetails ? (
                      <LoadingState label="Loading preview..." />
                    ) : selectedRows.length ? (
                      <div className="mt-4 overflow-x-auto rounded-lg border border-stone-200 bg-white">
                        <table className="min-w-full text-left text-sm">
                          <thead className="bg-stone-50 text-[11px] uppercase tracking-[0.16em] text-stone-400">
                            <tr>
                              {(tableDetails?.preview?.columns ?? []).slice(0, 6).map(column => (
                                <th key={column} className="px-3 py-3">{column}</th>
                              ))}
                            </tr>
                          </thead>
                          <tbody className="divide-y divide-stone-100">
                            {selectedRows.map((row, index) => (
                              <TableRowPreview key={`${selectedTable.path}-${index}`} row={row} />
                            ))}
                          </tbody>
                        </table>
                      </div>
                    ) : tableDetails?.errors?.length ? (
                      <EmptyState title="Preview unavailable" detail={tableDetails.errors.join(' - ')} />
                    ) : (
                      <EmptyState title="No preview rows yet" detail="Refresh the selection to fetch a small sample from the selected table." />
                    )}
                  </div>

                  <div className="rounded-lg border border-stone-200 bg-white p-4 text-sm text-stone-600">
                    Path: <span className="font-mono text-stone-700">{selectedTable.path}</span>
                  </div>
                </div>
              ) : (
                <EmptyState title="No table selected" detail="Pick a table from the catalog to inspect its schema and preview rows." />
              )}

              {hasPartialState ? (
                <div className="mt-4 space-y-2">
                  {error ? <div className="rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-900">Catalog: {error}</div> : null}
                  {liveData?.errors?.length ? (
                    <div className="rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-900">
                      {liveData.errors.map(item => (
                        <p key={item}>{item}</p>
                      ))}
                    </div>
                  ) : null}
                  {tableDetails?.errors?.length ? (
                    <div className="rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-900">
                      {tableDetails.errors.map(item => (
                        <p key={item}>{item}</p>
                      ))}
                    </div>
                  ) : null}
                  {detailsError ? <div className="rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-900">Detail query: {detailsError}</div> : null}
                </div>
              ) : null}
            </div>
          </section>
        </div>
      )}
    </div>
  )
}
