import React, { useCallback, useEffect, useMemo, useState } from 'react'
import {
  ChevronDown,
  ChevronRight,
  Columns3,
  Eye,
  FileSearch,
  RefreshCw,
  Search,
  Table2,
} from 'lucide-react'
import {
  getDataSchema,
  listCatalogSources,
  previewDataTable,
  queryStream,
  type CatalogSource,
  type CatalogSourcesResponse,
  type DataQueryResponse,
  type DataSchemaResponse,
  type DataTableSummary,
} from '../../api/client'
import { EmptyState, ErrorState, LoadingState, ResourceMeta } from '../shared/AsyncState'
import { usePollingResource } from '../../hooks/usePollingResource'

type TableDetails = {
  schema: DataSchemaResponse | null
  preview: DataQueryResponse | null
  errors: string[]
  note?: string
}

type StorageGroup = {
  label: string
  detail: string
  count: number
  tables: DataTableSummary[]
}

type Row = Record<string, unknown>

type CatalogBrowserTable = DataTableSummary & {
  schema?: string
  qualified_name?: string
  source_id?: string
  source_label?: string
  source_status?: CatalogSource['status']
  source_kind?: CatalogSource['kind']
}

type SourceWithTables = CatalogSource & {
  browserTables: CatalogBrowserTable[]
}

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

function errorMessage(reason: unknown): string {
  return reason instanceof Error ? reason.message : 'request failed'
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

function sourceDetail(source: CatalogSource): string {
  if (source.source === 'cache') {
    return 'Served from the gateway cache after a recent Hive metastore read.'
  }
  return source.detail
}

function SourceCard({
  source,
  expanded,
  selectedPath,
  onToggle,
  onSelect,
}: {
  source: SourceWithTables
  expanded: boolean
  selectedPath: string | null
  onToggle: () => void
  onSelect: (table: CatalogBrowserTable) => void
}) {
  const visibleTables = expanded ? source.browserTables : source.browserTables.slice(0, 4)
  const hiddenCount = Math.max(source.browserTables.length - visibleTables.length, 0)

  return (
    <div className="rounded-lg border border-stone-200 bg-stone-50 p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">{source.label}</p>
          <p className="mt-2 text-sm text-stone-500">{sourceDetail(source)}</p>
          <p className="mt-1 text-xs uppercase tracking-[0.16em] text-stone-400">
            {source.browserTables.length} tables / {source.source ?? 'gateway'} / {source.source_type ?? 'metadata'}
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <span className={`rounded-md border px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.16em] ${toneForSource(source.status)}`}>
            {source.status}
          </span>
          <button
            type="button"
            onClick={onToggle}
            className="inline-flex items-center gap-1 rounded-md border border-stone-200 bg-white px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.16em] text-stone-500 transition hover:bg-stone-100"
          >
            {expanded ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
            {expanded ? 'Collapse' : 'Tables'}
          </button>
        </div>
      </div>
      {visibleTables.length ? (
        <div className={expanded ? 'mt-3 max-h-72 space-y-2 overflow-y-auto pr-1' : 'mt-3 flex flex-wrap gap-2'}>
          {visibleTables.map(table => {
            const selected = selectedPath === table.path
            return (
              <button
                key={`${source.id}-${table.qualified_name ?? table.path ?? table.name}`}
                type="button"
                onClick={() => onSelect(table)}
                className={expanded
                  ? `w-full rounded-md border px-3 py-2 text-left text-sm transition ${selected ? 'border-stone-900 bg-white text-stone-900' : 'border-stone-200 bg-white text-stone-600 hover:border-stone-400'}`
                  : `rounded-md border px-2 py-1 text-xs transition ${selected ? 'border-stone-900 bg-white text-stone-900' : 'border-stone-200 bg-white text-stone-600 hover:border-stone-400'}`
                }
              >
                <span className="font-medium">{table.name}</span>
                {expanded ? (
                  <span className="mt-1 block break-all text-xs text-stone-400">
                    {table.qualified_name ?? table.path}
                  </span>
                ) : null}
              </button>
            )
          })}
          {hiddenCount ? (
            <button
              type="button"
              onClick={onToggle}
              className="rounded-md border border-stone-200 bg-white px-2 py-1 text-xs text-stone-500 transition hover:bg-stone-100"
            >
              +{hiddenCount} more
            </button>
          ) : null}
        </div>
      ) : (
        <div className="mt-3">
          <EmptyState title="No tables reported" detail="This source did not return table-level metadata." />
        </div>
      )}
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

function isSafeSqlIdentifier(value?: string): value is string {
  return Boolean(value && /^[A-Za-z_][A-Za-z0-9_]*$/.test(value))
}

function risingWaveIdentifier(table: CatalogBrowserTable): string | null {
  const schema = table.schema ?? table.qualified_name?.split('.')[0]
  const name = table.name ?? table.qualified_name?.split('.').at(-1)
  if (!isSafeSqlIdentifier(schema) || !isSafeSqlIdentifier(name)) return null
  return `"${schema}"."${name}"`
}

function supportsDeltaPreview(table: CatalogBrowserTable): boolean {
  const path = table.path ?? ''
  return path.startsWith('s3://') || path.startsWith('/') || path.includes('://')
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

export const DataCatalog: React.FC = () => {
  const [search, setSearch] = useState('')
  const [selectedTable, setSelectedTable] = useState<CatalogBrowserTable | null>(null)
  const [expandedSourceIds, setExpandedSourceIds] = useState<Set<string>>(() => new Set())

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

  const liveSources = useMemo(() => catalogSources?.live_sources ?? [], [catalogSources?.live_sources])
  const staticSources = useMemo(() => catalogSources?.static_sources ?? [], [catalogSources?.static_sources])
  const sourceSummary = catalogSources?.summary ?? null
  const tables = useMemo<CatalogBrowserTable[]>(() => {
    return [...liveSources, ...staticSources].flatMap(source => source.tables.map(table => {
      const qualifiedName = table.qualified_name ?? (table.schema ? `${table.schema}.${table.name}` : undefined)
      return {
        name: table.name,
        path: table.path ?? qualifiedName ?? table.name,
        schema: table.schema,
        qualified_name: qualifiedName,
        family: table.family ?? source.source_family,
        source: source.source,
        source_type: source.source_type,
        source_id: source.id,
        source_label: source.label,
        source_status: source.status,
        source_kind: source.kind,
      }
    }))
  }, [liveSources, staticSources])
  const sourcesWithTables = useMemo<SourceWithTables[]>(() => {
    return [...liveSources, ...staticSources].map(source => ({
      ...source,
      browserTables: tables.filter(table => table.source_id === source.id),
    }))
  }, [liveSources, staticSources, tables])
  const filteredTables = useMemo(() => tables.filter(table => {
    const haystack = `${table.name} ${table.path} ${table.source_label ?? ''} ${table.family ?? ''}`.toLowerCase()
    return haystack.includes(search.trim().toLowerCase())
  }), [search, tables])

  const effectiveSelectedTable = selectedTable ?? tables[0] ?? null
  const selectedTablePath = effectiveSelectedTable?.path ?? null

  const toggleSource = useCallback((sourceId: string) => {
    setExpandedSourceIds(current => {
      const next = new Set(current)
      if (next.has(sourceId)) {
        next.delete(sourceId)
      } else {
        next.add(sourceId)
      }
      return next
    })
  }, [])

  const loadTableDetails = useCallback(async (): Promise<TableDetails> => {
    if (!effectiveSelectedTable) {
      return { schema: null, preview: null, errors: [] }
    }

    if (effectiveSelectedTable.family === 'streaming_sql') {
      const identifier = risingWaveIdentifier(effectiveSelectedTable)
      if (!identifier) {
        return {
          schema: null,
          preview: null,
          errors: [],
          note: 'RisingWave preview needs a valid schema and table name.',
        }
      }
      const preview = await queryStream(`SELECT * FROM ${identifier} LIMIT 10`)
      return {
        schema: {
          table_path: effectiveSelectedTable.qualified_name ?? effectiveSelectedTable.path,
          fields: (preview.columns ?? []).map(column => ({ name: column, type: 'reported by preview' })),
        },
        preview,
        errors: [],
      }
    }

    if (effectiveSelectedTable.family === 'postgres') {
      return {
        schema: null,
        preview: null,
        errors: [],
        note: 'Operational SQL metadata is listed here; row preview is not exposed through the data adapter.',
      }
    }

    if (!supportsDeltaPreview(effectiveSelectedTable)) {
      return {
        schema: null,
        preview: null,
        errors: [],
        note: 'Hive metadata is available, but this table did not report a Delta path for schema or preview.',
      }
    }

    const [schemaRes, previewRes] = await Promise.allSettled([
      getDataSchema(effectiveSelectedTable.path),
      previewDataTable(effectiveSelectedTable.path, 10),
    ])

    const errors: string[] = []
    const schema = schemaRes.status === 'fulfilled' ? schemaRes.value : null
    const preview = previewRes.status === 'fulfilled' ? previewRes.value : null

    if (schemaRes.status === 'rejected') {
      errors.push(`Schema: ${errorMessage(schemaRes.reason)}`)
    }
    if (previewRes.status === 'rejected') {
      errors.push(`Preview: ${errorMessage(previewRes.reason)}`)
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
  const sourceLabel = effectiveSelectedTable?.source_label ?? 'Source-aware'
  const storageGroups = useMemo(() => groupTablesByStorage(tables), [tables])
  const hasPartialState = Boolean(
    tableDetails?.errors.length
    || detailsError
    || sourcesError
  )

  return (
    <div className="mx-auto max-w-7xl space-y-6">
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
              {sourcesWithTables.map(source => (
                <SourceCard
                  key={source.id}
                  source={source}
                  expanded={expandedSourceIds.has(source.id)}
                  selectedPath={effectiveSelectedTable?.path ?? null}
                  onToggle={() => toggleSource(source.id)}
                  onSelect={setSelectedTable}
                />
              ))}
            </div>
          </>
        )}
      </section>

      {sourcesLoading && !catalogSources ? (
        <LoadingState label="Loading catalog browser..." />
      ) : sourcesError && tables.length === 0 ? (
        <ErrorState title="Data catalog unavailable" detail={sourcesError} onRetry={() => void refreshCatalogSources()} />
      ) : (
        <div className="grid gap-6 xl:grid-cols-[minmax(0,0.95fr)_minmax(0,1.05fr)]">
          <section className="rounded-lg border border-stone-200 bg-white p-5 shadow-sm">
            <div className="flex flex-wrap items-center justify-between gap-3 border-b border-stone-200 pb-4">
              <h3 className="flex items-center gap-2 text-lg font-semibold text-stone-900">
                <Table2 size={18} className="text-stone-500" />
                Catalog browser
              </h3>
              <div className="flex items-center gap-3">
                <ResourceMeta lastUpdated={sourcesLastUpdated} refreshing={sourcesRefreshing} stale={sourcesStale} />
                <button
                  type="button"
                  onClick={() => void refreshCatalogSources()}
                  className="inline-flex items-center gap-2 rounded-lg border border-stone-200 bg-white px-3 py-2 text-sm text-stone-600 transition hover:bg-stone-100"
                >
                  <RefreshCw size={14} className={sourcesRefreshing ? 'animate-spin' : ''} />
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
                          <p className="mt-1 text-xs uppercase tracking-[0.16em] text-stone-400">
                            {table.source_label ?? 'Unknown source'} / {storageFamilyLabel(table.family ?? 'other')}
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
                  disabled={!effectiveSelectedTable}
                  className="inline-flex items-center gap-2 rounded-lg border border-stone-200 bg-white px-3 py-2 text-sm text-stone-600 transition hover:bg-stone-100 disabled:cursor-not-allowed disabled:opacity-50"
                >
                  <RefreshCw size={14} className={detailsRefreshing ? 'animate-spin' : ''} />
                  Refresh selection
                </button>
              </div>

              {effectiveSelectedTable ? (
                <div className="mt-4 space-y-4">
                  <div className="grid gap-3 md:grid-cols-2">
                    <div className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                      <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Table name</p>
                      <p className="mt-2 text-sm font-semibold text-stone-900">{effectiveSelectedTable.name}</p>
                    </div>
                    <div className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                      <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Source</p>
                      <p className="mt-2 text-sm font-semibold text-stone-900">{sourceLabel}</p>
                      <p className="mt-1 text-xs uppercase tracking-[0.16em] text-stone-500">{storageFamilyLabel(effectiveSelectedTable.family ?? 'other')}</p>
                    </div>
                  </div>

                  {tableDetails?.note ? (
                    <div className="rounded-lg border border-stone-200 bg-white px-4 py-3 text-sm text-stone-600">
                      {tableDetails.note}
                    </div>
                  ) : null}

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
                              <TableRowPreview key={`${effectiveSelectedTable.path}-${index}`} row={row} />
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
                    Path: <span className="font-mono text-stone-700">{effectiveSelectedTable.path}</span>
                  </div>
                </div>
              ) : (
                <EmptyState title="No table selected" detail="Pick a table from the catalog to inspect its schema and preview rows." />
              )}

              {hasPartialState ? (
                <div className="mt-4 space-y-2">
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
