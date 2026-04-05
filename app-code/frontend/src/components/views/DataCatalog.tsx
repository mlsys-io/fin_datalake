import React, { useMemo, useState } from 'react'
import { sendIntent } from '../../api/client'
import { Database, RefreshCw, Search, Table2 } from 'lucide-react'
import { EmptyState, ErrorState, LoadingState, ResourceMeta } from '../shared/AsyncState'
import { usePollingResource } from '../../hooks/usePollingResource'

export const DataCatalog: React.FC = () => {
    const [search, setSearch] = useState('')
    const {
        data: tablesData,
        loading,
        refreshing,
        error,
        lastUpdated,
        stale,
        refresh,
    } = usePollingResource(
        async () => {
            const res = await sendIntent<{ tables?: { name: string; path: string }[] }>('data', 'list_tables')
            return (res.tables || []) as { name: string; path: string }[]
        },
        { pollIntervalMs: 60_000 },
    )
    const tables = tablesData ?? []

    const filteredTables = useMemo(() => tables.filter(t =>
        t.name.toLowerCase().includes(search.toLowerCase())
    ), [search, tables])

    return (
        <div className="space-y-6 max-w-6xl mx-auto">
            {/* Header Stats */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <div className="bg-white p-6 rounded-lg border border-stone-200 shadow-sm flex items-center gap-4">
                    <div className="p-3 bg-stone-100 rounded-lg text-stone-700">
                        <Database size={24} />
                    </div>
                    <div>
                        <p className="text-stone-500 text-sm">Active Tables</p>
                        <p className="text-2xl font-bold text-stone-900">{tables.length}</p>
                    </div>
                </div>
            </div>

            {/* Catalog */}
            <div className="bg-white rounded-lg border border-stone-200 shadow-sm overflow-hidden">
                <div className="p-6 border-b border-stone-200 flex justify-between items-center">
                    <h3 className="text-lg font-semibold text-stone-900 flex items-center gap-2">
                        <Table2 size={20} className="text-stone-500" />
                        Delta Lake Tables
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
                        <div className="relative">
                            <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-stone-400" size={16} />
                            <input
                                type="text"
                                placeholder="Search tables..."
                                className="pl-9 pr-4 py-2 bg-white border border-stone-200 rounded text-sm text-stone-900 placeholder-stone-400 focus:outline-none focus:ring-2 focus:ring-stone-400"
                                value={search}
                                onChange={e => setSearch(e.target.value)}
                            />
                        </div>
                    </div>
                </div>

                <div className="p-6">
                    {error && tables.length > 0 && (
                        <div className="mb-4">
                            <ErrorState title="Data catalog refresh failed" detail={error} onRetry={() => void refresh()} />
                        </div>
                    )}
                    {loading ? (
                        <LoadingState label="Loading Delta Lake tables..." />
                    ) : error && tables.length === 0 ? (
                        <ErrorState title="Data catalog unavailable" detail={error} onRetry={() => void refresh()} />
                    ) : filteredTables.length === 0 ? (
                        <EmptyState title={search ? 'No tables match your search' : 'No tables found in Delta Lake'} detail={search ? 'Try a broader search term.' : 'The data catalog is currently empty.'} />
                    ) : (
                        <div className="grid grid-cols-1 gap-4">
                            {filteredTables.map((t, idx) => (
                                <div key={t.name || idx} className="p-4 bg-stone-50 border border-stone-200 rounded-lg hover:border-stone-400 transition-colors cursor-pointer group">
                                    <h4 className="text-stone-900 font-medium">{t.name}</h4>
                                    <p className="text-sm text-stone-500 mt-2">Path: <span className="font-mono text-xs">{t.path}</span></p>
                                    <p className="text-sm text-stone-500 mt-1">Format: Delta</p>
                                </div>
                            ))}
                        </div>
                    )}
                </div>
            </div>
        </div>
    )
}
