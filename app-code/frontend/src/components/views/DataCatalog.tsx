import React, { useState, useEffect } from 'react'
import { sendIntent } from '../../api/client'
import { Database, Search, Table2 } from 'lucide-react'

export const DataCatalog: React.FC = () => {
    const [tables, setTables] = useState<{ name: string, path: string }[]>([])
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState('')
    const [search, setSearch] = useState('')

    useEffect(() => {
        const fetchTables = async () => {
            try {
                const res = await sendIntent('data', 'list_tables')
                setTables(res.tables || [])
            } catch (e: any) {
                setError(e.message)
            } finally {
                setLoading(false)
            }
        }
        fetchTables()
    }, [])

    const filteredTables = tables.filter(t =>
        t.name.toLowerCase().includes(search.toLowerCase())
    )

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

                <div className="p-6">
                    {loading ? (
                        <div className="animate-pulse flex space-x-4">
                            <div className="flex-1 space-y-4 py-1">
                                <div className="h-4 bg-stone-200 rounded w-3/4"></div>
                                <div className="h-4 bg-stone-200 rounded w-1/2"></div>
                            </div>
                        </div>
                    ) : error ? (
                        <p className="text-red-500">{error}</p>
                    ) : filteredTables.length === 0 ? (
                        <p className="text-stone-500 text-center py-8">{search ? 'No tables match your search.' : 'No tables found in Delta Lake.'}</p>
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
