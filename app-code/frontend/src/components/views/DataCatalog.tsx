import React, { useState, useEffect } from 'react'
import { sendIntent } from '../../api/client'
import { Database, Search, Table2 } from 'lucide-react'

export const DataCatalog: React.FC = () => {
    const [tables, setTables] = useState<{ name: string, path: string }[]>([])
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState('')

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

    return (
        <div className="space-y-6 max-w-6xl mx-auto">
            {/* Header Stats */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <div className="bg-slate-800 p-6 rounded-xl border border-slate-700 shadow-sm flex items-center gap-4">
                    <div className="p-3 bg-indigo-500/20 rounded-lg text-indigo-400">
                        <Database size={24} />
                    </div>
                    <div>
                        <p className="text-slate-400 text-sm">Active Tables</p>
                        <p className="text-2xl font-bold text-slate-100">{tables.length}</p>
                    </div>
                </div>
            </div>

            {/* Catalog */}
            <div className="bg-slate-800 rounded-xl border border-slate-700 shadow-sm overflow-hidden">
                <div className="p-6 border-b border-slate-700 flex justify-between items-center">
                    <h3 className="text-lg font-semibold text-slate-100 flex items-center gap-2">
                        <Table2 size={20} className="text-indigo-400" />
                        Delta Lake Tables
                    </h3>
                    <div className="relative">
                        <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" size={16} />
                        <input
                            type="text"
                            placeholder="Search tables..."
                            className="pl-9 pr-4 py-2 bg-slate-900 border border-slate-700 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500"
                        />
                    </div>
                </div>

                <div className="p-6">
                    {loading ? (
                        <div className="animate-pulse flex space-x-4">
                            <div className="flex-1 space-y-4 py-1">
                                <div className="h-4 bg-slate-700 rounded w-3/4"></div>
                                <div className="h-4 bg-slate-700 rounded w-1/2"></div>
                            </div>
                        </div>
                    ) : error ? (
                        <p className="text-red-400">{error}</p>
                    ) : tables.length === 0 ? (
                        <p className="text-slate-400 text-center py-8">No tables found in Delta Lake.</p>
                    ) : (
                        <div className="grid grid-cols-1 gap-4">
                            {tables.map((t, idx) => (
                                <div key={t.name || idx} className="p-4 bg-slate-900/50 border border-slate-700 rounded-lg hover:border-indigo-500/50 transition-colors cursor-pointer group">
                                    <h4 className="text-indigo-300 font-mono font-medium group-hover:text-indigo-400">{t.name}</h4>
                                    <p className="text-sm text-slate-500 mt-2">Path: <span className="font-mono text-xs">{t.path}</span></p>
                                    <p className="text-sm text-slate-500 mt-1">Format: Delta /// Last Modified: Just now</p>
                                </div>
                            ))}
                        </div>
                    )}
                </div>
            </div>
        </div>
    )
}
