import React, { useState, useEffect } from 'react'
import { fetchOverseerSnapshots, fetchOverseerAlerts } from '../../api/client'
import { 
    Shield, 
    Activity, 
    AlertTriangle, 
    CheckCircle, 
    Zap, 
    Clock,
    Server
} from 'lucide-react'

interface ServiceMetrics {
    healthy: boolean
    data: any
    error: string | null
}

interface Snapshot {
    timestamp: string
    services: Record<string, ServiceMetrics>
}

interface Alert {
    timestamp: string
    level: 'info' | 'warning' | 'error' | 'critical'
    type: string
    action: string
    target: string
    detail: string
}

export const SystemOverseer: React.FC = () => {
    const [snapshots, setSnapshots] = useState<Snapshot[]>([])
    const [alerts, setAlerts] = useState<Alert[]>([])
    const [loading, setLoading] = useState(true)
    const [lastRefresh, setLastRefresh] = useState(new Date())

    const fetchData = async () => {
        try {
            const [snapRes, alertRes] = await Promise.all([
                fetchOverseerSnapshots(50),
                fetchOverseerAlerts(20)
            ])
            setSnapshots(snapRes)
            setAlerts(alertRes)
            setLastRefresh(new Date())
        } catch (e) {
            console.error('Failed to fetch Overseer data', e)
        } finally {
            setLoading(false)
        }
    }

    useEffect(() => {
        fetchData()
        const interval = setInterval(fetchData, 15000) // Auto-poll every 15s
        return () => clearInterval(interval)
    }, [])

    const latestSnap = snapshots[snapshots.length - 1]
    const services = latestSnap?.services || {}
    const isDegraded = Object.values(services).some(s => !s.healthy)

    const getStatusIcon = (healthy: boolean) => {
        return healthy 
            ? <CheckCircle className="text-emerald-500" size={18} />
            : <AlertTriangle className="text-amber-500" size={18} />
    }

    const getAlertLevelClass = (level: string) => {
        switch(level) {
            case 'critical': return 'bg-red-100 text-red-700 border-red-200'
            case 'error': return 'bg-orange-100 text-orange-700 border-orange-200'
            case 'warning': return 'bg-amber-100 text-amber-700 border-amber-200'
            default: return 'bg-blue-100 text-blue-700 border-blue-200'
        }
    }

    return (
        <div className="space-y-8 max-w-7xl mx-auto pb-12">
            {/* Top State Cards */}
            <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
                <div className="bg-white p-6 rounded-xl border border-stone-200 shadow-sm flex items-center gap-4 group hover:border-stone-400 transition-all">
                    <div className={`p-3 rounded-lg ${isDegraded ? 'bg-amber-50 text-amber-600' : 'bg-emerald-50 text-emerald-600'}`}>
                        <Shield size={24} />
                    </div>
                    <div>
                        <p className="text-stone-400 text-xs font-medium uppercase tracking-wider">System Status</p>
                        <p className="text-xl font-bold text-stone-900">{isDegraded ? 'Degraded' : 'Nominal'}</p>
                    </div>
                </div>

                <div className="bg-white p-6 rounded-xl border border-stone-200 shadow-sm flex items-center gap-4 group hover:border-stone-400 transition-all">
                    <div className="p-3 bg-blue-50 text-blue-600 rounded-lg">
                        <Activity size={24} />
                    </div>
                    <div>
                        <p className="text-stone-400 text-xs font-medium uppercase tracking-wider">MAPE-K Loop</p>
                        <p className="text-xl font-bold text-stone-900">Active</p>
                    </div>
                </div>

                <div className="bg-white p-6 rounded-xl border border-stone-200 shadow-sm flex items-center gap-4 group hover:border-stone-400 transition-all">
                    <div className="p-3 bg-purple-50 text-purple-600 rounded-lg">
                        <Zap size={24} />
                    </div>
                    <div>
                        <p className="text-stone-400 text-xs font-medium uppercase tracking-wider">Alert Window</p>
                        <p className="text-xl font-bold text-stone-900">{alerts.length} Today</p>
                    </div>
                </div>

                <div className="bg-white p-6 rounded-xl border border-stone-200 shadow-sm flex items-center gap-4">
                    <div className="p-3 bg-stone-50 text-stone-600 rounded-lg">
                        <Clock size={24} />
                    </div>
                    <div>
                        <p className="text-stone-400 text-xs font-medium uppercase tracking-wider">Last Sync</p>
                        <p className="text-xl font-bold text-stone-900">{lastRefresh.toLocaleTimeString()}</p>
                    </div>
                </div>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
                {/* Health Monitoring Panel */}
                <div className="lg:col-span-1 space-y-6">
                   <div className="bg-white p-6 rounded-xl border border-stone-200 shadow-sm h-full">
                        <h3 className="text-stone-900 font-bold mb-6 flex items-center gap-2">
                            <Server size={18} className="text-stone-400" />
                            Cluster Health
                        </h3>
                        <div className="space-y-1">
                            {Object.entries(services).map(([name, metrics]) => (
                                <div key={name} className="flex items-center justify-between p-3 hover:bg-stone-50 rounded-lg transition-colors border-b border-stone-50 last:border-0">
                                    <div className="flex items-center gap-3">
                                        <div className={`w-2 h-2 rounded-full ${metrics.healthy ? 'bg-emerald-500' : 'bg-red-500 animate-pulse'}`} />
                                        <span className="text-stone-700 font-medium capitalize">{name}</span>
                                    </div>
                                    <div className="flex items-center gap-2">
                                        {metrics.error && (
                                            <span className="text-[10px] bg-red-50 text-red-600 px-2 py-0.5 rounded border border-red-100">
                                                FAIL
                                            </span>
                                        )}
                                        {getStatusIcon(metrics.healthy)}
                                    </div>
                                </div>
                            ))}
                        </div>
                   </div>
                </div>

                {/* Autonomic Decision Log */}
                <div className="lg:col-span-2">
                    <div className="bg-white rounded-xl border border-stone-200 shadow-sm overflow-hidden h-full">
                        <div className="p-6 border-b border-stone-100 flex justify-between items-center">
                            <div>
                                <h3 className="text-stone-900 font-bold flex items-center gap-2">
                                    <Shield size={18} className="text-stone-400" />
                                    Autonomic Decision Log
                                </h3>
                                <p className="text-stone-400 text-xs mt-1">Real-time actions taken by the System Overseer</p>
                            </div>
                        </div>
                        
                        <div className="overflow-auto max-h-[500px]">
                            {loading ? (
                                <div className="p-8 text-center text-stone-400 animate-pulse">Loading orchestration history...</div>
                            ) : alerts.length === 0 ? (
                                <div className="p-12 text-center text-stone-400 italic">No autonomic actions logged in this window.</div>
                            ) : (
                                <table className="w-full text-left border-collapse">
                                    <thead className="bg-[#F7F7F5] border-b border-stone-200">
                                        <tr>
                                            <th className="px-6 py-3 text-xs font-semibold text-stone-500 uppercase tracking-wider">Event</th>
                                            <th className="px-6 py-3 text-xs font-semibold text-stone-500 uppercase tracking-wider">Action</th>
                                            <th className="px-6 py-3 text-xs font-semibold text-stone-500 uppercase tracking-wider">Details</th>
                                            <th className="px-6 py-3 text-xs font-semibold text-stone-500 uppercase tracking-wider">Time</th>
                                        </tr>
                                    </thead>
                                    <tbody className="divide-y divide-stone-100">
                                        {alerts.map((alert, idx) => (
                                            <tr key={idx} className="hover:bg-stone-50/50 transition-colors">
                                                <td className="px-6 py-4 whitespace-nowrap">
                                                    <span className={`text-[10px] font-bold px-2 py-1 rounded-full uppercase border ${getAlertLevelClass(alert.level)}`}>
                                                        {alert.level}
                                                    </span>
                                                </td>
                                                <td className="px-6 py-4 whitespace-nowrap">
                                                    <p className="text-stone-900 font-medium text-sm">{alert.type}</p>
                                                    <p className="text-stone-400 text-[10px]">{alert.action}</p>
                                                </td>
                                                <td className="px-6 py-4">
                                                    <p className="text-stone-600 text-xs line-clamp-1">{alert.detail}</p>
                                                    <p className="text-[10px] text-stone-400 mt-0.5 font-mono">{alert.target}</p>
                                                </td>
                                                <td className="px-6 py-4 whitespace-nowrap text-stone-400 text-xs font-mono">
                                                    {new Date(alert.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })}
                                                </td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            )}
                        </div>
                    </div>
                </div>
            </div>
        </div>
    )
}
