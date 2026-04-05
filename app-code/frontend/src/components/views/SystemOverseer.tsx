import React, { useCallback } from 'react'
import { fetchOverseerSnapshots, fetchOverseerAlerts } from '../../api/client'
import type { OverseerServiceMetrics } from '../../api/client'
import { 
    Shield, 
    Activity, 
    AlertTriangle, 
    CheckCircle, 
    Zap, 
    Server
} from 'lucide-react'
import { ErrorState, EmptyState, LoadingState, ResourceMeta } from '../shared/AsyncState'
import { usePollingResource } from '../../hooks/usePollingResource'
import { formatLocalTimeOnly } from '../../lib/time'

function titleCase(value: string): string {
    return value
        .split(/[_\-. ]+/)
        .filter(Boolean)
        .map(part => part.charAt(0).toUpperCase() + part.slice(1))
        .join(' ')
}

export const SystemOverseer: React.FC = () => {
    const loadOverseerData = useCallback(async () => {
        const [snapshots, alerts] = await Promise.all([
            fetchOverseerSnapshots(50),
            fetchOverseerAlerts(20),
        ])
        return { snapshots, alerts }
    }, [])

    const {
        data,
        loading,
        refreshing,
        error,
        lastUpdated,
        stale,
        refresh,
    } = usePollingResource(loadOverseerData, { pollIntervalMs: 15_000 })
    const snapshots = data?.snapshots ?? []
    const alerts = data?.alerts ?? []

    const latestSnap = snapshots[snapshots.length - 1]
    const services = latestSnap?.services || {}
    const isDegraded = Object.values(services).some(s => !s.healthy)
    const agentControl = services['agent_control']
    const deploymentSummary = agentControl?.data?.summary ?? {
        total: 0,
        ready: 0,
        degraded: 0,
        recovering: 0,
        missing: 0,
        offline: 0,
    }

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
                        <p className="text-stone-400 text-xs font-medium uppercase tracking-wider">Recovering</p>
                        <p className="text-xl font-bold text-stone-900">{deploymentSummary.recovering ?? 0}</p>
                    </div>
                </div>

                <div className="bg-white p-6 rounded-xl border border-stone-200 shadow-sm flex items-center gap-4">
                    <div className="p-3 bg-stone-50 text-stone-600 rounded-lg">
                        <Server size={24} />
                    </div>
                    <div>
                        <p className="text-stone-400 text-xs font-medium uppercase tracking-wider">Managed Deployments</p>
                        <p className="text-xl font-bold text-stone-900">{deploymentSummary.total ?? 0}</p>
                        <p className="mt-1 text-[11px] text-stone-400">
                            {lastUpdated ? `Last sync ${formatLocalTimeOnly(lastUpdated)}` : 'Awaiting first sync'}
                        </p>
                    </div>
                </div>
            </div>

            <div className="flex justify-end">
                <ResourceMeta lastUpdated={lastUpdated} refreshing={refreshing} stale={stale} />
            </div>

            {error && snapshots.length > 0 && (
                <ErrorState title="Overseer refresh failed" detail={error} onRetry={() => void refresh()} />
            )}

            <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
                {/* Health Monitoring Panel */}
                <div className="lg:col-span-1 space-y-6">
                   <div className="bg-white p-6 rounded-xl border border-stone-200 shadow-sm h-full">
                        <h3 className="text-stone-900 font-bold mb-6 flex items-center gap-2">
                            <Server size={18} className="text-stone-400" />
                            Cluster Health
                        </h3>
                        <div className="mb-4 grid grid-cols-2 gap-3 rounded-xl border border-stone-200 bg-stone-50 p-3">
                            {[
                                { label: 'Ready', value: deploymentSummary.ready ?? 0, tone: 'text-emerald-600' },
                                { label: 'Degraded', value: deploymentSummary.degraded ?? 0, tone: 'text-amber-600' },
                                { label: 'Missing', value: deploymentSummary.missing ?? 0, tone: 'text-rose-600' },
                                { label: 'Offline', value: deploymentSummary.offline ?? 0, tone: 'text-stone-600' },
                            ].map(item => (
                                <div key={item.label} className="rounded-lg bg-white px-3 py-2">
                                    <p className="text-[10px] font-semibold uppercase tracking-[0.16em] text-stone-400">{item.label}</p>
                                    <p className={`mt-1 text-lg font-bold ${item.tone}`}>{item.value}</p>
                                </div>
                            ))}
                        </div>
                        <div className="space-y-1">
                            {Object.entries(services as Record<string, OverseerServiceMetrics>).map(([name, metrics]) => (
                                <div key={name} className="flex items-center justify-between p-3 hover:bg-stone-50 rounded-lg transition-colors border-b border-stone-50 last:border-0">
                                    <div className="flex items-center gap-3">
                                        <div className={`w-2 h-2 rounded-full ${metrics.healthy ? 'bg-emerald-500' : 'bg-red-500 animate-pulse'}`} />
                                        <div>
                                            <span className="text-stone-700 font-medium capitalize">{name}</span>
                                            {name === 'agent_control' && (
                                                <p className="text-[10px] text-stone-400">
                                                    {metrics.data?.summary?.ready ?? 0} ready, {metrics.data?.summary?.recovering ?? 0} recovering, {metrics.data?.summary?.missing ?? 0} missing
                                                </p>
                                            )}
                                        </div>
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
                                <LoadingState label="Loading orchestration history..." />
                            ) : error && snapshots.length === 0 ? (
                                <ErrorState title="Overseer data unavailable" detail={error} onRetry={() => void refresh()} />
                            ) : alerts.length === 0 ? (
                                <EmptyState title="No autonomic actions logged" detail="No autonomic actions were recorded in the current window." />
                            ) : (
                                <table className="w-full text-left border-collapse">
                                    <thead className="bg-[#F7F7F5] border-b border-stone-200">
                                        <tr>
                                            <th className="px-6 py-3 text-xs font-semibold text-stone-500 uppercase tracking-wider">Event</th>
                                            <th className="px-6 py-3 text-xs font-semibold text-stone-500 uppercase tracking-wider">Action</th>
                                            <th className="px-6 py-3 text-xs font-semibold text-stone-500 uppercase tracking-wider">Target</th>
                                            <th className="px-6 py-3 text-xs font-semibold text-stone-500 uppercase tracking-wider">Result</th>
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
                                                    <p className="text-stone-900 font-medium text-sm">{titleCase(alert.action || alert.type)}</p>
                                                    <p className="text-stone-400 text-[10px]">{alert.type}</p>
                                                </td>
                                                <td className="px-6 py-4 whitespace-nowrap">
                                                    <p className="text-stone-700 text-xs font-medium">{alert.deployment_name || alert.target}</p>
                                                </td>
                                                <td className="px-6 py-4 whitespace-nowrap">
                                                    <span className={`rounded-full px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.14em] border ${
                                                        alert.status === 'failed'
                                                            ? 'border-red-200 bg-red-50 text-red-700'
                                                            : 'border-emerald-200 bg-emerald-50 text-emerald-700'
                                                    }`}>
                                                        {alert.status ?? 'logged'}
                                                    </span>
                                                </td>
                                                <td className="px-6 py-4">
                                                    <p className="text-stone-600 text-xs line-clamp-2">{alert.detail}</p>
                                                    {(alert.result_error || alert.result_detail) && (
                                                        <p className="text-[10px] text-stone-400 mt-0.5 line-clamp-1">
                                                            {alert.result_error || alert.result_detail}
                                                        </p>
                                                    )}
                                                </td>
                                                <td className="px-6 py-4 whitespace-nowrap text-stone-400 text-xs font-mono">
                                                    {formatLocalTimeOnly(alert.timestamp)}
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
