import React, { useEffect, useMemo, useState } from 'react'
import {
    Bot,
    RefreshCw,
    Wifi,
    WifiOff,
    Zap,
    Tag,
    Info,
    AlertTriangle,
} from 'lucide-react'
import { fetchAgents, sendIntent } from '../../api/client'

type AgentSummary = {
    name: string
    capabilities?: string[]
    metadata?: Record<string, unknown>
    alive?: boolean
}

const POLL_INTERVAL_MS = 15_000

export const AgentHub: React.FC = () => {
    const [agents, setAgents] = useState<AgentSummary[]>([])
    const [loading, setLoading] = useState(true)
    const [lastRefresh, setLastRefresh] = useState<Date | null>(null)
    const [error, setError] = useState<string | null>(null)
    const [notifyingAgent, setNotifyingAgent] = useState<string | null>(null)
    const [notifyResult, setNotifyResult] = useState<Record<string, string>>({})
    const [expandedAgent, setExpandedAgent] = useState<string | null>(null)

    const aliveAgents = useMemo(() => agents.filter(a => a.alive !== false), [agents])
    const deadAgents = useMemo(() => agents.filter(a => a.alive === false), [agents])

    const load = async (silent = false) => {
        if (!silent) setLoading(true)
        setError(null)
        try {
            const data = await fetchAgents()
            setAgents(data)
            setLastRefresh(new Date())
        } catch (err) {
            setError(err instanceof Error ? err.message : 'Failed to fetch agents')
        } finally {
            setLoading(false)
        }
    }

    useEffect(() => {
        void load()
        const interval = setInterval(() => void load(true), POLL_INTERVAL_MS)
        return () => clearInterval(interval)
    }, [])

    const handlePing = async (agentName: string) => {
        setNotifyingAgent(agentName)
        try {
            await sendIntent('agent', 'notify', {
                payload: { type: 'ping', from: 'dashboard' },
            })
            setNotifyResult(prev => ({ ...prev, [agentName]: 'Ping sent ✓' }))
        } catch {
            setNotifyResult(prev => ({ ...prev, [agentName]: 'Failed ✗' }))
        } finally {
            setNotifyingAgent(null)
            setTimeout(() => setNotifyResult(prev => {
                const next = { ...prev }
                delete next[agentName]
                return next
            }), 3000)
        }
    }

    const toggleExpand = (name: string) => {
        setExpandedAgent(prev => prev === name ? null : name)
    }

    return (
        <div className="mx-auto max-w-7xl space-y-6 pb-12">
            {/* ── Header ─────────────────────────────────────────── */}
            <div className="flex flex-wrap items-center justify-between gap-4">
                <div>
                    <p className="text-xs font-semibold uppercase tracking-[0.22em] text-stone-500">Intelligence Plane</p>
                    <h2 className="mt-1 text-2xl font-bold text-stone-900">Agent Fleet</h2>
                    <p className="mt-0.5 text-sm text-stone-500">
                        Real-time status of all Ray Serve agents registered in the Hub.
                    </p>
                </div>
                <div className="flex items-center gap-3">
                    {lastRefresh && (
                        <span className="text-xs text-stone-400">
                            Last synced {lastRefresh.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })}
                        </span>
                    )}
                    <button
                        type="button"
                        onClick={() => void load()}
                        className="flex items-center gap-2 rounded-full border border-stone-200 bg-white px-4 py-2 text-sm font-medium text-stone-700 shadow-sm transition hover:border-stone-300 hover:bg-stone-50"
                    >
                        <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
                        Refresh
                    </button>
                </div>
            </div>

            {/* ── Summary Stats ──────────────────────────────────── */}
            <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
                {[
                    { label: 'Total Agents', value: agents.length, color: 'text-stone-900' },
                    { label: 'Alive', value: aliveAgents.length, color: 'text-emerald-600' },
                    { label: 'Unreachable', value: deadAgents.length, color: 'text-red-500' },
                    { label: 'Poll Interval', value: `${POLL_INTERVAL_MS / 1000}s`, color: 'text-stone-600' },
                ].map(stat => (
                    <div
                        key={stat.label}
                        className="rounded-2xl border border-stone-200 bg-white px-5 py-4 shadow-sm"
                    >
                        <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-stone-400">{stat.label}</p>
                        <p className={`mt-2 text-2xl font-bold ${stat.color}`}>{stat.value}</p>
                    </div>
                ))}
            </div>

            {/* ── Error Banner ───────────────────────────────────── */}
            {error && (
                <div className="flex items-center gap-3 rounded-2xl border border-red-200 bg-red-50 px-5 py-4 text-sm text-red-700">
                    <AlertTriangle size={16} className="shrink-0" />
                    {error}
                </div>
            )}

            {/* ── Agent Grid ─────────────────────────────────────── */}
            {loading && agents.length === 0 ? (
                <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
                    {[...Array(3)].map((_, i) => (
                        <div key={i} className="h-40 animate-pulse rounded-3xl border border-stone-100 bg-stone-100" />
                    ))}
                </div>
            ) : agents.length === 0 ? (
                <div className="rounded-3xl border border-dashed border-stone-300 bg-white py-20 text-center text-stone-500">
                    <Bot size={40} className="mx-auto mb-3 text-stone-300" />
                    <p className="font-medium">No agents registered</p>
                    <p className="mt-1 text-sm">Deploy agents to the Ray cluster and they will appear here automatically.</p>
                </div>
            ) : (
                <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
                    {agents.map(agent => {
                        const alive = agent.alive !== false
                        const expanded = expandedAgent === agent.name
                        const hasMetadata = agent.metadata && Object.keys(agent.metadata).length > 0

                        return (
                            <div
                                key={agent.name}
                                className="flex flex-col overflow-hidden rounded-3xl border border-stone-200 bg-white shadow-sm transition hover:shadow-md"
                            >
                                {/* Card Header */}
                                <div className="flex items-start gap-4 border-b border-stone-100 p-5">
                                    <div className={`flex h-11 w-11 shrink-0 items-center justify-center rounded-2xl ${alive ? 'bg-emerald-50 text-emerald-700' : 'bg-red-50 text-red-500'}`}>
                                        <Bot size={20} />
                                    </div>
                                    <div className="min-w-0 flex-1">
                                        <div className="flex items-center gap-2">
                                            <p className="truncate text-sm font-semibold text-stone-900">{agent.name}</p>
                                            {/* Live pulse dot */}
                                            <span className="relative flex h-2 w-2 shrink-0">
                                                {alive ? (
                                                    <>
                                                        <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-emerald-400 opacity-75" />
                                                        <span className="relative inline-flex h-2 w-2 rounded-full bg-emerald-500" />
                                                    </>
                                                ) : (
                                                    <span className="inline-flex h-2 w-2 rounded-full bg-red-400" />
                                                )}
                                            </span>
                                        </div>
                                        <p className={`mt-0.5 flex items-center gap-1 text-xs font-medium ${alive ? 'text-emerald-600' : 'text-red-500'}`}>
                                            {alive ? <Wifi size={11} /> : <WifiOff size={11} />}
                                            {alive ? 'Alive' : 'Unreachable'}
                                        </p>
                                    </div>
                                </div>

                                {/* Capabilities */}
                                <div className="flex-1 p-5 space-y-4">
                                    {agent.capabilities && agent.capabilities.length > 0 ? (
                                        <div>
                                            <p className="flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-[0.2em] text-stone-400">
                                                <Tag size={10} /> Capabilities
                                            </p>
                                            <div className="mt-2 flex flex-wrap gap-1.5">
                                                {agent.capabilities.map(cap => (
                                                    <span
                                                        key={cap}
                                                        className="rounded-full border border-stone-200 bg-stone-50 px-2.5 py-0.5 text-[11px] font-medium text-stone-600"
                                                    >
                                                        {cap}
                                                    </span>
                                                ))}
                                            </div>
                                        </div>
                                    ) : (
                                        <p className="text-xs text-stone-400 italic">No capabilities declared.</p>
                                    )}

                                    {/* Metadata expandable */}
                                    {hasMetadata && (
                                        <div>
                                            <button
                                                type="button"
                                                onClick={() => toggleExpand(agent.name)}
                                                className="flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-[0.2em] text-stone-400 transition hover:text-stone-700"
                                            >
                                                <Info size={10} />
                                                Metadata
                                                <span className="ml-auto text-[10px]">{expanded ? '▲' : '▼'}</span>
                                            </button>
                                            {expanded && (
                                                <div className="mt-2 rounded-xl border border-stone-100 bg-stone-50 px-3 py-2">
                                                    {Object.entries(agent.metadata!).map(([k, v]) => (
                                                        <div key={k} className="flex justify-between gap-2 py-0.5 text-xs">
                                                            <span className="font-medium text-stone-500">{k}</span>
                                                            <span className="truncate text-stone-700 font-mono text-right">
                                                                {typeof v === 'object' ? JSON.stringify(v) : String(v)}
                                                            </span>
                                                        </div>
                                                    ))}
                                                </div>
                                            )}
                                        </div>
                                    )}
                                </div>

                                {/* Footer Action */}
                                <div className="border-t border-stone-100 px-5 py-3">
                                    <div className="flex items-center justify-between gap-2">
                                        {notifyResult[agent.name] ? (
                                            <span className="text-xs text-stone-500">{notifyResult[agent.name]}</span>
                                        ) : (
                                            <span className="text-[11px] text-stone-400">Agent ID: <span className="font-mono">{agent.name}</span></span>
                                        )}
                                        <button
                                            type="button"
                                            disabled={!alive || notifyingAgent === agent.name}
                                            onClick={() => void handlePing(agent.name)}
                                            className="flex items-center gap-1.5 rounded-full border border-stone-200 bg-white px-3 py-1.5 text-xs font-medium text-stone-700 transition hover:border-stone-300 hover:bg-stone-50 disabled:cursor-not-allowed disabled:opacity-40"
                                        >
                                            <Zap size={11} className={notifyingAgent === agent.name ? 'animate-pulse' : ''} />
                                            {notifyingAgent === agent.name ? 'Pinging...' : 'Ping'}
                                        </button>
                                    </div>
                                </div>
                            </div>
                        )
                    })}
                </div>
            )}
        </div>
    )
}
