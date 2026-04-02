import React, { useEffect, useMemo, useState } from 'react'
import {
    AlertTriangle,
    Bot,
    Braces,
    Info,
    MessageSquare,
    Play,
    Radio,
    RefreshCw,
    Tag,
    Wifi,
    WifiOff,
} from 'lucide-react'
import {
    broadcastAgentEvent,
    chatWithAgent,
    fetchAgents,
    invokeAgent,
} from '../../api/client'
import type { AgentSummary } from '../../api/client'

const POLL_INTERVAL_MS = 15_000

function getInteractionModes(agent: AgentSummary): string[] {
    const modes = new Set<string>()

    for (const spec of agent.capability_specs ?? []) {
        const mode = String(spec.interaction_mode ?? '').trim()
        if (mode) modes.add(mode)
    }

    const metadataModes = agent.metadata?.interaction_modes
    if (Array.isArray(metadataModes)) {
        for (const mode of metadataModes) {
            const text = String(mode).trim()
            if (text) modes.add(text)
        }
    }

    return Array.from(modes)
}

function supportsMode(agent: AgentSummary, mode: string): boolean {
    return getInteractionModes(agent).includes(mode)
}

function buildDefaultInvokePayload(agent: AgentSummary): unknown {
    const primarySpec = agent.capability_specs?.[0]
    const inputType = String(primarySpec?.input_type ?? '').toLowerCase()

    if (inputType === 'text') return 'Test payload from AgentHub'
    if (inputType === 'timeseries') return { values: [100, 103, 106] }
    if (inputType === 'object') return { message: 'Test payload from AgentHub' }

    return {
        sample: true,
        agent: agent.name,
        capability: primarySpec?.id ?? null,
    }
}

function formatMetadataValue(value: unknown): string {
    if (typeof value === 'string') return value
    if (typeof value === 'number' || typeof value === 'boolean') return String(value)
    if (value == null) return 'null'
    try {
        return JSON.stringify(value)
    } catch {
        return String(value)
    }
}

function prettyJson(value: unknown): string {
    if (typeof value === 'string') return value
    try {
        return JSON.stringify(value, null, 2)
    } catch {
        return String(value)
    }
}

function titleCase(value: string): string {
    return value
        .split(/[_\-. ]+/)
        .filter(Boolean)
        .map(part => part.charAt(0).toUpperCase() + part.slice(1))
        .join(' ')
}

export const AgentHub: React.FC = () => {
    const [agents, setAgents] = useState<AgentSummary[]>([])
    const [loading, setLoading] = useState(true)
    const [lastRefresh, setLastRefresh] = useState<Date | null>(null)
    const [error, setError] = useState<string | null>(null)
    const [selectedAgentName, setSelectedAgentName] = useState<string | null>(null)
    const [broadcasting, setBroadcasting] = useState(false)
    const [broadcastResult, setBroadcastResult] = useState<string | null>(null)
    const [chatMessage, setChatMessage] = useState('Hello from AgentHub')
    const [invokePayloadText, setInvokePayloadText] = useState('{\n  "sample": true\n}')
    const [actionPending, setActionPending] = useState(false)
    const [actionLabel, setActionLabel] = useState<string | null>(null)
    const [actionError, setActionError] = useState<string | null>(null)
    const [actionResult, setActionResult] = useState<unknown>(null)

    const aliveAgents = useMemo(() => agents.filter(agent => agent.alive !== false), [agents])
    const deadAgents = useMemo(() => agents.filter(agent => agent.alive === false), [agents])
    const chatCapableCount = useMemo(() => agents.filter(agent => supportsMode(agent, 'chat')).length, [agents])
    const invokeCapableCount = useMemo(() => agents.filter(agent => supportsMode(agent, 'invoke')).length, [agents])
    const selectedAgent = useMemo(
        () => agents.find(agent => agent.name === selectedAgentName) ?? null,
        [agents, selectedAgentName],
    )

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

    useEffect(() => {
        if (agents.length === 0) {
            setSelectedAgentName(null)
            return
        }
        if (!selectedAgentName || !agents.some(agent => agent.name === selectedAgentName)) {
            setSelectedAgentName(agents[0].name)
        }
    }, [agents, selectedAgentName])

    useEffect(() => {
        if (!selectedAgent) return
        setChatMessage('Hello from AgentHub')
        setInvokePayloadText(prettyJson(buildDefaultInvokePayload(selectedAgent)))
        setActionLabel(null)
        setActionError(null)
        setActionResult(null)
    }, [selectedAgentName])

    const handleBroadcastPing = async () => {
        setBroadcasting(true)
        setBroadcastResult(null)
        try {
            const result = await broadcastAgentEvent({
                type: 'ping',
                from: 'agent-hub-dashboard',
                issued_at: new Date().toISOString(),
            })
            setBroadcastResult(`Broadcast delivered to ${result.delivered_to}/${result.total_targets} agents`)
        } catch (err) {
            setBroadcastResult(err instanceof Error ? err.message : 'Broadcast failed')
        } finally {
            setBroadcasting(false)
        }
    }

    const handleChatTest = async () => {
        if (!selectedAgent || !chatMessage.trim()) return
        setActionPending(true)
        setActionLabel('Chat Response')
        setActionError(null)
        setActionResult(null)
        try {
            const result = await chatWithAgent(selectedAgent.name, chatMessage.trim(), `agenthub-${selectedAgent.name}`)
            setActionResult(result)
        } catch (err) {
            setActionError(err instanceof Error ? err.message : 'Chat request failed')
        } finally {
            setActionPending(false)
        }
    }

    const handleInvokeTest = async () => {
        if (!selectedAgent) return

        let payload: unknown
        try {
            payload = JSON.parse(invokePayloadText)
        } catch {
            setActionLabel('Invoke Response')
            setActionError('Payload must be valid JSON. Use quotes if you want to send a raw string.')
            setActionResult(null)
            return
        }

        setActionPending(true)
        setActionLabel('Invoke Response')
        setActionError(null)
        setActionResult(null)
        try {
            const result = await invokeAgent(selectedAgent.name, payload)
            setActionResult(result)
        } catch (err) {
            setActionError(err instanceof Error ? err.message : 'Invoke request failed')
        } finally {
            setActionPending(false)
        }
    }

    return (
        <div className="mx-auto max-w-7xl space-y-6 pb-12">
            <div className="flex flex-wrap items-center justify-between gap-4">
                <div>
                    <p className="text-xs font-semibold uppercase tracking-[0.22em] text-stone-500">Intelligence Plane</p>
                    <h2 className="mt-1 text-2xl font-bold text-stone-900">Agent Fleet</h2>
                    <p className="mt-0.5 text-sm text-stone-500">
                        Discover agents, inspect typed capabilities, and run safe manual tests.
                    </p>
                </div>
                <div className="flex flex-wrap items-center justify-end gap-3">
                    {lastRefresh && (
                        <span className="text-xs text-stone-400">
                            Last synced {lastRefresh.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })}
                        </span>
                    )}
                    <button
                        type="button"
                        onClick={() => void handleBroadcastPing()}
                        disabled={broadcasting}
                        className="flex items-center gap-2 rounded-full border border-stone-200 bg-white px-4 py-2 text-sm font-medium text-stone-700 shadow-sm transition hover:border-stone-300 hover:bg-stone-50 disabled:cursor-not-allowed disabled:opacity-50"
                    >
                        <Radio size={14} className={broadcasting ? 'animate-pulse' : ''} />
                        {broadcasting ? 'Broadcasting...' : 'Broadcast Ping'}
                    </button>
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

            <div className="grid grid-cols-2 gap-4 lg:grid-cols-5">
                {[
                    { label: 'Total Agents', value: agents.length, color: 'text-stone-900' },
                    { label: 'Alive', value: aliveAgents.length, color: 'text-emerald-600' },
                    { label: 'Unreachable', value: deadAgents.length, color: 'text-red-500' },
                    { label: 'Chat Capable', value: chatCapableCount, color: 'text-sky-600' },
                    { label: 'Invoke Capable', value: invokeCapableCount, color: 'text-amber-600' },
                ].map(stat => (
                    <div key={stat.label} className="rounded-2xl border border-stone-200 bg-white px-5 py-4 shadow-sm">
                        <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-stone-400">{stat.label}</p>
                        <p className={`mt-2 text-2xl font-bold ${stat.color}`}>{stat.value}</p>
                    </div>
                ))}
            </div>

            {broadcastResult && (
                <div className="rounded-2xl border border-stone-200 bg-white px-5 py-3 text-sm text-stone-600 shadow-sm">
                    {broadcastResult}
                </div>
            )}

            {error && (
                <div className="flex items-center gap-3 rounded-2xl border border-red-200 bg-red-50 px-5 py-4 text-sm text-red-700">
                    <AlertTriangle size={16} className="shrink-0" />
                    {error}
                </div>
            )}

            <div className="grid gap-6 xl:grid-cols-[minmax(0,1.7fr)_minmax(360px,1fr)]">
                <div className="space-y-4">
                    {loading && agents.length === 0 ? (
                        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
                            {[...Array(4)].map((_, i) => (
                                <div key={i} className="h-48 animate-pulse rounded-3xl border border-stone-100 bg-stone-100" />
                            ))}
                        </div>
                    ) : agents.length === 0 ? (
                        <div className="rounded-3xl border border-dashed border-stone-300 bg-white py-20 text-center text-stone-500">
                            <Bot size={40} className="mx-auto mb-3 text-stone-300" />
                            <p className="font-medium">No agents registered</p>
                            <p className="mt-1 text-sm">Deploy agents to the Ray cluster and they will appear here automatically.</p>
                        </div>
                    ) : (
                        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
                            {agents.map(agent => {
                                const alive = agent.alive !== false
                                const selected = selectedAgentName === agent.name
                                const modes = getInteractionModes(agent)
                                const primarySpec = agent.capability_specs?.[0]

                                return (
                                    <button
                                        key={agent.name}
                                        type="button"
                                        onClick={() => setSelectedAgentName(agent.name)}
                                        className={`flex flex-col rounded-3xl border bg-white p-5 text-left shadow-sm transition hover:shadow-md ${
                                            selected ? 'border-stone-900 ring-1 ring-stone-900/10' : 'border-stone-200'
                                        }`}
                                    >
                                        <div className="flex items-start gap-4">
                                            <div className={`flex h-11 w-11 shrink-0 items-center justify-center rounded-2xl ${alive ? 'bg-emerald-50 text-emerald-700' : 'bg-red-50 text-red-500'}`}>
                                                <Bot size={20} />
                                            </div>
                                            <div className="min-w-0 flex-1">
                                                <div className="flex items-center gap-2">
                                                    <p className="truncate text-sm font-semibold text-stone-900">{agent.name}</p>
                                                    <span className={`rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.18em] ${alive ? 'bg-emerald-50 text-emerald-700' : 'bg-red-50 text-red-600'}`}>
                                                        {alive ? 'Alive' : 'Down'}
                                                    </span>
                                                </div>
                                                <p className={`mt-1 flex items-center gap-1 text-xs font-medium ${alive ? 'text-emerald-600' : 'text-red-500'}`}>
                                                    {alive ? <Wifi size={11} /> : <WifiOff size={11} />}
                                                    {alive ? 'Responding through AgentHub' : 'Not currently reachable'}
                                                </p>
                                            </div>
                                        </div>

                                        <div className="mt-4 space-y-3">
                                            <div>
                                                <p className="text-[10px] font-semibold uppercase tracking-[0.2em] text-stone-400">Interaction</p>
                                                <div className="mt-2 flex flex-wrap gap-1.5">
                                                    {modes.length > 0 ? modes.map(mode => (
                                                        <span
                                                            key={mode}
                                                            className="rounded-full border border-stone-200 bg-stone-50 px-2.5 py-0.5 text-[11px] font-medium text-stone-600"
                                                        >
                                                            {titleCase(mode)}
                                                        </span>
                                                    )) : (
                                                        <span className="rounded-full border border-stone-200 bg-stone-50 px-2.5 py-0.5 text-[11px] font-medium text-stone-500">
                                                            Read Only
                                                        </span>
                                                    )}
                                                </div>
                                            </div>

                                            <div>
                                                <p className="flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-[0.2em] text-stone-400">
                                                    <Tag size={10} /> Capabilities
                                                </p>
                                                <div className="mt-2 flex flex-wrap gap-1.5">
                                                    {(agent.capabilities ?? []).slice(0, 3).map(capability => (
                                                        <span
                                                            key={capability}
                                                            className="rounded-full border border-stone-200 bg-stone-50 px-2.5 py-0.5 text-[11px] font-medium text-stone-600"
                                                        >
                                                            {capability}
                                                        </span>
                                                    ))}
                                                    {(agent.capabilities?.length ?? 0) > 3 && (
                                                        <span className="rounded-full border border-stone-200 bg-stone-50 px-2.5 py-0.5 text-[11px] font-medium text-stone-500">
                                                            +{(agent.capabilities?.length ?? 0) - 3} more
                                                        </span>
                                                    )}
                                                </div>
                                            </div>

                                            {primarySpec && (
                                                <div className="rounded-2xl border border-stone-100 bg-stone-50 px-3 py-3">
                                                    <p className="text-xs font-semibold text-stone-700">{primarySpec.id}</p>
                                                    <p className="mt-1 text-xs text-stone-500">{primarySpec.description}</p>
                                                </div>
                                            )}
                                        </div>
                                    </button>
                                )
                            })}
                        </div>
                    )}
                </div>

                <div className="rounded-3xl border border-stone-200 bg-white p-5 shadow-sm xl:sticky xl:top-6 xl:self-start">
                    {!selectedAgent ? (
                        <div className="py-10 text-center text-stone-500">
                            <Info size={24} className="mx-auto mb-3 text-stone-300" />
                            <p className="font-medium">Select an agent</p>
                            <p className="mt-1 text-sm">Inspect typed capabilities and run a manual interaction test.</p>
                        </div>
                    ) : (
                        <div className="space-y-5">
                            <div className="border-b border-stone-100 pb-4">
                                <div className="flex items-start justify-between gap-3">
                                    <div>
                                        <p className="text-xs font-semibold uppercase tracking-[0.2em] text-stone-400">Agent Detail</p>
                                        <h3 className="mt-1 text-xl font-bold text-stone-900">{selectedAgent.name}</h3>
                                        <p className={`mt-1 flex items-center gap-1 text-sm font-medium ${selectedAgent.alive !== false ? 'text-emerald-600' : 'text-red-500'}`}>
                                            {selectedAgent.alive !== false ? <Wifi size={13} /> : <WifiOff size={13} />}
                                            {selectedAgent.alive !== false ? 'Alive and routable' : 'Registered but currently unreachable'}
                                        </p>
                                    </div>
                                    {selectedAgent.registered_at && (
                                        <span className="rounded-full border border-stone-200 bg-stone-50 px-3 py-1 text-[11px] text-stone-500">
                                            {new Date(selectedAgent.registered_at).toLocaleString()}
                                        </span>
                                    )}
                                </div>

                                <div className="mt-3 flex flex-wrap gap-1.5">
                                    {getInteractionModes(selectedAgent).map(mode => (
                                        <span
                                            key={mode}
                                            className="rounded-full border border-stone-200 bg-stone-50 px-2.5 py-0.5 text-[11px] font-medium text-stone-600"
                                        >
                                            {titleCase(mode)}
                                        </span>
                                    ))}
                                </div>
                            </div>

                            <div>
                                <p className="flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-[0.2em] text-stone-400">
                                    <Tag size={10} /> Capability Specs
                                </p>
                                <div className="mt-3 space-y-3">
                                    {(selectedAgent.capability_specs ?? []).map(spec => (
                                        <div key={spec.id} className="rounded-2xl border border-stone-200 bg-stone-50 px-4 py-3">
                                            <div className="flex items-start justify-between gap-3">
                                                <div>
                                                    <p className="text-sm font-semibold text-stone-900">{spec.id}</p>
                                                    <p className="mt-1 text-xs text-stone-500">{spec.description}</p>
                                                </div>
                                                {spec.interaction_mode && (
                                                    <span className="rounded-full border border-stone-200 bg-white px-2.5 py-0.5 text-[11px] font-medium text-stone-600">
                                                        {titleCase(spec.interaction_mode)}
                                                    </span>
                                                )}
                                            </div>
                                            <div className="mt-3 grid grid-cols-2 gap-3 text-xs text-stone-600">
                                                <div>
                                                    <p className="font-semibold text-stone-500">Input</p>
                                                    <p className="mt-1">{spec.input_type ?? 'n/a'}</p>
                                                </div>
                                                <div>
                                                    <p className="font-semibold text-stone-500">Output</p>
                                                    <p className="mt-1">{spec.output_type ?? 'n/a'}</p>
                                                </div>
                                            </div>
                                            {(spec.tags?.length ?? 0) > 0 && (
                                                <div className="mt-3 flex flex-wrap gap-1.5">
                                                    {spec.tags?.map(tag => (
                                                        <span
                                                            key={tag}
                                                            className="rounded-full border border-stone-200 bg-white px-2.5 py-0.5 text-[11px] font-medium text-stone-600"
                                                        >
                                                            {tag}
                                                        </span>
                                                    ))}
                                                </div>
                                            )}
                                            {(spec.aliases?.length ?? 0) > 0 && (
                                                <p className="mt-3 text-[11px] text-stone-500">
                                                    Aliases: {spec.aliases?.join(', ')}
                                                </p>
                                            )}
                                        </div>
                                    ))}
                                </div>
                            </div>

                            {selectedAgent.metadata && Object.keys(selectedAgent.metadata).length > 0 && (
                                <div>
                                    <p className="flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-[0.2em] text-stone-400">
                                        <Info size={10} /> Metadata
                                    </p>
                                    <div className="mt-3 rounded-2xl border border-stone-200 bg-stone-50 px-4 py-3">
                                        {Object.entries(selectedAgent.metadata).map(([key, value]) => (
                                            <div key={key} className="flex justify-between gap-3 py-1 text-xs">
                                                <span className="font-medium text-stone-500">{key}</span>
                                                <span className="max-w-[60%] break-words text-right font-mono text-stone-700">
                                                    {formatMetadataValue(value)}
                                                </span>
                                            </div>
                                        ))}
                                    </div>
                                </div>
                            )}

                            <div>
                                <p className="text-[10px] font-semibold uppercase tracking-[0.2em] text-stone-400">Manual Test</p>
                                <div className="mt-3 space-y-4">
                                    {supportsMode(selectedAgent, 'chat') && (
                                        <div className="rounded-2xl border border-stone-200 bg-stone-50 px-4 py-4">
                                            <div className="flex items-center gap-2">
                                                <MessageSquare size={14} className="text-sky-600" />
                                                <p className="text-sm font-semibold text-stone-800">Chat Test</p>
                                            </div>
                                            <textarea
                                                value={chatMessage}
                                                onChange={event => setChatMessage(event.target.value)}
                                                rows={3}
                                                className="mt-3 w-full rounded-2xl border border-stone-200 bg-white px-3 py-2 text-sm text-stone-800 outline-none transition focus:border-stone-400"
                                                placeholder="Enter a chat message"
                                            />
                                            <button
                                                type="button"
                                                onClick={() => void handleChatTest()}
                                                disabled={selectedAgent.alive === false || actionPending || !chatMessage.trim()}
                                                className="mt-3 inline-flex items-center gap-2 rounded-full border border-stone-200 bg-white px-4 py-2 text-sm font-medium text-stone-700 transition hover:border-stone-300 hover:bg-stone-100 disabled:cursor-not-allowed disabled:opacity-50"
                                            >
                                                <MessageSquare size={14} />
                                                {actionPending && actionLabel === 'Chat Response' ? 'Sending...' : 'Run Chat'}
                                            </button>
                                        </div>
                                    )}

                                    {supportsMode(selectedAgent, 'invoke') && (
                                        <div className="rounded-2xl border border-stone-200 bg-stone-50 px-4 py-4">
                                            <div className="flex items-center gap-2">
                                                <Braces size={14} className="text-amber-600" />
                                                <p className="text-sm font-semibold text-stone-800">Invoke Test</p>
                                            </div>
                                            <textarea
                                                value={invokePayloadText}
                                                onChange={event => setInvokePayloadText(event.target.value)}
                                                rows={10}
                                                className="mt-3 w-full rounded-2xl border border-stone-200 bg-white px-3 py-2 font-mono text-xs text-stone-800 outline-none transition focus:border-stone-400"
                                                spellCheck={false}
                                            />
                                            <button
                                                type="button"
                                                onClick={() => void handleInvokeTest()}
                                                disabled={selectedAgent.alive === false || actionPending}
                                                className="mt-3 inline-flex items-center gap-2 rounded-full border border-stone-200 bg-white px-4 py-2 text-sm font-medium text-stone-700 transition hover:border-stone-300 hover:bg-stone-100 disabled:cursor-not-allowed disabled:opacity-50"
                                            >
                                                <Play size={14} />
                                                {actionPending && actionLabel === 'Invoke Response' ? 'Running...' : 'Run Invoke'}
                                            </button>
                                        </div>
                                    )}

                                    {!supportsMode(selectedAgent, 'chat') && !supportsMode(selectedAgent, 'invoke') && (
                                        <div className="rounded-2xl border border-dashed border-stone-300 bg-stone-50 px-4 py-6 text-sm text-stone-500">
                                            This agent does not advertise a manual chat or invoke interface.
                                        </div>
                                    )}
                                </div>
                            </div>

                            {Boolean(actionError || actionResult) && (
                                <div>
                                    <p className="text-[10px] font-semibold uppercase tracking-[0.2em] text-stone-400">
                                        {actionLabel ?? 'Result'}
                                    </p>
                                    <div className={`mt-3 rounded-2xl border px-4 py-4 ${
                                        actionError ? 'border-red-200 bg-red-50 text-red-700' : 'border-stone-200 bg-stone-900 text-stone-100'
                                    }`}>
                                        {actionError ? (
                                            <div className="flex items-start gap-2 text-sm">
                                                <AlertTriangle size={16} className="mt-0.5 shrink-0" />
                                                <span>{actionError}</span>
                                            </div>
                                        ) : (
                                            <pre className="overflow-x-auto whitespace-pre-wrap text-xs">
                                                {prettyJson(actionResult)}
                                            </pre>
                                        )}
                                    </div>
                                </div>
                            )}
                        </div>
                    )}
                </div>
            </div>
        </div>
    )
}
