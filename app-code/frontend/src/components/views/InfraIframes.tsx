import React, { useMemo, useState } from 'react'
import { AlertTriangle, ExternalLink, MonitorPlay, RefreshCw, Server } from 'lucide-react'
import { fetchInfraStatus } from '../../api/client'
import { ErrorState, LoadingState, ResourceMeta } from '../shared/AsyncState'
import { usePollingResource } from '../../hooks/usePollingResource'

// In production (via Nginx on port 8080): iframes load through the auth bouncer.
// In local dev (no Nginx): open localUrl directly in a separate tab.
const IFRAMES = [
    { id: 'prefect', name: 'Prefect Dashboard', icon: MonitorPlay, url: '/prefect/', localUrl: 'http://localhost:4200' },
    { id: 'ray', name: 'Ray Cluster UI', icon: Server, url: '/ray/', localUrl: 'http://localhost:32382' },
]

export const InfraIframes: React.FC = () => {
    const [activeIframe, setActiveIframe] = useState(IFRAMES[0])
    const {
        data,
        loading,
        refreshing,
        error,
        lastUpdated,
        stale,
        refresh,
    } = usePollingResource(fetchInfraStatus, { pollIntervalMs: 30_000 })
    const statusById = data?.targets ?? {}

    const activeStatus = useMemo(() => statusById[activeIframe.id], [statusById, activeIframe.id])

    return (
        <div className="h-full flex flex-col space-y-4">
            <div className="flex items-center justify-between gap-4 border-b border-stone-200 pb-4">
                <div className="flex gap-4">
                    {IFRAMES.map(f => (
                        <button
                            key={f.id}
                            onClick={() => setActiveIframe(f)}
                            className={`flex items-center gap-2 px-4 py-2 rounded-lg transition-colors ${activeIframe.id === f.id
                                ? 'bg-stone-900 text-white ring-1 ring-stone-400'
                                : 'bg-white border border-stone-200 hover:bg-stone-100 text-stone-600'
                                }`}
                        >
                            <f.icon size={16} />
                            {f.name}
                        </button>
                    ))}
                </div>
                <button
                    type="button"
                    onClick={() => void refresh()}
                    className="inline-flex items-center gap-2 rounded-lg border border-stone-200 bg-white px-3 py-2 text-sm text-stone-600 transition hover:bg-stone-100"
                >
                    <RefreshCw size={14} className={refreshing ? 'animate-spin' : ''} />
                    Refresh Status
                </button>
            </div>

            <div className="flex justify-end">
                <ResourceMeta lastUpdated={lastUpdated} refreshing={refreshing} stale={stale} />
            </div>

            <div className="flex-1 bg-[#F7F7F5] rounded-xl border border-stone-200 overflow-hidden relative shadow-sm">
                {loading ? (
                    <div className="bg-white h-full">
                        <LoadingState label="Checking internal dashboard availability..." />
                    </div>
                ) : error ? (
                    <div className="flex h-full items-center justify-center p-8">
                        <div className="max-w-lg w-full">
                            <ErrorState title="Infrastructure status check failed" detail={error} onRetry={() => void refresh()} />
                        </div>
                    </div>
                ) : activeStatus?.ok === false ? (
                    <div className="flex h-full items-center justify-center p-8">
                        <div className="max-w-xl rounded-xl border border-amber-200 bg-amber-50 p-6 text-amber-900 shadow-sm">
                            <div className="flex items-start gap-3">
                                <AlertTriangle size={18} className="mt-0.5 shrink-0" />
                                <div>
                                    <p className="font-semibold">{activeIframe.name} is not reachable right now</p>
                                    <p className="mt-2 text-sm">
                                        The gateway probe could not reach `{activeStatus.url}`.
                                        {activeStatus.detail ? ` Detail: ${activeStatus.detail}` : ''}
                                    </p>
                                    <div className="mt-4 flex flex-wrap gap-3">
                                        <a
                                            href={activeIframe.localUrl}
                                            target="_blank"
                                            rel="noreferrer"
                                            className="inline-flex items-center gap-2 rounded-lg border border-amber-300 bg-white px-3 py-2 text-sm font-medium text-amber-900 transition hover:bg-amber-100"
                                        >
                                            <ExternalLink size={14} />
                                            Open Local URL
                                        </a>
                                        <button
                                            type="button"
                                            onClick={() => void refresh()}
                                            className="inline-flex items-center gap-2 rounded-lg border border-amber-300 bg-white px-3 py-2 text-sm font-medium text-amber-900 transition hover:bg-amber-100"
                                        >
                                            <RefreshCw size={14} />
                                            Retry
                                        </button>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                ) : (
                    <>
                        <div className="absolute inset-x-0 top-0 z-10 flex justify-end p-3 pointer-events-none">
                            <div className="rounded-full border border-emerald-200 bg-emerald-50 px-3 py-1 text-xs font-medium text-emerald-700 shadow-sm">
                                {activeIframe.name} reachable
                            </div>
                        </div>
                        <iframe
                            key={activeIframe.id}
                            src={activeIframe.url}
                            className="w-full h-full border-none"
                            title={activeIframe.name}
                        />
                    </>
                )}
            </div>
        </div>
    )
}
