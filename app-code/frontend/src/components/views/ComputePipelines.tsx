import React, { useCallback } from 'react'
import { Activity, AlertTriangle, ExternalLink, RefreshCw } from 'lucide-react'
import { fetchInfraStatus, type InfraStatusResponse } from '../../api/client'
import { LoadingState, ResourceMeta } from '../shared/AsyncState'
import { usePollingResource } from '../../hooks/usePollingResource'

export const ComputePipelines: React.FC = () => {
  const loadPrefectStatus = useCallback(async (): Promise<{
    target: InfraStatusResponse['targets'][string] | null
    error: string | null
  }> => {
    try {
      const status = await fetchInfraStatus()
      return {
        target: status.targets.prefect ?? null,
        error: null,
      }
    } catch (error) {
      return {
        target: null,
        error: error instanceof Error ? error.message : 'Unable to probe Prefect availability',
      }
    }
  }, [])

  const {
    data,
    loading,
    refreshing,
    lastUpdated,
    stale,
    refresh,
  } = usePollingResource(loadPrefectStatus, { pollIntervalMs: 30_000 })

  const prefectStatus = data?.target ?? null
  const reachable = prefectStatus?.ok === true
  const localPrefectUrl = 'http://localhost:4200'
  const unavailableDetail =
    prefectStatus?.detail ??
    data?.error ??
    'The local frontend cannot reach the Prefect UI through the gateway probe right now.'

  return (
    <div className="flex h-full flex-col space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-4 border-b border-stone-200 pb-4">
        <div>
          <h3 className="flex items-center gap-2 text-xl font-bold text-stone-900">
            <Activity className="text-stone-700" />
            Compute & Pipelines
          </h3>
          <p className="mt-1 text-sm text-stone-500">Prefect is only embedded when the gateway probe says it is reachable.</p>
        </div>
        <div className="flex items-center gap-3">
          <ResourceMeta lastUpdated={lastUpdated} refreshing={refreshing} stale={stale} />
          <button
            type="button"
            onClick={() => void refresh()}
            className="inline-flex items-center gap-2 rounded-lg border border-stone-200 bg-white px-3 py-2 text-sm text-stone-600 transition hover:bg-stone-100"
          >
            <RefreshCw size={14} className={refreshing ? 'animate-spin' : ''} />
            Refresh status
          </button>
        </div>
      </div>

      {loading && !data ? (
        <LoadingState label="Checking Prefect availability..." />
      ) : reachable ? (
        <div className="relative min-h-[600px] flex-1 overflow-hidden rounded-lg border border-stone-200 bg-[#F7F7F5] shadow-sm">
          <div className="absolute inset-x-0 top-0 z-10 flex justify-end p-3 pointer-events-none">
            <div className="rounded-full border border-emerald-200 bg-emerald-50 px-3 py-1 text-xs font-medium text-emerald-700 shadow-sm">
              Prefect reachable
            </div>
          </div>
          <iframe
            src="/prefect/"
            className="h-full w-full border-none"
            title="Prefect Dashboard"
          />
        </div>
      ) : (
        <div className="flex min-h-[600px] flex-1 items-center justify-center rounded-lg border border-stone-200 bg-[#F7F7F5] p-8 shadow-sm">
          <div className="w-full max-w-2xl rounded-lg border border-amber-200 bg-amber-50 p-6 text-amber-950 shadow-sm">
            <div className="flex items-start gap-3">
              <AlertTriangle size={18} className="mt-0.5 shrink-0 text-amber-700" />
              <div className="min-w-0 flex-1">
                <p className="font-semibold">Prefect Dashboard is not reachable</p>
                <p className="mt-2 text-sm leading-6 text-amber-900/90">
                  {unavailableDetail}
                </p>
                <div className="mt-4 grid gap-3 sm:grid-cols-2">
                  <div className="rounded-md border border-amber-200 bg-white px-3 py-2">
                    <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-amber-700">Probed URL</p>
                    <p className="mt-1 break-words text-sm text-amber-950">{prefectStatus?.url ?? '/prefect/'}</p>
                  </div>
                  <div className="rounded-md border border-amber-200 bg-white px-3 py-2">
                    <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-amber-700">Local URL</p>
                    <p className="mt-1 break-words text-sm text-amber-950">{localPrefectUrl}</p>
                  </div>
                </div>
                <div className="mt-4 flex flex-wrap gap-3">
                  <a
                    href={localPrefectUrl}
                    target="_blank"
                    rel="noreferrer"
                    className="inline-flex items-center gap-2 rounded-lg border border-amber-300 bg-white px-3 py-2 text-sm font-medium text-amber-950 transition hover:bg-amber-100"
                  >
                    <ExternalLink size={14} />
                    Open local Prefect
                  </a>
                  <button
                    type="button"
                    onClick={() => void refresh()}
                    className="inline-flex items-center gap-2 rounded-lg border border-amber-300 bg-white px-3 py-2 text-sm font-medium text-amber-950 transition hover:bg-amber-100"
                  >
                    <RefreshCw size={14} className={refreshing ? 'animate-spin' : ''} />
                    Retry probe
                  </button>
                </div>
                <p className="mt-4 text-xs leading-5 text-amber-900/80">
                  The local dev frontend does not proxy the Prefect app by itself. When the gateway or Nginx route is available, this page will embed it automatically.
                </p>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
