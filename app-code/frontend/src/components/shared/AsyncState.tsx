import React from 'react'
import { AlertTriangle, Info, LoaderCircle, RefreshCw } from 'lucide-react'

import { formatLocalTimeOnly } from '../../lib/time'

export const LoadingState: React.FC<{ label: string }> = ({ label }) => (
  <div className="flex items-center justify-center py-12 text-stone-500">
    <div className="flex items-center gap-3 rounded-lg border border-stone-200 bg-white px-4 py-3 shadow-sm">
      <LoaderCircle size={18} className="animate-spin" />
      {label}
    </div>
  </div>
)

export const ErrorState: React.FC<{
  title: string
  detail: string
  onRetry?: () => void
}> = ({ title, detail, onRetry }) => (
  <div className="rounded-2xl border border-red-200 bg-red-50 px-5 py-4 text-red-700">
    <div className="flex items-start gap-3">
      <AlertTriangle size={16} className="mt-0.5 shrink-0" />
      <div className="min-w-0 flex-1">
        <p className="font-semibold">{title}</p>
        <p className="mt-1 text-sm">{detail}</p>
        {onRetry && (
          <button
            type="button"
            onClick={onRetry}
            className="mt-3 inline-flex items-center gap-2 rounded-full border border-red-200 bg-white px-3 py-1.5 text-xs font-medium text-red-700 transition hover:bg-red-100"
          >
            <RefreshCw size={12} />
            Retry
          </button>
        )}
      </div>
    </div>
  </div>
)

export const EmptyState: React.FC<{
  title: string
  detail: string
}> = ({ title, detail }) => (
  <div className="rounded-2xl border border-dashed border-stone-300 bg-white py-12 text-center text-stone-500">
    <Info size={28} className="mx-auto mb-3 text-stone-300" />
    <p className="font-medium">{title}</p>
    <p className="mt-1 text-sm">{detail}</p>
  </div>
)

export const ResourceMeta: React.FC<{
  lastUpdated: Date | null
  refreshing?: boolean
  stale?: boolean
}> = ({ lastUpdated, refreshing = false, stale = false }) => {
  if (!lastUpdated && !refreshing) return null

  return (
    <span className={`text-xs ${stale ? 'text-amber-600' : 'text-stone-400'}`}>
      {refreshing ? 'Refreshing...' : `Last synced ${formatLocalTimeOnly(lastUpdated)}`}
      {stale ? ' - showing last successful data' : ''}
    </span>
  )
}
