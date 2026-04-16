import React, { useCallback, useMemo, useState } from 'react'
import { Activity, ArrowRight, ChevronDown, ChevronRight, Filter, RefreshCw, Search, Server, ShieldCheck } from 'lucide-react'
import { ViewShell } from '../shared/ViewShell'
import { EmptyState, ErrorState, LoadingState, ResourceMeta } from '../shared/AsyncState'
import { usePollingResource } from '../../hooks/usePollingResource'
import {
  fetchAuditLogs,
  fetchReadiness,
  fetchOverseerAlerts,
  fetchOverseerSnapshots,
  fetchSystemHealth,
  fetchSystemLogs,
  type AuditLogEntry,
  type AuditLogsResponse,
  type ReadinessResponse,
  type OverseerAlert,
  type OverseerSnapshot,
  type SystemHealthResponse,
  type SystemLogEntry,
  type SystemLogsResponse,
} from '../../api/client'
import { formatLocalTimeOnly, formatLocalTimestamp } from '../../lib/time'

type HealthStatus = 'healthy' | 'degraded' | 'unknown' | 'error'
type Tone = 'gateway' | 'agent' | 'data' | 'recovery' | 'warning' | 'neutral'

type HealthComponentSnapshot = {
  healthy?: boolean
  error?: string | null
}

type DeploymentSummary = {
  total: number
  ready: number
  degraded: number
  recovering: number
  missing: number
  offline: number
}

type ObservabilityData = {
  readiness: ReadinessResponse | null
  health: SystemHealthResponse | null
  logs: SystemLogsResponse | null
  auditLogs: AuditLogsResponse | null
  snapshots: OverseerSnapshot[] 
  alerts: OverseerAlert[]
  errors: string[]
}

function normalizeHealthStatus(value?: string): HealthStatus {
  const status = (value ?? '').toLowerCase()
  if (status === 'healthy') return 'healthy'
  if (status === 'degraded') return 'degraded'
  if (status === 'error') return 'error'
  return 'unknown'
}

function toneClass(status: HealthStatus): string {
  if (status === 'healthy') return 'bg-emerald-50 text-emerald-700 border-emerald-200'
  if (status === 'degraded') return 'bg-amber-50 text-amber-700 border-amber-200'
  if (status === 'error') return 'bg-rose-50 text-rose-700 border-rose-200'
  return 'bg-stone-100 text-stone-700 border-stone-200'
}

function levelTone(level?: string): string {
  const value = (level ?? '').toUpperCase()
  if (value === 'ERROR' || value === 'CRITICAL') return 'bg-rose-50 text-rose-700 border-rose-200'
  if (value === 'WARNING') return 'bg-amber-50 text-amber-700 border-amber-200'
  if (value === 'INFO') return 'bg-sky-50 text-sky-700 border-sky-200'
  return 'bg-stone-100 text-stone-700 border-stone-200'
}

function toneLabel(tone: Tone): string {
  if (tone === 'gateway') return 'bg-sky-50 text-sky-700 border-sky-200'
  if (tone === 'agent') return 'bg-emerald-50 text-emerald-700 border-emerald-200'
  if (tone === 'data') return 'bg-stone-100 text-stone-700 border-stone-200'
  if (tone === 'recovery') return 'bg-amber-50 text-amber-700 border-amber-200'
  if (tone === 'warning') return 'bg-rose-50 text-rose-700 border-rose-200'
  return 'bg-stone-100 text-stone-700 border-stone-200'
}

function titleCase(value: string): string {
  return value
    .split(/[_\-. ]+/)
    .filter(Boolean)
    .map(part => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ')
}

function asRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null
  return value as Record<string, unknown>
}

function getMeta(value: unknown, key: string): string | undefined {
  const record = asRecord(value)
  const item = record ? record[key] : undefined
  if (typeof item === 'string' && item.trim()) return item.trim()
  if (typeof item === 'number' && Number.isFinite(item)) return String(item)
  if (typeof item === 'boolean') return item ? 'true' : 'false'
  return undefined
}

function textForSearch(row: SystemLogEntry): string {
  const meta = asRecord(row.metadata)
  return [
    row.time,
    row.component,
    row.level,
    row.message,
    row.trace_id,
    row.agent_name,
    meta ? JSON.stringify(meta) : '',
  ]
    .filter(Boolean)
    .map(value => String(value).toLowerCase())
    .join(' ')
}

function classifyTone(row: SystemLogEntry): Tone {
  const message = (row.message ?? '').toLowerCase()
  const component = (row.component ?? '').toLowerCase()
  if (row.level === 'ERROR' || row.level === 'CRITICAL') return 'warning'
  if (component === 'overseer' || /recover|restore|reconcile|restart/.test(message)) return 'recovery'
  if (component === 'gateway' || getMeta(row.metadata, 'source_protocol')) return 'gateway'
  if (component === 'agent' || row.agent_name) return 'agent'
  if (component === 'pipeline' || /query|load|persist|ingest|table/.test(message)) return 'data'
  if (row.level === 'WARNING') return 'warning'
  return 'neutral'
}

function deriveTimeline(logs: SystemLogEntry[]) {
  return logs.slice(0, 8).map(row => ({
    title: row.component ? `${row.component} activity` : 'System activity',
    detail: row.message ?? 'No message available',
    time: row.time,
    traceId: row.trace_id,
    tone: classifyTone(row),
  }))
}

function deriveAuditTrail(records: AuditLogEntry[]) {
  return records.slice(0, 6).map(record => {
    const protocol = (record.source_protocol ?? 'unknown').toLowerCase()
    return {
      time: record.timestamp,
      protocol,
      domain: record.domain ?? 'system',
      action: record.action ?? 'request',
      status: typeof record.status_code === 'number' ? String(record.status_code) : 'n/a',
      duration: typeof record.duration_ms === 'number' ? `${record.duration_ms.toFixed(1)} ms` : 'n/a',
      detail: record.error_detail ?? 'Audit record persisted successfully.',
      traceId: record.request_id ?? null,
    }
  })
}

function extractRecoverySummary(snapshot: OverseerSnapshot | null): DeploymentSummary {
  const services = snapshot?.services ?? {}
  const entries = Object.values(services)
  const agentControl = asRecord(services['agent_control']?.data)
  const summary = asRecord(agentControl?.summary)
  const healthyCount = entries.filter(item => item.healthy).length
  const degradedCount = entries.filter(item => !item.healthy).length

  return {
    total: typeof summary?.total === 'number' ? summary.total : entries.length,
    ready: typeof summary?.ready === 'number' ? summary.ready : healthyCount,
    degraded: typeof summary?.degraded === 'number' ? summary.degraded : degradedCount,
    recovering: typeof summary?.recovering === 'number' ? summary.recovering : 0,
    missing: typeof summary?.missing === 'number' ? summary.missing : 0,
    offline: typeof summary?.offline === 'number' ? summary.offline : 0,
  }
}

function MetricCard({ label, value, detail, tone }: { label: string; value: string; detail: string; tone: string }) {
  return (
    <div className="rounded-lg border border-stone-200 bg-white p-4 shadow-sm">
      <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">{label}</p>
      <p className="mt-2 text-2xl font-bold text-stone-900">{value}</p>
      <p className={`mt-3 inline-flex rounded-md border px-3 py-1 text-sm font-medium ${tone}`}>{detail}</p>
    </div>
  )
}

function Pill({ tone, children }: { tone: string; children: React.ReactNode }) {
  return <span className={`rounded-md border px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.16em] ${tone}`}>{children}</span>
}

export const Observability: React.FC = () => {
  const [component, setComponent] = useState('')
  const [level, setLevel] = useState('')
  const [since, setSince] = useState('1h')
  const [agentName, setAgentName] = useState('')
  const [traceId, setTraceId] = useState('')
  const [auditRequestId, setAuditRequestId] = useState('')
  const [searchTerm, setSearchTerm] = useState('')
  const [expandedKey, setExpandedKey] = useState<string | null>(null)

  const loadObservability = useCallback(async (): Promise<ObservabilityData> => {
    const errors: string[] = []
    const [readinessRes, healthRes, logsRes, auditRes, snapshotsRes, alertsRes] = await Promise.allSettled([
      fetchReadiness(),
      fetchSystemHealth(),
      fetchSystemLogs({ component: component || undefined, level: level || undefined, since, agent_name: agentName || undefined, trace_id: traceId || undefined, limit: 100 }),
      fetchAuditLogs({ since, limit: 20, request_id: auditRequestId || undefined }),
      fetchOverseerSnapshots(8),
      fetchOverseerAlerts(8),
    ])

    const readiness = readinessRes.status === 'fulfilled' ? readinessRes.value : null
    const health = healthRes.status === 'fulfilled' ? healthRes.value : null
    const logs = logsRes.status === 'fulfilled' ? logsRes.value : null
    const auditLogs = auditRes.status === 'fulfilled' ? auditRes.value : null
    const snapshots = snapshotsRes.status === 'fulfilled' ? snapshotsRes.value : []
    const alerts = alertsRes.status === 'fulfilled' ? alertsRes.value : []

    if (readinessRes.status === 'rejected') errors.push(`Readiness: ${readinessRes.reason instanceof Error ? readinessRes.reason.message : 'request failed'}`)
    if (healthRes.status === 'rejected') errors.push(`System health: ${healthRes.reason instanceof Error ? healthRes.reason.message : 'request failed'}`)
    if (logsRes.status === 'rejected') errors.push(`System logs: ${logsRes.reason instanceof Error ? logsRes.reason.message : 'request failed'}`)
    if (auditRes.status === 'rejected') errors.push(`Audit logs: ${auditRes.reason instanceof Error ? auditRes.reason.message : 'request failed'}`)
    if (snapshotsRes.status === 'rejected') errors.push(`Overseer snapshots: ${snapshotsRes.reason instanceof Error ? snapshotsRes.reason.message : 'request failed'}`)
    if (alertsRes.status === 'rejected') errors.push(`Overseer alerts: ${alertsRes.reason instanceof Error ? alertsRes.reason.message : 'request failed'}`)

    return { readiness, health, logs, auditLogs, snapshots, alerts, errors }
  }, [agentName, auditRequestId, component, level, since, traceId])

  const { data, loading, refreshing, error, lastUpdated, stale, refresh } = usePollingResource(loadObservability, { pollIntervalMs: 20_000 })
  const rawLogs = useMemo(() => data?.logs?.logs ?? [], [data?.logs])
  const rawAuditLogs = useMemo(() => data?.auditLogs?.audit_logs ?? [], [data?.auditLogs])
  const logs = useMemo(() => {
    const term = searchTerm.trim().toLowerCase()
    if (!term) return rawLogs
    return rawLogs.filter(row => textForSearch(row).includes(term))
  }, [rawLogs, searchTerm])

  const timeline = useMemo(() => deriveTimeline(logs), [logs])
  const auditTrail = useMemo(() => deriveAuditTrail(rawAuditLogs), [rawAuditLogs])
  const healthEntries = useMemo(() => Object.entries(data?.health?.components ?? {}), [data?.health])
  const readinessChecks = Object.entries(data?.readiness?.checks ?? {})
  const readyChecks = readinessChecks.map(([, check]) => check)
  const readyCount = readyChecks.filter(check => check.ready || check.configured).length
  const healthyCount = healthEntries.filter(([, metrics]) => Boolean((metrics as HealthComponentSnapshot | undefined)?.healthy)).length
  const overall = normalizeHealthStatus(data?.health?.status)
  const visibleCount = logs.length
  const fetchedCount = data?.logs?.count ?? rawLogs.length
  const auditCount = data?.auditLogs?.count ?? rawAuditLogs.length
  const auditReturned = data?.auditLogs?.returned_count ?? rawAuditLogs.length
  const partial = (data?.errors.length ?? 0) > 0
  const latestSnapshot = data?.snapshots?.length ? data.snapshots[data.snapshots.length - 1] : null
  const recoverySummary = extractRecoverySummary(latestSnapshot)
  const latestAlert = data?.alerts?.[0] ?? null
  const snapshotServices = Object.entries(latestSnapshot?.services ?? {})
  const recoveryHealthyCount = snapshotServices.filter(([, metrics]) => Boolean((metrics as HealthComponentSnapshot | undefined)?.healthy)).length

  const applyFilters = async () => {
    setExpandedKey(null)
    await refresh()
  }

  return (
    <ViewShell
      eyebrow="System"
      title="Observability"
      description="Health, logs, gateway activity, and overseer recovery state are visible from the same monitoring surface."
      actions={(
        <>
          <ResourceMeta lastUpdated={lastUpdated} refreshing={refreshing} stale={stale} />
          <button type="button" onClick={() => void refresh()} className="inline-flex items-center gap-2 rounded-md border border-stone-200 bg-white px-3 py-2 text-sm text-stone-700 transition hover:bg-stone-50">
            <RefreshCw size={14} className={refreshing ? 'animate-spin' : ''} />
            Refresh
          </button>
        </>
      )}
    >
      {loading && !data ? (
        <LoadingState label="Loading observability data..." />
      ) : error && !data ? (
        <ErrorState title="Observability unavailable" detail={error} onRetry={() => void refresh()} />
      ) : (
        <div className="space-y-6">
          {partial ? (
            <div className="rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-900">
              <p className="font-semibold">Partial data loaded</p>
              <p className="mt-1">One or more observability sources did not respond, but the rest of the page is still usable.</p>
            </div>
          ) : null}

          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <MetricCard label="Platform readiness" value={data?.readiness ? (data.readiness.ready ? 'Ready' : 'Not ready') : 'Unknown'} detail={data?.readiness ? `${readyCount} checks passing` : 'Waiting for readiness'} tone={data?.readiness?.ready ? toneClass('healthy') : toneClass('unknown')} />
            <MetricCard label="System health" value={data?.health?.status ?? 'Unknown'} detail={data?.health?.source ?? 'Waiting for health summary'} tone={toneClass(overall)} />
            <MetricCard label="Healthy components" value={`${healthyCount}/${healthEntries.length || 0}`} detail={healthEntries.length ? 'Component view from overseer snapshot' : 'No components reported yet'} tone={healthyCount === healthEntries.length && healthEntries.length > 0 ? toneClass('healthy') : toneClass('unknown')} />
            <MetricCard label="Visible logs" value={String(visibleCount)} detail={`Fetched ${fetchedCount} rows${searchTerm.trim() ? ' before search filter' : ''}`} tone="bg-sky-50 text-sky-700 border-sky-200" />
          </div>

          <section className="rounded-lg border border-stone-200 bg-white p-5 shadow-sm">
            <div className="flex items-center gap-2">
              <Activity size={16} className="text-stone-500" />
              <p className="text-sm font-semibold text-stone-900">Readiness Checks</p>
            </div>
            <p className="mt-2 text-sm text-stone-500">These checks summarize the gateway and adjacent services before the rest of the monitoring surface is inspected.</p>
            <div className="mt-4 grid gap-3 md:grid-cols-2 xl:grid-cols-3">
              {readinessChecks.length ? readinessChecks.map(([name, check]) => {
                const statusTone = check.ready ? toneClass('healthy') : check.configured ? toneClass('degraded') : toneClass('unknown')
                const statusLabel = check.ready ? 'ready' : check.configured ? 'configured' : 'pending'
                return (
                  <div key={name} className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0">
                        <p className="text-sm font-medium text-stone-900">{titleCase(name)}</p>
                        <p className="mt-1 text-xs text-stone-500">{check.detail ?? 'No additional detail'}</p>
                      </div>
                      <Pill tone={statusTone}>{statusLabel}</Pill>
                    </div>
                  </div>
                )
              }) : (
                <div className="md:col-span-2 xl:col-span-3">
                  <EmptyState title="No readiness checks yet" detail="The gateway readiness summary will appear here once the backend exposes it." />
                </div>
              )}
            </div>
          </section>

          <div className="grid gap-6 xl:grid-cols-[minmax(0,0.95fr)_minmax(0,1.3fr)]">
            <div className="space-y-6">
              <section className="rounded-lg border border-stone-200 bg-white p-5 shadow-sm">
                <div className="flex items-center gap-2">
                  <Server size={16} className="text-stone-500" />
                  <p className="text-sm font-semibold text-stone-900">Health Overview</p>
                </div>
                <div className="mt-4 rounded-lg border border-stone-200 bg-stone-50 p-4">
                  <div className="flex flex-wrap items-start justify-between gap-3">
                    <div className="min-w-0">
                      <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Overall status</p>
                      <p className="mt-2 text-2xl font-bold text-stone-900">{data?.health?.status ?? 'Unknown'}</p>
                      <p className="mt-2 max-w-3xl text-sm text-stone-500">{data?.health?.message ?? 'The overseer snapshot is the current source of truth for service health.'}</p>
                    </div>
                    <div className="flex flex-col items-start gap-2">
                      <Pill tone={toneClass(overall)}>{overall}</Pill>
                      <span className="rounded-md border border-stone-200 bg-white px-3 py-1 text-xs text-stone-500">{data?.health?.timestamp ? `Snapshot ${formatLocalTimestamp(data.health.timestamp)}` : 'No timestamp yet'}</span>
                    </div>
                  </div>
                  <div className="mt-4 grid gap-2 sm:grid-cols-2">
                    <div className="rounded-md border border-stone-200 bg-white px-3 py-2">
                      <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-stone-400">Source</p>
                      <p className="mt-1 text-sm text-stone-700">{data?.health?.source ?? 'n/a'}</p>
                    </div>
                    <div className="rounded-md border border-stone-200 bg-white px-3 py-2">
                      <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-stone-400">Components</p>
                      <p className="mt-1 text-sm text-stone-700">{healthEntries.length ? `${healthyCount} healthy of ${healthEntries.length}` : 'n/a'}</p>
                    </div>
                  </div>
                </div>

                <div className="mt-4 grid gap-3 md:grid-cols-2">
                  {healthEntries.length ? healthEntries.map(([name, metrics]) => {
                    const status: HealthStatus = (metrics as HealthComponentSnapshot | undefined)?.healthy ? 'healthy' : (metrics as HealthComponentSnapshot | undefined)?.error ? 'error' : 'degraded'
                    return (
                      <div key={name} className="rounded-lg border border-stone-200 bg-stone-50 p-3">
                        <div className="flex items-start justify-between gap-3">
                          <div className="min-w-0">
                            <p className="text-sm font-medium text-stone-900">{name}</p>
                            <p className="mt-1 text-xs text-stone-500">{(metrics as HealthComponentSnapshot | undefined)?.error ?? ((metrics as HealthComponentSnapshot | undefined)?.healthy ? 'Healthy' : 'Attention needed')}</p>
                          </div>
                          <Pill tone={toneClass(status)}>{status}</Pill>
                        </div>
                      </div>
                    )
                  }) : (
                    <div className="md:col-span-2">
                      <EmptyState title="No health snapshot yet" detail="The overseer snapshot will appear here once the gateway has cached a response." />
                    </div>
                  )}
                </div>
              </section>

              <section className="rounded-lg border border-stone-200 bg-white p-5 shadow-sm">
                <div className="flex items-center gap-2">
                  <ShieldCheck size={16} className="text-stone-500" />
                  <p className="text-sm font-semibold text-stone-900">Overseer & Recovery</p>
                </div>
                <div className="mt-4 grid gap-4 xl:grid-cols-[minmax(0,1fr)_minmax(0,0.95fr)]">
                  <div className="space-y-4">
                    <div className="grid gap-4 md:grid-cols-2">
                      <MetricCard
                        label="Latest snapshot"
                        value={latestSnapshot ? formatLocalTimestamp(latestSnapshot.timestamp) : 'No snapshot'}
                        detail={latestSnapshot ? `${recoverySummary.total} services tracked` : 'Waiting for overseer cache'}
                        tone={latestSnapshot ? toneClass('healthy') : toneClass('unknown')}
                      />
                      <MetricCard
                        label="Alerts"
                        value={String(data?.alerts.length ?? 0)}
                        detail={latestAlert ? `${latestAlert.level.toUpperCase()} - ${latestAlert.action}` : 'No recent recovery alerts'}
                        tone={latestAlert ? toneClass('degraded') : toneClass('unknown')}
                      />
                      <MetricCard
                        label="Recovering"
                        value={String(recoverySummary.recovering)}
                        detail="Active recovery actions"
                        tone={recoverySummary.recovering > 0 ? toneClass('degraded') : toneClass('healthy')}
                      />
                      <MetricCard
                        label="Missing / Offline"
                        value={`${recoverySummary.missing}/${recoverySummary.offline}`}
                        detail="Control-plane gaps"
                        tone={recoverySummary.missing + recoverySummary.offline > 0 ? toneClass('degraded') : toneClass('healthy')}
                      />
                    </div>

                    <div className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                      <div className="flex flex-wrap items-start justify-between gap-3">
                        <div className="min-w-0">
                          <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Current recovery state</p>
                          <p className="mt-1 text-sm text-stone-600">
                            {latestSnapshot ? 'The latest overseer snapshot is folded into the monitoring view.' : 'No overseer snapshot has been cached yet.'}
                          </p>
                        </div>
                        <Pill tone={toneLabel(latestSnapshot ? 'recovery' : 'neutral')}>
                          {latestSnapshot ? 'active' : 'pending'}
                        </Pill>
                      </div>
                      <div className="mt-4 grid gap-2 sm:grid-cols-2">
                        <div className="rounded-md border border-stone-200 bg-white px-3 py-2">
                          <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-stone-400">Ready</p>
                          <p className="mt-1 text-sm text-stone-700">{recoverySummary.ready}</p>
                        </div>
                        <div className="rounded-md border border-stone-200 bg-white px-3 py-2">
                          <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-stone-400">Degraded</p>
                          <p className="mt-1 text-sm text-stone-700">{recoverySummary.degraded}</p>
                        </div>
                      </div>
                    </div>
                  </div>

                  <div className="space-y-4">
                    <div className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                      <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Latest alert</p>
                      {latestAlert ? (
                        <div className="mt-3 space-y-3">
                          <div className="flex flex-wrap items-center gap-2">
                            <Pill tone={toneLabel(latestAlert.level === 'error' || latestAlert.level === 'critical' ? 'warning' : 'recovery')}>
                              {latestAlert.level}
                            </Pill>
                            <p className="text-sm font-semibold text-stone-900">{latestAlert.action}</p>
                          </div>
                          <p className="text-sm text-stone-600">{latestAlert.detail}</p>
                          <div className="grid gap-2 sm:grid-cols-2">
                            <div className="rounded-md border border-stone-200 bg-white px-3 py-2">
                              <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-stone-400">Target</p>
                              <p className="mt-1 text-sm text-stone-700">{latestAlert.target}</p>
                            </div>
                            <div className="rounded-md border border-stone-200 bg-white px-3 py-2">
                              <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-stone-400">Timestamp</p>
                              <p className="mt-1 text-sm text-stone-700">{formatLocalTimestamp(latestAlert.timestamp)}</p>
                            </div>
                          </div>
                        </div>
                      ) : (
                        <EmptyState title="No recovery alerts yet" detail="The overseer section will populate once the control loop reports activity." />
                      )}
                    </div>

                    <div className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                      <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Service view</p>
                      <p className="mt-1 text-sm text-stone-600">{snapshotServices.length ? `${recoveryHealthyCount} healthy services in the latest snapshot` : 'No services surfaced yet'}</p>
                      <div className="mt-3 grid gap-2">
                        {snapshotServices.length ? snapshotServices.map(([name, metrics]) => (
                          <div key={name} className="flex flex-wrap items-start justify-between gap-3 rounded-md border border-stone-200 bg-white px-3 py-2">
                            <div className="min-w-0">
                              <p className="text-sm font-medium text-stone-900">{name}</p>
                              <p className="mt-1 text-xs text-stone-500">{(metrics as HealthComponentSnapshot | undefined)?.error ?? ((metrics as HealthComponentSnapshot | undefined)?.healthy ? 'Healthy' : 'Attention needed')}</p>
                            </div>
                            <Pill tone={toneClass((metrics as HealthComponentSnapshot | undefined)?.healthy ? 'healthy' : (metrics as HealthComponentSnapshot | undefined)?.error ? 'error' : 'degraded')}>
                              {(metrics as HealthComponentSnapshot | undefined)?.healthy ? 'healthy' : (metrics as HealthComponentSnapshot | undefined)?.error ? 'error' : 'degraded'}
                            </Pill>
                          </div>
                        )) : (
                          <EmptyState title="No service snapshot yet" detail="The latest overseer service list will appear here when available." />
                        )}
                      </div>
                    </div>
                  </div>
                </div>
              </section>

              <section className="rounded-lg border border-stone-200 bg-white p-5 shadow-sm">
                <div className="flex items-center gap-2">
                  <ShieldCheck size={16} className="text-stone-500" />
                  <p className="text-sm font-semibold text-stone-900">Gateway Audit Trail</p>
                </div>
                <p className="mt-2 text-sm text-stone-500">Recent persisted audit records from the gateway. Use request ID to narrow the trace when needed.</p>
                <div className="mt-4 grid gap-3 sm:grid-cols-3">
                  <MetricCard
                    label="Matched"
                    value={String(auditCount)}
                    detail="Records matching the current audit query"
                    tone={auditCount > 0 ? toneClass('healthy') : toneClass('unknown')}
                  />
                  <MetricCard
                    label="Returned"
                    value={String(auditReturned)}
                    detail="Rows shown on screen"
                    tone={auditReturned > 0 ? toneClass('healthy') : toneClass('unknown')}
                  />
                  <MetricCard
                    label="Request ID"
                    value={auditRequestId.trim() || 'Any'}
                    detail="Audit trace focus"
                    tone={toneLabel(auditRequestId.trim() ? 'gateway' : 'neutral')}
                  />
                </div>
                <div className="mt-4 grid gap-3 sm:grid-cols-2">
                  <label className="grid gap-1 text-sm">
                    <span className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Request ID</span>
                    <input
                      value={auditRequestId}
                      onChange={event => setAuditRequestId(event.target.value)}
                      placeholder="Optional"
                      className="rounded-lg border border-stone-200 bg-white px-3 py-2 text-sm text-stone-800 outline-none focus:border-stone-400"
                    />
                  </label>
                  <div className="flex items-end">
                    <button type="button" onClick={() => void applyFilters()} className="inline-flex items-center justify-center gap-2 rounded-md border border-stone-200 bg-stone-900 px-4 py-2 text-sm font-medium text-white transition hover:bg-stone-800">
                      <ArrowRight size={14} />
                      Refresh audit logs
                    </button>
                  </div>
                </div>
                <div className="mt-4 space-y-3">
                  {auditTrail.length ? auditTrail.map((event, index) => (
                    <div key={`${event.time ?? 'audit'}-${index}`} className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                      <div className="flex flex-wrap items-start justify-between gap-3">
                        <div className="min-w-0">
                          <p className="text-sm font-semibold text-stone-900">{event.domain} / {event.action}</p>
                          <p className="mt-1 text-xs text-stone-500">{event.detail}</p>
                        </div>
                        <div className="flex flex-wrap items-center gap-2">
                          <Pill tone={toneLabel(event.protocol === 'mcp' || event.protocol === 'rest' ? 'gateway' : 'neutral')}>
                            {event.protocol.toUpperCase()}
                          </Pill>
                          <span className="rounded-md border border-stone-200 bg-white px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.16em] text-stone-500">{event.status}</span>
                        </div>
                      </div>
                      <div className="mt-3 grid gap-2 sm:grid-cols-2">
                        <div className="rounded-md border border-stone-200 bg-white px-3 py-2">
                          <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-stone-400">Duration</p>
                          <p className="mt-1 text-sm text-stone-700">{event.duration}</p>
                        </div>
                        <div className="rounded-md border border-stone-200 bg-white px-3 py-2">
                          <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-stone-400">Request ID</p>
                          <p className="mt-1 break-words text-sm text-stone-700">{event.traceId ?? 'n/a'}</p>
                        </div>
                      </div>
                      <p className="mt-3 text-xs text-stone-400">{formatLocalTimestamp(event.time ?? null)}</p>
                    </div>
                  )) : (
                    <EmptyState title="No audit records matched" detail="Try widening the time window or clearing the request ID filter." />
                  )}
                </div>
              </section>
            </div>

            <div className="space-y-6">
              <section className="rounded-lg border border-stone-200 bg-white p-5 shadow-sm">
                <div className="flex items-center gap-2">
                  <Filter size={16} className="text-stone-500" />
                  <p className="text-sm font-semibold text-stone-900">Log Explorer</p>
                </div>
                <div className="mt-4 grid gap-3">
                  <label className="grid gap-1 text-sm">
                    <span className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Search</span>
                    <div className="flex items-center gap-2 rounded-lg border border-stone-200 bg-white px-3 py-2">
                      <Search size={14} className="shrink-0 text-stone-400" />
                      <input value={searchTerm} onChange={event => setSearchTerm(event.target.value)} placeholder="Search message, component, trace, or metadata" className="w-full bg-transparent text-sm text-stone-800 outline-none placeholder:text-stone-400" />
                    </div>
                  </label>
                  <label className="grid gap-1 text-sm">
                    <span className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Component</span>
                    <select value={component} onChange={event => setComponent(event.target.value)} className="rounded-lg border border-stone-200 bg-white px-3 py-2 text-sm text-stone-800 outline-none focus:border-stone-400">
                      {['', 'gateway', 'overseer', 'agent', 'hub', 'context', 'pipeline'].map(option => <option key={option || 'all'} value={option}>{option || 'All components'}</option>)}
                    </select>
                  </label>
                  <div className="grid gap-3 sm:grid-cols-2">
                    <label className="grid gap-1 text-sm">
                      <span className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Level</span>
                      <select value={level} onChange={event => setLevel(event.target.value)} className="rounded-lg border border-stone-200 bg-white px-3 py-2 text-sm text-stone-800 outline-none focus:border-stone-400">
                        {['', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'].map(option => <option key={option || 'all'} value={option}>{option || 'All levels'}</option>)}
                      </select>
                    </label>
                    <label className="grid gap-1 text-sm">
                      <span className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Since</span>
                      <select value={since} onChange={event => setSince(event.target.value)} className="rounded-lg border border-stone-200 bg-white px-3 py-2 text-sm text-stone-800 outline-none focus:border-stone-400">
                        {['15m', '1h', '24h'].map(option => <option key={option} value={option}>{option}</option>)}
                      </select>
                    </label>
                  </div>
                  <div className="grid gap-3 sm:grid-cols-2">
                    <label className="grid gap-1 text-sm">
                      <span className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Agent name</span>
                      <input value={agentName} onChange={event => setAgentName(event.target.value)} className="rounded-lg border border-stone-200 bg-white px-3 py-2 text-sm text-stone-800 outline-none focus:border-stone-400" placeholder="Optional" />
                    </label>
                    <label className="grid gap-1 text-sm">
                      <span className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Trace ID</span>
                      <input value={traceId} onChange={event => setTraceId(event.target.value)} className="rounded-lg border border-stone-200 bg-white px-3 py-2 text-sm text-stone-800 outline-none focus:border-stone-400" placeholder="Optional" />
                    </label>
                  </div>
                  <button type="button" onClick={() => void applyFilters()} className="inline-flex items-center justify-center gap-2 rounded-md border border-stone-200 bg-stone-900 px-4 py-2 text-sm font-medium text-white transition hover:bg-stone-800">
                    <ArrowRight size={14} />
                    Apply filters
                  </button>
                </div>
              </section>

              <section className="rounded-lg border border-stone-200 bg-white p-5 shadow-sm">
                <div className="flex flex-wrap items-end justify-between gap-3">
                  <div>
                    <p className="text-sm font-semibold text-stone-900">Recent Logs</p>
                    <p className="mt-1 text-xs text-stone-500">{searchTerm.trim() ? `${visibleCount} of ${fetchedCount} rows match the current search` : `Showing ${visibleCount} rows from the latest ${fetchedCount} fetched records`}</p>
                  </div>
                  <div className="rounded-md border border-stone-200 bg-stone-50 px-3 py-1.5 text-xs text-stone-500">
                    Window {since}{component ? ` - ${component}` : ''}{level ? ` - ${level}` : ''}
                  </div>
                </div>

                {logs.length ? (
                  <div className="mt-4 space-y-3">
                    {logs.map((row, index) => {
                      const key = `${row.time ?? 'log'}-${index}-${row.trace_id ?? row.component ?? 'row'}`
                      const open = expandedKey === key
                      return (
                        <div key={key} className="overflow-hidden rounded-lg border border-stone-200">
                      <button
                        type="button"
                        aria-expanded={open}
                        aria-controls={`log-detail-${key}`}
                        onClick={() => setExpandedKey(open ? null : key)}
                        className="flex w-full items-start justify-between gap-3 bg-white px-4 py-3 text-left transition hover:bg-stone-50"
                      >
                            <div className="min-w-0 flex-1">
                              <div className="flex flex-wrap items-center gap-2">
                                <Pill tone={levelTone(row.level)}>{row.level ?? 'n/a'}</Pill>
                                <span className="text-xs font-semibold uppercase tracking-[0.16em] text-stone-400">{row.component ?? 'n/a'}</span>
                                {row.agent_name ? <span className="rounded-md border border-stone-200 bg-stone-50 px-2 py-1 text-[11px] text-stone-500">{row.agent_name}</span> : null}
                              </div>
                              <p className="mt-2 line-clamp-2 text-sm text-stone-700">{row.message ?? 'No message available'}</p>
                              <p className="mt-2 text-xs text-stone-400">{formatLocalTimeOnly(row.time ?? null)}{row.trace_id ? ` - trace ${row.trace_id}` : ''}</p>
                            </div>
                            <span className="mt-0.5 shrink-0 text-stone-400">{open ? <ChevronDown size={16} /> : <ChevronRight size={16} />}</span>
                          </button>
                          {open ? (
                            <div id={`log-detail-${key}`} className="border-t border-stone-200 bg-stone-50 p-4">
                              <div className="grid gap-3 md:grid-cols-2">
                                <div className="rounded-md border border-stone-200 bg-white px-3 py-2">
                                  <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-stone-400">Message</p>
                                  <p className="mt-1 break-words text-sm text-stone-700">{row.message ?? 'n/a'}</p>
                                </div>
                                <div className="rounded-md border border-stone-200 bg-white px-3 py-2">
                                  <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-stone-400">Time</p>
                                  <p className="mt-1 text-sm text-stone-700">{formatLocalTimestamp(row.time ?? null)}</p>
                                </div>
                                <div className="rounded-md border border-stone-200 bg-white px-3 py-2">
                                  <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-stone-400">Trace</p>
                                  <p className="mt-1 break-words text-sm text-stone-700">{row.trace_id ?? 'n/a'}</p>
                                </div>
                                <div className="rounded-md border border-stone-200 bg-white px-3 py-2">
                                  <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-stone-400">Metadata</p>
                                  <p className="mt-1 break-words text-sm text-stone-700">{asRecord(row.metadata) ? JSON.stringify(row.metadata) : 'n/a'}</p>
                                </div>
                              </div>
                            </div>
                          ) : null}
                        </div>
                      )
                    })}
                  </div>
                ) : (
                  <div className="mt-4">
                    <EmptyState title="No log rows match the current filter" detail="Adjust the component, level, search, or time window to widen the query." />
                  </div>
                )}
              </section>
            </div>
          </div>

          <section className="rounded-lg border border-stone-200 bg-white p-5 shadow-sm">
            <div className="flex items-center gap-2">
              <Activity size={16} className="text-stone-500" />
              <p className="text-sm font-semibold text-stone-900">Event Timeline</p>
            </div>
            <p className="mt-2 text-sm text-stone-500">This short narrative is built from the latest filtered logs so the view has a quick story, not just a table.</p>
            <div className="mt-4 space-y-3">
              {timeline.length ? timeline.map((event, index) => (
                <div key={`${event.time ?? 'timeline'}-${index}`} className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                  <div className="flex flex-wrap items-start justify-between gap-3">
                    <div className="min-w-0 flex-1">
                      <div className="flex flex-wrap items-center gap-2">
                        <Pill tone={toneLabel(event.tone)}>{event.tone}</Pill>
                        <p className="text-sm font-semibold text-stone-900">{event.title}</p>
                      </div>
                      <p className="mt-2 text-sm text-stone-600">{event.detail}</p>
                    </div>
                    <div className="text-right text-xs text-stone-400">
                      <p>{formatLocalTimestamp(event.time ?? null)}</p>
                      {event.traceId ? <p className="mt-1">trace {event.traceId}</p> : null}
                    </div>
                  </div>
                </div>
              )) : <EmptyState title="No timeline events yet" detail="Once the query returns logs, this section summarizes the last few actions in order." />}
            </div>
          </section>
        </div>
      )}
    </ViewShell>
  )
}
