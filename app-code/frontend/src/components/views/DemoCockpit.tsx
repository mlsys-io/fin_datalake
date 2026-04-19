import React, { useCallback, useMemo } from 'react'
import { RefreshCw, ShieldCheck } from 'lucide-react'
import { ViewShell } from '../shared/ViewShell'
import { EmptyState, ErrorState, LoadingState, ResourceMeta } from '../shared/AsyncState'
import { usePollingResource } from '../../hooks/usePollingResource'
import { getRisingWaveSchema } from '../../lib/risingwave'
import {
  fetchAgents,
  fetchOverseerAlerts,
  fetchOverseerSnapshots,
  fetchReadiness,
  fetchSystemHealth,
  queryStream,
  type AgentSummary,
  type OverseerAlert,
  type OverseerSnapshot,
  type ReadinessResponse,
  type DataQueryResponse,
  type SystemHealthResponse,
} from '../../api/client'
import { formatLocalTimeOnly, formatLocalTimestamp } from '../../lib/time'

type Row = Record<string, unknown>

type CockpitData = {
  readiness: ReadinessResponse | null
  health: SystemHealthResponse | null
  agents: AgentSummary[]
  snapshots: OverseerSnapshot[]
  alerts: OverseerAlert[]
  signalRows: Row[]
  priceRows: Row[]
  errors: string[]
}

function rowsToObjects(result: DataQueryResponse | null | undefined): Row[] {
  if (!result?.columns?.length || !result.rows?.length) return []
  return result.rows.map(row => Object.fromEntries(result.columns!.map((column, index) => [column, row[index]])))
}

function asString(value: unknown): string {
  if (value == null) return 'n/a'
  if (typeof value === 'number') return Number.isInteger(value) ? String(value) : value.toFixed(4)
  if (typeof value === 'boolean') return value ? 'true' : 'false'
  return String(value)
}

function errorMessage(reason: unknown): string {
  return reason instanceof Error ? reason.message : 'request failed'
}

function isMissingTableError(reason: unknown): boolean {
  return /table or source not found|relation .* does not exist|catalog error/i.test(errorMessage(reason))
}

function asNumber(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value
  if (typeof value === 'string' && value.trim() && !Number.isNaN(Number(value))) return Number(value)
  return null
}

function formatMetric(value: unknown, suffix = ''): string {
  const numeric = asNumber(value)
  if (numeric == null) return 'n/a'
  return `${numeric.toFixed(Math.abs(numeric) >= 100 ? 1 : 2)}${suffix}`
}

function signalTone(action: unknown): string {
  const value = String(action ?? '').toUpperCase()
  if (value === 'BUY') return 'bg-emerald-50 text-emerald-700 border-emerald-200'
  if (value === 'SELL') return 'bg-rose-50 text-rose-700 border-rose-200'
  if (value === 'HOLD') return 'bg-amber-50 text-amber-700 border-amber-200'
  return 'bg-stone-100 text-stone-700 border-stone-200'
}

function metricTone(delta?: number | null): string {
  if (delta == null) return 'bg-stone-100 text-stone-700 border-stone-200'
  if (delta > 0) return 'bg-emerald-50 text-emerald-700 border-emerald-200'
  if (delta < 0) return 'bg-rose-50 text-rose-700 border-rose-200'
  return 'bg-amber-50 text-amber-700 border-amber-200'
}

function titleCase(value: string): string {
  return value
    .split(/[_\-. ]+/)
    .filter(Boolean)
    .map(part => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ')
}

function deriveMarketState(signal: Row | null, prices: Row[]): Array<{ label: string; value: string; tone: string }> {
  const latestPrice = prices[0] ?? null
  const returnPct = asNumber(latestPrice?.price_return_pct ?? null)
  const volatility = asNumber(latestPrice?.volatility_estimate ?? null)
  const confidence = asNumber(signal?.confidence ?? null)

  return [
    { label: 'Last Price', value: formatMetric(signal?.last_price ?? latestPrice?.close), tone: metricTone(confidence) },
    { label: 'SMA 5', value: formatMetric(signal?.sma_5 ?? latestPrice?.sma_5), tone: metricTone(returnPct) },
    { label: 'SMA 20', value: formatMetric(signal?.sma_20 ?? latestPrice?.sma_20), tone: metricTone(returnPct) },
    { label: 'VWAP', value: formatMetric(signal?.vwap ?? latestPrice?.vwap), tone: metricTone(returnPct) },
    { label: 'Return %', value: formatMetric(signal?.price_return_pct ?? latestPrice?.price_return_pct, '%'), tone: metricTone(returnPct) },
    { label: 'Volatility', value: formatMetric(signal?.volatility_estimate ?? latestPrice?.volatility_estimate), tone: metricTone(volatility) },
  ]
}

function AgentPill({ text, tone }: { text: string; tone: string }) {
  return <span className={`rounded-md border px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.16em] ${tone}`}>{text}</span>
}

function MetricCard({
  label,
  value,
  detail,
  tone,
}: {
  label: string
  value: string
  detail: string
  tone: string
}) {
  return (
    <div className="rounded-lg border border-stone-200 bg-white p-4 shadow-sm">
      <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">{label}</p>
      <p className="mt-2 text-2xl font-bold text-stone-900">{value}</p>
      <p className={`mt-3 inline-flex rounded-md border px-3 py-1 text-sm font-medium ${tone}`}>{detail}</p>
    </div>
  )
}

function SectionCard({
  eyebrow,
  title,
  children,
}: {
  eyebrow: string
  title: string
  children: React.ReactNode
}) {
  return (
    <section className="rounded-lg border border-stone-200 bg-white p-5 shadow-sm">
      <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">{eyebrow}</p>
      <h3 className="mt-2 text-lg font-semibold text-stone-900">{title}</h3>
      <div className="mt-4">{children}</div>
    </section>
  )
}

function SignalTable({ rows }: { rows: Row[] }) {
  if (!rows.length) {
    return <EmptyState title="No persisted signal found" detail="Run the Market Pulse workflow so the cockpit can surface the latest saved result." />
  }

  return (
    <div className="overflow-hidden rounded-lg border border-stone-200">
      <table className="w-full text-left text-sm">
        <thead className="bg-stone-50 text-[11px] uppercase tracking-[0.16em] text-stone-400">
          <tr>
            <th className="px-4 py-3">Time</th>
            <th className="px-4 py-3">Symbol</th>
            <th className="px-4 py-3">Action</th>
            <th className="px-4 py-3">Confidence</th>
            <th className="px-4 py-3">Sentiment</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-stone-100">
          {rows.map((row, index) => (
            <tr key={`${asString(row.timestamp_ms)}-${index}`}>
              <td className="px-4 py-3 font-mono text-xs text-stone-500">{formatLocalTimeOnly(row.timestamp_ms as string | number | Date | null)}</td>
              <td className="px-4 py-3 font-medium text-stone-900">{asString(row.symbol)}</td>
              <td className="px-4 py-3"><AgentPill text={String(row.action ?? 'HOLD').toUpperCase()} tone={signalTone(row.action)} /></td>
              <td className="px-4 py-3 font-mono text-stone-700">{formatMetric(row.confidence, '')}</td>
              <td className="px-4 py-3 text-stone-600">{`${asString(row.sentiment_label)} (${formatMetric(row.sentiment_score)})`}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function PriceTable({ rows }: { rows: Row[] }) {
  if (!rows.length) {
    return <EmptyState title="No price rows yet" detail="The price window will appear here once the live data path has been populated." />
  }

  return (
    <div className="overflow-hidden rounded-lg border border-stone-200">
      <table className="w-full text-left text-sm">
        <thead className="bg-stone-50 text-[11px] uppercase tracking-[0.16em] text-stone-400">
          <tr>
            <th className="px-4 py-3">Time</th>
            <th className="px-4 py-3">Symbol</th>
            <th className="px-4 py-3">Close</th>
            <th className="px-4 py-3">VWAP</th>
            <th className="px-4 py-3">Return</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-stone-100">
          {rows.map((row, index) => (
            <tr key={`${asString(row.timestamp_ms)}-${index}`}>
              <td className="px-4 py-3 font-mono text-xs text-stone-500">{formatLocalTimeOnly(row.timestamp_ms as string | number | Date | null)}</td>
              <td className="px-4 py-3 font-medium text-stone-900">{asString(row.symbol)}</td>
              <td className="px-4 py-3 font-mono text-stone-700">{formatMetric(row.close)}</td>
              <td className="px-4 py-3 font-mono text-stone-700">{formatMetric(row.vwap)}</td>
              <td className="px-4 py-3 font-mono text-stone-700">{formatMetric(row.price_return_pct, '%')}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export const DemoCockpit: React.FC = () => {
  const risingwaveSchema = getRisingWaveSchema()

  const loadCockpit = useCallback(async (): Promise<CockpitData> => {
    const errors: string[] = []
    const [readinessRes, healthRes, agentsRes, snapshotsRes, alertsRes, signalRes, pricesRes] = await Promise.allSettled([
      fetchReadiness(),
      fetchSystemHealth(),
      fetchAgents(),
      fetchOverseerSnapshots(5),
      fetchOverseerAlerts(5),
      queryStream(`SELECT symbol, action, confidence, sentiment_label, sentiment_score, analyst_summary, last_price, sma_5, sma_20, vwap, price_return_pct, volatility_estimate, timestamp_ms FROM ${risingwaveSchema}.market_pulse_signals ORDER BY timestamp_ms DESC LIMIT 5`),
      queryStream(`SELECT symbol, close, vwap, sma_5, sma_20, price_return_pct, volatility_estimate, timestamp_ms FROM ${risingwaveSchema}.market_pulse_prices ORDER BY timestamp_ms DESC LIMIT 10`),
    ])

    const readiness = readinessRes.status === 'fulfilled' ? readinessRes.value : null
    const health = healthRes.status === 'fulfilled' ? healthRes.value : null
    const agents = agentsRes.status === 'fulfilled' ? agentsRes.value : []
    const snapshots = snapshotsRes.status === 'fulfilled' ? snapshotsRes.value : []
    const alerts = alertsRes.status === 'fulfilled' ? alertsRes.value : []
    const signalRows = signalRes.status === 'fulfilled' ? rowsToObjects(signalRes.value) : []
    const priceRows = pricesRes.status === 'fulfilled' ? rowsToObjects(pricesRes.value) : []

    if (readinessRes.status === 'rejected') errors.push(`Readiness: ${errorMessage(readinessRes.reason)}`)
    if (healthRes.status === 'rejected') errors.push(`System health: ${errorMessage(healthRes.reason)}`)
    if (agentsRes.status === 'rejected') errors.push(`Agents: ${errorMessage(agentsRes.reason)}`)
    if (snapshotsRes.status === 'rejected') errors.push(`Overseer snapshots: ${errorMessage(snapshotsRes.reason)}`)
    if (alertsRes.status === 'rejected') errors.push(`Overseer alerts: ${errorMessage(alertsRes.reason)}`)
    if (signalRes.status === 'rejected' && !isMissingTableError(signalRes.reason)) errors.push(`Signal query: ${errorMessage(signalRes.reason)}`)
    if (pricesRes.status === 'rejected' && !isMissingTableError(pricesRes.reason)) errors.push(`Price query: ${errorMessage(pricesRes.reason)}`)

    return { readiness, health, agents, snapshots, alerts, signalRows, priceRows, errors }
  }, [risingwaveSchema])

  const { data, loading, refreshing, error, lastUpdated, stale, refresh } = usePollingResource(loadCockpit, { pollIntervalMs: 30_000 })

  const latestSignal = data?.signalRows?.[0] ?? null
  const signalSummaryRows = useMemo(() => data?.signalRows?.slice(0, 5) ?? [], [data?.signalRows])
  const priceRows = useMemo(() => data?.priceRows?.slice(0, 10) ?? [], [data?.priceRows])
  const latestSnapshot = data?.snapshots.at(-1) ?? null
  const readinessChecks = Object.values(data?.readiness?.checks ?? {})
  const readyCount = readinessChecks.filter(check => check.ready || check.configured).length
  const aliveAgents = data?.agents.filter(agent => agent.alive === true) ?? []
  const marketState = deriveMarketState(latestSignal, priceRows)
  const controlStatus = data?.health?.status ?? 'unknown'

  const marketAgents = useMemo(() => {
    const terms = ['market', 'strategy', 'analyst', 'sentiment', 'pulse']
    return (data?.agents ?? []).filter(agent => {
      const haystack = [
        agent.name,
        agent.status,
        agent.desired_status,
        agent.observed_status,
        ...(agent.capabilities ?? []),
      ]
        .filter(Boolean)
        .join(' ')
        .toLowerCase()
      return terms.some(term => haystack.includes(term))
    })
  }, [data?.agents])

  const controlSummary = latestSnapshot?.services ?? {}
  const controlCount = Object.keys(controlSummary).length
  const headlineAlerts = data?.alerts?.slice(0, 3) ?? []

  return (
    <ViewShell
      eyebrow="Showcase"
      title="Demo Cockpit"
      description="The first screen for the presentation: latest signal, market state, persistence proof, agents, and control-plane health in one place."
      actions={(
        <>
          <ResourceMeta lastUpdated={lastUpdated} refreshing={refreshing} stale={stale} />
          <button
            type="button"
            onClick={() => void refresh()}
            className="inline-flex items-center gap-2 rounded-md border border-stone-200 bg-white px-3 py-2 text-sm text-stone-700 transition hover:bg-stone-50"
          >
            <RefreshCw size={14} className={refreshing ? 'animate-spin' : ''} />
            Refresh
          </button>
        </>
      )}
    >
      {loading && !data ? (
        <LoadingState label="Loading demo cockpit..." />
      ) : error && !data ? (
        <ErrorState title="Demo cockpit unavailable" detail={error} onRetry={() => void refresh()} />
      ) : (
        <div className="space-y-6">
          {(data?.errors.length ?? 0) > 0 ? (
            <div className="rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-900">
              <p className="font-semibold">Partial data loaded</p>
              <p className="mt-1">Some sources did not respond, but the cockpit can still support the demo.</p>
            </div>
          ) : null}

          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <MetricCard
              label="Latest signal"
              value={latestSignal ? String(latestSignal.action ?? 'HOLD').toUpperCase() : 'No signal yet'}
              detail={latestSignal ? `${asString(latestSignal.symbol)} at ${formatLocalTimeOnly(latestSignal.timestamp_ms as string | number | Date | null)}` : 'Waiting for persisted rows'}
              tone={signalTone(latestSignal?.action)}
            />
            <MetricCard
              label="Market state"
              value={latestSignal ? asString(latestSignal.symbol) : 'n/a'}
              detail={latestSignal ? 'Latest persisted market snapshot' : 'Waiting for persisted rows'}
              tone="bg-sky-50 text-sky-700 border-sky-200"
            />
            <MetricCard
              label="Agent fleet"
              value={`${data?.agents.length ?? 0}`}
              detail={`${aliveAgents.length} alive`}
              tone="bg-stone-100 text-stone-700 border-stone-200"
            />
            <MetricCard
              label="Readiness"
              value={readinessChecks.length ? `${readyCount}/${readinessChecks.length}` : 'Unknown'}
              detail={readinessChecks.length ? 'Checks passing' : 'No readiness data yet'}
              tone="bg-emerald-50 text-emerald-700 border-emerald-200"
            />
          </div>

          <div className="grid gap-6 xl:grid-cols-[minmax(0,1.4fr)_minmax(0,0.9fr)]">
            <div className="space-y-6">
              <SectionCard eyebrow="Latest Signal" title="The last saved decision">
                {latestSignal ? (
                  <div className="space-y-4">
                    <div className="flex flex-wrap items-start justify-between gap-4">
                      <div>
                        <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-stone-400">Signal summary</p>
                        <h4 className="mt-1 text-2xl font-bold text-stone-900">{String(latestSignal.action ?? 'HOLD').toUpperCase()} {asString(latestSignal.symbol)}</h4>
                      </div>
                      <span className={`rounded-md border px-3 py-1 text-xs font-semibold uppercase tracking-[0.16em] ${signalTone(latestSignal.action)}`}>
                        {String(latestSignal.action ?? 'HOLD').toUpperCase()}
                      </span>
                    </div>

                    <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                      <MetricCard label="Confidence" value={formatMetric(latestSignal.confidence)} detail="Model conviction" tone={signalTone(latestSignal.action)} />
                      <MetricCard label="Sentiment" value={asString(latestSignal.sentiment_label)} detail={formatMetric(latestSignal.sentiment_score)} tone="bg-sky-50 text-sky-700 border-sky-200" />
                      <MetricCard label="Trend" value={formatMetric(latestSignal.trend_score)} detail="Directional score" tone="bg-stone-100 text-stone-700 border-stone-200" />
                      <MetricCard label="Time" value={formatLocalTimeOnly(latestSignal.timestamp_ms as string | number | Date | null)} detail="Persisted row timestamp" tone="bg-amber-50 text-amber-700 border-amber-200" />
                    </div>

                    <div className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                      <p className="text-xs font-semibold uppercase tracking-[0.18em] text-stone-400">Analyst summary</p>
                      <p className="mt-2 text-sm leading-6 text-stone-600">{asString(latestSignal.analyst_summary) !== 'n/a' ? asString(latestSignal.analyst_summary) : 'No analyst summary was persisted with this signal.'}</p>
                    </div>
                  </div>
                ) : (
                  <EmptyState title="No persisted signal found" detail="Run the Market Pulse workflow so the cockpit can surface the latest saved result." />
                )}
              </SectionCard>

              <SectionCard eyebrow="Persistence Proof" title="The gateway can read back what the pipeline wrote">
                <div className="space-y-5">
                  <div>
                    <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Recent signals</p>
                    <div className="mt-3">
                      <SignalTable rows={signalSummaryRows} />
                    </div>
                  </div>
                  <div>
                    <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Recent prices</p>
                    <div className="mt-3">
                      <PriceTable rows={priceRows} />
                    </div>
                  </div>
                  <div className="rounded-lg border border-stone-200 bg-stone-50 px-4 py-3 text-sm text-stone-600">
                    Query path: frontend {'->'} /api/v1/intent {'->'} DataAdapter.query_stream {'->'} RisingWave
                  </div>
                </div>
                </SectionCard>
              </div>

            <div className="space-y-6">
              <SectionCard eyebrow="Market State" title="Supporting context for the latest signal">
                <div className="grid gap-3 sm:grid-cols-2">
                  {marketState.map(item => (
                    <div key={item.label} className="rounded-lg border border-stone-200 bg-stone-50 px-3 py-3">
                      <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-stone-400">{item.label}</p>
                      <p className="mt-1 text-2xl font-bold text-stone-900">{item.value}</p>
                      <p className={`mt-2 inline-flex rounded-md border px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.16em] ${item.tone}`}>{item.label}</p>
                    </div>
                  ))}
                </div>
              </SectionCard>

              <SectionCard eyebrow="Agent Readiness" title="The relevant agents are visible and alive">
                {marketAgents.length ? (
                  <div className="space-y-3">
                    {marketAgents.map(agent => (
                      <div key={agent.name} className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                        <div className="flex flex-wrap items-start justify-between gap-3">
                          <div>
                            <p className="text-sm font-semibold text-stone-900">{agent.name}</p>
                            <p className="mt-1 text-xs text-stone-500">{agent.capabilities?.join(', ') ?? 'No listed capabilities'}</p>
                          </div>
                          <AgentPill
                            text={agent.alive ? 'Alive' : 'Offline'}
                            tone={agent.alive ? 'bg-emerald-50 text-emerald-700 border-emerald-200' : 'bg-stone-100 text-stone-700 border-stone-200'}
                          />
                        </div>
                        <div className="mt-3 grid gap-2 sm:grid-cols-2">
                          <div className="rounded-md border border-stone-200 bg-white px-3 py-2 text-xs text-stone-700">
                            <span className="font-semibold text-stone-500">Managed</span> {agent.managed_by_overseer ? 'yes' : 'no'}
                          </div>
                          <div className="rounded-md border border-stone-200 bg-white px-3 py-2 text-xs text-stone-700">
                            <span className="font-semibold text-stone-500">Recovery</span> {asString(agent.recovery_state)}
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <EmptyState title="No agents detected" detail="Deploy the strategy and analyst agents, then refresh the cockpit." />
                )}
              </SectionCard>

              <SectionCard eyebrow="Control Plane" title="The overseer and system health remain visible">
                <div className="space-y-4">
                  <div className="grid gap-3 sm:grid-cols-2">
                    <MetricCard label="Snapshots" value={`${data?.snapshots.length ?? 0}`} detail="Control-plane history" tone="bg-sky-50 text-sky-700 border-sky-200" />
                    <MetricCard label="Alerts" value={`${data?.alerts.length ?? 0}`} detail="Recent overseer actions" tone="bg-amber-50 text-amber-700 border-amber-200" />
                  </div>
                  <div className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                    <div className="flex flex-wrap items-start justify-between gap-3">
                      <div>
                        <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Latest overseer snapshot</p>
                        <p className="mt-1 text-sm text-stone-600">
                          {latestSnapshot ? formatLocalTimestamp(latestSnapshot.timestamp) : 'No snapshot yet'}
                        </p>
                      </div>
                      <ShieldCheck size={18} className="text-stone-500" />
                    </div>
                    <div className="mt-4 grid gap-2 sm:grid-cols-2">
                      <div className="rounded-md border border-stone-200 bg-white px-3 py-2 text-sm text-stone-700">
                        <span className="font-semibold text-stone-500">Services</span> {controlCount}
                      </div>
                      <div className="rounded-md border border-stone-200 bg-white px-3 py-2 text-sm text-stone-700">
                        <span className="font-semibold text-stone-500">Readiness</span> {data?.readiness ? (data.readiness.ready ? 'ready' : 'not ready') : 'unknown'}
                      </div>
                      <div className="rounded-md border border-stone-200 bg-white px-3 py-2 text-sm text-stone-700 sm:col-span-2">
                        <span className="font-semibold text-stone-500">System health</span> {controlStatus}
                      </div>
                    </div>
                  </div>
                  <div>
                    <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Latest alerts</p>
                    <div className="mt-3 space-y-2">
                      {headlineAlerts.length ? headlineAlerts.map((alert, index) => (
                        <div key={`${alert.timestamp}-${index}`} className="rounded-lg border border-stone-200 bg-stone-50 px-4 py-3 text-sm text-stone-600">
                          <p className="font-medium text-stone-900">{titleCase(alert.action || alert.type)}</p>
                          <p className="mt-1 text-xs text-stone-500">{alert.detail}</p>
                        </div>
                      )) : (
                        <EmptyState title="No overseer alerts yet" detail="The recovery timeline will appear once the overseer has taken action." />
                      )}
                    </div>
                  </div>
                </div>
              </SectionCard>

              <SectionCard eyebrow="Runbook" title="Presentation flow">
                <div className="space-y-3">
                  {[
                    ['1', 'Run the CLI flow', 'Use the terminal for the actual workflow execution.'],
                    ['2', 'Open the cockpit', 'Use the UI to point at the persisted output.'],
                    ['3', 'Switch to observability', 'Show logs and health when the story needs evidence.'],
                    ['4', 'Close with benchmarks', 'Use the comparison view to explain why the integrated path wins.'],
                  ].map(([step, label, detail]) => (
                    <div key={step} className="flex gap-3 rounded-lg border border-stone-200 bg-stone-50 p-3">
                      <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-md bg-stone-900 text-xs font-semibold text-white">{step}</div>
                      <div className="min-w-0">
                        <p className="text-sm font-semibold text-stone-900">{label}</p>
                        <p className="mt-1 text-sm text-stone-500">{detail}</p>
                      </div>
                    </div>
                  ))}
                </div>
              </SectionCard>
            </div>
          </div>

          {(data?.errors.length ?? 0) > 0 ? (
            <div className="rounded-lg border border-stone-200 bg-white p-5 shadow-sm">
              <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-stone-400">Source Warnings</p>
              <ul className="mt-3 space-y-2 text-sm text-stone-600">
                {data?.errors.map((item, index) => (
                  <li key={`${item}-${index}`} className="rounded-md border border-stone-200 bg-stone-50 px-3 py-2">{item}</li>
                ))}
              </ul>
            </div>
          ) : null}

          <div className="rounded-lg border border-stone-200 bg-stone-50 p-4 text-sm text-stone-600">
            The cockpit stays readable if one of the supporting queries goes missing, so the demo can keep moving without a backend detour.
          </div>
        </div>
      )}
    </ViewShell>
  )
}
