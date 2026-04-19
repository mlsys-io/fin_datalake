import React, { useCallback, useMemo } from 'react'
import { Layers3 } from 'lucide-react'
import { ViewShell } from '../shared/ViewShell'
import { EmptyState, ErrorState, LoadingState, ResourceMeta } from '../shared/AsyncState'
import { usePollingResource } from '../../hooks/usePollingResource'
import { fetchInterfaceInventory, type InterfaceInventoryResponse } from '../../api/client'

type StatusTone = 'ready' | 'partial' | 'pending' | 'risk' | 'planned' | 'neutral'

function toneClass(status: StatusTone): string {
  if (status === 'ready') return 'bg-emerald-50 text-emerald-700 border-emerald-200'
  if (status === 'partial') return 'bg-amber-50 text-amber-700 border-amber-200'
  if (status === 'risk') return 'bg-rose-50 text-rose-700 border-rose-200'
  return 'bg-stone-100 text-stone-700 border-stone-200'
}

function toneForState(state?: string, ok?: boolean): StatusTone {
  if (ok === true) return 'ready'
  if (ok === false) return 'risk'

  const normalized = state?.toLowerCase()
  if (!normalized) return 'neutral'
  if (['ready', 'registered', 'implemented', 'available', 'reachable', 'ok'].includes(normalized)) return 'ready'
  if (['partial', 'degraded', 'warning'].includes(normalized)) return 'partial'
  if (['failed', 'error', 'unavailable', 'blocked'].includes(normalized)) return 'risk'
  if (normalized === 'pending') return 'pending'
  if (normalized === 'planned') return 'planned'
  return 'neutral'
}

function labelForState(state?: string, ok?: boolean): string {
  if (ok === true) return 'available'
  if (ok === false) return 'unavailable'
  return state ?? 'reported'
}

function StatusPill({ status, children }: { status: StatusTone; children: React.ReactNode }) {
  return <span className={`inline-flex rounded-md border px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.16em] ${toneClass(status)}`}>{children}</span>
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

function MetricCard({ label, value, detail }: { label: string; value: number; detail: string }) {
  return (
    <div className="rounded-lg border border-stone-200 bg-stone-50 p-4">
      <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-stone-400">{label}</p>
      <p className="mt-2 text-3xl font-bold text-stone-900">{value}</p>
      <p className="mt-1 text-sm text-stone-500">{detail}</p>
    </div>
  )
}

function inputNames(schema?: Record<string, unknown>): string[] {
  const properties = schema?.properties
  if (!properties || typeof properties !== 'object' || Array.isArray(properties)) return []
  return Object.keys(properties as Record<string, unknown>)
}

export const InterfaceMcp: React.FC = () => {
  const loadInventory = useCallback(async (): Promise<InterfaceInventoryResponse> => {
    return fetchInterfaceInventory()
  }, [])

  const {
    data: inventory,
    loading,
    refreshing,
    error,
    lastUpdated,
    stale,
    refresh,
  } = usePollingResource(loadInventory, { pollIntervalMs: 60_000 })

  const inventorySummary = inventory?.summary ?? null
  const inventoryDomains = inventory?.domains ?? []
  const inventoryTools = useMemo(() => inventory?.mcp_tools ?? [], [inventory?.mcp_tools])
  const inventoryRoutes = inventory?.routes ?? []
  const inventoryProxies = inventory?.proxies ?? []

  const actionCount = inventorySummary?.actions ?? inventoryDomains.reduce((count, domain) => count + (domain.actions?.length ?? 0), 0)
  const routeCount = inventorySummary?.routes ?? inventoryRoutes.length
  const proxyCount = inventorySummary?.proxies ?? inventoryProxies.length
  const availableProxyCount = inventoryProxies.filter(proxy => proxy.ok).length

  return (
    <ViewShell
      eyebrow="System"
      title="Interfaces"
      description="Gateway-discovered domains, routes, tools, and proxy targets."
    >
      <section className="rounded-lg border border-stone-200 bg-white p-5 shadow-sm">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="min-w-0">
            <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Live inventory</p>
            <p className="mt-2 text-sm text-stone-500">Current interface inventory reported by the gateway.</p>
          </div>
          <div className="flex items-center gap-3">
            <ResourceMeta lastUpdated={lastUpdated} refreshing={refreshing} stale={stale} />
            <button
              type="button"
              onClick={() => void refresh()}
              className="inline-flex items-center gap-2 rounded-lg border border-stone-200 bg-white px-3 py-2 text-sm text-stone-600 transition hover:bg-stone-100"
            >
              <Layers3 size={14} className={refreshing ? 'animate-spin' : ''} />
              Refresh inventory
            </button>
          </div>
        </div>

        {loading && !inventory ? (
          <LoadingState label="Loading interface inventory..." />
        ) : error && !inventory ? (
          <ErrorState title="Interface inventory unavailable" detail={error} onRetry={() => void refresh()} />
        ) : (
          <div className="mt-4 grid gap-4 md:grid-cols-2 xl:grid-cols-5">
            <MetricCard label="Domains" value={inventorySummary?.domains ?? inventoryDomains.length} detail="registered adapters" />
            <MetricCard label="Actions" value={actionCount} detail="gateway operations" />
            <MetricCard label="MCP tools" value={inventorySummary?.mcp_tools ?? inventoryTools.length} detail="tool registrations" />
            <MetricCard label="Routes" value={routeCount} detail="HTTP surfaces" />
            <MetricCard label="Proxies" value={proxyCount} detail={`${availableProxyCount}/${proxyCount} available`} />
          </div>
        )}
      </section>

      {inventory ? (
        <>
          <SectionCard eyebrow="Domains" title="Registered Gateway Domains">
            {inventoryDomains.length ? (
              <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
                {inventoryDomains.map(domain => (
                  <div key={domain.name} className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                    <div className="flex flex-wrap items-start justify-between gap-3">
                      <div className="min-w-0">
                        <p className="font-semibold text-stone-900">{domain.name}</p>
                        <p className="mt-1 text-sm text-stone-500">{domain.adapter ?? 'adapter unavailable'}</p>
                      </div>
                      <StatusPill status={toneForState(domain.state)}>{labelForState(domain.state)}</StatusPill>
                    </div>
                    <p className="mt-4 text-3xl font-bold text-stone-900">{domain.action_count ?? domain.actions.length}</p>
                    <p className="mt-1 text-sm text-stone-500">actions</p>
                    {domain.actions.length ? (
                      <div className="mt-4 flex flex-wrap gap-2">
                        {domain.actions.map(action => (
                          <span key={action.name} className="rounded-md border border-stone-200 bg-white px-2.5 py-1 text-xs text-stone-600">
                            {action.name}
                          </span>
                        ))}
                      </div>
                    ) : null}
                  </div>
                ))}
              </div>
            ) : (
              <EmptyState title="No domains reported" detail="Gateway domain registrations will appear after the inventory endpoint returns them." />
            )}
          </SectionCard>

          <SectionCard eyebrow="MCP Registry" title="Registered Tool Surface">
            {inventoryTools.length ? (
              <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
                {inventoryTools.map(tool => {
                  const inputs = inputNames(tool.input_schema)
                  return (
                    <div key={tool.name} className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                      <div className="flex flex-wrap items-start justify-between gap-3">
                        <div className="min-w-0">
                          <p className="font-semibold text-stone-900">{tool.name}</p>
                          <p className="mt-1 text-xs uppercase tracking-[0.16em] text-stone-400">
                            {tool.domain ?? 'unknown'} / {tool.action ?? 'unknown'}
                          </p>
                        </div>
                        <StatusPill status={toneForState(tool.state)}>{labelForState(tool.state)}</StatusPill>
                      </div>
                      <p className="mt-3 text-sm leading-6 text-stone-600">{tool.description ?? 'No description reported.'}</p>
                      <div className="mt-3 rounded-md border border-stone-200 bg-white px-3 py-2">
                        <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-stone-400">Inputs</p>
                        <p className="mt-1 text-sm text-stone-700">{inputs.length ? inputs.join(', ') : 'none'}</p>
                      </div>
                    </div>
                  )
                })}
              </div>
            ) : (
              <EmptyState title="No MCP tools reported" detail="Registered MCP tools will appear when the gateway inventory includes tool metadata." />
            )}
          </SectionCard>

          <div className="grid gap-6 xl:grid-cols-[minmax(0,1.15fr)_minmax(0,0.85fr)]">
            <SectionCard eyebrow="Routes" title="HTTP Routes Reported By Gateway">
              {inventoryRoutes.length ? (
                <div className="space-y-2">
                  {inventoryRoutes.map(route => (
                    <div key={`${route.method}-${route.path}`} className="rounded-md border border-stone-200 bg-stone-50 px-3 py-2 text-sm text-stone-600">
                      <div className="flex flex-wrap items-center justify-between gap-2">
                        <p><span className="font-semibold text-stone-900">{route.method}</span> {route.path}</p>
                        <StatusPill status={toneForState(route.state)}>{labelForState(route.state)}</StatusPill>
                      </div>
                      {(route.domain || route.action) ? (
                        <p className="mt-1 text-xs uppercase tracking-[0.16em] text-stone-400">
                          {route.domain ?? 'unknown'} / {route.action ?? 'unknown'}
                        </p>
                      ) : null}
                    </div>
                  ))}
                </div>
              ) : (
                <EmptyState title="No routes reported" detail="Route inventory will appear once the gateway reports its HTTP surface." />
              )}
            </SectionCard>

            <SectionCard eyebrow="Proxy Targets" title="Internal Dashboards And Services">
              {inventoryProxies.length ? (
                <div className="space-y-2">
                  {inventoryProxies.map(proxy => (
                    <div key={proxy.name} className="rounded-md border border-stone-200 bg-stone-50 px-3 py-2 text-sm text-stone-600">
                      <div className="flex flex-wrap items-start justify-between gap-3">
                        <div className="min-w-0">
                          <p className="font-semibold text-stone-900">{proxy.name}</p>
                          <p className="mt-1 break-all text-xs text-stone-500">{proxy.url ?? 'url unavailable'}</p>
                        </div>
                        <StatusPill status={toneForState(proxy.state, proxy.ok)}>{labelForState(proxy.state, proxy.ok)}</StatusPill>
                      </div>
                      <p className="mt-2 text-xs text-stone-500">
                        status {proxy.status_code ?? 'n/a'}{proxy.detail ? ` - ${proxy.detail}` : ''}
                      </p>
                    </div>
                  ))}
                </div>
              ) : (
                <EmptyState title="No proxy targets reported" detail="Proxy target health will appear when the gateway returns infrastructure probes." />
              )}
            </SectionCard>
          </div>
        </>
      ) : null}
    </ViewShell>
  )
}
