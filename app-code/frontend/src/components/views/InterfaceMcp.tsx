import React, { useCallback, useMemo } from 'react'
import { Code2, Layers3 } from 'lucide-react'
import { ViewShell } from '../shared/ViewShell'
import { ErrorState, LoadingState, ResourceMeta } from '../shared/AsyncState'
import { usePollingResource } from '../../hooks/usePollingResource'
import { fetchInterfaceInventory, type InterfaceInventoryResponse } from '../../api/client'
import {
  deploymentStatuses,
  dispatchPath,
  implementationNotes,
  interfaceCapabilities,
  interfaceContract,
  interfaceSurfaces,
  mcpTools,
  sampleCalls,
} from '../../data/interfaceMcpEvidence'

type StatusTone = 'implemented' | 'partial' | 'pending' | 'ready' | 'risk' | 'planned'

function toneClass(status: StatusTone): string {
  if (status === 'implemented' || status === 'ready') return 'bg-emerald-50 text-emerald-700 border-emerald-200'
  if (status === 'partial') return 'bg-amber-50 text-amber-700 border-amber-200'
  if (status === 'risk') return 'bg-rose-50 text-rose-700 border-rose-200'
  return 'bg-stone-100 text-stone-700 border-stone-200'
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

function CodeBlock({
  title,
  description,
  code,
}: {
  title: string
  description: string
  code: string
}) {
  return (
    <div className="rounded-lg border border-stone-200 bg-stone-50 p-4">
      <div className="flex items-center gap-2">
        <Code2 size={14} className="text-stone-500" />
        <p className="text-sm font-semibold text-stone-900">{title}</p>
      </div>
      <p className="mt-1 text-sm text-stone-500">{description}</p>
      <pre className="mt-3 overflow-x-auto rounded-md border border-stone-200 bg-white p-3 text-xs leading-6 text-stone-700">
        <code>{code}</code>
      </pre>
    </div>
  )
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

  const surfaceCounts = useMemo(() => interfaceSurfaces.reduce((acc, surface) => {
    acc[surface.state] = (acc[surface.state] ?? 0) + 1
    return acc
  }, {} as Record<'ready' | 'partial' | 'pending' | 'planned', number>), [])
  const inventorySummary = inventory?.summary ?? null
  const inventoryDomains = inventory?.domains ?? []
  const inventoryTools = inventory?.mcp_tools ?? []
  const inventoryRoutes = inventory?.routes ?? []
  const inventoryProxies = inventory?.proxies ?? []

  return (
    <ViewShell
      eyebrow="System"
      title="Interfaces"
      description="A live inventory of the gateway-adjacent surfaces we already have, with a static fallback for the parts that still need discovery."
    >
      <section className="rounded-lg border border-stone-200 bg-white p-5 shadow-sm">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="min-w-0">
            <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Live inventory</p>
            <p className="mt-2 text-sm text-stone-500">The gateway can now describe its own domains, routes, tools, and proxy targets.</p>
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
          <>
            <div className="mt-4 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
              <div className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-stone-400">Domains</p>
                <p className="mt-2 text-3xl font-bold text-stone-900">{inventorySummary?.domains ?? inventoryDomains.length}</p>
              </div>
              <div className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-stone-400">Actions</p>
                <p className="mt-2 text-3xl font-bold text-stone-900">{inventorySummary?.actions ?? inventoryDomains.reduce((count, domain) => count + (domain.actions?.length ?? 0), 0)}</p>
              </div>
              <div className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-stone-400">MCP tools</p>
                <p className="mt-2 text-3xl font-bold text-stone-900">{inventorySummary?.mcp_tools ?? inventoryTools.length}</p>
              </div>
              <div className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-stone-400">Proxy targets</p>
                <p className="mt-2 text-3xl font-bold text-stone-900">{inventorySummary?.proxies ?? inventoryProxies.length}</p>
              </div>
            </div>

            <div className="mt-4 grid gap-4 md:grid-cols-2 xl:grid-cols-3">
              {inventoryDomains.map(domain => (
                <div key={domain.name} className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                  <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">{domain.name}</p>
                  <p className="mt-2 text-sm text-stone-500">{domain.adapter ?? 'Adapter not exposed'}</p>
                  <p className="mt-3 text-2xl font-bold text-stone-900">{domain.action_count ?? domain.actions.length}</p>
                  <p className="mt-1 text-sm text-stone-500">actions</p>
                </div>
              ))}
            </div>

            <div className="mt-4 grid gap-4 xl:grid-cols-[minmax(0,1.15fr)_minmax(0,0.85fr)]">
              <div className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Routes</p>
                <div className="mt-3 space-y-2">
                  {inventoryRoutes.map(route => (
                    <div key={`${route.method}-${route.path}`} className="rounded-md border border-stone-200 bg-white px-3 py-2 text-sm text-stone-600">
                      <div className="flex flex-wrap items-center justify-between gap-2">
                        <p><span className="font-semibold text-stone-900">{route.method}</span> {route.path}</p>
                        {route.state ? <span className="text-[10px] uppercase tracking-[0.16em] text-stone-400">{route.state}</span> : null}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
              <div className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">Proxy status</p>
                <div className="mt-3 space-y-2">
                  {inventoryProxies.map(proxy => (
                    <div key={proxy.name} className="rounded-md border border-stone-200 bg-white px-3 py-2 text-sm text-stone-600">
                      <p className="font-semibold text-stone-900">{proxy.name}</p>
                      <p className="mt-1 text-xs text-stone-500">{proxy.ok ? 'available' : 'unavailable'} · {proxy.url ?? 'n/a'}</p>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </>
        )}
      </section>

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        {[
          { label: 'Ready', value: surfaceCounts.ready ?? 0, detail: 'Live surfaces already in use', status: 'implemented' as StatusTone },
          { label: 'Partial', value: surfaceCounts.partial ?? 0, detail: 'Available, but gateway-dependent', status: 'partial' as StatusTone },
          { label: 'Pending', value: surfaceCounts.pending ?? 0, detail: 'Still to be hardened', status: 'pending' as StatusTone },
          { label: 'Planned', value: surfaceCounts.planned ?? 0, detail: 'Future exposure candidates', status: 'planned' as StatusTone },
        ].map(item => (
          <div key={item.label} className="rounded-lg border border-stone-200 bg-white p-4 shadow-sm">
            <div className="flex items-start justify-between gap-3">
              <div>
                <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-stone-400">{item.label}</p>
                <p className="mt-2 text-3xl font-bold text-stone-900">{item.value}</p>
                <p className="mt-1 text-sm text-stone-500">{item.detail}</p>
              </div>
              <StatusPill status={item.status}>{item.label.toLowerCase()}</StatusPill>
            </div>
          </div>
        ))}
      </div>

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        {interfaceSurfaces.map(surface => (
          <div key={surface.label} className="rounded-lg border border-stone-200 bg-white p-4 shadow-sm">
            <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-stone-400">{surface.label}</p>
            <div className="mt-3 flex items-start justify-between gap-3">
              <p className="text-sm leading-6 text-stone-600">{surface.detail}</p>
              <StatusPill status={surface.state}>{surface.state}</StatusPill>
            </div>
          </div>
        ))}
      </div>

      <SectionCard eyebrow="Interface Contract" title="One platform, multiple surfaces">
        <p className="max-w-4xl text-sm leading-6 text-stone-600">{interfaceContract.summary}</p>
        <div className="mt-4 grid gap-3 lg:grid-cols-2">
          <div className="rounded-lg border border-stone-200 bg-stone-50 p-4">
            <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">REST / MCP path</p>
            <p className="mt-2 text-sm leading-6 text-stone-700">{interfaceContract.path}</p>
          </div>
          <div className="rounded-lg border border-stone-200 bg-stone-50 p-4">
            <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">MCP dispatch path</p>
            <p className="mt-2 text-sm leading-6 text-stone-700">{interfaceContract.mcpPath}</p>
          </div>
        </div>
      </SectionCard>

      <div className="grid gap-6 xl:grid-cols-[minmax(0,1.25fr)_minmax(0,0.95fr)]">
        <SectionCard eyebrow="Gateway Mapping" title="The current actions exposed through the interface layer">
          <div className="overflow-hidden rounded-lg border border-stone-200">
            <table className="w-full text-left text-sm">
              <thead className="bg-stone-50 text-[11px] uppercase tracking-[0.16em] text-stone-400">
                <tr>
                  <th className="px-4 py-3">Capability</th>
                  <th className="px-4 py-3">REST</th>
                  <th className="px-4 py-3">MCP Tool</th>
                  <th className="px-4 py-3">Adapter</th>
                  <th className="px-4 py-3">Permission</th>
                  <th className="px-4 py-3">Status</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-stone-100">
                {interfaceCapabilities.map(row => (
                  <tr key={row.id}>
                    <td className="px-4 py-3">
                      <p className="font-medium text-stone-900">{row.capability}</p>
                      <p className="mt-1 text-xs text-stone-500">{row.note}</p>
                    </td>
                    <td className="px-4 py-3 text-stone-600">{row.rest}</td>
                    <td className="px-4 py-3 text-stone-600">{row.mcpTool}</td>
                    <td className="px-4 py-3 text-stone-600">{row.adapter}</td>
                    <td className="px-4 py-3 text-stone-600">{row.permission}</td>
                    <td className="px-4 py-3">
                      <StatusPill status={row.status}>{row.status}</StatusPill>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </SectionCard>

        <SectionCard eyebrow="Deployment Status" title="What is ready, and what still needs hardening">
          <div className="space-y-3">
            {deploymentStatuses.map(item => (
              <div key={item.label} className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <p className="text-sm font-medium text-stone-900">{item.label}</p>
                  <StatusPill status={item.status}>{item.status}</StatusPill>
                </div>
                <p className="mt-2 text-sm text-stone-500">{item.detail}</p>
              </div>
            ))}
          </div>
        </SectionCard>
      </div>

      <div className="grid gap-6 xl:grid-cols-[minmax(0,1.15fr)_minmax(0,0.85fr)]">
        <SectionCard eyebrow="Registered Tools" title="Tool surfaces still map back to the gateway">
          <div className="grid gap-3 md:grid-cols-2">
            {mcpTools.map(tool => (
              <div key={tool.name} className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <div className="min-w-0">
                    <p className="font-semibold text-stone-900">{tool.name}</p>
                    <p className="mt-1 text-xs uppercase tracking-[0.16em] text-stone-400">
                      {tool.domain} / {tool.action}
                    </p>
                  </div>
                  <StatusPill status={tool.status}>{tool.status}</StatusPill>
                </div>
                <div className="mt-3 grid gap-2 sm:grid-cols-2">
                  <div className="rounded-md border border-stone-200 bg-white px-3 py-2">
                    <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-stone-400">Inputs</p>
                    <p className="mt-1 text-sm text-stone-700">{tool.inputs.length ? tool.inputs.join(', ') : 'none'}</p>
                  </div>
                  <div className="rounded-md border border-stone-200 bg-white px-3 py-2">
                    <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-stone-400">Permission</p>
                    <p className="mt-1 text-sm text-stone-700">{tool.permission}</p>
                  </div>
                </div>
                <p className="mt-3 text-sm text-stone-600">{tool.proof}</p>
              </div>
            ))}
          </div>
        </SectionCard>

        <SectionCard eyebrow="Implementation Notes" title="Honest points to carry into the hardening pass">
          <div className="space-y-3">
            {implementationNotes.map(note => (
              <div key={note.label} className="rounded-lg border border-stone-200 bg-stone-50 p-4">
                <div className="flex items-center justify-between gap-3">
                  <p className="text-sm font-medium text-stone-900">{note.label}</p>
                  <StatusPill status={note.tone}>{note.tone}</StatusPill>
                </div>
                <p className="mt-2 text-sm text-stone-500">{note.detail}</p>
              </div>
            ))}
          </div>
        </SectionCard>
      </div>

      <div className="grid gap-6 xl:grid-cols-[minmax(0,1.05fr)_minmax(0,0.95fr)]">
        <SectionCard eyebrow="Request Flow" title="REST and MCP converge into the same gateway machinery">
          <div className="space-y-3">
            {dispatchPath.map((step, index) => (
              <div key={step.label} className="flex items-start gap-3">
                <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-md bg-stone-900 text-xs font-semibold text-white">
                  {index + 1}
                </div>
                <div className="min-w-0 flex-1 rounded-lg border border-stone-200 bg-stone-50 p-3">
                  <p className="text-sm font-medium text-stone-900">{step.label}</p>
                  <p className="mt-1 text-sm text-stone-600">{step.detail}</p>
                </div>
              </div>
            ))}
          </div>
        </SectionCard>

        <SectionCard eyebrow="Sample Requests" title="The same request shape in both protocols">
          <div className="space-y-3">
            {sampleCalls.map(sample => (
              <CodeBlock key={sample.label} title={sample.label} description={sample.description} code={sample.payload} />
            ))}
          </div>
        </SectionCard>
      </div>

      <div className="rounded-lg border border-stone-200 bg-white p-5 shadow-sm">
        <div className="flex items-center gap-2">
          <Layers3 size={16} className="text-stone-500" />
          <p className="text-sm font-semibold text-stone-900">Notes</p>
        </div>
        <p className="mt-3 max-w-4xl text-sm leading-6 text-stone-600">
          This page stays static on purpose until live interface discovery is exposed. It is still useful as a map of what already exists, what is proxied, and what can be added later without inventing a separate backend.
        </p>
        <div className="mt-4 flex flex-wrap gap-2">
          <StatusPill status="ready">gateway inventory</StatusPill>
          <StatusPill status="partial">proxy dependent</StatusPill>
          <StatusPill status="partial">live discovery</StatusPill>
          <StatusPill status="implemented">shared audit trail</StatusPill>
        </div>
      </div>
    </ViewShell>
  )
}
