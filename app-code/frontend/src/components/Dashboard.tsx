import React, { useEffect, useMemo } from 'react'
import {
  Activity,
  BarChart3,
  Bot,
  CircleSlash,
  Cpu,
  Database,
  LayoutDashboard,
  LogOut,
  Plug,
  Settings,
} from 'lucide-react'
import { useAuthStore } from '../store/useAuthStore'
import { DataCatalog } from './views/DataCatalog'
import { ComputePipelines } from './views/ComputePipelines'
import { AgentHub } from './views/AgentHub'
import { InfraIframes } from './views/InfraIframes'
import { DemoCockpit } from './views/DemoCockpit'
import { InterfaceMcp } from './views/InterfaceMcp'
import { Observability } from './views/Observability'
import { BenchmarkEvidence } from './views/BenchmarkEvidence'
import { useRoutePath } from '../hooks/useRoutePath'

type ViewType = 'cockpit' | 'interfaces' | 'observability' | 'benchmarks' | 'data' | 'compute' | 'agents' | 'infra'
type NavGroup = 'showcase' | 'system' | 'admin'

type NavItem = {
  key: ViewType
  path: string
  label: string
  description: string
  headerTitle: string
  icon: React.ComponentType<{ size?: number; className?: string }>
  group: NavGroup
  visible: boolean
}

export const Dashboard: React.FC = () => {
  const { user, setUser } = useAuthStore()
  const { pathname, navigate } = useRoutePath()

  const handleLogout = async () => {
    try {
      await fetch('/api/v1/auth/logout', { method: 'POST', credentials: 'include' })
      setUser(null)
    } catch (e) {
      console.error('Logout failed', e)
    }
  }

  const canViewCompute = user?.permissions?.includes('compute:read')
  const canViewInfra = user?.roles?.includes('Admin')

  const navItems = useMemo<NavItem[]>(() => ([
    { key: 'cockpit', path: '/cockpit', label: 'Demo Cockpit', description: 'Latest signal, platform state, and demo readiness.', headerTitle: 'Demo Cockpit', icon: LayoutDashboard, group: 'showcase', visible: true },
    { key: 'benchmarks', path: '/benchmarks', label: 'Baseline Evidence', description: 'Key comparison artifacts from the evaluation run.', headerTitle: 'Baseline Evidence', icon: BarChart3, group: 'showcase', visible: true },
    { key: 'data', path: '/data', label: 'Data Catalog', description: 'Delta Lake inventory and search.', headerTitle: 'Data Catalog', icon: Database, group: 'system', visible: true },
    { key: 'agents', path: '/agents', label: 'Agents', description: 'Fleet status, capabilities, and manual tests.', headerTitle: 'Agents', icon: Bot, group: 'system', visible: true },
    { key: 'compute', path: '/compute', label: 'Compute & Pipelines', description: 'Prefect and Ray orchestration surface.', headerTitle: 'Compute & Pipelines', icon: Cpu, group: 'system', visible: Boolean(canViewCompute) },
    { key: 'observability', path: '/observability', label: 'Observability', description: 'Health, readiness, logs, and recovery activity.', headerTitle: 'Observability', icon: Activity, group: 'system', visible: true },
    { key: 'interfaces', path: '/interfaces', label: 'Interfaces', description: 'Gateway and protocol surfaces connected to the platform.', headerTitle: 'Interfaces', icon: Plug, group: 'system', visible: true },
    { key: 'infra', path: '/infra', label: 'Infrastructure', description: 'Internal dashboards behind the gateway.', headerTitle: 'Infrastructure', icon: Settings, group: 'admin', visible: Boolean(canViewInfra) },
  ]), [canViewCompute, canViewInfra])

  const visibleItems = navItems.filter(item => item.visible)
  const visibleGroups = useMemo(() => ([
    { label: 'Showcase', items: visibleItems.filter(item => item.group === 'showcase') },
    { label: 'System', items: visibleItems.filter(item => item.group === 'system') },
    { label: 'Admin', items: visibleItems.filter(item => item.group === 'admin') },
  ]).filter(section => section.items.length > 0), [visibleItems])

  const defaultPath = visibleItems.find(item => item.key === 'cockpit')?.path ?? visibleItems[0]?.path ?? '/cockpit'
  const knownItem = navItems.find(item => item.path === pathname) ?? null
  const activeItem = visibleItems.find(item => item.path === pathname) ?? visibleItems[0]
  const activeView = activeItem?.key ?? 'cockpit'
  const isUnknownPath = pathname !== '/' && knownItem == null
  const isUnauthorizedPath = knownItem != null && !knownItem.visible

  useEffect(() => {
    if (pathname === '/' || isUnauthorizedPath) {
      navigate(defaultPath, { replace: true })
    }
  }, [defaultPath, isUnauthorizedPath, navigate, pathname])

  const renderView = () => {
    if (isUnknownPath) {
      return (
        <div className="mx-auto flex max-w-2xl flex-col items-center justify-center rounded-lg border border-dashed border-stone-300 bg-stone-50 px-8 py-20 text-center">
          <div className="mb-5 rounded-lg bg-stone-900 p-4 text-white">
            <CircleSlash size={28} />
          </div>
          <p className="text-xs font-semibold uppercase tracking-[0.22em] text-stone-400">404</p>
          <h3 className="mt-2 text-2xl font-bold text-stone-900">Page not found</h3>
          <p className="mt-3 max-w-xl text-sm text-stone-500">
            The path <span className="font-mono text-stone-700">{pathname}</span> does not match any dashboard view.
          </p>
          <button
            type="button"
            onClick={() => navigate(defaultPath)}
            className="mt-6 rounded-lg border border-stone-200 bg-white px-5 py-2.5 text-sm font-medium text-stone-700 transition hover:bg-stone-100"
          >
            Return to Demo Cockpit
          </button>
        </div>
      )
    }

    switch (activeView) {
      case 'cockpit': return <DemoCockpit />
      case 'interfaces': return <InterfaceMcp />
      case 'observability': return <Observability />
      case 'benchmarks': return <BenchmarkEvidence />
      case 'data': return <DataCatalog />
      case 'compute': return <ComputePipelines />
      case 'agents': return <AgentHub />
      case 'infra': return <InfraIframes />
      default: return <DemoCockpit />
    }
  }

  return (
    <div className="flex h-screen bg-white text-stone-900 font-sans">
      <aside aria-label="Main navigation" className="flex w-72 flex-col border-r border-stone-200 bg-[#F7F7F5]">
        <div className="border-b border-stone-200 p-6">
          <div className="flex items-center gap-3">
            <Database className="text-stone-900" />
            <div>
              <h1 className="text-lg font-bold text-stone-900">Launchpad</h1>
              <p className="text-xs text-stone-500">Demo showcase</p>
            </div>
          </div>
        </div>

        <nav aria-label="Dashboard views" className="flex-1 space-y-5 overflow-y-auto p-4">
          {visibleGroups.map(section => (
            <div key={section.label}>
              <p className="px-2 text-[11px] font-semibold uppercase tracking-[0.22em] text-stone-400">{section.label}</p>
              <div className="mt-2 space-y-2">
                {section.items.map(item => {
                  const Icon = item.icon
                  const isActive = item.key === activeView
                  return (
                    <button
                      key={item.key}
                      type="button"
                      aria-current={isActive ? 'page' : undefined}
                      onClick={() => navigate(item.path)}
                      className={`flex w-full items-start gap-3 rounded-lg border px-3 py-3 text-left transition focus:outline-none focus-visible:ring-2 focus-visible:ring-stone-500 focus-visible:ring-offset-2 focus-visible:ring-offset-[#F7F7F5] ${
                        isActive
                          ? 'border-stone-900 bg-white text-stone-900 shadow-sm'
                          : 'border-transparent bg-transparent text-stone-600 hover:border-stone-200 hover:bg-white'
                      }`}
                    >
                      <div className={`flex h-9 w-9 shrink-0 items-center justify-center rounded-lg ${isActive ? 'bg-stone-900 text-white' : 'bg-stone-100 text-stone-600'}`}>
                        <Icon size={16} />
                      </div>
                      <div className="min-w-0 flex-1">
                        <div className="flex items-center justify-between gap-2">
                          <span className="truncate text-sm font-medium">{item.label}</span>
                          {isActive && (
                            <span className="rounded-md border border-stone-200 bg-stone-50 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.16em] text-stone-500">
                              Open
                            </span>
                          )}
                        </div>
                        <p className="mt-1 text-xs leading-5 text-stone-500">{item.description}</p>
                      </div>
                    </button>
                  )
                })}
              </div>
            </div>
          ))}
        </nav>

        <div className="border-t border-stone-200 p-4">
          <div className="mb-4 rounded-lg border border-stone-200 bg-white px-3 py-3">
            <p className="text-sm font-medium text-stone-500">Logged in as</p>
            <p className="mt-1 break-words text-sm font-mono text-stone-900">{user?.username ?? 'Unknown'}</p>
            <p className="mt-1 text-xs uppercase text-stone-400">{user?.roles?.join(', ') ?? 'No roles'}</p>
          </div>
          <button
            type="button"
            onClick={handleLogout}
            className="flex w-full items-center justify-center gap-2 rounded-lg border border-stone-200 bg-white px-4 py-2 text-sm text-stone-900 transition-colors hover:bg-stone-100"
          >
            <LogOut size={16} />
            Sign Out
          </button>
        </div>
      </aside>

      <main className="flex flex-1 flex-col overflow-hidden bg-white">
        <header className="sticky top-0 z-10 border-b border-stone-200 bg-white px-8 py-4">
          <div className="flex flex-wrap items-center justify-between gap-4">
            <div className="min-w-0">
              <p className="text-[11px] font-semibold uppercase tracking-[0.22em] text-stone-400">
                {activeItem?.group === 'showcase' ? 'Showcase' : activeItem?.group === 'system' ? 'System' : 'Admin'}
              </p>
              <h2 className="mt-1 text-xl font-semibold text-stone-900">
                {isUnknownPath ? 'Not Found' : activeItem?.headerTitle ?? 'Launchpad'}
              </h2>
              <p className="mt-1 max-w-3xl text-sm text-stone-500">
                {isUnknownPath
                  ? `The path ${pathname} does not map to a dashboard view.`
                  : activeItem?.description ?? 'Select a view to continue.'}
              </p>
            </div>
          </div>
        </header>

        <div className="flex-1 overflow-auto bg-white p-8">
          {renderView()}
        </div>
      </main>
    </div>
  )
}
