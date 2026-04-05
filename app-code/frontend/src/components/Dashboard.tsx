import React, { useEffect, useMemo } from 'react'
import { Database, Cpu, Bot, Settings, LogOut, LayoutDashboard, Shield, CircleSlash } from 'lucide-react'
import { useAuthStore } from '../store/useAuthStore'
import { DataCatalog } from './views/DataCatalog'
import { ComputePipelines } from './views/ComputePipelines'
import { AgentHub } from './views/AgentHub'
import { InfraIframes } from './views/InfraIframes'
import { SystemOverseer } from './views/SystemOverseer'
import { useRoutePath } from '../hooks/useRoutePath'

type ViewType = 'data' | 'compute' | 'agents' | 'infra' | 'overseer'

type NavItem = {
    key: ViewType
    path: string
    label: string
    icon: React.ComponentType<{ size?: number; className?: string }>
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

    // Determine visibility based on IAM roles
    const canViewCompute = user?.permissions?.includes('compute:read')
    const canViewInfra = user?.roles?.includes('Admin')
    const navItems = useMemo<NavItem[]>(() => ([
        { key: 'data', path: '/data', label: 'Data Catalog', icon: LayoutDashboard, visible: true },
        { key: 'compute', path: '/compute', label: 'Compute & Pipelines', icon: Cpu, visible: Boolean(canViewCompute) },
        { key: 'agents', path: '/agents', label: 'AI Agents', icon: Bot, visible: true },
        { key: 'overseer', path: '/overseer', label: 'System Overseer', icon: Shield, visible: true },
        { key: 'infra', path: '/infra', label: 'Infrastructure', icon: Settings, visible: Boolean(canViewInfra) },
    ]), [canViewCompute, canViewInfra])

    const visibleItems = navItems.filter(item => item.visible)
    const defaultPath = visibleItems[0]?.path ?? '/data'
    const knownItem = navItems.find(item => item.path === pathname) ?? null
    const activeItem = visibleItems.find(item => item.path === pathname) ?? visibleItems[0]
    const activeView = activeItem?.key ?? 'data'
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
                <div className="mx-auto flex max-w-2xl flex-col items-center justify-center rounded-3xl border border-dashed border-stone-300 bg-stone-50 px-8 py-20 text-center">
                    <div className="mb-5 rounded-2xl bg-stone-900 p-4 text-white">
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
                        className="mt-6 rounded-full border border-stone-200 bg-white px-5 py-2.5 text-sm font-medium text-stone-700 transition hover:bg-stone-100"
                    >
                        Return to Launchpad
                    </button>
                </div>
            )
        }

        switch (activeView) {
            case 'data': return <DataCatalog />
            case 'compute': return <ComputePipelines />
            case 'agents': return <AgentHub />
            case 'infra': return <InfraIframes />
            case 'overseer': return <SystemOverseer />
            default: return <DataCatalog />
        }
    }

    return (
        <div className="flex h-screen bg-white text-stone-900 font-sans">
            {/* Sidebar */}
            <aside className="w-64 bg-[#F7F7F5] border-r border-stone-200 flex flex-col">
                <div className="p-6 flex items-center gap-3 border-b border-stone-200">
                    <Database className="text-stone-900" />
                    <h1 className="font-bold text-lg text-stone-900">Launchpad</h1>
                </div>

                <nav className="flex-1 p-4 space-y-2">
                    {visibleItems.map(item => {
                        const Icon = item.icon
                        const isActive = item.key === activeView
                        return (
                            <button
                                key={item.key}
                                onClick={() => navigate(item.path)}
                                className={`w-full flex items-center gap-3 px-4 py-3 rounded transition-colors ${isActive ? 'bg-stone-200 text-stone-900 font-medium' : 'hover:bg-stone-200/50 text-stone-600'}`}
                            >
                                <Icon size={20} />
                                {item.label}
                            </button>
                        )
                    })}
                </nav>

                <div className="p-4 border-t border-stone-200">
                    <div className="mb-4 px-2">
                        <p className="text-sm font-medium text-stone-500">Logged in as</p>
                        <p className="text-sm text-stone-900 font-mono mt-1">{user?.username}</p>
                        <p className="text-xs text-stone-400 mt-1 uppercase">{user?.roles.join(', ')}</p>
                    </div>
                    <button
                        onClick={handleLogout}
                        className="w-full flex items-center justify-center gap-2 px-4 py-2 bg-white border border-stone-200 hover:bg-stone-100 rounded text-sm text-stone-900 transition-colors"
                    >
                        <LogOut size={16} />
                        Sign Out
                    </button>
                </div>
            </aside>

            {/* Main Content Area */}
            <main className="flex-1 flex flex-col overflow-hidden bg-white">
                <header className="h-16 bg-white border-b border-stone-200 flex items-center px-8 z-10 sticky top-0">
                    <h2 className="text-xl font-semibold text-stone-900 capitalize">
                        {isUnknownPath
                            ? 'Not Found'
                            : activeView === 'infra'
                                ? 'Infrastructure Overviews'
                                : (activeView === 'overseer' ? 'Autonomic Controller' : activeItem?.label ?? 'Launchpad')}
                    </h2>
                </header>
                <div className="flex-1 overflow-auto p-8 bg-white">
                    {renderView()}
                </div>
            </main>

        </div>
    )
}
