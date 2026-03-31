import React, { useState } from 'react'
import { Database, Cpu, Bot, Settings, LogOut, LayoutDashboard, Shield } from 'lucide-react'
import { useAuthStore } from '../store/useAuthStore'
import { DataCatalog } from './views/DataCatalog'
import { ComputePipelines } from './views/ComputePipelines'
import { AgentHub } from './views/AgentHub'
import { InfraIframes } from './views/InfraIframes'
import { SystemOverseer } from './views/SystemOverseer'

type ViewType = 'data' | 'compute' | 'agents' | 'infra' | 'overseer'

export const Dashboard: React.FC = () => {
    const { user, setUser } = useAuthStore()
    const [activeView, setActiveView] = useState<ViewType>('data')

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

    const renderView = () => {
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
                    <button
                        onClick={() => setActiveView('data')}
                        className={`w-full flex items-center gap-3 px-4 py-3 rounded transition-colors ${activeView === 'data' ? 'bg-stone-200 text-stone-900 font-medium' : 'hover:bg-stone-200/50 text-stone-600'}`}
                    >
                        <LayoutDashboard size={20} />
                        Data Catalog
                    </button>

                    {canViewCompute && (
                        <button
                            onClick={() => setActiveView('compute')}
                            className={`w-full flex items-center gap-3 px-4 py-3 rounded transition-colors ${activeView === 'compute' ? 'bg-stone-200 text-stone-900 font-medium' : 'hover:bg-stone-200/50 text-stone-600'}`}
                        >
                            <Cpu size={20} />
                            Compute & Pipelines
                        </button>
                    )}

                    <button
                        onClick={() => setActiveView('agents')}
                        className={`w-full flex items-center gap-3 px-4 py-3 rounded transition-colors ${activeView === 'agents' ? 'bg-stone-200 text-stone-900 font-medium' : 'hover:bg-stone-200/50 text-stone-600'}`}
                    >
                        <Bot size={20} />
                        AI Agents
                    </button>

                    <button
                        onClick={() => setActiveView('overseer')}
                        className={`w-full flex items-center gap-3 px-4 py-3 rounded transition-colors ${activeView === 'overseer' ? 'bg-stone-200 text-stone-900 font-medium' : 'hover:bg-stone-200/50 text-stone-600'}`}
                    >
                        <Shield size={20} />
                        System Overseer
                    </button>

                    {canViewInfra && (
                        <button
                            onClick={() => setActiveView('infra')}
                            className={`w-full flex items-center gap-3 px-4 py-3 rounded transition-colors ${activeView === 'infra' ? 'bg-stone-200 text-stone-900 font-medium' : 'hover:bg-stone-200/50 text-stone-600'}`}
                        >
                            <Settings size={20} />
                            Infrastructure
                        </button>
                    )}
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
                        {activeView === 'infra' ? 'Infrastructure Overviews' : (activeView === 'overseer' ? 'Autonomic Controller' : activeView)}
                    </h2>
                </header>
                <div className="flex-1 overflow-auto p-8 bg-white">
                    {renderView()}
                </div>
            </main>

        </div>
    )
}
