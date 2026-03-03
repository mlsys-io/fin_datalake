import React, { useState } from 'react'
import { Database, Cpu, Bot, Settings, LogOut, LayoutDashboard } from 'lucide-react'
import { useAuthStore } from '../store/useAuthStore'
import { DataCatalog } from './views/DataCatalog'
import { ComputePipelines } from './views/ComputePipelines'
import { AgentHub } from './views/AgentHub'
import { InfraIframes } from './views/InfraIframes'

type ViewType = 'data' | 'compute' | 'agents' | 'infra'

export const Dashboard: React.FC = () => {
    const { user, setUser } = useAuthStore()
    const [activeView, setActiveView] = useState<ViewType>('data')

    const handleLogout = async () => {
        try {
            await fetch('/api/v1/auth/logout', { method: 'POST' })
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
            default: return <DataCatalog />
        }
    }

    return (
        <div className="flex h-screen bg-slate-900 text-slate-200 font-sans">
            {/* Sidebar */}
            <aside className="w-64 bg-slate-800 border-r border-slate-700 flex flex-col">
                <div className="p-6 flex items-center gap-3 border-b border-slate-700">
                    <Database className="text-indigo-400" />
                    <h1 className="font-bold text-lg text-slate-100">Launchpad</h1>
                </div>

                <nav className="flex-1 p-4 space-y-2">
                    <button
                        onClick={() => setActiveView('data')}
                        className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg transition-colors ${activeView === 'data' ? 'bg-indigo-600 text-white' : 'hover:bg-slate-700 text-slate-300'}`}
                    >
                        <LayoutDashboard size={20} />
                        Data Catalog
                    </button>

                    {canViewCompute && (
                        <button
                            onClick={() => setActiveView('compute')}
                            className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg transition-colors ${activeView === 'compute' ? 'bg-indigo-600 text-white' : 'hover:bg-slate-700 text-slate-300'}`}
                        >
                            <Cpu size={20} />
                            Compute & Pipelines
                        </button>
                    )}

                    <button
                        onClick={() => setActiveView('agents')}
                        className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg transition-colors ${activeView === 'agents' ? 'bg-indigo-600 text-white' : 'hover:bg-slate-700 text-slate-300'}`}
                    >
                        <Bot size={20} />
                        AI Agents
                    </button>

                    {canViewInfra && (
                        <button
                            onClick={() => setActiveView('infra')}
                            className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg transition-colors ${activeView === 'infra' ? 'bg-indigo-600 text-white' : 'hover:bg-slate-700 text-slate-300'}`}
                        >
                            <Settings size={20} />
                            Infrastructure
                        </button>
                    )}
                </nav>

                <div className="p-4 border-t border-slate-700">
                    <div className="mb-4 px-2">
                        <p className="text-sm font-medium text-slate-300">Logged in as</p>
                        <p className="text-xs text-indigo-400 font-mono mt-1">{user?.username}</p>
                        <p className="text-xs text-slate-500 mt-1 uppercase">{user?.roles.join(', ')}</p>
                    </div>
                    <button
                        onClick={handleLogout}
                        className="w-full flex items-center justify-center gap-2 px-4 py-2 bg-slate-700 hover:bg-slate-600 rounded-lg text-sm text-slate-200 transition-colors"
                    >
                        <LogOut size={16} />
                        Sign Out
                    </button>
                </div>
            </aside>

            {/* Main Content Area */}
            <main className="flex-1 flex flex-col overflow-hidden">
                <header className="h-16 bg-slate-800/50 border-b border-slate-700 flex items-center px-8 backdrop-blur-sm">
                    <h2 className="text-xl font-semibold text-slate-100 capitalize">
                        {activeView === 'infra' ? 'Infrastructure Overviews' : activeView}
                    </h2>
                </header>
                <div className="flex-1 overflow-auto p-8">
                    {renderView()}
                </div>
            </main>
        </div>
    )
}
