import React from 'react'
import { Activity, ExternalLink } from 'lucide-react'

export const ComputePipelines: React.FC = () => {
    return (
        <div className="h-full flex flex-col space-y-4">
            <div className="flex items-center justify-between pb-4 border-b border-slate-700">
                <div>
                    <h3 className="text-xl font-bold text-slate-100 flex items-center gap-2">
                        <Activity className="text-indigo-400" />
                        Compute & Pipelines
                    </h3>
                    <p className="text-slate-400 mt-1 text-sm">Native Prefect Orchestration Dashboard</p>
                </div>
            </div>

            <div className="flex-1 bg-slate-900 rounded-xl border border-slate-700 overflow-hidden relative shadow-inner w-full min-h-[600px]">
                {/* 
                  * In production (via Nginx): src="/prefect/" — auth_request bouncer enforces login.
                  * In local dev without Nginx: src="http://localhost:4200"
                  * To test with Nginx, start Nginx and change this to /prefect/
                */}
                <iframe
                    src="/prefect/"
                    className="w-full h-full border-none"
                    title="Prefect Dashboard"
                />
            </div>
            <div className="flex items-center gap-2 text-xs text-slate-500">
                <ExternalLink size={12} />
                <span>Proxied via Nginx Bouncer — authenticated session required. <a href="http://localhost:4200" target="_blank" rel="noreferrer" className="text-indigo-400 hover:underline">Open directly (dev only)</a></span>
            </div>
        </div>
    )
}
