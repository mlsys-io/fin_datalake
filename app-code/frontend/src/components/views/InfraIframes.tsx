import React, { useState } from 'react'
import { Server, MonitorPlay } from 'lucide-react'

// In production (via Nginx on port 8080): iframes load through the auth bouncer.
// In local dev (no Nginx): open localUrl directly in a separate tab.
const IFRAMES = [
    { id: 'prefect', name: 'Prefect Dashboard', icon: MonitorPlay, url: '/prefect/', localUrl: 'http://localhost:4200' },
    { id: 'ray', name: 'Ray Cluster UI', icon: Server, url: '/ray/', localUrl: 'http://localhost:8265' },
]

export const InfraIframes: React.FC = () => {
    const [activeIframe, setActiveIframe] = useState(IFRAMES[0])

    return (
        <div className="h-full flex flex-col space-y-4">
            <div className="flex gap-4 border-b border-stone-200 pb-4">
                {IFRAMES.map(f => (
                    <button
                        key={f.id}
                        onClick={() => setActiveIframe(f)}
                        className={`flex items-center gap-2 px-4 py-2 rounded-lg transition-colors ${activeIframe.id === f.id
                            ? 'bg-stone-900 text-white ring-1 ring-stone-400'
                            : 'bg-white border border-stone-200 hover:bg-stone-100 text-stone-600'
                            }`}
                    >
                        <f.icon size={16} />
                        {f.name}
                    </button>
                ))}
            </div>

            <div className="flex-1 bg-[#F7F7F5] rounded-xl border border-stone-200 overflow-hidden relative shadow-sm">
                {/* Placeholder overlay for dev environment if services aren't running */}
                <div className="absolute inset-0 bg-white/50 backdrop-blur-sm flex items-center justify-center p-8 text-center pointer-events-none z-10 opacity-0 hover:opacity-100 transition-opacity">
                    <div className="bg-white p-6 rounded-lg border border-stone-200 shadow-sm">
                        <h3 className="text-xl font-bold text-stone-900 mb-2">Native UI Passthrough</h3>
                        <p className="text-stone-500 text-sm max-w-md">
                            In production, this iframe loads internal dashboards secured by the Nginx Bouncer `auth_request` directive. If it refuses to connect locally, ensure the target service is running.
                        </p>
                    </div>
                </div>

                <iframe
                    key={activeIframe.id}
                    src={activeIframe.url}
                    className="w-full h-full border-none"
                    title={activeIframe.name}
                />
            </div>
        </div>
    )
}
