import React, { useState } from 'react'
import { Bot, Send } from 'lucide-react'
import { sendIntent } from '../../api/client'

export const AgentHub: React.FC = () => {
    const [messages, setMessages] = useState<{ role: 'user' | 'agent', content: string }[]>([])
    const [input, setInput] = useState('')
    const [loading, setLoading] = useState(false)

    const handleSend = async (e: React.FormEvent) => {
        e.preventDefault()
        if (!input.trim()) return

        const userMsg = input
        setMessages(prev => [...prev, { role: 'user', content: userMsg }])
        setInput('')
        setLoading(true)

        try {
            const res = await sendIntent('agent', 'chat', { agent_name: 'Coordinator', message: userMsg })
            setMessages(prev => [...prev, { role: 'agent', content: res.response || JSON.stringify(res) }])
        } catch (err: any) {
            setMessages(prev => [...prev, { role: 'agent', content: `[Error communicating with agent]: ${err.message}` }])
        } finally {
            setLoading(false)
        }
    }

    return (
        <div className="max-w-4xl mx-auto h-[calc(100vh-12rem)] flex flex-col bg-slate-800 rounded-xl border border-slate-700 shadow-sm overflow-hidden">
            <div className="p-4 border-b border-slate-700 bg-slate-800/80 backdrop-blur flex items-center gap-3">
                <div className="w-10 h-10 rounded-full bg-indigo-500/20 flex items-center justify-center text-indigo-400">
                    <Bot size={24} />
                </div>
                <div>
                    <h3 className="font-bold text-slate-100">Coordinator Agent</h3>
                    <p className="text-xs text-green-400">● Online in Ray Cluster</p>
                </div>
            </div>

            <div className="flex-1 overflow-auto p-6 space-y-4 bg-slate-900/50">
                {messages.length === 0 && (
                    <div className="text-center text-slate-500 mt-10">
                        Start a conversation with the Ray-backed LLM agents.
                    </div>
                )}
                {messages.map((m, i) => (
                    <div key={i} className={`flex ${m.role === 'user' ? 'justify-end' : 'justify-start'}`}>
                        <div className={`max-w-[80%] rounded-2xl px-5 py-3 ${m.role === 'user' ? 'bg-indigo-600 text-white rounded-br-none' : 'bg-slate-800 border border-slate-700 text-slate-200 rounded-bl-none'}`}>
                            {m.content}
                        </div>
                    </div>
                ))}
                {loading && (
                    <div className="flex justify-start">
                        <div className="bg-slate-800 border border-slate-700 rounded-2xl rounded-bl-none px-5 py-3 h-12 w-16 flex items-center justify-center gap-1">
                            <div className="w-2 h-2 bg-slate-400 rounded-full animate-bounce"></div>
                            <div className="w-2 h-2 bg-slate-400 rounded-full animate-bounce delay-100"></div>
                            <div className="w-2 h-2 bg-slate-400 rounded-full animate-bounce delay-200"></div>
                        </div>
                    </div>
                )}
            </div>

            <div className="p-4 bg-slate-800 border-t border-slate-700">
                <form onSubmit={handleSend} className="relative flex items-center">
                    <input
                        type="text"
                        className="flex-1 bg-slate-900 border border-slate-700 rounded-full pl-6 pr-14 py-3 text-slate-100 focus:outline-none focus:ring-2 focus:ring-indigo-500"
                        placeholder="Ask the Coordinator agent..."
                        value={input}
                        onChange={e => setInput(e.target.value)}
                        disabled={loading}
                    />
                    <button
                        type="submit"
                        disabled={loading || !input.trim()}
                        className="absolute right-2 p-2 bg-indigo-600 text-white rounded-full hover:bg-indigo-700 disabled:opacity-50 disabled:hover:bg-indigo-600 transition"
                    >
                        <Send size={18} />
                    </button>
                </form>
            </div>
        </div>
    )
}
