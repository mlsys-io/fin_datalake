import React, { useState, useRef } from 'react'
import { Bot, Send } from 'lucide-react'
import { sendIntent } from '../../api/client'

export const AgentHub: React.FC = () => {
    const [messages, setMessages] = useState<{ id: number, role: 'user' | 'agent', content: string }[]>([])
    const [input, setInput] = useState('')
    const [loading, setLoading] = useState(false)
    const msgIdRef = useRef(0)

    const handleSend = async (e: React.FormEvent) => {
        e.preventDefault()
        if (!input.trim()) return

        const userMsg = input
        setMessages(prev => [...prev, { id: msgIdRef.current++, role: 'user', content: userMsg }])
        setInput('')
        setLoading(true)

        try {
            const res = await sendIntent('agent', 'chat', { agent_name: 'Coordinator', message: userMsg })
            setMessages(prev => [...prev, { id: msgIdRef.current++, role: 'agent', content: res.response || JSON.stringify(res) }])
        } catch (err: any) {
            setMessages(prev => [...prev, { id: msgIdRef.current++, role: 'agent', content: `[Error communicating with agent]: ${err.message}` }])
        } finally {
            setLoading(false)
        }
    }

    return (
        <div className="max-w-4xl mx-auto h-[calc(100vh-12rem)] flex flex-col bg-white rounded-lg border border-stone-200 shadow-sm overflow-hidden">
            <div className="p-4 border-b border-stone-200 bg-white/80 backdrop-blur flex items-center gap-3">
                <div className="w-10 h-10 rounded-full bg-stone-100 flex items-center justify-center text-stone-700">
                    <Bot size={24} />
                </div>
                <div>
                    <h3 className="font-bold text-stone-900">Coordinator Agent</h3>
                    <p className="text-xs text-stone-400 font-medium">Ray Cluster Agent</p>
                </div>
            </div>

            <div className="flex-1 overflow-auto p-6 space-y-4 bg-[#F7F7F5]">
                {messages.length === 0 && (
                    <div className="text-center text-stone-500 mt-10">
                        Start a conversation with the Ray-backed LLM agents.
                    </div>
                )}
                {messages.map((m) => (
                    <div key={m.id} className={`flex ${m.role === 'user' ? 'justify-end' : 'justify-start'}`}>
                        <div className={`max-w-[80%] rounded-2xl px-5 py-3 shadow-sm ${m.role === 'user' ? 'bg-stone-900 text-white rounded-br-none' : 'bg-white border border-stone-200 text-stone-900 rounded-bl-none'}`}>
                            {m.content}
                        </div>
                    </div>
                ))}
                {loading && (
                    <div className="flex justify-start">
                        <div className="bg-white border border-stone-200 shadow-sm rounded-2xl rounded-bl-none px-5 py-3 h-12 w-16 flex items-center justify-center gap-1">
                            <div className="w-2 h-2 bg-stone-300 rounded-full animate-bounce"></div>
                            <div className="w-2 h-2 bg-stone-300 rounded-full animate-bounce" style={{ animationDelay: '150ms' }}></div>
                            <div className="w-2 h-2 bg-stone-300 rounded-full animate-bounce" style={{ animationDelay: '300ms' }}></div>
                        </div>
                    </div>
                )}
            </div>

            <div className="p-4 bg-white border-t border-stone-200">
                <form onSubmit={handleSend} className="relative flex items-center shadow-sm rounded-full">
                    <input
                        type="text"
                        className="flex-1 bg-white border border-stone-200 rounded-full pl-6 pr-14 py-3 text-stone-900 placeholder-stone-400 focus:outline-none focus:ring-2 focus:ring-stone-400"
                        placeholder="Ask the Coordinator agent..."
                        value={input}
                        onChange={e => setInput(e.target.value)}
                        disabled={loading}
                    />
                    <button
                        type="submit"
                        disabled={loading || !input.trim()}
                        className="absolute right-2 p-2 bg-stone-900 text-white rounded-full hover:bg-stone-800 disabled:opacity-50 disabled:hover:bg-stone-900 transition"
                    >
                        <Send size={18} />
                    </button>
                </form>
            </div>
        </div>
    )
}
