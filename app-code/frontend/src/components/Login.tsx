import React, { useState } from 'react'
import { Database } from 'lucide-react'
import { fetchMe } from '../api/client'
import { useAuthStore } from '../store/useAuthStore'

export const Login: React.FC = () => {
    const [username, setUsername] = useState('')
    const [password, setPassword] = useState('')
    const [error, setError] = useState('')
    const [loading, setLoading] = useState(false)

    const setUser = useAuthStore((state) => state.setUser)

    const handleLogin = async (e: React.FormEvent) => {
        e.preventDefault()
        setError('')
        setLoading(true)

        try {
            const res = await fetch('/api/v1/auth/login', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ username, password })
            })

            if (!res.ok) {
                const errData = await res.json()
                throw new Error(errData.detail || 'Login failed')
            }

            // If login succeeded, fetch the user profile
            const userProfile = await fetchMe()
            if (userProfile) {
                setUser(userProfile)
            } else {
                throw new Error('Could not fetch user profile')
            }
        } catch (err: any) {
            setError(err.message)
        } finally {
            setLoading(false)
        }
    }

    return (
        <div className="min-h-screen flex items-center justify-center bg-[#F7F7F5]">
            <div className="max-w-md w-full p-8 bg-white rounded-lg shadow-sm border border-stone-200">
                <div className="flex justify-center mb-8 text-stone-900">
                    <Database size={48} />
                </div>

                <h2 className="text-3xl font-bold text-center text-stone-900 mb-8 tracking-tight">
                    Lakehouse Launchpad
                </h2>

                {error && (
                    <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded text-red-700 text-sm text-center">
                        {error}
                    </div>
                )}

                <form onSubmit={handleLogin} className="space-y-6">
                    <div>
                        <label className="block text-sm font-medium text-stone-700 mb-2">Username</label>
                        <input
                            type="text"
                            required
                            className="w-full px-4 py-3 bg-white border border-stone-200 rounded text-stone-900 placeholder-stone-400 focus:outline-none focus:ring-2 focus:ring-stone-400 focus:border-transparent transition"
                            placeholder="e.g. garret"
                            value={username}
                            onChange={(e) => setUsername(e.target.value)}
                        />
                    </div>

                    <div>
                        <label className="block text-sm font-medium text-stone-700 mb-2">Password</label>
                        <input
                            type="password"
                            required
                            className="w-full px-4 py-3 bg-white border border-stone-200 rounded text-stone-900 placeholder-stone-400 focus:outline-none focus:ring-2 focus:ring-stone-400 focus:border-transparent transition"
                            placeholder="••••••••"
                            value={password}
                            onChange={(e) => setPassword(e.target.value)}
                        />
                    </div>

                    <button
                        type="submit"
                        disabled={loading}
                        className="w-full py-3 px-4 bg-stone-900 hover:bg-stone-800 disabled:opacity-50 disabled:cursor-not-allowed text-white font-medium rounded shadow-sm transition-all focus:outline-none focus:ring-2 focus:ring-stone-500 focus:ring-offset-1 focus:ring-offset-white"
                    >
                        {loading ? 'Authenticating...' : 'Sign In'}
                    </button>
                </form>
            </div>
        </div>
    )
}
