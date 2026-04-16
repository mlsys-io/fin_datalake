import { useEffect } from 'react'
import { useAuthStore } from './store/useAuthStore'
import { fetchMe } from './api/client'
import { Login } from './components/Login'
import { Dashboard } from './components/Dashboard'

const DEMO_USER = {
  username: 'demo',
  email: null,
  roles: ['Admin'],
  permissions: ['compute:read', 'data:read', 'agent:read', 'agent:invoke', 'system:read'],
}

function App() {
  const { isAuthenticated, isLoading, setUser } = useAuthStore()
  const demoAuthEnabled = import.meta.env.VITE_DEMO_AUTH === 'true'

  // On mount, check if there's a valid gateway_token cookie
  useEffect(() => {
    if (demoAuthEnabled) {
      setUser(DEMO_USER)
      return
    }

    const verifySession = async () => {
      const userProfile = await fetchMe()
      setUser(userProfile) // sets user and flips isLoading to false
    }
    verifySession()
  }, [demoAuthEnabled, setUser])

  if (isLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-[#F7F7F5] text-stone-500">
        <div className="animate-pulse">Loading Lakehouse Environment...</div>
      </div>
    )
  }

  if (!isAuthenticated) {
    return <Login />
  }

  return <Dashboard />
}

export default App
