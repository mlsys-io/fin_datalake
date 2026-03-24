import { useEffect } from 'react'
import { useAuthStore } from './store/useAuthStore'
import { fetchMe } from './api/client'
import { Login } from './components/Login'
import { Dashboard } from './components/Dashboard'

function App() {
  const { isAuthenticated, isLoading, setUser } = useAuthStore()

  // On mount, check if there's a valid gateway_token cookie
  useEffect(() => {
    const verifySession = async () => {
      const userProfile = await fetchMe()
      setUser(userProfile) // sets user and flips isLoading to false
    }
    verifySession()
  }, [setUser])

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
