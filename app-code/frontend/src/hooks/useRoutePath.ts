import { useCallback, useEffect, useState } from 'react'

export function useRoutePath() {
  const [pathname, setPathname] = useState(() => window.location.pathname || '/')

  useEffect(() => {
    const handlePopState = () => setPathname(window.location.pathname || '/')
    window.addEventListener('popstate', handlePopState)
    return () => window.removeEventListener('popstate', handlePopState)
  }, [])

  const navigate = useCallback((nextPath: string, options?: { replace?: boolean }) => {
    const normalized = nextPath.startsWith('/') ? nextPath : `/${nextPath}`
    if (normalized === window.location.pathname) {
      setPathname(normalized)
      return
    }

    if (options?.replace) {
      window.history.replaceState({}, '', normalized)
    } else {
      window.history.pushState({}, '', normalized)
    }
    setPathname(normalized)
  }, [])

  return { pathname, navigate }
}
