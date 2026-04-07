import { useCallback, useEffect, useRef, useState } from 'react'

type PollingReason = 'initial' | 'manual' | 'poll'

type UsePollingResourceOptions<T> = {
  pollIntervalMs?: number
  initialData?: T | null
}

type UsePollingResourceResult<T> = {
  data: T | null
  loading: boolean
  refreshing: boolean
  error: string | null
  lastUpdated: Date | null
  stale: boolean
  refresh: () => Promise<void>
}

export function usePollingResource<T>(
  loader: () => Promise<T>,
  options: UsePollingResourceOptions<T> = {},
): UsePollingResourceResult<T> {
  const { pollIntervalMs, initialData = null } = options
  const [data, setData] = useState<T | null>(initialData)
  const [loading, setLoading] = useState(initialData == null)
  const [refreshing, setRefreshing] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)
  const mounted = useRef(true)
  const hasLoadedRef = useRef(initialData != null)
  const loaderRef = useRef(loader)

  useEffect(() => {
    loaderRef.current = loader
  }, [loader])

  const runLoad = useCallback(async (reason: PollingReason) => {
    const firstLoad = reason === 'initial' && !hasLoadedRef.current
    if (firstLoad) {
      setLoading(true)
    } else {
      setRefreshing(true)
    }

    try {
      const next = await loaderRef.current()
      if (!mounted.current) return
      setData(next)
      setError(null)
      setLastUpdated(new Date())
      hasLoadedRef.current = true
    } catch (err) {
      if (!mounted.current) return
      setError(err instanceof Error ? err.message : 'Request failed')
    } finally {
      if (!mounted.current) return
      setLoading(false)
      setRefreshing(false)
    }
  }, [])

  useEffect(() => {
    mounted.current = true
    void runLoad('initial')

    if (!pollIntervalMs) {
      return () => {
        mounted.current = false
      }
    }

    const interval = window.setInterval(() => {
      void runLoad('poll')
    }, pollIntervalMs)

    return () => {
      mounted.current = false
      window.clearInterval(interval)
    }
  }, [pollIntervalMs, runLoad])

  const refresh = useCallback(async () => {
    await runLoad('manual')
  }, [runLoad])

  return {
    data,
    loading,
    refreshing,
    error,
    lastUpdated,
    stale: Boolean(error && data != null),
    refresh,
  }
}
