/**
 * API Client for interacting with the FastAPI Gateway.
 * Assumes the frontend is served from the same domain (or using Vite proxy),
 * so cookies (`gateway_token`) are automatically included in requests.
 */

// Basic error class for API failures
export class APIError extends Error {
    status: number;
    constructor(status: number, message: string) {
        super(message)
        this.status = status
    }
}

/**
 * Send a Universal Intent to the Gateway.
 */
export async function sendIntent(domain: string, action: string, parameters: Record<string, any> = {}) {
    const res = await fetch('/api/v1/intent', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ domain, action, parameters }),
    })

    const data = await res.json()
    if (!res.ok) {
        throw new APIError(res.status, data.detail || 'Intent execution failed')
    }
    return data
}

/**
 * List registered agents exposed by the gateway.
 */
export async function fetchAgents() {
    const data = await sendIntent('agent', 'list')
    return data.agents ?? []
}

/**
 * Verify current session and fetch user details.
 */
export async function fetchMe() {
    const res = await fetch('/api/v1/auth/me', { credentials: 'include' })
    if (!res.ok) return null
    return res.json()
}

/**
 * Fetch historical heartbeat snapshots from the Overseer.
 */
export async function fetchOverseerSnapshots(n: number = 50) {
    const res = await fetch(`/api/v1/system/overseer/snapshots?n=${n}`, { credentials: 'include' })
    if (!res.ok) {
        const data = await res.json()
        throw new APIError(res.status, data.detail || 'Failed to fetch snapshots')
    }
    return res.json()
}

/**
 * Fetch recent alert logs from the Overseer.
 */
export async function fetchOverseerAlerts(n: number = 20) {
    const res = await fetch(`/api/v1/system/overseer/alerts?n=${n}`, { credentials: 'include' })
    if (!res.ok) {
        const data = await res.json()
        throw new APIError(res.status, data.detail || 'Failed to fetch alerts')
    }
    return res.json()
}
