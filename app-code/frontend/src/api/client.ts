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

export type AgentCapabilitySpec = {
    id: string
    description: string
    tags?: string[]
    aliases?: string[]
    input_type?: string | null
    output_type?: string | null
    interaction_mode?: string | null
}

export type AgentSummary = {
    name: string
    capabilities?: string[]
    capability_specs?: AgentCapabilitySpec[]
    metadata?: Record<string, unknown>
    deployment_metadata?: Record<string, unknown>
    registered_at?: string
    last_seen_at?: string
    last_heartbeat_at?: string
    status?: string
    managed_by_overseer?: boolean
    desired_status?: string
    observed_status?: string
    health_status?: string
    recovery_state?: string
    last_reconciled_at?: string
    last_failure_reason?: string | null
    last_action_type?: string | null
    reconcile_notes?: string | null
    runtime_source?: string | null
    runtime_namespace?: string | null
    route_prefix?: string | null
    source?: string
    alive?: boolean
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
    const res = await fetch('/api/v1/agents', { credentials: 'include' })
    const data = await res.json()
    if (!res.ok) {
        throw new APIError(res.status, data.detail || 'Failed to fetch agents')
    }
    return (data.agents ?? []) as AgentSummary[]
}

/**
 * Send a chat-style message to an agent.
 */
export async function chatWithAgent(agentName: string, message: string, sessionId?: string) {
    const res = await fetch(`/api/v1/agents/${encodeURIComponent(agentName)}/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({
        message,
        session_id: sessionId,
        }),
    })
    const data = await res.json()
    if (!res.ok) {
        throw new APIError(res.status, data.detail || 'Agent chat failed')
    }
    return data
}

/**
 * Send an arbitrary payload to an agent's invoke path.
 */
export async function invokeAgent(agentName: string, payload: unknown, sessionId?: string) {
    const res = await fetch(`/api/v1/agents/${encodeURIComponent(agentName)}/invoke`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({
        payload,
        session_id: sessionId,
        }),
    })
    const data = await res.json()
    if (!res.ok) {
        throw new APIError(res.status, data.detail || 'Agent invoke failed')
    }
    return data
}

/**
 * Broadcast an event to all alive agents.
 */
export async function broadcastAgentEvent(payload: Record<string, unknown>) {
    const res = await fetch('/api/v1/agents/broadcast', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ payload }),
    })
    const data = await res.json()
    if (!res.ok) {
        throw new APIError(res.status, data.detail || 'Agent broadcast failed')
    }
    return data
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

/**
 * Probe internal dashboard targets such as Prefect, Ray, and MinIO.
 */
export async function fetchInfraStatus() {
    const res = await fetch('/api/v1/system/infra/status', { credentials: 'include' })
    const data = await res.json()
    if (!res.ok) {
        throw new APIError(res.status, data.detail || 'Failed to fetch infrastructure status')
    }
    return data as {
        targets: Record<string, {
            ok: boolean
            status_code: number | null
            url: string
            detail: string | null
        }>
    }
}
