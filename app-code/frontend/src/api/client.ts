/**
 * API Client for interacting with the FastAPI Gateway.
 * Assumes the frontend is served from the same domain (or using Vite proxy),
 * so cookies (`gateway_token`) are automatically included in requests.
 */

// Basic error class for API failures
export class APIError extends Error {
    status: number;
    code: string;
    context: Record<string, unknown> | null;
    constructor(status: number, message: string, code: string = 'api_error', context: Record<string, unknown> | null = null) {
        super(message)
        this.status = status
        this.code = code
        this.context = context
    }
}

export type APIErrorEnvelope = {
    detail: string
    code: string
    context?: Record<string, unknown> | null
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
    state_source?: string
    state_updated_at?: string
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

export type AgentListResponse = {
    agents: AgentSummary[]
    available: boolean
    detail: string | null
}

export type OverseerServiceMetrics = {
    healthy: boolean
    data: unknown
    error: string | null
}

export type OverseerSnapshot = {
    timestamp: string | number
    services: Record<string, OverseerServiceMetrics>
}

export type OverseerAlert = {
    timestamp: string | number
    level: 'info' | 'warning' | 'error' | 'critical'
    type: string
    action: string
    target: string
    detail: string
    status?: string
    deployment_name?: string
    result_detail?: string | null
    result_error?: string | null
}

export type InfraStatusResponse = {
    targets: Record<string, {
        ok: boolean
        status_code: number | null
        url: string
        detail: string | null
    }>
}

export type ReadinessResponse = {
    ready: boolean
    timestamp: string
    checks: Record<string, {
        ready?: boolean
        configured?: boolean
        detail: string | null
    }>
}

export type SystemHealthResponse = {
    status?: string
    message?: string
    source?: string
    timestamp?: string
    components?: Record<string, {
        healthy?: boolean
        error?: string | null
    }>
}

export type SystemLogEntry = {
    time?: string
    component?: string
    level?: string
    message?: string
    trace_id?: string | null
    agent_name?: string | null
    metadata?: Record<string, unknown> | null
}

export type SystemLogsResponse = {
    logs?: SystemLogEntry[]
    count?: number
    available?: boolean
    error?: string | null
    query?: Record<string, unknown>
}

export type AuditLogEntry = {
    id?: string
    request_id?: string
    timestamp?: string
    user_id?: string
    domain?: string
    action?: string
    parameters?: Record<string, unknown> | unknown
    source_protocol?: string
    status_code?: number
    duration_ms?: number
    error_detail?: string | null
}

export type AuditLogsResponse = {
    audit_logs?: AuditLogEntry[]
    count?: number
    returned_count?: number
    query?: Record<string, unknown>
}

export type DataQueryResponse = {
    success?: boolean
    mode?: string
    backend?: string
    columns?: string[]
    rows?: unknown[][]
    row_count?: number
    error_type?: string
    error_message?: string
}

export type SystemLogFilters = {
    component?: string
    level?: string
    since?: string
    agent_name?: string
    trace_id?: string
    limit?: number
}

export type DataTableSummary = {
    name: string
    path: string
    family?: string
    source?: string
    source_type?: string
}

export type DataCatalogResponse = {
    tables?: DataTableSummary[]
    source?: string
    error?: string
}

export type CatalogSourceTable = {
    name: string
    path?: string
    schema?: string
    qualified_name?: string
    family?: string
}

export type CatalogSource = {
    id: string
    label: string
    kind: 'live' | 'static'
    status: 'available' | 'partial' | 'pending' | 'planned'
    detail: string
    tables: CatalogSourceTable[]
    source?: string
    source_family?: string
    source_type?: string
    error?: string
}

export type CatalogSourcesResponse = {
    generated_at?: string
    live_sources?: CatalogSource[]
    static_sources?: CatalogSource[]
    summary?: {
        total_sources?: number
        available_sources?: number
        total_tables?: number
    }
}

export type DataSchemaResponse = {
    table_path: string
    fields?: {
        name: string
        type: string
    }[]
}

export type InterfaceInventoryAction = {
    name: string
    description?: string
    permission?: string
    protocols?: string[]
    state?: string
    source?: string
}

export type InterfaceInventoryDomain = {
    name: string
    adapter?: string | null
    actions: InterfaceInventoryAction[]
    action_count?: number
    state?: string
    source?: string
}

export type InterfaceInventoryRoute = {
    method: string
    path: string
    domain?: string
    action?: string
    state?: string
    source?: string
}

export type InterfaceInventoryProxy = {
    name: string
    ok: boolean
    status_code?: number | null
    url?: string
    detail?: string | null
    state?: string
    source?: string
}

export type InterfaceInventoryResponse = {
    generated_at?: string
    domains?: InterfaceInventoryDomain[]
    mcp_tools?: {
        name: string
        description?: string
        domain?: string
        action?: string
        input_schema?: Record<string, unknown>
        state?: string
        source?: string
    }[]
    routes?: InterfaceInventoryRoute[]
    proxies?: InterfaceInventoryProxy[]
    summary?: {
        domains?: number
        actions?: number
        mcp_tools?: number
        routes?: number
        proxies?: number
    }
}

async function parseAPIError(response: Response, fallback: string): Promise<APIError> {
    let detail = fallback
    let code = 'api_error'
    let context: Record<string, unknown> | null = null

    try {
        const data = await response.json()
        if (typeof data?.detail === 'string') {
            detail = data.detail
            code = data.code ?? code
            context = data.context ?? null
        } else if (data?.detail && typeof data.detail === 'object') {
            detail = data.detail.detail ?? fallback
            code = data.detail.code ?? data.code ?? code
            context = data.detail.context ?? data.context ?? null
        }
    } catch {
        // Fall back to the provided message.
    }

    return new APIError(response.status, detail, code, context)
}

async function parseJSONOrThrow<T>(response: Response, fallback: string): Promise<T> {
    if (!response.ok) {
        throw await parseAPIError(response, fallback)
    }
    return response.json() as Promise<T>
}

/**
 * Send a Universal Intent to the Gateway.
 */
export async function sendIntent<T = unknown>(domain: string, action: string, parameters: Record<string, unknown> = {}) {
    const res = await fetch('/api/v1/intent', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ domain, action, parameters }),
    })

    return parseJSONOrThrow<T>(res, 'Intent execution failed')
}

/**
 * List registered agents exposed by the gateway.
 */
export async function fetchAgents() {
    const res = await fetch('/api/v1/agents', { credentials: 'include' })
    const data = await parseJSONOrThrow<AgentListResponse>(res, 'Failed to fetch agents')
    return data.agents ?? []
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
    return parseJSONOrThrow(res, 'Agent chat failed')
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
    return parseJSONOrThrow(res, 'Agent invoke failed')
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
    return parseJSONOrThrow<{ delivered_to: number; total_targets: number }>(res, 'Agent broadcast failed')
}

export async function loginWithPassword(username: string, password: string) {
    const res = await fetch('/api/v1/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ username, password }),
    })
    return parseJSONOrThrow(res, 'Login failed')
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
    return parseJSONOrThrow<OverseerSnapshot[]>(res, 'Failed to fetch snapshots')
}

/**
 * Fetch recent alert logs from the Overseer.
 */
export async function fetchOverseerAlerts(n: number = 20) {
    const res = await fetch(`/api/v1/system/overseer/alerts?n=${n}`, { credentials: 'include' })
    return parseJSONOrThrow<OverseerAlert[]>(res, 'Failed to fetch alerts')
}

/**
 * Probe internal dashboard targets such as Prefect, Ray, and MinIO.
 */
export async function fetchInfraStatus() {
    const res = await fetch('/api/v1/system/infra/status', { credentials: 'include' })
    return parseJSONOrThrow<InfraStatusResponse>(res, 'Failed to fetch infrastructure status')
}

export async function fetchReadiness() {
    const res = await fetch('/readyz', { credentials: 'include' })
    return parseJSONOrThrow<ReadinessResponse>(res, 'Failed to fetch gateway readiness')
}

export async function fetchSystemHealth() {
    return sendIntent<SystemHealthResponse>('system', 'health')
}

export async function fetchSystemLogs(filters: SystemLogFilters = {}) {
    return sendIntent<SystemLogsResponse>('system', 'logs', filters)
}

export async function fetchAuditLogs(filters: {
    since?: string
    limit?: number
    request_id?: string
    source_protocol?: string
    domain?: string
    action?: string
    status_code?: number
    user_id?: string
} = {}) {
    const res = await fetch('/api/v1/system/audit-logs?' + new URLSearchParams(
        Object.entries(filters)
            .filter(([, value]) => value !== undefined && value !== null && value !== '')
            .map(([key, value]) => [key, String(value)]),
    ).toString(), { credentials: 'include' })
    return parseJSONOrThrow<AuditLogsResponse>(res, 'Failed to fetch audit logs')
}

export async function queryStream(sql: string) {
    return sendIntent<DataQueryResponse>('data', 'query_stream', { sql })
}

export async function runSql(sql: string) {
    return sendIntent<DataQueryResponse>('data', 'run_sql', { sql })
}

export async function listDataTables() {
    return sendIntent<DataCatalogResponse>('data', 'list_tables')
}

export async function listCatalogSources() {
    return sendIntent<CatalogSourcesResponse>('data', 'catalog_sources')
}

export async function getDataSchema(tablePath: string) {
    return sendIntent<DataSchemaResponse>('data', 'get_schema', { table_path: tablePath })
}

export async function previewDataTable(tablePath: string, limit: number = 10) {
    return sendIntent<DataQueryResponse>('data', 'preview', { table_path: tablePath, limit })
}

export async function fetchInterfaceInventory() {
    return sendIntent<InterfaceInventoryResponse>('system', 'interface_inventory')
}
