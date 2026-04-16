export type CapabilityStatus = 'implemented' | 'partial' | 'pending'

export type InterfaceCapability = {
  id: string
  capability: string
  rest: string
  mcpTool: string
  domainAction: string
  adapter: string
  permission: string
  status: CapabilityStatus
  note: string
}

export type InterfaceSurface = {
  label: string
  state: 'ready' | 'partial' | 'pending' | 'planned'
  detail: string
}

export type McpToolEvidence = {
  name: string
  domain: string
  action: string
  inputs: string[]
  permission: string
  status: CapabilityStatus
  proof: string
}

export type DeploymentStatusTone = 'ready' | 'partial' | 'pending' | 'risk'

export type DeploymentStatus = {
  label: string
  status: DeploymentStatusTone
  detail: string
}

export type DispatchStep = {
  label: string
  detail: string
}

export type SampleCall = {
  label: string
  description: string
  payload: string
}

export type ImplementationNote = {
  label: string
  detail: string
  tone: 'ready' | 'partial' | 'pending' | 'risk'
}

export const interfaceContract = {
  summary:
    'REST, dashboard routes, and MCP all enter through gateway-controlled intents. The protocol changes, but dispatch, RBAC, adapter routing, and audit logging stay the same.',
  path:
    'Client -> REST or MCP -> UserIntent -> InterfaceRegistry -> Adapter -> Runtime -> Audit log',
  mcpPath:
    'MCP tool call -> gateway dispatch(source_protocol="mcp") -> same domain/action adapters',
}

export const interfaceSurfaces: InterfaceSurface[] = [
  {
    label: 'REST Gateway',
    state: 'ready',
    detail: 'Primary frontend and service requests already flow through the intent router.',
  },
  {
    label: 'Intent API',
    state: 'ready',
    detail: 'Domain/action dispatch is the main entry point for system actions.',
  },
  {
    label: 'Agent API',
    state: 'ready',
    detail: 'Agent list, chat, and invoke routes are available for direct inspection.',
  },
  {
    label: 'Data and Log Intents',
    state: 'ready',
    detail: 'Data, health, and log reads already expose the gateway-backed platform state.',
  },
  {
    label: 'Dashboard Proxies',
    state: 'partial',
    detail: 'Prefect and other internal tools depend on the proxy or probe being reachable.',
  },
  {
    label: 'MCP Local Server',
    state: 'ready',
    detail: 'The stdio server is implemented and maps tool calls onto the same gateway path.',
  },
  {
    label: 'MCP Remote Deployment',
    state: 'pending',
    detail: 'Remote deployment and live discovery remain to be hardened.',
  },
  {
    label: 'Audit Query',
    state: 'ready',
    detail: 'Gateway audit records are queryable through REST, intent, and MCP.',
  },
  {
    label: 'Live Interface Inventory',
    state: 'ready',
    detail: 'The gateway now exposes its own domains, routes, proxy targets, and MCP tools.',
  },
]

export const interfaceCapabilities: InterfaceCapability[] = [
  {
    id: 'data-query',
    capability: 'Query Delta data',
    rest: 'POST /api/v1/intent',
    mcpTool: 'query_data',
    domainAction: 'data.run_sql',
    adapter: 'DataAdapter',
    permission: 'data:read',
    status: 'implemented',
    note: 'SQL and table discovery are mediated by the same read-only data adapter.',
  },
  {
    id: 'data-list',
    capability: 'List Delta tables',
    rest: 'POST /api/v1/intent',
    mcpTool: 'list_tables',
    domainAction: 'data.list_tables',
    adapter: 'DataAdapter',
    permission: 'data:read',
    status: 'implemented',
    note: 'The MCP tool mirrors the gateway list-table action directly.',
  },
  {
    id: 'data-schema',
    capability: 'Inspect table schema',
    rest: 'POST /api/v1/intent',
    mcpTool: 'get_schema',
    domainAction: 'data.get_schema',
    adapter: 'DataAdapter',
    permission: 'data:read',
    status: 'implemented',
    note: 'Schema inspection remains read-only and stays on the gateway path.',
  },
  {
    id: 'data-stream',
    capability: 'Query RisingWave stream',
    rest: 'POST /api/v1/intent',
    mcpTool: 'query_stream',
    domainAction: 'data.query_stream',
    adapter: 'DataAdapter',
    permission: 'data:read',
    status: 'implemented',
    note: 'Live stream reads use the same read-only data adapter boundary.',
  },
  {
    id: 'data-preview',
    capability: 'Preview table rows',
    rest: 'POST /api/v1/intent',
    mcpTool: 'preview_table',
    domainAction: 'data.preview',
    adapter: 'DataAdapter',
    permission: 'data:read',
    status: 'implemented',
    note: 'Table previews remain governed and stay inside the gateway.',
  },
  {
    id: 'catalog-sources',
    capability: 'Inspect catalog sources',
    rest: 'POST /api/v1/intent',
    mcpTool: 'list_catalog_sources',
    domainAction: 'data.catalog_sources',
    adapter: 'DataAdapter',
    permission: 'data:read',
    status: 'implemented',
    note: 'The catalog can now describe live and fallback storage families directly from the gateway.',
  },
  {
    id: 'agent-list',
    capability: 'List agents',
    rest: 'GET /api/v1/agents',
    mcpTool: 'list_agents',
    domainAction: 'agent.list',
    adapter: 'AgentAdapter',
    permission: 'agent:read',
    status: 'implemented',
    note: 'Human and AI clients see the same catalog of active agents.',
  },
  {
    id: 'agent-chat',
    capability: 'Chat with agent',
    rest: 'POST /api/v1/agents/:agent_name/chat',
    mcpTool: 'chat_agent',
    domainAction: 'agent.chat',
    adapter: 'AgentAdapter',
    permission: 'agent:interact',
    status: 'implemented',
    note: 'Interactive agent calls share the same permission boundary in both protocols.',
  },
  {
    id: 'agent-invoke',
    capability: 'Invoke agent parity',
    rest: 'POST /api/v1/agents/:agent_name/invoke',
    mcpTool: 'invoke_agent',
    domainAction: 'agent.invoke',
    adapter: 'AgentAdapter',
    permission: 'agent:interact',
    status: 'implemented',
    note: 'REST and MCP now expose the same invocation boundary.',
  },
  {
    id: 'system-health',
    capability: 'Get system health',
    rest: 'POST /api/v1/intent',
    mcpTool: 'get_system_health',
    domainAction: 'system.health',
    adapter: 'SystemAdapter',
    permission: 'system:read',
    status: 'implemented',
    note: 'Read-level observability is routed through the same adapter and dispatch path.',
  },
  {
    id: 'system-logs',
    capability: 'Query system logs',
    rest: 'POST /api/v1/intent',
    mcpTool: 'query_system_logs',
    domainAction: 'system.logs',
    adapter: 'SystemAdapter',
    permission: 'system:read',
    status: 'implemented',
    note: 'Backend and MCP now expose the same read-level log filters and query metadata.',
  },
  {
    id: 'audit-logs',
    capability: 'Query audit logs',
    rest: 'GET /api/v1/system/audit-logs',
    mcpTool: 'query_audit_logs',
    domainAction: 'system.audit_logs',
    adapter: 'SystemAdapter',
    permission: 'system:read',
    status: 'implemented',
    note: 'Persisted gateway audit records are queryable through the same governed system surface.',
  },
  {
    id: 'overseer-snapshots',
    capability: 'Query overseer snapshots',
    rest: 'GET /api/v1/system/overseer/snapshots',
    mcpTool: 'get_overseer_snapshots',
    domainAction: 'system.overseer_snapshots',
    adapter: 'SystemAdapter',
    permission: 'system:read',
    status: 'implemented',
    note: 'Recovery snapshots are available to the same read-level audience.',
  },
  {
    id: 'overseer-alerts',
    capability: 'Query overseer alerts',
    rest: 'GET /api/v1/system/overseer/alerts',
    mcpTool: 'get_overseer_alerts',
    domainAction: 'system.overseer_alerts',
    adapter: 'SystemAdapter',
    permission: 'system:read',
    status: 'implemented',
    note: 'Recent recovery actions remain visible through the same system surface.',
  },
  {
    id: 'infra-status',
    capability: 'Probe infrastructure targets',
    rest: 'GET /api/v1/system/infra/status',
    mcpTool: 'get_infra_status',
    domainAction: 'system.infra_status',
    adapter: 'SystemAdapter',
    permission: 'system:read',
    status: 'implemented',
    note: 'Infra probes are routed through the gateway rather than a separate path.',
  },
  {
    id: 'interface-inventory',
    capability: 'Inspect interface inventory',
    rest: 'GET /api/v1/system/interfaces',
    mcpTool: 'get_interface_inventory',
    domainAction: 'system.interface_inventory',
    adapter: 'SystemAdapter',
    permission: 'system:read',
    status: 'implemented',
    note: 'The gateway can now report its domains, routes, proxy targets, and MCP tools.',
  },
]

export const mcpTools: McpToolEvidence[] = [
  {
    name: 'query_data',
    domain: 'data',
    action: 'run_sql',
    inputs: ['sql'],
    permission: 'data:read',
    status: 'implemented',
    proof: 'Executes SQL through the same gateway dispatch path used by REST intent calls.',
  },
  {
    name: 'list_tables',
    domain: 'data',
    action: 'list_tables',
    inputs: [],
    permission: 'data:read',
    status: 'implemented',
    proof: 'Returns the current table inventory without bypassing the adapter boundary.',
  },
  {
    name: 'get_schema',
    domain: 'data',
    action: 'get_schema',
    inputs: ['table_path'],
    permission: 'data:read',
    status: 'implemented',
    proof: 'Maps table metadata requests back into the same governed data adapter.',
  },
  {
    name: 'query_stream',
    domain: 'data',
    action: 'query_stream',
    inputs: ['sql'],
    permission: 'data:read',
    status: 'implemented',
    proof: 'Executes live stream reads through the same read-only gateway path.',
  },
  {
    name: 'preview_table',
    domain: 'data',
    action: 'preview',
    inputs: ['table_path', 'limit'],
    permission: 'data:read',
    status: 'implemented',
    proof: 'Returns preview rows without bypassing the data adapter.',
  },
  {
    name: 'list_catalog_sources',
    domain: 'data',
    action: 'catalog_sources',
    inputs: [],
    permission: 'data:read',
    status: 'implemented',
    proof: 'Returns live and fallback catalog sources without bypassing the gateway.',
  },
  {
    name: 'query_system_logs',
    domain: 'system',
    action: 'logs',
    inputs: ['component', 'level', 'since', 'agent_name', 'trace_id', 'limit'],
    permission: 'system:read',
    status: 'implemented',
    proof: 'Reads centralized logs through the gateway with the same filter set exposed by the backend.',
  },
  {
    name: 'get_system_health',
    domain: 'system',
    action: 'health',
    inputs: [],
    permission: 'system:read',
    status: 'implemented',
    proof: 'Surfaces overseer health through the same intent dispatcher as the REST side.',
  },
  {
    name: 'query_audit_logs',
    domain: 'system',
    action: 'audit_logs',
    inputs: ['since', 'request_id', 'source_protocol', 'domain', 'action', 'status_code', 'user_id'],
    permission: 'system:read',
    status: 'implemented',
    proof: 'Exposes persisted gateway audit records through REST-equivalent governance.',
  },
  {
    name: 'get_overseer_snapshots',
    domain: 'system',
    action: 'overseer_snapshots',
    inputs: ['limit'],
    permission: 'system:read',
    status: 'implemented',
    proof: 'Surfaces recovery snapshots through the same monitoring boundary.',
  },
  {
    name: 'get_overseer_alerts',
    domain: 'system',
    action: 'overseer_alerts',
    inputs: ['limit'],
    permission: 'system:read',
    status: 'implemented',
    proof: 'Exposes recent recovery alerts through the governed system adapter.',
  },
  {
    name: 'get_infra_status',
    domain: 'system',
    action: 'infra_status',
    inputs: [],
    permission: 'system:read',
    status: 'implemented',
    proof: 'Probes internal dashboard targets through the same access surface.',
  },
  {
    name: 'get_interface_inventory',
    domain: 'system',
    action: 'interface_inventory',
    inputs: [],
    permission: 'system:read',
    status: 'implemented',
    proof: 'Returns gateway interface, route, proxy, and MCP tool inventory through the same governed boundary.',
  },
  {
    name: 'list_agents',
    domain: 'agent',
    action: 'list',
    inputs: [],
    permission: 'agent:read',
    status: 'implemented',
    proof: 'Shows the same agent catalog that the REST UI already consumes.',
  },
  {
    name: 'chat_agent',
    domain: 'agent',
    action: 'chat',
    inputs: ['agent_name', 'message'],
    permission: 'agent:interact',
    status: 'implemented',
    proof: 'Wraps agent conversation in the same permission and audit path as REST chat.',
  },
  {
    name: 'broadcast_agents',
    domain: 'agent',
    action: 'notify',
    inputs: ['payload'],
    permission: 'agent:broadcast',
    status: 'implemented',
    proof: 'Broadcasts to alive agents through the same dispatch path.',
  },
  {
    name: 'invoke_agent',
    domain: 'agent',
    action: 'invoke',
    inputs: ['agent_name', 'payload'],
    permission: 'agent:interact',
    status: 'implemented',
    proof: 'REST invoke and MCP invoke now point to the same gateway action.',
  },
  {
    name: 'submit_job',
    domain: 'compute',
    action: 'submit_job',
    inputs: ['pipeline', 'params'],
    permission: 'compute:write',
    status: 'implemented',
    proof: 'Starts Prefect jobs via the compute adapter, not a separate MCP-only path.',
  },
  {
    name: 'get_job_status',
    domain: 'compute',
    action: 'get_status',
    inputs: ['job_id'],
    permission: 'compute:read',
    status: 'implemented',
    proof: 'Reads back flow-run state through the same dispatcher.',
  },
  {
    name: 'list_connections',
    domain: 'broker',
    action: 'list_connections',
    inputs: [],
    permission: 'broker:read',
    status: 'implemented',
    proof: 'Exposes broker metadata while keeping credential vending separate.',
  },
  {
    name: 'get_s3_creds',
    domain: 'broker',
    action: 'get_s3_creds',
    inputs: [],
    permission: 'broker:vend',
    status: 'implemented',
    proof: 'Vends direct storage credentials through the broker adapter.',
  },
  {
    name: 'get_psql_string',
    domain: 'broker',
    action: 'get_psql_string',
    inputs: [],
    permission: 'broker:vend',
    status: 'implemented',
    proof: 'Vends the TimescaleDB connection string through the broker adapter.',
  },
]

export const dispatchPath: DispatchStep[] = [
  { label: 'REST', detail: '/api/v1/intent or direct agent routes enter the gateway' },
  { label: 'MCP', detail: 'Tool call lands in the MCP server and resolves to the same dispatch pipeline' },
  { label: 'UserIntent', detail: 'Domain, action, parameters, user, roles, and request ID are normalized' },
  { label: 'Registry', detail: 'InterfaceRegistry picks the domain adapter for execution' },
  { label: 'Adapter', detail: 'Data, agent, compute, broker, or system logic runs behind one policy boundary' },
  { label: 'Audit', detail: 'source_protocol is recorded so the trace remains attributable' },
]

export const deploymentStatuses: DeploymentStatus[] = [
  {
    label: 'REST gateway',
    status: 'ready',
    detail: 'The gateway intent path is already live and used by the frontend.',
  },
  {
    label: 'MCP local server',
    status: 'ready',
    detail: 'The stdio MCP server is implemented and wires the tool modules at startup.',
  },
  {
    label: 'MCP remote deployment',
    status: 'pending',
    detail: 'Remote deployment and operator-facing discovery are still to be hardened.',
  },
  {
    label: 'Tool registry',
    status: 'ready',
    detail: 'MCP tools are consolidated through one registry and call path.',
  },
  {
    label: 'Audit source protocol',
    status: 'ready',
    detail: 'dispatch() records whether the request arrived via REST or MCP.',
  },
  {
    label: 'Frontend introspection',
    status: 'partial',
    detail: 'The UI still presents the protocol story statically until live MCP probing is exposed.',
  },
]

export const sampleCalls: SampleCall[] = [
  {
    label: 'REST intent',
    description: 'A gateway request for recent system logs.',
    payload: `{
  "domain": "system",
  "action": "logs",
  "parameters": {
    "component": "gateway",
    "level": "INFO",
    "since": "1h",
    "limit": 20
  }
}`,
  },
  {
    label: 'MCP tool call',
    description: 'The same request through the MCP tool surface.',
    payload: `{
  "tool": "query_system_logs",
  "arguments": {
    "component": "gateway",
    "level": "INFO",
    "since": "1h",
    "limit": 20
  }
}`,
  },
  {
    label: 'Audit record',
    description: 'How the gateway records protocol provenance.',
    payload: `{
  "source_protocol": "mcp",
  "domain": "system",
  "action": "logs",
  "status_code": 200,
  "duration_ms": 42.7
}`,
  },
]

export const implementationNotes: ImplementationNote[] = [
  {
    label: 'Read-level system access',
    detail: 'Health, logs, overseer recovery, infra probes, and audit records are guarded at system:read.',
    tone: 'ready',
  },
  {
    label: 'Audit query surface',
    detail: 'Persisted audit logs are now exposed through REST, intent, and MCP.',
    tone: 'ready',
  },
  {
    label: 'MCP readiness',
    detail: 'The local server is present, but remote deployment and discovery remain pending.',
    tone: 'pending',
  },
  {
    label: 'Protocol parity',
    detail: 'REST and MCP converge on the same UserIntent, registry, adapter, and audit path.',
    tone: 'ready',
  },
]
