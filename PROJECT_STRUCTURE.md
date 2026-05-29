# GPU Supply Chain MRA System - Project Structure

```
gpu-mra-system/
│
├── core/                              # Core services & infrastructure
│   ├── __init__.py
│   ├── acs.py                         # Access Control Service (credential injection)
│   ├── ejms.py                        # Enterprise Job Management Service
│   ├── kafka_config.py                # Kafka broker configuration & topics
│   └── secrets_manager.py             # Integration with Vault/AWS Secrets
│
├── agents/                            # Multi-tier agent system
│   ├── __init__.py
│   ├── base_agent.py                  # Abstract agent interface
│   │
│   ├── mra/                           # Master Reasoning Agent (LangGraph)
│   │   ├── __init__.py
│   │   ├── state_schema.py            # TypedDict/Pydantic state definitions
│   │   ├── graph.py                   # LangGraph graph definition (DAG)
│   │   ├── nodes.py                   # Planner, Dispatcher, Synthesizer nodes
│   │   ├── chains.py                  # State chains & fan-out/fan-in logic
│   │   └── prompts.py                 # System prompts for reasoning
│   │
│   ├── specialists/                   # N-tier specialist agents
│   │   ├── __init__.py
│   │   ├── geological_scout.py        # Raw materials & mining deposits
│   │   ├── chemical_infra.py          # Chemical processing facilities
│   │   ├── logistics_analyst.py       # Supply chain & trade flows
│   │   ├── fab_locator.py             # Semiconductor fab/OSAT facilities
│   │   ├── workforce_analyzer.py      # Skilled labor availability
│   │   ├── thermal_specialist.py      # Thermal interface materials
│   │   └── base_specialist.py         # Base class for specialists
│   │
│   ├── io/                            # Input/Output Gateway Agent
│   │   ├── __init__.py
│   │   ├── request_handler.py         # Request validation & formatting
│   │   ├── response_formatter.py      # Blueprint consolidation & output
│   │   └── contracts.py               # IO Agent contracts
│   │
│   └── eca/                           # Event Correlation Agent
│       ├── __init__.py
│       ├── kafka_listener.py          # Kafka consumer for system events
│       ├── event_router.py            # Event classification & routing
│       ├── anomaly_detector.py        # Detects faults & anomalies
│       └── ticket_generator.py        # Generates sub-tasks for MRA
│
├── mcp_servers/                       # Model Context Protocol server definitions
│   ├── __init__.py
│   ├── geological_mcp.py              # Geological survey MCP server
│   ├── industrial_mcp.py              # Industrial directory MCP server
│   ├── patent_mcp.py                  # Patent database MCP server
│   ├── trade_mcp.py                   # Trade data MCP server
│   └── secure_mcp_wrapper.py          # ACS-integrated MCP wrapper
│
├── schemas/                           # Data schemas & contracts
│   ├── __init__.py
│   ├── contract_mra.py                # MRA contracts (state, requests, responses)
│   ├── contract_acs.py                # ACS contracts (credential requests)
│   ├── contract_eca.py                # ECA contracts (event schemas)
│   ├── contract_ejms.py               # EJMS contracts (job definitions)
│   ├── domain_models.py               # GPU supply chain domain models
│   └── errors.py                      # Custom exception classes
│
├── utils/                             # Utilities & helpers
│   ├── __init__.py
│   ├── logger.py                      # OpenTelemetry logger wrapper
│   ├── exceptions.py                  # Custom exceptions with context
│   ├── validators.py                  # Input validation utilities
│   ├── formatters.py                  # Output formatting utilities
│   ├── retry_policy.py                # Exponential backoff & retry logic
│   └── crypto.py                      # Encryption/decryption utilities
│
├── observability/                     # Monitoring & observability configs
│   ├── __init__.py
│   ├── prometheus_exporter.py         # Prometheus metrics exporter
│   ├── otel_config.py                 # OpenTelemetry initialization
│   ├── dashboards/
│   │   ├── mra_dashboard.json         # Grafana dashboard for MRA
│   │   ├── agent_dashboard.json       # Agent execution & performance
│   │   └── system_health.json         # System health & latency
│   ├── prometheus.yml                 # Prometheus configuration
│   └── otel_collector_config.yaml     # OpenTelemetry collector config
│
├── configs/                           # Configuration files
│   ├── __init__.py
│   ├── settings.py                    # Environment-based settings
│   ├── agents.yaml                    # Agent profiles & routing
│   ├── mcp_registry.yaml              # MCP server registry
│   └── llm_config.yaml                # LLM provider configuration
│
├── tests/                             # Unit & integration tests
│   ├── __init__.py
│   ├── unit/
│   │   ├── test_mra.py               # MRA graph logic
│   │   ├── test_acs.py               # ACS credential injection
│   │   └── test_ejms.py              # Job management
│   └── integration/
│       ├── test_end_to_end.py        # Full scenario testing
│       └── test_eca_events.py        # Event correlation
│
├── docs/                              # Documentation
│   ├── ARCHITECTURE.md                # System architecture overview
│   ├── CONTRACTS.md                   # Component contracts & interfaces
│   ├── EXECUTION_FLOW.md              # Execution specs with data flow
│   ├── DEPLOYMENT.md                  # Deployment guide
│   └── TROUBLESHOOTING.md             # Debugging & troubleshooting
│
├── scripts/                           # Operational scripts
│   ├── bootstrap.sh                   # System initialization
│   ├── health_check.py                # Health check script
│   └── performance_test.py            # Load testing
│
├── docker-compose.yml                 # Docker stack (Kafka, Prometheus, etc.)
├── requirements.txt                   # Python dependencies
├── pyproject.toml                     # Modern Python packaging
├── main.py                            # System entry point
├── .env.example                       # Environment variables template
└── README.md                          # Project overview
```

## Directory Descriptions

### `core/`
Core infrastructure services:
- **ACS (Access Control Service)**: Manages credentials, integrates with Vault/AWS Secrets Manager
- **EJMS (Enterprise Job Management)**: Async job dispatch via Redis/Kafka, tracks job status
- **Kafka Config**: Topic definitions, broker configuration, serialization
- **Secrets Manager**: Vault integration for secure credential rotation

### `agents/mra/`
Master Reasoning Agent (LangGraph-based):
- **state_schema.py**: Complete state definition flowing through graph
- **graph.py**: DAG with planner → dispatcher → synthesizer
- **nodes.py**: Individual node implementations (reasoning, dispatch, synthesis)
- **chains.py**: State transitions and parallel fan-out/fan-in

### `agents/specialists/`
6-8+ specialist agents:
- Each has dedicated MCP tool integration
- Localized prompts for domain expertise
- Inherits from `base_specialist.py`
- Examples: Geological Scout, Chemical Infra, Logistics Analyst, Fab Locator

### `agents/io/`
Input/Output Gateway:
- Validates user requests
- Formats responses
- Returns consolidated blueprints
- Communicates with MRA via EJMS

### `agents/eca/`
Event Correlation Agent:
- Listens to Kafka topics: `syslog-events`, `agent-errors`, `data-updates`
- Detects anomalies (timeouts, missing data)
- Generates tickets (Jira/internal format)
- Feeds mitigation tasks back to IO Agent/MRA

### `mcp_servers/`
MCP server definitions and wrappers:
- Geological, Industrial, Patent, Trade data servers
- `secure_mcp_wrapper.py`: Enforces ACS credential injection before tool execution

### `schemas/`
Data contracts & interfaces:
- Pydantic models for all data structures
- MRA state schema
- ACS credential request/response
- ECA event schema
- EJMS job definition

### `utils/`
Utility functions:
- **logger.py**: OpenTelemetry tracing wrapper
- **exceptions.py**: Custom exceptions with trace context
- **validators.py**: Input validation
- **retry_policy.py**: Exponential backoff

### `observability/`
Monitoring infrastructure:
- Prometheus exporters
- OpenTelemetry configuration
- Grafana dashboards (MRA, agents, system health)
- Collector configuration

---

## Key Design Patterns

1. **Fan-Out/Fan-In**: MRA dispatches to 6+ specialist agents in parallel via EJMS
2. **Graph-Based State Machine**: LangGraph DAG for workflow orchestration
3. **Credential Injection**: ACS provides temporary tokens before MCP tool execution
4. **Event-Driven Monitoring**: ECA correlates Kafka events, detects faults, triggers mitigation
5. **Observable by Default**: Every operation wrapped in OpenTelemetry traces
6. **Async-First**: Redis/Kafka for non-blocking job management

---

## Initialization Flow

```
1. Bootstrap -> Load configs & secrets
2. Initialize ACS -> Connect to Vault/AWS Secrets Manager
3. Connect to Kafka -> Subscribe to event topics
4. Start MRA graph -> LangGraph initialization
5. Start specialists -> MCP connections pooled (not started immediately)
6. Start ECA -> Begin listening to events
7. Start IO Agent -> Ready to accept user requests
8. Export OTel metrics -> Prometheus scrape enabled
```

