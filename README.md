# 🔬 GPU Supply Chain Multi-Agent System (MAS)
### India's GPU Manufacturing Supply Chain Intelligence Platform

[![Python](https://img.shields.io/badge/Python-3.11%2B-blue?logo=python)](https://python.org)
[![LangGraph](https://img.shields.io/badge/LangGraph-0.2%2B-green)](https://github.com/langchain-ai/langgraph)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-3.6-orange?logo=apachekafka)](https://kafka.apache.org)
[![Redis](https://img.shields.io/badge/Redis-7.2-red?logo=redis)](https://redis.io)
[![OpenTelemetry](https://img.shields.io/badge/OpenTelemetry-1.25-blueviolet)](https://opentelemetry.io)
[![Grafana](https://img.shields.io/badge/Grafana-10.4-orange?logo=grafana)](https://grafana.com)
[![License](https://img.shields.io/badge/License-MIT-lightgrey)](LICENSE)

---

An **event-driven, self-healing multi-agent ecosystem** that maps India's GPU manufacturing supply chain — covering raw materials, chemical precursors, semiconductor fabs, logistics, and regulatory compliance — using a **fan-out/fan-in LangGraph architecture** with decoupled async job execution, centralized secure credential management, and autonomous telemetry-driven healing.

---

## 📋 Table of Contents

1. [Architecture Overview](#-architecture-overview)
2. [System Components](#-system-components)
3. [Directory Structure](#-directory-structure)
4. [Prerequisites](#-prerequisites)
5. [Quickstart](#-quickstart)
6. [Configuration Guide](#-configuration-guide)
7. [Running a Query](#-running-a-query)
8. [Observability](#-observability)
9. [Development Guide](#-development-guide)
10. [Adding a New DSW Specialist](#-adding-a-new-dsw-specialist)
11. [Security Model](#-security-model)
12. [Troubleshooting](#-troubleshooting)

---

## 🏗️ Architecture Overview

```
                          ┌─────────────────────────────────────────────────────┐
                          │              OBSERVABILITY LAYER                     │
                          │   Prometheus ◄── OTel Collector ◄── All Agents      │
                          │   Grafana Dashboards  │  Structured JSON Logs        │
                          └─────────────┬───────────────────────────────────────┘
                                        │ Metrics & Traces
  User/Client                           │
      │                                 │
      ▼                                 │
 ┌──────────┐     REST      ┌───────────┴──────────────────────────────────────┐
 │   GIA    │──────────────▶│           Central Orchestration Graph (COG)       │
 │ Gateway  │◀──────────────│                   [LangGraph]                     │
 │Interface │   Blueprint   │  planning_node → fan_out_node → synthesis_node    │
 │  Agent   │               └───────────┬──────────────────────────────────────┘
 └──────────┘                           │ Publishes TaskEnvelopes
                                        ▼
                          ┌─────────────────────────┐
                          │  Distributed Task Broker │   ◄── Redis Streams
                          │         (DTB)            │       (per worker queue)
                          └────────────┬────────────┘
                                       │ Fan-Out (11 parallel workers)
              ┌────────────────────────┼─────────────────────────┐
              ▼            ▼           ▼           ▼             ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐
        │Geological│ │Chemical  │ │Logistics │ │Trade     │ │   ...8   │
        │ Expert   │ │ Infra    │ │Coordinator│ │ Policy   │ │  more    │
        │  (DSW)   │ │ Analyst  │ │  (DSW)   │ │ Expert   │ │  DSWs    │
        └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘
             │             │             │             │             │
             └─────────────┴──────┬──────┴─────────────┴─────────────┘
                                  │  SVB token per call
                                  ▼
                         ┌─────────────────┐
                         │  Secure Vault   │◄── HashiCorp Vault
                         │  Broker (SVB)   │    AWS Secrets Manager
                         └────────────────┘

  ┌──────────────────────────────────────────────────────────────┐
  │  Telemetry & Healing Agent (THA)         [Background Process] │
  │                                                               │
  │  Kafka Consumer: sys-events, agent-faults                     │
  │  → Classifies faults → Generates tickets → Injects healing   │
  └──────────────────────────────────────────────────────────────┘
```

---

## 🧩 System Components

| Component | Abbreviation | Role | Technology |
|-----------|-------------|------|------------|
| Gateway Interface Agent | **GIA** | Synchronous frontend edge node. Validates user queries, formats GIARequest, delivers final blueprint | FastAPI |
| Central Orchestration Graph | **COG** | LangGraph state machine. Decomposes queries, manages fan-out, synthesizes results | LangGraph + Gemini |
| Distributed Task Broker | **DTB** | Async job queue engine. Publishes/consumes tasks, handles retries + DLQ | Redis Streams |
| Secure Vault Broker | **SVB** | JIT credential injector. Issues ephemeral scoped tokens per MCP call | HashiCorp Vault |
| Telemetry & Healing Agent | **THA** | Autonomous watchdog. Detects faults, generates tickets, pushes healing directives | Kafka Consumer |
| Domain Specialist Workers | **DSW** | 11 scoped AI agents, each expert in one supply chain domain | LangChain + MCP |
| Observability Layer | — | Distributed tracing + metrics for every agent and tool | OpenTelemetry + Prometheus + Grafana |

### Domain Specialist Workers (DSWs)

| DSW | Domain | MCP Server |
|-----|--------|-----------|
| `geological_expert` | HPQ deposits, rare earth minerals, GSI survey data | `geological-mcp-server` |
| `chemical_infra_analyst` | Chemical processing plants, reagent supply | `chemical-mcp-server` |
| `supply_chain_forecaster` | 3-year supply projections, risk modeling | `trade-mcp-server` |
| `logistics_coordinator` | Ports, transport routes, warehouses | `logistics-mcp-server` |
| `fab_locator` | Semiconductor fabs, OSAT, cleanrooms | `industrial-mcp-server` |
| `mining_lease_analyst` | Active mining leases, legal encumbrances | `lease-mcp-server` |
| `environmental_compliance` | Green clearances, pollution norms | `env-mcp-server` |
| `workforce_analyst` | Skilled labor, training institutes, wages | `workforce-mcp-server` |
| `trade_policy_expert` | PLI schemes, import tariffs, restrictions | `trade-policy-mcp-server` |
| `thermal_materials_expert` | TIM substrates, thermal interface specs | `materials-mcp-server` |
| `semiconductor_grade_qa` | SEMI F49 purity specs, certification requirements | `qa-mcp-server` |

---

## 📁 Directory Structure

```
gpu-mra-system/
│
├── core_services/              # Infrastructure backbone
│   ├── dtb.py                  # Distributed Task Broker (Redis Streams)
│   ├── svb.py                  # Secure Vault Broker (credential injection)
│   └── kafka_streams.py        # Kafka topic manager & stream initializer
│
├── orchestration/
│   └── cog/                    # Central Orchestration Graph
│       ├── state_schema.py     # Global COGState TypedDict + Pydantic models
│       ├── graph.py            # LangGraph StateGraph compilation
│       ├── nodes.py            # planning_node, fan_out_node, synthesis_node
│       └── prompts.py          # LLM system prompts (planner + synthesizer)
│
├── agents/
│   ├── edge/
│   │   ├── gia.py              # Gateway Interface Agent (FastAPI)
│   │   └── tha.py              # Telemetry & Healing Agent (Kafka consumer)
│   └── specialists/
│       ├── base_dsw.py         # BaseDSW (OTel + SVB + MCP wrapper)
│       ├── geological_expert.py
│       ├── chemical_infra_analyst.py
│       ├── supply_chain_forecaster.py
│       ├── logistics_coordinator.py
│       ├── fab_locator.py
│       ├── mining_lease_analyst.py
│       ├── environmental_compliance.py
│       ├── workforce_analyst.py
│       ├── trade_policy_expert.py
│       ├── thermal_materials_expert.py
│       └── semiconductor_grade_qa.py
│
├── utils/
│   ├── telemetry.py            # OpenTelemetry tracer, decorator, metrics
│   ├── logger.py               # Structured JSON logger
│   ├── exceptions.py           # Custom exception hierarchy
│   ├── validators.py
│   ├── formatters.py
│   └── retry_policy.py
│
├── observability/
│   ├── prometheus.yml          # Prometheus scrape config (all components)
│   ├── otel_collector_config.yaml
│   └── dashboards/
│       └── gpu_supply_chain_main.json   # Grafana dashboard
│
├── docker-compose.yml          # Full infra stack (Kafka, Redis, Vault, OTel, Prometheus, Grafana)
├── requirements.txt
├── .env.example                # Environment variable template
└── README.md
```

---

## ⚙️ Prerequisites

| Requirement | Version | Notes |
|------------|---------|-------|
| Python | 3.11+ | Required for `TypedDict` features |
| Docker + Docker Compose | 24.0+ | For infrastructure stack |
| Google AI API Key | — | Gemini 2.0 Flash access |
| HashiCorp Vault | 1.16+ | Or set `SVB_BACKEND=env` for dev mode |
| 8 GB RAM | — | Minimum for full stack |

---

## 🚀 Quickstart

### Step 1 — Clone & Install

```bash
git clone https://github.com/your-org/gpu-mra-system.git
cd gpu-mra-system

python -m venv .venv
.venv\Scripts\activate          # Windows
# source .venv/bin/activate     # Linux/macOS

pip install -r requirements.txt
```

### Step 2 — Configure Environment

```bash
copy .env.example .env          # Windows
# cp .env.example .env          # Linux/macOS
```

Edit `.env` and fill in the required values (see [Configuration Guide](#-configuration-guide)).

### Step 3 — Start Infrastructure

```bash
docker-compose up -d
```

This starts:
- **Kafka** (port `9092`) + Zookeeper + auto-topic creation
- **Redis** (port `6379`) — DTB job queues
- **HashiCorp Vault** (port `8200`, dev mode)
- **OpenTelemetry Collector** (gRPC port `4317`)
- **Prometheus** (port `9090`)
- **Grafana** (port `3000`, default password: `gpu-chain-admin`)

Wait for all services to be healthy:
```bash
docker-compose ps
```

### Step 4 — Seed Vault Secrets (dev mode)

```bash
# Load MCP server API keys into Vault KV store
python scripts/seed_vault.py
```

Or set them as environment variables for dev mode (`SVB_BACKEND=env`):

```env
MCP_GEOLOGICAL_MCP_SERVER_API_KEY=your-key
MCP_LOGISTICS_MCP_SERVER_API_KEY=your-key
# ... one per MCP server
```

### Step 5 — Initialize Kafka Topics

The `docker-compose.yml` `kafka-init` container handles this automatically.
To verify:
```bash
docker exec -it gpu-mra-system-kafka-1 \
  kafka-topics --bootstrap-server localhost:9092 --list
```

Expected topics: `sys-events`, `agent-faults`, `tha-remediations`, `data-updates`, `audit-trail`

### Step 6 — Start the System

**Terminal 1 — GIA + COG (main server):**
```bash
python -m uvicorn agents.edge.gia:create_gia_app --factory --host 0.0.0.0 --port 8080 --reload
```

**Terminal 2 — THA (background watchdog):**
```bash
python -m agents.edge.tha
```

**Terminal 3 — DSW Workers (one process per specialist type):**
```bash
python -m agents.specialists.worker --type geological_expert
python -m agents.specialists.worker --type logistics_coordinator
# ... one per DSW type, or use the batch launcher:
python scripts/start_workers.py --all
```

---

## 🔧 Configuration Guide

### Environment Variables (`.env`)

```env
# ── LLM ─────────────────────────────────────────────────────────────────────
GOOGLE_API_KEY=your-gemini-api-key
GEMINI_MODEL=gemini-2.0-flash

# ── Redis (DTB) ──────────────────────────────────────────────────────────────
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB_DTB=1                      # Separate DB from EJMS legacy

# ── Kafka ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_SECURITY_PROTOCOL=PLAINTEXT   # SASL_SSL for production

# ── Secure Vault Broker ──────────────────────────────────────────────────────
SVB_BACKEND=vault                   # vault | aws | env (dev only)
VAULT_ADDR=http://localhost:8200
VAULT_TOKEN=dev-root-token          # Replace in production
SVB_VAULT_PATH=secret/gpu-supply-chain/mcp-servers
SVB_HMAC_KEY=change-me-in-production

# ── DSW Workers ──────────────────────────────────────────────────────────────
DSW_HMAC_KEY=change-me-in-production   # Must match SVB_HMAC_KEY
DSW_DEFAULT_TIMEOUT_S=45
DSW_MAX_RETRIES=3

# ── OpenTelemetry ────────────────────────────────────────────────────────────
OTEL_SERVICE_NAME=gpu-supply-chain-mas
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
PROMETHEUS_PORT=8000
ENV=development                     # development | staging | production

# ── GIA ──────────────────────────────────────────────────────────────────────
GIA_COG_TIMEOUT_SECONDS=180
GIA_MAX_QUERY_LENGTH=4096
```

### Kafka Topic Reference

| Topic | Partitions | Retention | Purpose |
|-------|-----------|-----------|---------|
| `sys-events` | 8 | 24h | System lifecycle (planning, dispatch, completion) |
| `agent-faults` | 4 | 7 days | DSW errors, MCP timeouts, DLQ events |
| `tha-remediations` | 2 | 1h | THA healing directives → COG |
| `data-updates` | 4 | 12h | MCP data freshness signals |
| `audit-trail` | 2 | ∞ | SVB token issuances (compliance, compacted) |

### MCP Server Registry

MCP servers are registered in `configs/mcp_registry.yaml`:

```yaml
mcp_servers:
  geological-mcp-server:
    url: http://geological-mcp:9000
    vault_path: secret/gpu-supply-chain/mcp-servers/geological-mcp-server
    fallback: geological-mcp-backup

  logistics-mcp-server:
    url: http://logistics-mcp:9001
    vault_path: secret/gpu-supply-chain/mcp-servers/logistics-mcp-server
    fallback: trade-api-fallback
  # ... one entry per MCP server
```

---

## 🧪 Running a Query

### Via REST API (curl)

```bash
curl -X POST http://localhost:8080/v1/supply-chain/analyze \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Provide a viability report for sourcing High-Purity Quartz and Copper foils in Southern India for GPU manufacturing",
    "user_id": "analyst-007",
    "priority": 2
  }'
```

### Sample Response

```json
{
  "request_id": "req-a1b2c3d4e5f6",
  "session_id": "sess-9e7f2a1b",
  "status": "partial",
  "viability_score": 6.8,
  "confidence_overall": 0.763,
  "degraded_mode": true,
  "processing_time_ms": 38050,
  "warnings": [
    "THA: Logistics port data obtained via fallback route query (TKT-A3F7B2). Confidence reduced."
  ],
  "blueprint": {
    "title": "GPU Supply Chain Viability Report: HPQ & Copper Foil — Southern India",
    "executive_summary": "Southern India presents a viable, though conditionally complex, supply chain...",
    "material_profiles": [
      {
        "material": "High-Purity Quartz",
        "availability_status": "Abundant",
        "primary_sources": ["Gudur AP", "Hassan KA", "Nellore AP"],
        "estimated_reserves": "2.1M tonnes",
        "quality_grade": "semiconductor_grade",
        "purity_achievable": "99.998% (SEMI F49 compliant)",
        "sourcing_risk_score": 2.3
      },
      {
        "material": "Copper Foil",
        "availability_status": "Moderate (import-dependent)",
        "sourcing_risk_score": 5.8
      }
    ],
    "viability_score": 6.8,
    "risk_assessment": {
      "geological": "LOW — extensive HPQ deposits with active leases",
      "logistical": "MEDIUM — copper import infrastructure viable but port data incomplete",
      "regulatory": "LOW — PLI 2.0 favorable",
      "overall_risk_level": "Medium"
    },
    "recommendations": [
      "Engage IBM for HPQ deposit feasibility study at Gudur and Hassan",
      "Secure mining lease renewals before 2026 expiry",
      "Establish JIT copper foil import contract via Kattupalli port"
    ],
    "data_gaps": ["logistics-mcp-server port throughput data (timeout — fallback used)"]
  }
}
```

### Status Codes

| `status` | Meaning |
|----------|---------|
| `success` | All DSWs completed, full confidence blueprint |
| `partial` | THA fallbacks used, `degraded_mode=true`, confidence reduced |
| `failed` | Unrecoverable error (planning LLM failure, Redis down) |

### Health Check

```bash
curl http://localhost:8080/v1/health
# {"status": "healthy", "service": "gia", "timestamp": "2026-05-30T..."}
```

---

## 📊 Observability

### Grafana Dashboard

Open **http://localhost:3000** → login `admin / gpu-chain-admin`

The pre-loaded dashboard (`GPU Supply Chain MAS — System Observability`) includes:

| Panel Group | Metrics Shown |
|------------|--------------|
| 🏭 System Overview | Active sessions, total faults (1h), THA tickets, avg viability score, MCP call volume |
| ⚡ Agent Execution | Latency p50/p95/p99 per agent, MCP tool latency by tool name |
| 🛡️ THA Self-Healing | Ticket rate by fault type, MCP error rate by tool & status |
| 🗄️ DTB Queue | Redis stream depth per worker queue, Kafka consumer lag |
| 🔑 SVB Credentials | Token issuances per worker, auth failures, Vault fetch latency |

### Prometheus

Open **http://localhost:9090**

Key metrics:

```promql
# Agent execution latency p95
histogram_quantile(0.95,
  sum(rate(agent_execution_duration_ms_bucket[5m])) by (le, agent_role)
)

# MCP timeout rate
sum(rate(mcp_tool_calls_total{status="timeout"}[5m])) by (tool_name)

# THA healing tickets per hour
sum(increase(tha_tickets_total[1h])) by (fault_type)

# Viability score distribution
histogram_quantile(0.50, rate(synthesis_viability_score_bucket[1h]))

# Kafka consumer lag (THA)
kafka_consumer_group_lag{group="tha-consumer-group"}
```

### Distributed Traces

Traces export via OTLP gRPC to the OTel Collector (`localhost:4317`).
Configure a Jaeger or Tempo datasource in Grafana to visualize trace waterfalls.

Every trace captures the full request journey:
```
gia.handle_request [38s]
├── cog.planning_node [2.75s]
├── cog.fan_out_node [95ms]
├── dsw.geological_expert [550ms]
│   ├── svb.token_issued [12ms]
│   ├── mcp.query_mining_deposits [190ms]
│   └── mcp.get_geological_survey [68ms]
├── dsw.logistics_coordinator [790ms] ← fallback
│   └── mcp.query_transport_routes [420ms]
├── ... (9 more DSW spans)
├── cog.healing_node [35ms]
└── cog.synthesis_node [3.45s]
```

### Structured Logs

All components emit JSON logs with OTel trace context:

```json
{
  "timestamp": "2026-05-30T00:06:37.412Z",
  "level": "WARNING",
  "service": "gpu-supply-chain-mas",
  "logger": "agents.edge.tha",
  "message": "[THA] Ticket created: TKT-A3F7B2 | fault=mcp_timeout | worker=logistics_coordinator | strategy=use_alternate_tool",
  "trace_id": "4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a",
  "span_id":  "1a2b3c4d5e6f7a8b"
}
```

Pipe logs to any aggregator (Loki, Elasticsearch, CloudWatch) — the `trace_id` field links every log line to its Grafana trace.

---

## 🛠️ Development Guide

### Running Tests

```bash
# Unit tests
pytest tests/unit/ -v

# Integration tests (requires running infrastructure)
pytest tests/integration/ -v --timeout=120

# Specific component
pytest tests/unit/test_svb.py -v
pytest tests/unit/test_cog_nodes.py -v
```

### Simulating THA Fault Injection

```bash
# Manually publish a fault event to test THA healing
python scripts/inject_fault.py \
  --event-type mcp_timeout \
  --worker logistics_coordinator \
  --session-id sess-test-001
```

### Inspecting the DTB Queue

```bash
# Check queue depth per worker
python scripts/queue_status.py

# View dead-letter queue contents
python scripts/dlq_inspector.py --worker-type geological_expert
```

### LangGraph State Inspector

```bash
# Inspect COG state for a live session
python scripts/inspect_state.py --session-id sess-9e7f2a1b
```

---

## ➕ Adding a New DSW Specialist

1. **Create the specialist file:**

```python
# agents/specialists/my_new_specialist.py
from agents.specialists.base_dsw import BaseDSW
from orchestration.cog.state_schema import DSWWorkerType
from core_services.svb import MCPScope

class MyNewSpecialistDSW(BaseDSW):
    worker_type    = DSWWorkerType.MY_NEW_TYPE       # Add to DSWWorkerType enum
    mcp_server_id  = "my-new-mcp-server"
    required_scopes = [MCPScope.MY_SCOPE.value]       # Add to MCPScope enum
    available_tools = ["my_tool_1", "my_tool_2"]

    def execute_core(self, token, query, region_focus, material_focus, span):
        result, call = self.call_mcp_tool("my_tool_1", {...}, token, span)
        return (
            {"summary": "...", "data": result},  # result_data
            [call],                               # mcp_calls
            0.85,                                 # confidence
            ["my-mcp-server/my_tool_1"],          # sources
        )
```

2. **Register in `DSWWorkerType` enum** (`orchestration/cog/state_schema.py`):
```python
MY_NEW_TYPE = "my_new_type"
```

3. **Add scope policy** (`core_services/svb.py`):
```python
WORKER_SCOPE_POLICY["my_new_type"] = [MCPScope.MY_SCOPE.value]
```

4. **Register MCP server** (`configs/mcp_registry.yaml` and `THA`'s `MCP_FALLBACK_REGISTRY`).

5. **Add to specialists `__init__.py`** and the **planner prompt table** (`orchestration/cog/prompts.py`).

---

## 🔒 Security Model

### Zero-Trust Credential Flow

```
DSW never holds credentials. Every MCP call requires a fresh SVB token.

1. DSW signs CredentialRequest with HMAC-SHA256(request_id:task_id:worker_id)
2. SVB verifies HMAC → validates scope policy → fetches from Vault KV
3. SVB returns EphemeralToken (TTL ≤ 15 min, scoped to ONE MCP server)
4. DSW uses token inline (Authorization: Bearer <token>)
5. Token goes out of scope — never written to disk, DB, or logs
```

### Security Checklist

- [x] HMAC-signed credential requests (prevents forged requests)
- [x] Scope policy enforcement (workers can't over-request permissions)
- [x] Anti-replay: `task_id` must be active in DTB
- [x] Token TTL: max 15 minutes, default 5 minutes
- [x] Vault KV audit trail: every issuance logged with token hash
- [x] Token revocation list: SVB in-memory + Kafka broadcast
- [x] Prompt injection prevention in GIA (regex pattern filter)
- [x] Kafka topics use consumer groups (at-least-once delivery)
- [x] All secrets via environment variables — no hardcoded values

### Production Hardening Checklist

- [ ] Enable Kafka SASL/SSL (`KAFKA_SECURITY_PROTOCOL=SASL_SSL`)
- [ ] Replace Vault dev mode with Vault HA cluster
- [ ] Enable Redis AUTH (`requirepass`)
- [ ] Rotate `SVB_HMAC_KEY` and `DSW_HMAC_KEY` via Vault dynamic secrets
- [ ] Configure Alertmanager for critical THA ticket alerts
- [ ] Enable Grafana OIDC SSO

---

## 🐛 Troubleshooting

### Common Issues

**GIA returns `status: failed` with `fatal_error: planning_node LLM failure`**
```
→ Check GOOGLE_API_KEY is set and Gemini API is accessible
→ Verify rate limits: planning uses temperature=0.1, JSON mode
```

**DSW results all `status: timeout`**
```
→ MCP servers unreachable: check configs/mcp_registry.yaml URLs
→ Check Redis: redis-cli -p 6379 ping
→ Verify DTB queue: python scripts/queue_status.py
```

**THA not generating healing tickets**
```
→ Check THA is running: ps aux | grep tha
→ Verify Kafka connectivity: docker-compose logs kafka
→ Check consumer group: kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group tha-consumer-group
```

**SVB `SVBAuthFailure: HMAC signature verification failed`**
```
→ SVB_HMAC_KEY and DSW_HMAC_KEY must match exactly
→ Both must be set in .env — no trailing whitespace
```

**Grafana shows no data**
```
→ Verify Prometheus targets: http://localhost:9090/targets
→ Check OTel Collector: docker-compose logs otel-collector
→ Ensure agents are running with PROMETHEUS_PORT=8000
```

### Log Levels

```bash
# Enable debug logging for a specific component
LOG_LEVEL=DEBUG python -m agents.edge.tha

# Trace all Kafka messages
KAFKA_DEBUG=all python -m agents.edge.tha
```

### Useful Commands

```bash
# View all Kafka messages on agent-faults topic
docker exec -it gpu-mra-system-kafka-1 \
  kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic agent-faults --from-beginning

# Flush dead-letter queue (re-queue failed jobs)
python scripts/dlq_requeue.py --worker-type logistics_coordinator

# Reset a COG session (clear LangGraph checkpoint)
python scripts/reset_session.py --session-id sess-9e7f2a1b

# View Vault secrets (dev mode)
VAULT_ADDR=http://localhost:8200 VAULT_TOKEN=dev-root-token \
  vault kv list secret/gpu-supply-chain/mcp-servers
```

---

## 📐 Data Flow Reference

```
User Query
  │
  ▼ [GIA] Validate + Extract (materials, region)
  │
  ▼ [COG planning_node] LLM → 11 PlanSteps
  │
  ▼ [COG fan_out_node] → DTB publishes 11 jobs to Redis Streams
  │
  ├─▶ [DSW-1] SVB token → MCP tool calls → DSWResult appended
  ├─▶ [DSW-2] SVB token → MCP tool calls → DSWResult appended
  ├─▶ ... (up to 11 parallel)
  │
  │   [THA] Kafka: agent-faults → detect timeout → generate ticket
  │   → inject THAInjection → healing_node → re-dispatch to fallback
  │
  ▼ [COG synthesis_node] Aggregate 11 DSWResults → LLM → Blueprint
  │
  ▼ [GIA] Format GIAResponse → HTTP 200 to user
```

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/new-dsw-specialist`
3. Follow the [Adding a New DSW Specialist](#-adding-a-new-dsw-specialist) guide
4. Add unit tests in `tests/unit/`
5. Submit a pull request

---

*Built for India's GPU Manufacturing Initiative — powering semiconductor supply chain intelligence with autonomous AI agents.*
