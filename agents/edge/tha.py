"""
agents/edge/tha.py
===================
Telemetry & Healing Agent (THA)

The THA is the autonomous watchdog and self-healing engine. It runs as a
dedicated background process subscribing to Kafka telemetry topics and
autonomously responds to system faults without blocking the main workflow.

Architecture:
  Kafka Consumer (sys-events, agent-faults)
       │
       ▼ Raw event
  TelemetryEventClassifier
       │
       ▼ Classified fault
  FaultHandler
       │
       ├─ [LOW/INFO]     → Log only, no action
       ├─ [MEDIUM]       → Log ticket + generate THAInjection (alternate tool)
       ├─ [HIGH]         → Log ticket + generate THAInjection (swap specialist)
       └─ [CRITICAL/DLQ] → Log ticket + escalate + inject PARTIAL_ACCEPT
       │
       ▼ THAInjection
  COG State Update (via Redis LangGraph checkpoint)
       │
       ▼
  Kafka Publisher → tha-remediations topic (for COG consumption)

Kafka subscriptions:
  - sys-events:   General system events (timeouts, slow queries, planning failures)
  - agent-faults: DSW worker errors, MCP connection failures, DLQ events

Self-healing capabilities:
  1. MCP connection timeout → retry with alternate MCP server
  2. DSW worker crash → swap to backup specialist worker
  3. Missing data → inject PARTIAL_RESULT_ACCEPT, flag as data gap
  4. LLM rate limit → exponential backoff + queue reorder
  5. Redis unavailability → activate local queue fallback
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


# ============================================================================
# FAULT CLASSIFICATION ENGINE
# ============================================================================

class FaultType(str, Enum):
    """THA-recognized fault types from Kafka events."""
    MCP_TIMEOUT           = "mcp_timeout"
    MCP_CONNECTION_ERROR  = "mcp_connection_error"
    MCP_AUTH_FAILURE      = "mcp_auth_failure"
    DSW_WORKER_CRASH      = "dsw_worker_crash"
    DSW_DATA_MISSING      = "dsw_data_missing"
    JOB_DLQ               = "job_dlq"
    JOB_TIMEOUT           = "job_timeout"
    LLM_RATE_LIMIT        = "llm_rate_limit"
    LLM_CONTEXT_OVERFLOW  = "llm_context_overflow"
    REDIS_UNAVAILABLE     = "redis_unavailable"
    PLANNING_FAILURE      = "planning_failure"
    SYNTHESIS_FAILURE     = "synthesis_failure"
    UNKNOWN               = "unknown"


# Fault type → healing strategy mapping
FAULT_HEALING_MAP: Dict[FaultType, str] = {
    FaultType.MCP_TIMEOUT:           "use_alternate_tool",
    FaultType.MCP_CONNECTION_ERROR:  "use_alternate_tool",
    FaultType.MCP_AUTH_FAILURE:      "retry_same_tool",     # SVB will re-issue token
    FaultType.DSW_WORKER_CRASH:      "swap_specialist",
    FaultType.DSW_DATA_MISSING:      "partial_result_accept",
    FaultType.JOB_DLQ:               "partial_result_accept",
    FaultType.JOB_TIMEOUT:           "retry_same_tool",
    FaultType.LLM_RATE_LIMIT:        "retry_same_tool",
    FaultType.LLM_CONTEXT_OVERFLOW:  "swap_specialist",
    FaultType.REDIS_UNAVAILABLE:     "partial_result_accept",
    FaultType.PLANNING_FAILURE:      "partial_result_accept",
    FaultType.SYNTHESIS_FAILURE:     "partial_result_accept",
    FaultType.UNKNOWN:               "partial_result_accept",
}

# Alternate MCP server fallback registry
# Primary → backup server mappings
MCP_FALLBACK_REGISTRY: Dict[str, str] = {
    "geological-mcp-server":     "geological-mcp-backup",
    "chemical-mcp-server":       "chemical-mcp-backup",
    "trade-mcp-server":          "trade-api-fallback",
    "logistics-mcp-server":      "logistics-mcp-backup",
    "industrial-mcp-server":     "industrial-db-fallback",
    "lease-mcp-server":          "govt-data-portal-fallback",
    "env-mcp-server":            "env-data-static-cache",
    "trade-policy-mcp-server":   "policy-mcp-backup",
    "materials-mcp-server":      "materials-db-fallback",
    "qa-mcp-server":             "qa-static-specs-server",
}

# Alternative tools to use on timeout
TOOL_FALLBACK_REGISTRY: Dict[str, str] = {
    "query_mining_deposits":     "get_geological_survey",       # Broader but less precise
    "get_lease_status":          "query_active_leases",
    "query_chemical_plants":     "get_industrial_directory",
    "get_port_data":             "query_trade_routes",
    "get_fab_facilities":        "query_osat_capacity",
}


# ============================================================================
# REMEDIATION TICKET
# ============================================================================

@dataclass
class RemediationTicket:
    """
    Auto-generated internal incident ticket created by THA.
    Analogous to a Jira ticket but fully automated and machine-readable.
    """
    ticket_id:      str       = field(default_factory=lambda: f"TKT-{uuid.uuid4().hex[:6].upper()}")
    created_at:     str       = field(default_factory=lambda: datetime.utcnow().isoformat())
    severity:       str       = "medium"
    fault_type:     str       = ""
    affected_worker: str      = ""
    affected_job_id: str      = ""
    session_id:     str       = ""
    trace_id:       str       = ""
    description:    str       = ""
    root_cause:     str       = ""
    healing_strategy: str     = ""
    fallback_tool:  str       = ""
    fallback_server: str      = ""
    auto_resolved:  bool      = False
    resolved_at:    Optional[str] = None
    sla_breach:     bool      = False      # True if fault exceeded 30s SLA


# ============================================================================
# TELEMETRY & HEALING AGENT
# ============================================================================

class TelemetryHealingAgent:
    """
    THA: Autonomous watchdog and self-healing engine.

    Runs three concurrent threads:
      1. sys_events_consumer   — subscribes to sys-events topic
      2. agent_faults_consumer — subscribes to agent-faults topic
      3. heartbeat_monitor     — checks for stale jobs via Redis TTL monitoring

    On fault detection:
      → Classify fault type and severity
      → Generate RemediationTicket (internal audit)
      → Create THAInjection (COG state update)
      → Publish healing directive to tha-remediations Kafka topic
      → Update LangGraph checkpoint to inject THAInjection into COG state
    """

    KAFKA_TOPICS_SUBSCRIBED = [
        "sys-events",    # General system lifecycle
        "agent-faults",  # DSW errors, MCP failures, DLQ
    ]

    SLA_THRESHOLD_SECONDS = 30    # Fault resolution SLA

    def __init__(
        self,
        kafka_bootstrap:  List[str] = None,
        redis_host:       str = "localhost",
        redis_port:       int = 6379,
        langgraph_store   = None,      # LangGraph checkpointer for state injection
    ):
        self.kafka_bootstrap  = kafka_bootstrap or ["localhost:9092"]
        self.redis_host       = redis_host
        self.redis_port       = redis_port
        self.langgraph_store  = langgraph_store

        self._running   = False
        self._threads: List[threading.Thread] = []
        self._tickets:  List[RemediationTicket] = []    # In-memory ticket store

    # ── Public API ───────────────────────────────────────────────────────────

    def start(self) -> None:
        """Start the THA's background consumer threads."""
        self._running = True
        logger.info("[THA] Starting Telemetry & Healing Agent...")

        # Thread 1: sys-events consumer
        t1 = threading.Thread(
            target = self._consume_loop,
            args   = ("sys-events", "tha-consumer-group", self._handle_sys_event),
            daemon = True,
            name   = "tha-sys-events",
        )

        # Thread 2: agent-faults consumer
        t2 = threading.Thread(
            target = self._consume_loop,
            args   = ("agent-faults", "tha-consumer-group", self._handle_agent_fault),
            daemon = True,
            name   = "tha-agent-faults",
        )

        # Thread 3: Heartbeat / stale job monitor
        t3 = threading.Thread(
            target = self._heartbeat_monitor,
            daemon = True,
            name   = "tha-heartbeat",
        )

        self._threads = [t1, t2, t3]
        for t in self._threads:
            t.start()

        logger.info("[THA] All consumer threads started.")

    def stop(self) -> None:
        """Gracefully stop the THA."""
        self._running = False
        for t in self._threads:
            t.join(timeout=5)
        logger.info("[THA] Stopped.")

    def get_tickets(self) -> List[RemediationTicket]:
        """Return all generated remediation tickets (for dashboard/audit)."""
        return list(self._tickets)

    # ── Kafka Consumer Loop ───────────────────────────────────────────────────

    def _consume_loop(
        self,
        topic:    str,
        group_id: str,
        handler:  Callable[[Dict[str, Any]], None],
    ) -> None:
        """
        Blocking Kafka consumer loop with auto-reconnect.
        Runs until self._running is False.
        """
        while self._running:
            try:
                from kafka import KafkaConsumer
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers      = self.kafka_bootstrap,
                    group_id               = group_id,
                    value_deserializer     = lambda v: json.loads(v.decode("utf-8")),
                    auto_offset_reset      = "earliest",
                    enable_auto_commit     = False,
                    session_timeout_ms     = 30_000,
                    consumer_timeout_ms    = 5_000,
                )

                logger.info(f"[THA] Consumer connected to topic: {topic}")

                for record in consumer:
                    if not self._running:
                        break
                    try:
                        handler(record.value)
                        consumer.commit()
                    except Exception as e:
                        logger.error(f"[THA] Handler error on {topic}: {e}")

                consumer.close()

            except Exception as e:
                if self._running:
                    logger.warning(f"[THA] Kafka connection lost on {topic}: {e}. Reconnecting in 5s...")
                    time.sleep(5)

    # ── Event Handlers ────────────────────────────────────────────────────────

    def _handle_sys_event(self, event: Dict[str, Any]) -> None:
        """
        Handler for sys-events topic messages.

        Filters for actionable faults:
          - job_timeout: A DTB job exceeded its deadline
          - planning_failure: COG planning_node failed
          - mcp_auth_failure: SVB token rejected by MCP server
        """
        event_type = event.get("event_type", "")
        severity   = event.get("severity", "low")

        if severity in ("low", "info"):
            logger.debug(f"[THA] sys-event [{severity}]: {event_type}")
            return

        logger.warning(f"[THA] Actionable sys-event: {event_type} | severity={severity}")

        # Map to fault type
        fault_type = self._classify_fault(event_type)
        if fault_type == FaultType.UNKNOWN and severity not in ("high", "critical"):
            return  # Skip low-signal unknowns

        self._process_fault(event, fault_type)

    def _handle_agent_fault(self, event: Dict[str, Any]) -> None:
        """
        Handler for agent-faults topic messages.

        All messages here are actionable — the DTB only publishes here for
        DLQ events and confirmed worker crashes.

        Fault flow example (Copper MCP timeout):
          1. DTB worker publishes `{"event_type": "mcp_timeout", "worker_type": "logistics_coordinator",
             "mcp_server_id": "logistics-mcp-server", "job_id": "job-xyz", "severity": "medium"}`
          2. THA receives here, creates TKT-XXXXXX
          3. Determines fallback: trade-api-fallback MCP + alternate tool
          4. Creates THAInjection with USE_ALTERNATE_TOOL strategy
          5. Publishes THAInjection to tha-remediations topic
          6. Injects into LangGraph checkpoint → healing_node picks up
        """
        logger.warning(f"[THA] Agent fault received: {event.get('event_type')} | worker={event.get('worker_type')}")

        fault_type = self._classify_fault(event.get("event_type", ""))
        self._process_fault(event, fault_type)

    # ── Fault Processing ──────────────────────────────────────────────────────

    def _process_fault(
        self,
        event:      Dict[str, Any],
        fault_type: FaultType,
    ) -> None:
        """
        Core THA healing workflow:
          1. Generate RemediationTicket
          2. Determine healing strategy + fallback resources
          3. Create THAInjection
          4. Publish to tha-remediations Kafka topic
          5. Optionally inject into LangGraph checkpoint
        """
        worker_type   = event.get("worker_type", "unknown")
        job_id        = event.get("job_id", "")
        session_id    = event.get("session_id", "")
        trace_id      = event.get("trace_id", "")
        mcp_server_id = event.get("mcp_server_id", "")
        severity      = event.get("severity", "medium")

        # ── 1. Create Remediation Ticket ─────────────────────────────────────
        strategy       = FAULT_HEALING_MAP.get(fault_type, "partial_result_accept")
        fallback_server = MCP_FALLBACK_REGISTRY.get(mcp_server_id, "")
        fallback_tool  = ""

        if fault_type in (FaultType.MCP_TIMEOUT, FaultType.MCP_CONNECTION_ERROR):
            failed_tool   = event.get("tool_name", "")
            fallback_tool = TOOL_FALLBACK_REGISTRY.get(failed_tool, "")

        ticket = RemediationTicket(
            severity         = severity,
            fault_type       = fault_type.value,
            affected_worker  = worker_type,
            affected_job_id  = job_id,
            session_id       = session_id,
            trace_id         = trace_id,
            description      = self._generate_ticket_description(fault_type, event),
            root_cause       = event.get("error_msg", event.get("detail", "Unknown")),
            healing_strategy = strategy,
            fallback_tool    = fallback_tool,
            fallback_server  = fallback_server,
            sla_breach       = self._check_sla_breach(event),
        )
        self._tickets.append(ticket)

        logger.warning(
            f"[THA] Ticket created: {ticket.ticket_id} | "
            f"fault={fault_type.value} | worker={worker_type} | "
            f"strategy={strategy}"
        )

        # ── 2. Create THAInjection ────────────────────────────────────────────
        tha_injection = {
            "injection_id":      str(uuid.uuid4()),
            "triggered_by":      event.get("event_id", ""),
            "target_step_id":    event.get("step_id", ""),
            "target_worker":     worker_type,
            "strategy":          strategy,
            "fallback_tool":     fallback_tool,
            "fallback_server":   fallback_server,
            "remediation_query": self._build_remediation_query(fault_type, event),
            "ticket_id":         ticket.ticket_id,
            "injected_at":       datetime.utcnow().isoformat(),
            "applied":           False,
            "session_id":        session_id,
        }

        # ── 3. Publish to tha-remediations ────────────────────────────────────
        self._publish_remediation(tha_injection, session_id)

        # ── 4. Inject into LangGraph checkpoint ──────────────────────────────
        if self.langgraph_store and session_id:
            self._inject_into_cog_state(session_id, tha_injection)

        logger.info(
            f"[THA] Healing directive published: ticket={ticket.ticket_id} | "
            f"session={session_id} | strategy={strategy}"
        )

    def _publish_remediation(
        self,
        injection: Dict[str, Any],
        session_id: str,
    ) -> None:
        """Publish THAInjection dict to tha-remediations Kafka topic."""
        try:
            from kafka import KafkaProducer
            producer = KafkaProducer(
                bootstrap_servers  = self.kafka_bootstrap,
                value_serializer   = lambda v: json.dumps(v).encode("utf-8"),
                key_serializer     = lambda k: k.encode("utf-8"),
            )
            producer.send("tha-remediations", injection, key=session_id)
            producer.flush()
            producer.close()
        except Exception as e:
            logger.error(f"[THA] Failed to publish remediation: {e}")

    def _inject_into_cog_state(
        self,
        session_id: str,
        injection:  Dict[str, Any],
    ) -> None:
        """
        Directly update the LangGraph checkpoint to inject THAInjection
        into the COG state. This allows the healing_node to pick it up
        on the next graph routing evaluation.

        In production: uses LangGraph's BaseCheckpointSaver.put() API
        to append the injection to the tha_injections field.
        """
        try:
            from orchestration.cog.state_schema import THAInjection, DSWWorkerType, HealingStrategy

            tha_inj = THAInjection(
                injection_id      = injection["injection_id"],
                triggered_by      = injection.get("triggered_by", ""),
                target_step_id    = injection.get("target_step_id", ""),
                target_worker     = injection["target_worker"],
                strategy          = HealingStrategy(injection["strategy"]),
                fallback_tool     = injection.get("fallback_tool"),
                fallback_server   = injection.get("fallback_server"),
                remediation_query = injection.get("remediation_query"),
                ticket_id         = injection["ticket_id"],
            )

            if self.langgraph_store:
                # Append to tha_injections in checkpoint
                thread_config = {"configurable": {"thread_id": session_id}}
                current_state = self.langgraph_store.get(thread_config)
                if current_state:
                    existing = current_state.values.get("tha_injections", [])
                    existing.append(tha_inj)
                    self.langgraph_store.put(
                        thread_config,
                        {"tha_injections": existing},
                        {},
                    )
        except Exception as e:
            logger.error(f"[THA] LangGraph injection failed: {e}")

    # ── Heartbeat Monitor ─────────────────────────────────────────────────────

    def _heartbeat_monitor(self) -> None:
        """
        Periodically check Redis for stale jobs that exceeded their deadline
        without emitting a fault event (silent failures).

        Runs every 15 seconds.
        """
        while self._running:
            try:
                import redis as redis_lib
                r = redis_lib.Redis(
                    host=self.redis_host,
                    port=self.redis_port,
                    db=1,
                    decode_responses=True,
                )
                # Scan for jobs in EXECUTING state past their deadline
                cursor = 0
                while True:
                    cursor, keys = r.scan(cursor, match="dtb:job:*", count=100)
                    for key in keys:
                        job = r.hgetall(key)
                        if job.get("status") == "executing":
                            deadline_str = job.get("deadline", "")
                            if deadline_str:
                                try:
                                    deadline = datetime.fromisoformat(deadline_str)
                                    if datetime.utcnow() > deadline:
                                        # Silent timeout — emit synthetic fault
                                        fault = {
                                            "event_type":  "job_timeout",
                                            "severity":    "medium",
                                            "job_id":      job.get("job_id", ""),
                                            "worker_type": job.get("worker_type", ""),
                                            "session_id":  job.get("session_id", ""),
                                            "trace_id":    job.get("trace_id", ""),
                                            "error_msg":   "Silent timeout detected by THA heartbeat",
                                        }
                                        self._process_fault(fault, FaultType.JOB_TIMEOUT)
                                except ValueError:
                                    pass
                    if cursor == 0:
                        break
            except Exception as e:
                logger.debug(f"[THA] Heartbeat monitor error: {e}")

            time.sleep(15)

    # ── Utility Methods ───────────────────────────────────────────────────────

    def _classify_fault(self, event_type: str) -> FaultType:
        """Map raw event_type string to FaultType enum."""
        mapping = {
            "mcp_timeout":            FaultType.MCP_TIMEOUT,
            "mcp_connection_error":   FaultType.MCP_CONNECTION_ERROR,
            "mcp_auth_failure":       FaultType.MCP_AUTH_FAILURE,
            "dsw_worker_crash":       FaultType.DSW_WORKER_CRASH,
            "dsw_data_missing":       FaultType.DSW_DATA_MISSING,
            "job_dlq":                FaultType.JOB_DLQ,
            "job_timeout":            FaultType.JOB_TIMEOUT,
            "llm_rate_limit":         FaultType.LLM_RATE_LIMIT,
            "llm_context_overflow":   FaultType.LLM_CONTEXT_OVERFLOW,
            "redis_unavailable":      FaultType.REDIS_UNAVAILABLE,
            "planning_failure":       FaultType.PLANNING_FAILURE,
            "synthesis_failure":      FaultType.SYNTHESIS_FAILURE,
        }
        return mapping.get(event_type, FaultType.UNKNOWN)

    def _generate_ticket_description(
        self,
        fault_type: FaultType,
        event:      Dict[str, Any],
    ) -> str:
        """Generate human-readable ticket description."""
        worker = event.get("worker_type", "unknown")
        job_id = event.get("job_id", "N/A")
        ts     = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        descriptions = {
            FaultType.MCP_TIMEOUT:
                f"MCP server timeout for {worker} worker (job: {job_id}) at {ts}. "
                f"Tool: {event.get('tool_name', 'unknown')}. Auto-retry with alternate tool.",
            FaultType.MCP_CONNECTION_ERROR:
                f"Cannot connect to MCP server {event.get('mcp_server_id', 'unknown')} "
                f"for {worker} (job: {job_id}). Routing to fallback server.",
            FaultType.JOB_DLQ:
                f"Job {job_id} for {worker} exhausted {event.get('retry_count', 3)} retries. "
                f"Moved to dead-letter queue. Accepting partial result.",
            FaultType.JOB_TIMEOUT:
                f"Job {job_id} for {worker} exceeded deadline. THA triggered re-dispatch.",
            FaultType.DSW_DATA_MISSING:
                f"{worker} returned empty dataset for job {job_id}. "
                f"Accepting partial result and flagging as data gap.",
        }
        return descriptions.get(
            fault_type,
            f"Unknown fault [{fault_type.value}] for {worker} job {job_id} at {ts}"
        )

    def _build_remediation_query(
        self,
        fault_type: FaultType,
        event:      Dict[str, Any],
    ) -> str:
        """Build a revised query for re-dispatch if needed."""
        original_query = event.get("original_query", "")
        if fault_type in (FaultType.MCP_TIMEOUT, FaultType.MCP_CONNECTION_ERROR):
            return f"{original_query} [RETRY via alternate data source — original MCP unavailable]"
        return original_query

    def _check_sla_breach(self, event: Dict[str, Any]) -> bool:
        """Check if the fault represents an SLA breach (>30s to detect)."""
        event_time_str = event.get("timestamp", "")
        if not event_time_str:
            return False
        try:
            event_time = datetime.fromisoformat(event_time_str)
            age_s = (datetime.utcnow() - event_time).total_seconds()
            return age_s > self.SLA_THRESHOLD_SECONDS
        except (ValueError, TypeError):
            return False
