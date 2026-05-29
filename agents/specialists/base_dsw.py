"""
agents/specialists/base_dsw.py
================================
Base class for all Domain Specialist Workers (DSW).

Every DSW inherits this class to get:
  - Standard MCP tool invocation with SVB credential injection
  - OpenTelemetry tracing (agent_role, task_id, mcp_tool_invoked, latency_ms)
  - Structured Kafka fault event publishing
  - Pydantic output validation
  - Retry-safe execution wrapper

DSW execution flow:
  DTB Worker picks up job
       │
       ▼ request_mcp_credentials(SVB, mcp_server_id, required_scopes)
  SVB returns EphemeralToken
       │
       ▼ execute(task_id, query, context, token)
  BaseDSW._traced_execute()
       │
       ├─ call_mcp_tool(tool_name, params, token)  [with OTel span]
       ├─ parse_result()
       └─ return DSWResult
       │
       ▼ DTB.complete_job(result) / fail_job(error)
"""

from __future__ import annotations

import json
import os
import time
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from orchestration.cog.state_schema import DSWResult, MCPToolCall, DSWWorkerType
from core_services.svb import EphemeralToken, SecureVaultBroker, request_mcp_credentials
from core_services.kafka_streams import KafkaStreamManager, KafkaTopic


# ============================================================================
# SYSTEM PROMPT TEMPLATE for all DSWs
# ============================================================================

DSW_BASE_SYSTEM_PROMPT = """
You are a {worker_role} specialized in India's GPU manufacturing supply chain.

Your mission: {mission}

Geographic focus: {region_focus}
Material focus: {material_focus}

## Your Capabilities
You have exclusive access to {mcp_server_id} via Model Context Protocol.
Available tools: {available_tools}

## Response Requirements
Return a JSON object with this exact structure:
{{
  "summary": "<2-3 sentence summary of findings>",
  "data": {{
    <domain-specific data fields>
  }},
  "confidence": <0.0-1.0>,
  "sources_cited": ["<source1>", "<source2>"],
  "data_quality": "<high|medium|low>",
  "caveats": ["<caveat1>"]
}}

## Critical Rules
1. Only use tools you have been granted access to
2. Be precise about quantities, grades, and geographic locations
3. If data is unavailable, say so explicitly — never hallucinate
4. Cite the specific MCP tool and query used for each finding
5. confidence=0.0 if no data found, 1.0 if fully verified
"""


# ============================================================================
# BASE DSW
# ============================================================================

class BaseDSW(ABC):
    """
    Abstract base for all Domain Specialist Workers.

    Subclasses must implement:
      - worker_type:   DSWWorkerType (class attribute)
      - mcp_server_id: str (class attribute)
      - required_scopes: List[str] (class attribute)
      - available_tools: List[str] (class attribute)
      - system_prompt(): str (generates role-specific prompt)
      - execute_core(): main tool execution logic
    """

    worker_type:      DSWWorkerType
    mcp_server_id:    str
    required_scopes:  List[str]
    available_tools:  List[str]

    def __init__(
        self,
        worker_id:  str = "",
        svb:        Optional[SecureVaultBroker] = None,
        kafka:      Optional[KafkaStreamManager] = None,
    ):
        self.worker_id  = worker_id or f"{self.worker_type.value}-{uuid.uuid4().hex[:6]}"
        self.svb        = svb or SecureVaultBroker(backend="env")
        self.kafka      = kafka or KafkaStreamManager()

    # ── Public execution interface ────────────────────────────────────────────

    def execute(
        self,
        task_id:      str,
        job_id:       str,
        session_id:   str,
        trace_id:     str,
        query:        str,
        region_focus: str,
        material_focus: str,
        **kwargs,
    ) -> DSWResult:
        """
        Execute this DSW's task:
          1. Request ephemeral credentials from SVB
          2. Run domain-specific logic (execute_core)
          3. Package result as DSWResult
          4. Emit telemetry events

        This method NEVER stores credentials — the EphemeralToken
        is used inline and goes out of scope after execute_core().

        Args:
            task_id:        DTB task identifier
            job_id:         DTB job identifier
            session_id:     COG session identifier
            trace_id:       OTel root trace ID
            query:          Specific sub-query from PlanStep
            region_focus:   Geographic region
            material_focus: Target material

        Returns:
            DSWResult with data or error info
        """
        from utils.telemetry import get_tracer

        tracer  = get_tracer()
        t_start = time.perf_counter()

        with tracer.start_as_current_span(f"dsw.{self.worker_type.value}") as span:
            span.set_attribute("agent_role",    self.worker_type.value)
            span.set_attribute("task_id",       task_id)
            span.set_attribute("job_id",        job_id)
            span.set_attribute("session_id",    session_id)
            span.set_attribute("trace_id",      trace_id)
            span.set_attribute("mcp_server_id", self.mcp_server_id)
            span.set_attribute("region",        region_focus)
            span.set_attribute("material",      material_focus)

            mcp_calls: List[MCPToolCall] = []

            try:
                # ── Step 1: Request credentials from SVB ─────────────────────
                token = request_mcp_credentials(
                    svb             = self.svb,
                    worker_id       = self.worker_id,
                    worker_type     = self.worker_type.value,
                    task_id         = task_id,
                    session_id      = session_id,
                    trace_id        = trace_id,
                    mcp_server_id   = self.mcp_server_id,
                    required_scopes = self.required_scopes,
                    worker_hmac_key = os.environ.get("DSW_HMAC_KEY", "dev-hmac-key"),
                )
                span.set_attribute("svb_token_obtained", True)

                # ── Step 2: Execute core domain logic ─────────────────────────
                result_data, mcp_calls, confidence, sources = self.execute_core(
                    token         = token,
                    query         = query,
                    region_focus  = region_focus,
                    material_focus = material_focus,
                    span          = span,
                )

                elapsed_ms = (time.perf_counter() - t_start) * 1000
                span.set_attribute("execution_latency_ms", round(elapsed_ms, 2))
                span.set_attribute("confidence",           round(confidence, 3))
                span.set_attribute("mcp_tool_invoked",     json.dumps([c.tool_name for c in mcp_calls]))

                # ── Step 3: Build DSWResult ───────────────────────────────────
                summary = result_data.pop("summary", "No summary available")
                return DSWResult(
                    job_id        = job_id,
                    step_id       = task_id,
                    worker_type   = self.worker_type,
                    status        = "completed",
                    data          = result_data,
                    summary       = summary,
                    confidence    = confidence,
                    sources_cited = sources,
                    mcp_calls     = mcp_calls,
                    execution_ms  = elapsed_ms,
                )

            except Exception as e:
                elapsed_ms = (time.perf_counter() - t_start) * 1000
                span.set_attribute("error",                str(e))
                span.set_attribute("execution_latency_ms", round(elapsed_ms, 2))
                span.set_attribute("status",               "failed")

                # Publish fault to Kafka for THA
                self._publish_fault_event(
                    job_id     = job_id,
                    task_id    = task_id,
                    session_id = session_id,
                    trace_id   = trace_id,
                    error      = e,
                    mcp_calls  = mcp_calls,
                )

                return DSWResult(
                    job_id        = job_id,
                    step_id       = task_id,
                    worker_type   = self.worker_type,
                    status        = "failed",
                    error_message = str(e),
                    mcp_calls     = mcp_calls,
                    execution_ms  = elapsed_ms,
                )

    # ── Abstract methods ──────────────────────────────────────────────────────

    @abstractmethod
    def execute_core(
        self,
        token:         EphemeralToken,
        query:         str,
        region_focus:  str,
        material_focus: str,
        span:          Any,
    ) -> tuple[Dict[str, Any], List[MCPToolCall], float, List[str]]:
        """
        Domain-specific execution logic.

        Args:
            token:          Ephemeral MCP credentials from SVB
            query:          Specific sub-query
            region_focus:   Geographic region
            material_focus: Target material
            span:           OTel span for adding attributes

        Returns:
            Tuple of:
              - result_data: Dict (must include "summary" key)
              - mcp_calls:   List[MCPToolCall] for audit
              - confidence:  Float 0.0–1.0
              - sources:     List[str] of data sources cited
        """
        ...

    # ── MCP Tool Invocation ───────────────────────────────────────────────────

    def call_mcp_tool(
        self,
        tool_name: str,
        params:    Dict[str, Any],
        token:     EphemeralToken,
        span:      Any = None,
    ) -> tuple[Dict[str, Any], MCPToolCall]:
        """
        Execute an MCP tool call with the ephemeral token.

        Wraps the call in an OTel child span and records timing.
        Token is ONLY used for the Authorization header — never stored.

        Args:
            tool_name: MCP tool identifier
            params:    Tool parameters
            token:     EphemeralToken from SVB
            span:      Parent OTel span

        Returns:
            Tuple of (result dict, MCPToolCall audit record)

        Raises:
            ConnectionError: On MCP server unreachable (triggers THA)
            TimeoutError:    On MCP server timeout (triggers THA retry)
        """
        from utils.telemetry import get_tracer
        t_start = time.perf_counter()

        with get_tracer().start_as_current_span(f"mcp.{tool_name}") as tool_span:
            tool_span.set_attribute("mcp_tool_invoked", tool_name)
            tool_span.set_attribute("mcp_server_id",    self.mcp_server_id)
            tool_span.set_attribute("worker_type",      self.worker_type.value)

            mcp_call = MCPToolCall(
                tool_name  = tool_name,
                server_id  = self.mcp_server_id,
                invoked_at = datetime.utcnow(),
            )

            try:
                import httpx
                response = httpx.post(
                    url     = f"http://{self.mcp_server_id}/tools/{tool_name}",
                    json    = params,
                    headers = token.as_bearer_header(),
                    timeout = 30.0,
                )
                response.raise_for_status()
                result = response.json()

                elapsed_ms        = (time.perf_counter() - t_start) * 1000
                mcp_call.latency_ms = elapsed_ms
                mcp_call.success    = True
                tool_span.set_attribute("mcp_latency_ms", round(elapsed_ms, 2))
                tool_span.set_attribute("mcp_status",     "success")

                return result, mcp_call

            except httpx.TimeoutException as e:
                elapsed_ms         = (time.perf_counter() - t_start) * 1000
                mcp_call.latency_ms  = elapsed_ms
                mcp_call.success     = False
                mcp_call.error_detail = f"Timeout after {elapsed_ms:.0f}ms"
                tool_span.set_attribute("mcp_status", "timeout")
                raise TimeoutError(f"MCP tool {tool_name} timed out: {e}")

            except Exception as e:
                elapsed_ms           = (time.perf_counter() - t_start) * 1000
                mcp_call.latency_ms  = elapsed_ms
                mcp_call.success     = False
                mcp_call.error_detail = str(e)
                tool_span.set_attribute("mcp_status", "error")
                raise ConnectionError(f"MCP tool {tool_name} failed: {e}")

    # ── Fault Publishing ──────────────────────────────────────────────────────

    def _publish_fault_event(
        self,
        job_id:     str,
        task_id:    str,
        session_id: str,
        trace_id:   str,
        error:      Exception,
        mcp_calls:  List[MCPToolCall],
    ) -> None:
        """Publish a fault event to Kafka agent-faults topic for THA."""
        fault_type = "mcp_timeout" if isinstance(error, TimeoutError) else \
                     "mcp_connection_error" if isinstance(error, ConnectionError) else \
                     "dsw_worker_crash"

        last_tool = mcp_calls[-1].tool_name if mcp_calls else "unknown"

        event = {
            "event_id":     str(uuid.uuid4()),
            "event_type":   fault_type,
            "severity":     "medium",
            "worker_type":  self.worker_type.value,
            "job_id":       job_id,
            "task_id":      task_id,
            "session_id":   session_id,
            "trace_id":     trace_id,
            "mcp_server_id": self.mcp_server_id,
            "tool_name":    last_tool,
            "error_msg":    str(error),
            "timestamp":    datetime.utcnow().isoformat(),
        }

        try:
            self.kafka.publish(KafkaTopic.AGENT_FAULTS, event, key=session_id)
        except Exception:
            pass  # Best-effort — don't crash DSW on Kafka failure
