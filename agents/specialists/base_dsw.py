"""
agents/specialists/base_dsw.py
================================
Autonomous LangGraph-powered Base class for all Domain Specialist Workers (DSW).

Refactored to use a high-performance deterministic architecture:
Instead of a dynamic ReAct loop, it imports a modular parallel StateGraph
from the `dsw_workflow` module.
"""

from __future__ import annotations

import os
import time
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional

from orchestration.cog.state_schema import DSWResult, DSWWorkerType
from core_services.svb import SecureVaultBroker, request_mcp_credentials
from core_services.kafka_streams import KafkaStreamManager, KafkaTopic

from agents.specialists.dsw_workflow.workflow import dsw_graph


class BaseDSW(ABC):
    worker_type:      DSWWorkerType
    mcp_server_id:    str
    required_scopes:  List[str]

    def __init__(
        self,
        worker_id:  str = "",
        svb:        Optional[SecureVaultBroker] = None,
        kafka:      Optional[KafkaStreamManager] = None,
    ):
        self.worker_id  = worker_id or f"{self.worker_type.value}-{uuid.uuid4().hex[:6]}"
        self.svb        = svb or SecureVaultBroker(backend="env")
        self.kafka      = kafka or KafkaStreamManager()
        self.graph      = dsw_graph

    @abstractmethod
    def get_system_prompt(self, region_focus: str, material_focus: str) -> str:
        """Return the persona and reasoning instructions for this DSW."""
        pass

    @abstractmethod
    def get_required_tools(self) -> List[str]:
        """Return a list of exact MCP tool names this specialist should execute in parallel."""
        pass

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
            
            try:
                # Request credentials from SVB
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
                
                # Execute Modular StateGraph Pipeline
                initial_state = {
                    "task_id": task_id,
                    "job_id": job_id,
                    "session_id": session_id,
                    "trace_id": trace_id,
                    "query": query,
                    "region_focus": region_focus,
                    "material_focus": material_focus,
                    "system_prompt": self.get_system_prompt(region_focus, material_focus),
                    "mcp_server_id": self.mcp_server_id,
                    "_ephemeral_token": token,
                    "required_tools": self.get_required_tools()
                }
                
                final_state = self.graph.invoke(initial_state)
                
                elapsed_ms = (time.perf_counter() - t_start) * 1000
                
                return DSWResult(
                    job_id        = job_id,
                    step_id       = task_id,
                    worker_type   = self.worker_type,
                    status        = final_state.get("status", "completed"),
                    data          = final_state.get("final_data", {}),
                    summary       = final_state.get("final_summary", ""),
                    confidence    = final_state.get("confidence", 0.0),
                    sources_cited = final_state.get("required_tools", []),
                    mcp_calls     = final_state.get("mcp_calls", []),
                    execution_ms  = elapsed_ms,
                )

            except Exception as e:
                elapsed_ms = (time.perf_counter() - t_start) * 1000
                self._publish_fault_event(job_id, task_id, session_id, trace_id, e)

                return DSWResult(
                    job_id        = job_id,
                    step_id       = task_id,
                    worker_type   = self.worker_type,
                    status        = "failed",
                    error_message = str(e),
                    mcp_calls     = [],
                    execution_ms  = elapsed_ms,
                )

    def _publish_fault_event(self, job_id, task_id, session_id, trace_id, error):
        event = {
            "event_id":     str(uuid.uuid4()),
            "event_type":   "dsw_worker_crash",
            "severity":     "medium",
            "worker_type":  self.worker_type.value,
            "job_id":       job_id,
            "task_id":      task_id,
            "session_id":   session_id,
            "trace_id":     trace_id,
            "mcp_server_id": self.mcp_server_id,
            "error_msg":    str(error),
            "timestamp":    datetime.utcnow().isoformat(),
        }
        try:
            self.kafka.publish(KafkaTopic.AGENT_FAULTS, event, key=session_id)
        except Exception:
            pass
