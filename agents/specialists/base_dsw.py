"""
agents/specialists/base_dsw.py
================================
Autonomous LangGraph-powered Base class for all Domain Specialist Workers (DSW).

Each DSW operates as a ReAct (Reason + Act) agent driven by Gemini.
Tools are dynamically bound via LangChain `@tool` wrappers that inject the
secure EphemeralToken for Model Context Protocol (MCP) interactions.
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
import httpx

from langchain_core.tools import tool
from langchain_core.messages import SystemMessage
from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.prebuilt import create_react_agent

from orchestration.cog.state_schema import DSWResult, MCPToolCall, DSWWorkerType
from core_services.svb import EphemeralToken, SecureVaultBroker, request_mcp_credentials
from core_services.kafka_streams import KafkaStreamManager, KafkaTopic

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

    @abstractmethod
    def get_system_prompt(self, region_focus: str, material_focus: str) -> str:
        """Return the persona and reasoning instructions for this DSW."""
        pass

    @abstractmethod
    def get_dynamic_tools(self, token: EphemeralToken, span: Any, mcp_calls: List[MCPToolCall]) -> List[Any]:
        """Return a list of LangChain @tool wrapped MCP endpoints."""
        pass

    def create_mcp_tool(
        self,
        tool_name: str,
        description: str,
        params_schema: type[BaseModel],
        token: EphemeralToken,
        span: Any,
        mcp_calls: List[MCPToolCall]
    ) -> Any:
        """Factory to generate a LangChain tool that securely wraps an MCP API call."""
        @tool(tool_name, args_schema=params_schema)
        def dynamic_mcp_tool(**kwargs) -> Dict[str, Any]:
            result, call_record = self.call_mcp_tool(tool_name, kwargs, token, span)
            mcp_calls.append(call_record)
            return result
        
        # Override the description for the LLM
        dynamic_mcp_tool.description = description
        return dynamic_mcp_tool

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
            span.set_attribute("region",        region_focus)
            span.set_attribute("material",      material_focus)

            mcp_calls: List[MCPToolCall] = []

            try:
                # ── Step 1: Request credentials from SVB
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

                # ── Step 2: Build LangGraph ReAct Agent
                tools = self.get_dynamic_tools(token, span, mcp_calls)
                
                # We use Gemini as the core reasoning engine
                llm = ChatGoogleGenerativeAI(model="gemini-1.5-pro", temperature=0)
                agent = create_react_agent(llm, tools=tools)

                # ── Step 3: Execute Reasoning Loop
                sys_msg = SystemMessage(content=self.get_system_prompt(region_focus, material_focus))
                
                enforcement_prompt = (
                    f"{query}\n\n"
                    "CRITICAL INSTRUCTION: You MUST format your final response as a RAW JSON object. "
                    "Do NOT wrap it in ```json blocks. "
                    "The JSON must have this exact structure:\n"
                    "{\n"
                    '  "summary": "Brief summary of your findings",\n'
                    '  "data": { "key": "value" },\n'
                    '  "confidence": 0.95,\n'
                    '  "sources_cited": ["mcp_tool_name(arg)", ...]\n'
                    "}"
                )

                span.add_event("LangGraph Agent Execution Started")
                
                inputs = {"messages": [sys_msg, ("user", enforcement_prompt)]}
                result = agent.invoke(inputs)
                
                span.add_event("LangGraph Agent Execution Finished")

                final_content = result["messages"][-1].content
                
                # Attempt to parse the JSON
                try:
                    # Clean up possible markdown code blocks
                    cleaned = final_content.strip()
                    if cleaned.startswith("```json"):
                        cleaned = cleaned[7:]
                    if cleaned.startswith("```"):
                        cleaned = cleaned[3:]
                    if cleaned.endswith("```"):
                        cleaned = cleaned[:-3]
                    
                    parsed_result = json.loads(cleaned.strip())
                except json.JSONDecodeError:
                    parsed_result = {
                        "summary": "Agent returned unstructured text.",
                        "data": {"raw": final_content},
                        "confidence": 0.5,
                        "sources_cited": []
                    }

                elapsed_ms = (time.perf_counter() - t_start) * 1000
                span.set_attribute("execution_latency_ms", round(elapsed_ms, 2))
                span.set_attribute("confidence", round(parsed_result.get("confidence", 0.0), 3))
                span.set_attribute("mcp_tool_invoked", json.dumps([c.tool_name for c in mcp_calls]))

                # ── Step 4: Build DSWResult
                return DSWResult(
                    job_id        = job_id,
                    step_id       = task_id,
                    worker_type   = self.worker_type,
                    status        = "completed",
                    data          = parsed_result.get("data", {}),
                    summary       = parsed_result.get("summary", "No summary available"),
                    confidence    = parsed_result.get("confidence", 0.0),
                    sources_cited = parsed_result.get("sources_cited", []),
                    mcp_calls     = mcp_calls,
                    execution_ms  = elapsed_ms,
                )

            except Exception as e:
                elapsed_ms = (time.perf_counter() - t_start) * 1000
                span.set_attribute("error",                str(e))
                span.set_attribute("execution_latency_ms", round(elapsed_ms, 2))
                span.set_attribute("status",               "failed")

                self._publish_fault_event(job_id, task_id, session_id, trace_id, e, mcp_calls)

                return DSWResult(
                    job_id        = job_id,
                    step_id       = task_id,
                    worker_type   = self.worker_type,
                    status        = "failed",
                    error_message = str(e),
                    mcp_calls     = mcp_calls,
                    execution_ms  = elapsed_ms,
                )

    def call_mcp_tool(
        self,
        tool_name: str,
        params:    Dict[str, Any],
        token:     EphemeralToken,
        span:      Any = None,
    ) -> tuple[Dict[str, Any], MCPToolCall]:
        from utils.telemetry import get_tracer
        t_start = time.perf_counter()

        with get_tracer().start_as_current_span(f"mcp.{tool_name}") as tool_span:
            tool_span.set_attribute("mcp_tool_invoked", tool_name)
            tool_span.set_attribute("mcp_server_id",    self.mcp_server_id)

            mcp_call = MCPToolCall(
                tool_name  = tool_name,
                server_id  = self.mcp_server_id,
                invoked_at = datetime.utcnow(),
            )

            try:
                response = httpx.post(
                    url     = f"http://{self.mcp_server_id}/tools/{tool_name}",
                    json    = params,
                    headers = token.as_bearer_header(),
                    timeout = 30.0,
                )
                response.raise_for_status()
                result = response.json()

                elapsed_ms = (time.perf_counter() - t_start) * 1000
                mcp_call.latency_ms = elapsed_ms
                mcp_call.success    = True
                tool_span.set_attribute("mcp_status", "success")

                return result, mcp_call

            except httpx.TimeoutException as e:
                elapsed_ms = (time.perf_counter() - t_start) * 1000
                mcp_call.latency_ms = elapsed_ms
                mcp_call.success = False
                mcp_call.error_detail = f"Timeout"
                tool_span.set_attribute("mcp_status", "timeout")
                raise TimeoutError(f"MCP timeout: {e}")

            except Exception as e:
                elapsed_ms = (time.perf_counter() - t_start) * 1000
                mcp_call.latency_ms = elapsed_ms
                mcp_call.success = False
                mcp_call.error_detail = str(e)
                tool_span.set_attribute("mcp_status", "error")
                raise ConnectionError(f"MCP failed: {e}")

    def _publish_fault_event(
        self, job_id: str, task_id: str, session_id: str, trace_id: str, error: Exception, mcp_calls: List[MCPToolCall]
    ) -> None:
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
            pass
