"""
agents/specialists/base_dsw.py
================================
Autonomous LangGraph-powered Base class for all Domain Specialist Workers (DSW).

Refactored to use a high-performance deterministic architecture:
Instead of a dynamic ReAct loop, it uses a deterministic parallel StateGraph:
Dispatch -> Parallel Tool Execution -> Merge -> Analysis -> Correlation -> Recommendation -> Report
"""

from __future__ import annotations

import json
import os
import time
import uuid
import asyncio
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional, TypedDict

from pydantic import BaseModel, Field

from langchain_core.messages import SystemMessage, HumanMessage
from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.graph import StateGraph, START, END

from orchestration.cog.state_schema import DSWResult, MCPToolCall, DSWWorkerType
from core_services.svb import EphemeralToken, SecureVaultBroker, request_mcp_credentials
from core_services.kafka_streams import KafkaStreamManager, KafkaTopic


class DSWWorkflowState(TypedDict):
    task_id: str
    job_id: str
    session_id: str
    trace_id: str
    query: str
    region_focus: str
    material_focus: str
    
    # Tool Execution
    required_tools: List[str]
    tool_results: Dict[str, str]
    mcp_calls: List[MCPToolCall]
    
    # LLM Pipeline
    analysis_result: str
    correlation_result: str
    recommendation_result: str
    
    # Final Output
    final_summary: str
    final_data: Dict[str, Any]
    confidence: float
    status: str
    error: str


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
        
        # Build the shared graph upon initialization
        self.graph = self._build_graph()
        # Initialize Gemini 2.0 Flash for blazing fast pipeline execution
        self.llm = ChatGoogleGenerativeAI(model="gemini-2.0-flash", temperature=0)

    @abstractmethod
    def get_system_prompt(self, region_focus: str, material_focus: str) -> str:
        """Return the persona and reasoning instructions for this DSW."""
        pass

    @abstractmethod
    def get_required_tools(self) -> List[str]:
        """Return a list of exact MCP tool names this specialist should execute in parallel."""
        pass

    def _build_graph(self):
        builder = StateGraph(DSWWorkflowState)
        
        builder.add_node("dispatch", self._node_dispatch)
        builder.add_node("parallel_execute", self._node_parallel_execute)
        builder.add_node("analyze", self._node_analyze)
        builder.add_node("correlate", self._node_correlate)
        builder.add_node("recommend", self._node_recommend)
        builder.add_node("report", self._node_report)
        
        builder.add_edge(START, "dispatch")
        builder.add_edge("dispatch", "parallel_execute")
        builder.add_edge("parallel_execute", "analyze")
        builder.add_edge("analyze", "correlate")
        builder.add_edge("correlate", "recommend")
        builder.add_edge("recommend", "report")
        builder.add_edge("report", END)
        
        return builder.compile()

    # --- LANGGRAPH NODES ---

    def _node_dispatch(self, state: DSWWorkflowState):
        """Prepare the workflow with the required tools."""
        return {"required_tools": self.get_required_tools(), "tool_results": {}, "mcp_calls": []}

    def _node_parallel_execute(self, state: DSWWorkflowState):
        """Execute all required tools in parallel via the MCP SSE Client."""
        import asyncio
        from mcp.client.sse import sse_client
        from mcp.client.session import ClientSession
        import mcp.types as types
        from utils.telemetry import get_tracer

        token = state.get("_ephemeral_token") # Injected before invoke
        
        async def _call_all():
            results = {}
            mcp_calls = []
            
            # Since all tools are on our unified mock FastMCP server:
            url = "http://localhost:8001/sse"
            headers = token.as_bearer_header() if token else {}
            
            try:
                async with sse_client(url, headers=headers) as streams:
                    async with ClientSession(streams[0], streams[1]) as session:
                        await session.initialize()
                        
                        # Fan-out: Execute all tools concurrently
                        tasks = []
                        for tool_name in state["required_tools"]:
                            params = {
                                "region": state["region_focus"],
                                "material": state["material_focus"],
                                "query": state["query"]
                            }
                            tasks.append(self._async_call_tool(session, tool_name, params, mcp_calls))
                        
                        completed = await asyncio.gather(*tasks, return_exceptions=True)
                        for tool_name, res in zip(state["required_tools"], completed):
                            if isinstance(res, Exception):
                                results[tool_name] = f"ERROR: {str(res)}"
                            else:
                                results[tool_name] = res
            except Exception as e:
                # If SSE fails entirely
                for tool_name in state["required_tools"]:
                    results[tool_name] = f"CONNECTION_ERROR: {str(e)}"
                    
            return {"tool_results": results, "mcp_calls": mcp_calls}

        # Run synchronously for LangGraph
        return asyncio.run(_call_all())

    async def _async_call_tool(self, session, tool_name, params, mcp_calls_ref):
        t_start = time.perf_counter()
        import mcp.types as types
        
        call_record = MCPToolCall(
            tool_name=tool_name,
            server_id=self.mcp_server_id,
            invoked_at=datetime.utcnow()
        )
        try:
            response = await session.call_tool(tool_name, arguments=params)
            
            call_record.success = True
            call_record.latency_ms = (time.perf_counter() - t_start) * 1000
            mcp_calls_ref.append(call_record)
            
            if response.content and isinstance(response.content[0], types.TextContent):
                return response.content[0].text
            return str(response.content)
        except Exception as e:
            call_record.success = False
            call_record.error_detail = str(e)
            call_record.latency_ms = (time.perf_counter() - t_start) * 1000
            mcp_calls_ref.append(call_record)
            raise e

    def _node_analyze(self, state: DSWWorkflowState):
        """LLM Analysis phase (like the IOS-XR reference)."""
        prompt = (
            f"Analyze the following raw tool data for the region: {state['region_focus']} "
            f"concerning material: {state['material_focus']}.\n\n"
            f"Tool Data:\n{json.dumps(state['tool_results'], indent=2)}\n\n"
            "Provide a factual, concise analysis extracting key metrics and findings."
        )
        res = self.llm.invoke([SystemMessage(content=self.get_system_prompt(state["region_focus"], state["material_focus"])), HumanMessage(content=prompt)])
        return {"analysis_result": res.content}

    def _node_correlate(self, state: DSWWorkflowState):
        """Correlate the analysis with the user's overarching query."""
        prompt = (
            f"Original Query: {state['query']}\n\n"
            f"Analysis Findings:\n{state['analysis_result']}\n\n"
            "Correlate the findings to directly answer the user's query. Highlight risks or alignment."
        )
        res = self.llm.invoke([SystemMessage(content=self.get_system_prompt(state["region_focus"], state["material_focus"])), HumanMessage(content=prompt)])
        return {"correlation_result": res.content}

    def _node_recommend(self, state: DSWWorkflowState):
        """Generate actionable recommendations."""
        prompt = (
            f"Correlated Context:\n{state['correlation_result']}\n\n"
            "Based strictly on this context, provide 3 strategic recommendations. Format them clearly."
        )
        res = self.llm.invoke([SystemMessage(content=self.get_system_prompt(state["region_focus"], state["material_focus"])), HumanMessage(content=prompt)])
        return {"recommendation_result": res.content}

    def _node_report(self, state: DSWWorkflowState):
        """Compile the final JSON report combining all phases."""
        prompt = (
            "Synthesize the preceding phases into a structured JSON payload.\n\n"
            f"Analysis:\n{state['analysis_result']}\n\n"
            f"Correlation:\n{state['correlation_result']}\n\n"
            f"Recommendations:\n{state['recommendation_result']}\n\n"
            "Output EXACTLY this JSON format (no markdown blocks):\n"
            "{\n"
            '  "summary": "High-level summary of the entire analysis",\n'
            '  "data": { "key": "value" },\n'
            '  "confidence": 0.95\n'
            "}"
        )
        res = self.llm.invoke([SystemMessage(content=self.get_system_prompt(state["region_focus"], state["material_focus"])), HumanMessage(content=prompt)])
        
        final_content = res.content.strip()
        if final_content.startswith("```json"): final_content = final_content[7:]
        if final_content.startswith("```"): final_content = final_content[3:]
        if final_content.endswith("```"): final_content = final_content[:-3]
        
        try:
            parsed = json.loads(final_content.strip())
        except json.JSONDecodeError:
            parsed = {
                "summary": "Failed to parse structured output.",
                "data": {"raw": final_content},
                "confidence": 0.5
            }
            
        return {
            "final_summary": parsed.get("summary", ""),
            "final_data": parsed.get("data", {}),
            "confidence": parsed.get("confidence", 0.0),
            "status": "completed"
        }

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
                
                # ── Step 2: Execute Parallel StateGraph Pipeline
                initial_state = {
                    "task_id": task_id,
                    "job_id": job_id,
                    "session_id": session_id,
                    "trace_id": trace_id,
                    "query": query,
                    "region_focus": region_focus,
                    "material_focus": material_focus,
                    "_ephemeral_token": token  # Pass token hidden in state
                }
                
                final_state = self.graph.invoke(initial_state)
                
                elapsed_ms = (time.perf_counter() - t_start) * 1000
                
                # ── Step 3: Build DSWResult
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
                
                # Send fault event to Kafka
                self._publish_fault_event(job_id, task_id, session_id, trace_id, e, [])

                return DSWResult(
                    job_id        = job_id,
                    step_id       = task_id,
                    worker_type   = self.worker_type,
                    status        = "failed",
                    error_message = str(e),
                    mcp_calls     = [],
                    execution_ms  = elapsed_ms,
                )

    def _publish_fault_event(self, job_id, task_id, session_id, trace_id, error, mcp_calls):
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
