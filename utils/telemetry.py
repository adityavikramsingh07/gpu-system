"""
utils/telemetry.py
===================
Standardized OpenTelemetry Logger & Tracer Utility

This module provides the canonical OTel integration for ALL agents in the
GPU Supply Chain MAS. Every agent execution chain, MCP tool call, and
COG node transition MUST be wrapped using these utilities.

Required trace attributes per component:
  All agents:     agent_role, task_id, session_id, trace_id, execution_latency_ms
  DSW workers:    + mcp_tool_invoked, mcp_server_id, mcp_latency_ms, confidence
  COG nodes:      + plan_steps_count, workers_dispatched, viability_score
  THA events:     + fault_type, ticket_id, healing_strategy
  SVB handshake:  + worker_type, mcp_server_id, scopes (hash only)

Exporters:
  - OTLP gRPC → OpenTelemetry Collector (localhost:4317)
  - Prometheus → Metrics (port 8000 scrape endpoint)
  - Console     → Local dev/debug

Usage:
    from utils.telemetry import get_tracer, trace_agent_execution, emit_telemetry_event

    # 1. Get tracer instance
    tracer = get_tracer()

    # 2. Wrap a full agent execution
    @trace_agent_execution(agent_role="geological_expert")
    def my_agent_function(task_id, ...):
        ...

    # 3. Manual span with all required attributes
    with tracer.start_as_current_span("cog.planning_node") as span:
        span.set_attribute("agent_role",          "cog_planner")
        span.set_attribute("task_id",             task_id)
        span.set_attribute("execution_latency_ms", elapsed_ms)
        span.set_attribute("mcp_tool_invoked",    json.dumps(tools))
"""

from __future__ import annotations

import functools
import json
import logging
import os
import time
import uuid
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, TypeVar

# OpenTelemetry core
from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource

# OTLP exporters
try:
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
    OTLP_AVAILABLE = True
except ImportError:
    OTLP_AVAILABLE = False

# Prometheus exporter
try:
    from opentelemetry.exporter.prometheus import PrometheusMetricReader
    from prometheus_client import start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

logger = logging.getLogger(__name__)
F = TypeVar("F", bound=Callable[..., Any])


# ============================================================================
# SERVICE RESOURCE DESCRIPTOR
# ============================================================================

SERVICE_RESOURCE = Resource.create({
    "service.name":      os.environ.get("OTEL_SERVICE_NAME",    "gpu-supply-chain-mas"),
    "service.version":   os.environ.get("OTEL_SERVICE_VERSION", "1.0.0"),
    "deployment.environment": os.environ.get("ENV", "production"),
    "service.namespace": "india-gpu-manufacturing",
})


# ============================================================================
# TELEMETRY INITIALIZATION
# ============================================================================

_tracer_provider:  Optional[TracerProvider]  = None
_meter_provider:   Optional[MeterProvider]   = None
_initialized:      bool                      = False


def initialize_telemetry(
    otlp_endpoint:       str  = "localhost:4317",
    prometheus_port:     int  = 8000,
    enable_console:      bool = False,
    enable_otlp:         bool = True,
    enable_prometheus:   bool = True,
) -> None:
    """
    Initialize the global OTel tracer and metrics providers.

    Call ONCE at system startup (in main.py or agent entrypoint).
    All subsequent get_tracer() calls return the same configured tracer.

    Args:
        otlp_endpoint:     gRPC endpoint for OTel Collector (host:port)
        prometheus_port:   Port to expose /metrics scrape endpoint
        enable_console:    Log spans to stdout (dev mode)
        enable_otlp:       Export to OTel Collector via gRPC
        enable_prometheus: Expose Prometheus metrics endpoint
    """
    global _tracer_provider, _meter_provider, _initialized

    if _initialized:
        return

    # ── Tracer Provider ──────────────────────────────────────────────────────
    _tracer_provider = TracerProvider(resource=SERVICE_RESOURCE)

    if enable_otlp and OTLP_AVAILABLE:
        otlp_exporter = OTLPSpanExporter(
            endpoint   = f"http://{otlp_endpoint}",
            insecure   = True,
        )
        _tracer_provider.add_span_processor(
            BatchSpanProcessor(
                otlp_exporter,
                max_queue_size    = 2048,
                max_export_batch_size = 512,
                export_timeout_millis = 30_000,
            )
        )

    if enable_console:
        _tracer_provider.add_span_processor(
            BatchSpanProcessor(ConsoleSpanExporter())
        )

    trace.set_tracer_provider(_tracer_provider)

    # ── Metrics Provider ─────────────────────────────────────────────────────
    metric_readers = []

    if enable_prometheus and PROMETHEUS_AVAILABLE:
        prometheus_reader = PrometheusMetricReader()
        metric_readers.append(prometheus_reader)
        try:
            start_http_server(prometheus_port)
            logger.info(f"Prometheus metrics endpoint started on port {prometheus_port}")
        except OSError:
            pass  # Port already in use (multiple workers)

    if enable_otlp and OTLP_AVAILABLE:
        otlp_metric_exporter = OTLPMetricExporter(
            endpoint = f"http://{otlp_endpoint}",
            insecure = True,
        )
        metric_readers.append(
            PeriodicExportingMetricReader(
                otlp_metric_exporter,
                export_interval_millis = 10_000,
            )
        )

    _meter_provider = MeterProvider(
        resource        = SERVICE_RESOURCE,
        metric_readers  = metric_readers,
    )
    metrics.set_meter_provider(_meter_provider)

    _initialized = True
    logger.info("OpenTelemetry initialized (tracer + metrics)")


def get_tracer(name: str = "gpu-supply-chain-mas") -> trace.Tracer:
    """
    Get the configured OTel tracer.

    If telemetry has not been initialized, falls back to a no-op tracer
    (safe for unit tests and environments without OTel infrastructure).

    Args:
        name: Instrumentation scope name (defaults to service name)

    Returns:
        opentelemetry.trace.Tracer instance
    """
    if not _initialized:
        # Auto-initialize with defaults if not explicitly called
        try:
            initialize_telemetry(enable_otlp=False, enable_prometheus=False, enable_console=False)
        except Exception:
            pass
    return trace.get_tracer(name)


def get_meter(name: str = "gpu-supply-chain-mas") -> metrics.Meter:
    """Get the configured OTel Meter for creating instruments."""
    return metrics.get_meter(name)


# ============================================================================
# STANDARD METRICS INSTRUMENTS
# ============================================================================

class AgentMetrics:
    """
    Singleton holder for all standard Prometheus/OTel metric instruments.

    Instruments:
      agent_execution_duration_ms  (Histogram)  — per agent_role
      mcp_tool_calls_total          (Counter)    — per tool_name, status
      mcp_tool_latency_ms           (Histogram)  — per tool_name
      agent_faults_total            (Counter)    — per agent_role, fault_type
      cog_planning_steps            (Histogram)  — plan steps per session
      synthesis_viability_score     (Histogram)  — final report scores
      tha_tickets_total             (Counter)    — per fault_type, strategy
    """

    _instance: Optional["AgentMetrics"] = None

    def __init__(self):
        meter = get_meter()

        self.agent_execution_duration = meter.create_histogram(
            name        = "agent_execution_duration_ms",
            description = "End-to-end agent execution time in milliseconds",
            unit        = "ms",
        )
        self.mcp_tool_calls_total = meter.create_counter(
            name        = "mcp_tool_calls_total",
            description = "Total MCP tool invocations",
        )
        self.mcp_tool_latency = meter.create_histogram(
            name        = "mcp_tool_latency_ms",
            description = "MCP tool response latency in milliseconds",
            unit        = "ms",
        )
        self.agent_faults_total = meter.create_counter(
            name        = "agent_faults_total",
            description = "Total agent execution faults by type",
        )
        self.cog_planning_steps = meter.create_histogram(
            name        = "cog_planning_steps_count",
            description = "Number of plan steps generated per session",
        )
        self.synthesis_viability_score = meter.create_histogram(
            name        = "synthesis_viability_score",
            description = "Final viability score from synthesis node (0.0-10.0)",
        )
        self.tha_tickets_total = meter.create_counter(
            name        = "tha_tickets_total",
            description = "THA-generated remediation tickets by fault and strategy",
        )

    @classmethod
    def get(cls) -> "AgentMetrics":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance


# ============================================================================
# DECORATOR: trace_agent_execution
# ============================================================================

def trace_agent_execution(
    agent_role:       str,
    include_result:   bool = False,
    record_metrics:   bool = True,
):
    """
    Decorator to wrap a complete agent execution in an OTel trace.

    Captures mandatory attributes:
      - agent_role:           The DSW or COG node role name
      - task_id:              DTB task identifier
      - execution_latency_ms: End-to-end time in milliseconds
      - mcp_tool_invoked:     JSON list of tool names called (if any)
      - session_id:           COG session identifier
      - trace_id:             Root OTel trace ID

    Usage:
        @trace_agent_execution(agent_role="geological_expert")
        def run_geological_analysis(task_id: str, session_id: str, trace_id: str, **kwargs):
            ...
            return result_dict

    The decorated function MUST accept `task_id`, `session_id`, `trace_id`
    as keyword arguments. They are extracted and set as span attributes.

    Args:
        agent_role:     Role name for this agent (set as span attribute)
        include_result: Whether to include result summary in span (dev only)
        record_metrics: Whether to update AgentMetrics counters/histograms
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            tracer     = get_tracer()
            task_id    = kwargs.get("task_id",    str(uuid.uuid4()))
            session_id = kwargs.get("session_id", "")
            trace_id   = kwargs.get("trace_id",   str(uuid.uuid4()))

            span_name = f"agent.{agent_role}"

            with tracer.start_as_current_span(span_name) as span:
                # ── Mandatory span attributes ─────────────────────────────────
                span.set_attribute("agent_role",    agent_role)
                span.set_attribute("task_id",       task_id)
                span.set_attribute("session_id",    session_id)
                span.set_attribute("trace_id",      trace_id)
                span.set_attribute("function_name", func.__name__)

                t_start    = time.perf_counter()
                status     = "success"
                error_type = ""

                try:
                    result = func(*args, **kwargs)

                    elapsed_ms = (time.perf_counter() - t_start) * 1000

                    # ── Set execution latency ─────────────────────────────────
                    span.set_attribute("execution_latency_ms", round(elapsed_ms, 2))
                    span.set_attribute("status",               "success")

                    # ── Extract MCP tool names from result if present ─────────
                    mcp_tools = []
                    if isinstance(result, dict):
                        mcp_tools = result.get("_mcp_tools_invoked", [])
                        if include_result:
                            summary = str(result.get("summary", result))[:500]
                            span.set_attribute("result_summary", summary)

                    if mcp_tools:
                        span.set_attribute("mcp_tool_invoked", json.dumps(mcp_tools))

                    # ── Record metrics ────────────────────────────────────────
                    if record_metrics:
                        m = AgentMetrics.get()
                        m.agent_execution_duration.record(
                            elapsed_ms,
                            {"agent_role": agent_role, "status": "success"},
                        )

                    return result

                except Exception as e:
                    elapsed_ms = (time.perf_counter() - t_start) * 1000
                    status     = "failed"
                    error_type = type(e).__name__

                    span.set_attribute("execution_latency_ms", round(elapsed_ms, 2))
                    span.set_attribute("status",               "failed")
                    span.set_attribute("error_type",           error_type)
                    span.set_attribute("error_message",        str(e)[:500])
                    span.record_exception(e)

                    if record_metrics:
                        m = AgentMetrics.get()
                        m.agent_execution_duration.record(
                            elapsed_ms,
                            {"agent_role": agent_role, "status": "failed"},
                        )
                        m.agent_faults_total.add(
                            1,
                            {"agent_role": agent_role, "error_type": error_type},
                        )

                    raise

        return wrapper   # type: ignore
    return decorator


# ============================================================================
# CONTEXT MANAGER: trace_mcp_tool_call
# ============================================================================

@contextmanager
def trace_mcp_tool_call(
    tool_name:     str,
    server_id:     str,
    agent_role:    str,
    task_id:       str,
    session_id:    str,
):
    """
    Context manager for tracing a single MCP tool invocation.

    Captures:
      - mcp_tool_invoked:   Tool name
      - mcp_server_id:      Server identifier
      - mcp_latency_ms:     Round-trip time
      - mcp_status:         success | timeout | error
      - agent_role:         Which DSW invoked this tool

    Usage:
        with trace_mcp_tool_call("query_mining_deposits", "geological-mcp-server",
                                  agent_role, task_id, session_id) as span:
            result = httpx.post(url, json=params, headers=token.as_bearer_header())
            span.set_attribute("mcp_result_count", len(result["deposits"]))

    Automatically records:
      - mcp_tool_calls_total counter
      - mcp_tool_latency histogram
    """
    tracer  = get_tracer()
    t_start = time.perf_counter()
    status  = "success"

    with tracer.start_as_current_span(f"mcp.{tool_name}") as span:
        # ── Mandatory MCP span attributes ────────────────────────────────────
        span.set_attribute("mcp_tool_invoked", tool_name)
        span.set_attribute("mcp_server_id",    server_id)
        span.set_attribute("agent_role",       agent_role)
        span.set_attribute("task_id",          task_id)
        span.set_attribute("session_id",       session_id)

        try:
            yield span
            elapsed_ms = (time.perf_counter() - t_start) * 1000
            span.set_attribute("mcp_latency_ms", round(elapsed_ms, 2))
            span.set_attribute("mcp_status",     "success")

            if AgentMetrics._instance:
                m = AgentMetrics.get()
                m.mcp_tool_calls_total.add(1, {"tool_name": tool_name, "status": "success"})
                m.mcp_tool_latency.record(elapsed_ms, {"tool_name": tool_name})

        except TimeoutError:
            elapsed_ms = (time.perf_counter() - t_start) * 1000
            span.set_attribute("mcp_latency_ms", round(elapsed_ms, 2))
            span.set_attribute("mcp_status",     "timeout")
            if AgentMetrics._instance:
                AgentMetrics.get().mcp_tool_calls_total.add(
                    1, {"tool_name": tool_name, "status": "timeout"}
                )
            raise

        except Exception as e:
            elapsed_ms = (time.perf_counter() - t_start) * 1000
            span.set_attribute("mcp_latency_ms", round(elapsed_ms, 2))
            span.set_attribute("mcp_status",     "error")
            span.set_attribute("mcp_error",      str(e)[:200])
            span.record_exception(e)
            if AgentMetrics._instance:
                AgentMetrics.get().mcp_tool_calls_total.add(
                    1, {"tool_name": tool_name, "status": "error"}
                )
            raise


# ============================================================================
# TELEMETRY EVENT PUBLISHER
# ============================================================================

def emit_telemetry_event(
    event:  Any,         # TelemetryEvent pydantic model or dict
    topic:  Any = None,  # KafkaTopic enum
) -> None:
    """
    Publish a TelemetryEvent to Kafka and create an OTel span event.

    This is the single function all components use to emit structured
    telemetry. It handles both OTel span events AND Kafka publishing.

    Args:
        event: TelemetryEvent pydantic model or plain dict
        topic: KafkaTopic enum (if None, uses KafkaTopic.SYS_EVENTS)
    """
    from core_services.kafka_streams import KafkaTopic, get_stream_manager

    kafka_topic = topic or KafkaTopic.SYS_EVENTS

    # Convert to dict if Pydantic model
    if hasattr(event, "dict"):
        event_dict = event.dict()
        # Convert datetime/enum to str for JSON serialization
        for k, v in event_dict.items():
            if hasattr(v, "isoformat"):
                event_dict[k] = v.isoformat()
            elif hasattr(v, "value"):
                event_dict[k] = v.value
    else:
        event_dict = dict(event)

    # Publish to Kafka
    try:
        manager = get_stream_manager()
        manager.publish(
            topic   = kafka_topic,
            payload = event_dict,
            key     = event_dict.get("session_id", ""),
        )
    except Exception as e:
        logger.debug(f"Telemetry event publish failed: {e}")

    # Also add as OTel span event on active span
    try:
        current_span = trace.get_current_span()
        if current_span and current_span.is_recording():
            current_span.add_event(
                name       = event_dict.get("event_type", "telemetry"),
                attributes = {
                    k: str(v) for k, v in event_dict.items()
                    if isinstance(v, (str, int, float, bool))
                }
            )
    except Exception:
        pass  # Never block on OTel failures


# ============================================================================
# STRUCTURED JSON LOG FORMATTER
# ============================================================================

class StructuredJSONFormatter(logging.Formatter):
    """
    JSON log formatter that enriches all log records with OTel trace context.

    Adds to every log line:
      - trace_id:   Current OTel trace ID (for Grafana log-trace correlation)
      - span_id:    Current span ID
      - timestamp:  ISO format UTC timestamp
      - service:    Service name from resource
    """

    def format(self, record: logging.LogRecord) -> str:
        # Extract OTel context from current span
        current_span = trace.get_current_span()
        ctx          = current_span.get_span_context() if current_span else None

        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "level":     record.levelname,
            "service":   os.environ.get("OTEL_SERVICE_NAME", "gpu-supply-chain-mas"),
            "logger":    record.name,
            "message":   record.getMessage(),
            "trace_id":  format(ctx.trace_id, "032x") if ctx and ctx.is_valid else "",
            "span_id":   format(ctx.span_id,  "016x") if ctx and ctx.is_valid else "",
        }

        # Include exception if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        # Include any extra fields attached to the log record
        for key in ("task_id", "session_id", "agent_role", "job_id"):
            if hasattr(record, key):
                log_entry[key] = getattr(record, key)

        return json.dumps(log_entry)


def configure_structured_logging(level: int = logging.INFO) -> None:
    """
    Configure root logger to output structured JSON with OTel trace context.

    Call once at startup before any logging occurs.
    All subsequent loggers will inherit this format.
    """
    handler = logging.StreamHandler()
    handler.setFormatter(StructuredJSONFormatter())

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    root.addHandler(handler)


# ============================================================================
# EXAMPLE USAGE (run as __main__ for dev verification)
# ============================================================================

if __name__ == "__main__":
    # Initialize with console output for testing
    initialize_telemetry(
        enable_otlp        = False,
        enable_prometheus  = False,
        enable_console     = True,
    )
    configure_structured_logging()

    tracer = get_tracer()

    # ── Example 1: Manual span with all required attributes ──────────────────
    with tracer.start_as_current_span("dsw.geological_expert") as span:
        task_id    = f"task-{uuid.uuid4().hex[:8]}"
        session_id = f"sess-{uuid.uuid4().hex[:8]}"
        trace_id   = str(uuid.uuid4())

        span.set_attribute("agent_role",           "geological_expert")
        span.set_attribute("task_id",              task_id)
        span.set_attribute("session_id",           session_id)
        span.set_attribute("trace_id",             trace_id)
        span.set_attribute("mcp_server_id",        "geological-mcp-server")

        # Simulate MCP tool call within same trace
        with trace_mcp_tool_call(
            tool_name  = "query_mining_deposits",
            server_id  = "geological-mcp-server",
            agent_role = "geological_expert",
            task_id    = task_id,
            session_id = session_id,
        ) as tool_span:
            time.sleep(0.042)   # Simulate 42ms MCP latency
            tool_span.set_attribute("deposits_found", 7)

        elapsed_ms = 42.0
        span.set_attribute("execution_latency_ms", elapsed_ms)
        span.set_attribute("mcp_tool_invoked",     '["query_mining_deposits"]')
        span.set_attribute("confidence",           0.87)

    # ── Example 2: Decorator-based tracing ───────────────────────────────────
    @trace_agent_execution(agent_role="synthesis_node", record_metrics=False)
    def synthesize(task_id: str, session_id: str, trace_id: str):
        time.sleep(0.05)
        return {"summary": "HPQ viable, Copper conditional", "viability_score": 7.2}

    result = synthesize(
        task_id    = task_id,
        session_id = session_id,
        trace_id   = trace_id,
    )

    print(f"\n[telemetry.py] Verification complete. Result: {result}")
    print("[telemetry.py] All spans captured with: agent_role, task_id, execution_latency_ms, mcp_tool_invoked")
