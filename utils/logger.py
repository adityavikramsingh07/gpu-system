"""
OpenTelemetry Logger Utility

Wraps all agent operations with distributed tracing,
structured logging (JSON), and Prometheus metrics export.
"""

import json
import logging
import time
from typing import Dict, Any, Optional, Callable
from functools import wraps
from contextlib import contextmanager
from datetime import datetime
import uuid

from opentelemetry import trace, metrics
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.prometheus import PrometheusMetricReader


class StructuredLogger:
    """
    Structured JSON logger integrated with OpenTelemetry.
    """

    def __init__(
        self,
        agent_id: str,
        service_name: str = "gpu-mra-system",
        jaeger_host: str = "localhost",
        jaeger_port: int = 6831
    ):
        """
        Initialize logger with OpenTelemetry.
        
        Args:
            agent_id: Unique ID for this agent
            service_name: Service name for tracing
            jaeger_host: Jaeger collector host
            jaeger_port: Jaeger collector port
        """
        self.agent_id = agent_id
        self.service_name = service_name
        
        # Initialize tracer (Jaeger)
        tracer_exporter = JaegerExporter(
            agent_host_name=jaeger_host,
            agent_port=jaeger_port,
        )
        tracer_provider = TracerProvider()
        tracer_provider.add_span_processor(BatchSpanProcessor(tracer_exporter))
        trace.set_tracer_provider(tracer_provider)
        self.tracer = trace.get_tracer(__name__)
        
        # Initialize metrics (Prometheus)
        prometheus_reader = PrometheusMetricReader()
        metrics_provider = MeterProvider(metric_readers=[prometheus_reader])
        metrics.set_meter_provider(metrics_provider)
        self.meter = metrics.get_meter(__name__)
        
        # Standard Python logger (for JSON output)
        self.logger = logging.getLogger(agent_id)
        self.logger.setLevel(logging.INFO)
        
        # JSON formatter handler
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        
        # Create metrics
        self.operation_duration = self.meter.create_histogram(
            name="operation_duration_ms",
            description="Duration of operations",
            unit="ms"
        )
        
        self.operation_counter = self.meter.create_counter(
            name="operations_total",
            description="Total operations executed"
        )
        
        self.error_counter = self.meter.create_counter(
            name="errors_total",
            description="Total errors encountered"
        )

    def _make_structured_log(
        self,
        level: str,
        message: str,
        trace_id: str,
        job_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Create structured JSON log entry.
        
        Returns:
            JSON string ready for export to logs aggregator
        """
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": level,
            "service": self.service_name,
            "agent_id": self.agent_id,
            "trace_id": trace_id,
            "job_id": job_id,
            "message": message,
            **kwargs  # Additional context
        }
        return json.dumps(log_entry)

    def info(
        self,
        message: str,
        trace_id: str,
        job_id: Optional[str] = None,
        **context
    ):
        """Log INFO level structured message."""
        log_json = self._make_structured_log("INFO", message, trace_id, job_id, **context)
        self.logger.info(log_json)

    def warning(
        self,
        message: str,
        trace_id: str,
        job_id: Optional[str] = None,
        **context
    ):
        """Log WARNING level structured message."""
        log_json = self._make_structured_log("WARNING", message, trace_id, job_id, **context)
        self.logger.warning(log_json)

    def error(
        self,
        message: str,
        trace_id: str,
        job_id: Optional[str] = None,
        exception: Optional[Exception] = None,
        **context
    ):
        """Log ERROR level structured message."""
        error_details = {}
        if exception:
            error_details["exception_type"] = type(exception).__name__
            error_details["exception_message"] = str(exception)
        
        log_json = self._make_structured_log(
            "ERROR", message, trace_id, job_id,
            **{**context, **error_details}
        )
        self.logger.error(log_json)
        self.error_counter.add(1, {"agent_id": self.agent_id})

    def critical(
        self,
        message: str,
        trace_id: str,
        job_id: Optional[str] = None,
        **context
    ):
        """Log CRITICAL level structured message."""
        log_json = self._make_structured_log("CRITICAL", message, trace_id, job_id, **context)
        self.logger.critical(log_json)
        self.error_counter.add(1, {"agent_id": self.agent_id})


# ============================================================================
# DECORATORS FOR AUTOMATICALLY TRACING OPERATIONS
# ============================================================================

def trace_agent_operation(
    operation_name: str,
    include_result: bool = False,
    include_args: bool = False
):
    """
    Decorator to automatically trace an agent operation.
    
    Wraps function execution with:
    - OpenTelemetry span creation & timing
    - Structured logging
    - Prometheus metrics
    - Exception handling
    
    Usage:
        @trace_agent_operation("geological_query", include_result=True)
        def query_geological_mcp(region: str, material: str):
            ...
    
    Example span output:
        {
            "status": "success",
            "operation": "geological_query",
            "agent_id": "geological_scout_001",
            "job_id": "job_42",
            "trace_id": "trace_xyz789",
            "execution_time_ms": 245,
            "result_summary": "Found 15 deposits"
        }
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Extract trace context from kwargs if present
            trace_id = kwargs.pop("trace_id", str(uuid.uuid4()))
            job_id = kwargs.pop("job_id", None)
            agent_id = kwargs.pop("agent_id", "unknown")
            
            # Create span
            with trace.get_tracer(__name__).start_as_current_span(
                f"{operation_name}"
            ) as span:
                span.set_attribute("agent_id", agent_id)
                span.set_attribute("job_id", job_id or "N/A")
                span.set_attribute("trace_id", trace_id)
                
                start_time = time.time()
                try:
                    # Execute operation
                    result = func(*args, **kwargs)
                    
                    # Success
                    elapsed_ms = (time.time() - start_time) * 1000
                    span.set_attribute("status", "success")
                    span.set_attribute("execution_time_ms", elapsed_ms)
                    
                    # Log structured entry
                    log_context = {
                        "operation": operation_name,
                        "execution_time_ms": int(elapsed_ms),
                        "args": repr(args[:2]) if include_args else "redacted"
                    }
                    if include_result:
                        log_context["result"] = repr(result)[:200]  # Truncate
                    
                    # (Would use logger here if passed)
                    print(json.dumps({
                        "timestamp": datetime.utcnow().isoformat(),
                        "level": "INFO",
                        "trace_id": trace_id,
                        "agent_id": agent_id,
                        **log_context
                    }))
                    
                    return result
                    
                except Exception as e:
                    # Failure
                    elapsed_ms = (time.time() - start_time) * 1000
                    span.set_attribute("status", "failure")
                    span.set_attribute("exception", type(e).__name__)
                    span.set_attribute("exception_message", str(e))
                    span.set_attribute("execution_time_ms", elapsed_ms)
                    
                    # Log error
                    print(json.dumps({
                        "timestamp": datetime.utcnow().isoformat(),
                        "level": "ERROR",
                        "trace_id": trace_id,
                        "agent_id": agent_id,
                        "operation": operation_name,
                        "exception": type(e).__name__,
                        "exception_message": str(e),
                        "execution_time_ms": int(elapsed_ms)
                    }))
                    
                    raise
        
        return wrapper
    return decorator


@contextmanager
def trace_block(
    block_name: str,
    trace_id: str,
    agent_id: str,
    job_id: Optional[str] = None
):
    """
    Context manager for tracing a block of code.
    
    Usage:
        with trace_block("fetch_data", trace_id, agent_id, job_id):
            data = mcp_tool.query(...)
            process_data(data)
    
    Automatically:
    - Creates span
    - Times execution
    - Logs start/end
    - Catches exceptions
    """
    span_name = f"{agent_id}/{block_name}"
    
    with trace.get_tracer(__name__).start_as_current_span(span_name) as span:
        span.set_attribute("agent_id", agent_id)
        span.set_attribute("job_id", job_id or "N/A")
        span.set_attribute("trace_id", trace_id)
        
        start_time = time.time()
        
        print(json.dumps({
            "timestamp": datetime.utcnow().isoformat(),
            "level": "INFO",
            "trace_id": trace_id,
            "agent_id": agent_id,
            "event": "block_start",
            "block": block_name
        }))
        
        try:
            yield span
            
            elapsed_ms = (time.time() - start_time) * 1000
            span.set_attribute("status", "success")
            span.set_attribute("execution_time_ms", elapsed_ms)
            
            print(json.dumps({
                "timestamp": datetime.utcnow().isoformat(),
                "level": "INFO",
                "trace_id": trace_id,
                "agent_id": agent_id,
                "event": "block_end",
                "block": block_name,
                "execution_time_ms": int(elapsed_ms),
                "status": "success"
            }))
            
        except Exception as e:
            elapsed_ms = (time.time() - start_time) * 1000
            span.set_attribute("status", "failure")
            span.set_attribute("exception", type(e).__name__)
            
            print(json.dumps({
                "timestamp": datetime.utcnow().isoformat(),
                "level": "ERROR",
                "trace_id": trace_id,
                "agent_id": agent_id,
                "event": "block_error",
                "block": block_name,
                "exception": type(e).__name__,
                "exception_message": str(e),
                "execution_time_ms": int(elapsed_ms)
            }))
            
            raise


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    # Initialize logger
    logger = StructuredLogger(
        agent_id="geological_scout_001",
        service_name="gpu-mra-system"
    )
    
    # Example 1: Direct logging
    trace_id = str(uuid.uuid4())
    logger.info(
        "Starting geological query",
        trace_id=trace_id,
        job_id="job_42",
        region="Tamil Nadu",
        material="copper"
    )
    
    # Example 2: Decorated function with auto-tracing
    @trace_agent_operation("geological_query", include_result=True)
    def query_geological():
        time.sleep(0.1)  # Simulate work
        return {"deposits": 15, "region": "TN"}
    
    try:
        result = query_geological(
            trace_id=trace_id,
            job_id="job_42",
            agent_id="geological_scout_001"
        )
        logger.info(
            "Query completed",
            trace_id=trace_id,
            job_id="job_42",
            deposits_found=result["deposits"]
        )
    except Exception as e:
        logger.error(
            "Query failed",
            trace_id=trace_id,
            job_id="job_42",
            exception=e
        )
    
    # Example 3: Context manager for block tracing
    with trace_block("data_processing", trace_id, "geological_scout_001", "job_42"):
        print("Processing results...")
        time.sleep(0.05)
        print("Processing complete")
    
    print("\n[Output above shows structured JSON logs with OpenTelemetry tracing]")
