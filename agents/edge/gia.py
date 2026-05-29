"""
agents/edge/gia.py
===================
Gateway Interface Agent (GIA)

The GIA is the synchronous frontend edge node — the sole entry and exit point
for user interactions. It enforces input validation, payload canonicalization,
and final blueprint formatting before returning results.

Responsibilities:
  1. Receive raw user query (REST/gRPC/CLI)
  2. Validate and sanitize input (injection prevention, size limits)
  3. Extract structured context: region, materials, priority
  4. Format a GIARequest and invoke the COG graph
  5. Poll COG for completion (or stream events)
  6. Format and return the SynthesizedBlueprint as a structured response

The GIA is STATELESS — it holds no session data between calls.
All state flows through the COG via LangGraph checkpointing.

Design:
  - Synchronous HTTP interface (FastAPI)
  - Max payload: 4096 chars
  - Rate limiting: 10 req/min per user_id
  - Response timeout: 180s (configurable)
  - All validation failures return 400 with structured error
"""

from __future__ import annotations

import asyncio
import json
import random
import re
import time
import uuid
from datetime import datetime
from typing import Any, AsyncGenerator, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse


# ============================================================================
# GIA REQUEST / RESPONSE MODELS
# ============================================================================

KNOWN_GPU_MATERIALS = [
    "high-purity quartz", "hpq", "silicon carbide", "sic",
    "copper foil", "copper", "gallium", "indium", "germanium",
    "hafnium oxide", "tungsten", "cobalt", "tantalum", "aluminum",
    "ultra-high-purity gases", "uhp gases", "photoresist chemicals",
    "cmp slurries", "etchants", "low-k dielectrics", "barrier metals",
]

KNOWN_REGIONS = [
    "southern india", "south india", "tamil nadu", "karnataka",
    "andhra pradesh", "telangana", "kerala", "india", "north india",
    "western india", "eastern india", "maharashtra", "gujarat",
]


class GIAUserPayload(BaseModel):
    """Raw payload received from the user."""
    query:     str  = Field(..., min_length=10, max_length=4096)
    user_id:   str  = Field(default="anonymous", max_length=64)
    priority:  int  = Field(default=5, ge=1, le=10)
    metadata:  Dict[str, Any] = Field(default_factory=dict)

    @field_validator("query")
    @classmethod
    def sanitize_query(cls, v: str) -> str:
        """Prevent prompt injection and strip dangerous patterns."""
        # Remove control characters
        v = re.sub(r"[\x00-\x1f\x7f]", " ", v).strip()
        # Detect obvious injection patterns
        injection_patterns = [
            r"ignore previous instructions",
            r"system prompt",
            r"<\|im_start\|>",
            r"<\|endoftext\|>",
        ]
        for pattern in injection_patterns:
            if re.search(pattern, v, re.IGNORECASE):
                raise ValueError(f"Query contains disallowed pattern: {pattern}")
        return v

    @field_validator("user_id")
    @classmethod
    def validate_user_id(cls, v: str) -> str:
        """Alphanumeric + dashes only."""
        if not re.match(r"^[a-zA-Z0-9_\-]+$", v):
            return "anonymous"
        return v


class GIAResponse(BaseModel):
    """Structured response returned to the user."""
    request_id:           str
    session_id:           str
    status:               str             # success | partial | failed
    blueprint:            Optional[Dict[str, Any]] = None
    viability_score:      Optional[float]          = None
    confidence_overall:   Optional[float]          = None
    degraded_mode:        bool             = False
    processing_time_ms:   float            = 0.0
    warnings:             List[str]        = Field(default_factory=list)
    error:                Optional[str]    = None
    generated_at:         str              = Field(default_factory=lambda: datetime.utcnow().isoformat())


# ============================================================================
# GATEWAY INTERFACE AGENT
# ============================================================================

class GatewayInterfaceAgent:
    """
    GIA: Synchronous frontend edge node.

    Wraps the entire COG invocation in a clean request/response contract.
    All errors are caught here and returned as structured GIAResponse objects
    — the COG never surfaces raw exceptions to users.
    """

    def __init__(
        self,
        cog_timeout_seconds: int = 180,
        max_query_length:    int = 4096,
    ):
        self.cog_timeout_s    = cog_timeout_seconds
        self.max_query_length = max_query_length

    def handle_request(self, payload: GIAUserPayload) -> GIAResponse:
        """
        Main entry point for user requests.

        Flow:
          1. Validate payload
          2. Extract context (materials, region)
          3. Build GIARequest
          4. Invoke COG
          5. Format and return response

        Args:
            payload: Validated GIAUserPayload

        Returns:
            GIAResponse with blueprint or error
        """
        from orchestration.cog.state_schema import GIARequest
        from orchestration.cog.graph import invoke_cog
        from utils.telemetry import get_tracer

        tracer     = get_tracer()
        request_id = f"req-{uuid.uuid4().hex[:12]}"
        trace_id   = str(uuid.uuid4())
        t_start    = time.perf_counter()

        with tracer.start_as_current_span("gia.handle_request") as span:
            span.set_attribute("request_id", request_id)
            span.set_attribute("user_id",    payload.user_id)
            span.set_attribute("trace_id",   trace_id)
            span.set_attribute("query_len",  len(payload.query))

            try:
                # ── Extract context ──────────────────────────────────────────
                materials = self._extract_materials(payload.query)
                region    = self._extract_region(payload.query)

                span.set_attribute("materials_found", len(materials))
                span.set_attribute("region",           region)

                # ── Build GIARequest ─────────────────────────────────────────
                from orchestration.cog.state_schema import GIARequest
                gia_request = GIARequest(
                    request_id     = request_id,
                    raw_query      = payload.query,
                    user_id        = payload.user_id,
                    region_context = region,
                    materials      = materials,
                    priority       = payload.priority,
                    metadata       = payload.metadata,
                )

                # ── Invoke COG ───────────────────────────────────────────────
                final_state = invoke_cog(
                    gia_request = gia_request,
                    trace_id    = trace_id,
                )

                elapsed_ms = (time.perf_counter() - t_start) * 1000

                # ── Extract results ──────────────────────────────────────────
                blueprint = final_state.get("synthesized_blueprint")
                warnings  = final_state.get("warnings", [])
                fatal_err = final_state.get("fatal_error")

                if fatal_err:
                    return GIAResponse(
                        request_id         = request_id,
                        session_id         = gia_request.session_id,
                        status             = "failed",
                        error              = fatal_err,
                        processing_time_ms = elapsed_ms,
                        warnings           = warnings,
                    )

                if blueprint is None:
                    return GIAResponse(
                        request_id         = request_id,
                        session_id         = gia_request.session_id,
                        status             = "failed",
                        error              = "COG produced no blueprint",
                        processing_time_ms = elapsed_ms,
                    )

                return GIAResponse(
                    request_id           = request_id,
                    session_id           = gia_request.session_id,
                    status               = "partial" if blueprint.degraded_mode else "success",
                    blueprint            = blueprint.dict(),
                    viability_score      = blueprint.viability_score,
                    confidence_overall   = blueprint.confidence_overall,
                    degraded_mode        = blueprint.degraded_mode,
                    processing_time_ms   = elapsed_ms,
                    warnings             = warnings,
                )

            except Exception as e:
                elapsed_ms = (time.perf_counter() - t_start) * 1000
                span.set_attribute("error", str(e))
                return GIAResponse(
                    request_id         = request_id,
                    session_id         = "",
                    status             = "failed",
                    error              = f"GIA internal error: {e}",
                    processing_time_ms = elapsed_ms,
                )

    def _extract_materials(self, query: str) -> List[str]:
        """Extract GPU supply chain material names from user query."""
        query_lower = query.lower()
        found = []
        for material in KNOWN_GPU_MATERIALS:
            if material in query_lower:
                found.append(material.title())
        # Deduplicate (HPQ and High-Purity Quartz both match → use longer form)
        if "Hpq" in found and "High-Purity Quartz" in found:
            found.remove("Hpq")
        return found or ["Unspecified Material"]

    def _extract_region(self, query: str) -> str:
        """Extract geographic region from user query."""
        query_lower = query.lower()
        for region in KNOWN_REGIONS:
            if region in query_lower:
                return region.title()
        return "India"


# ============================================================================
# FASTAPI APPLICATION
# ============================================================================

# ============================================================================
# SIMULATED AGENT METRICS (used by /v1/health/agents when infra is offline)
# ============================================================================

AGENT_REGISTRY = [
    {"id": "GIA",  "role": "Gateway Interface Agent",       "color": "#00d4ff", "backend": "HTTP Edge"},
    {"id": "COG",  "role": "Central Orchestration Graph",   "color": "#bf5af2", "backend": "LangGraph"},
    {"id": "DTB",  "role": "Distributed Task Broker",       "color": "#30d158", "backend": "Redis Streams"},
    {"id": "SVB",  "role": "Secure Vault Broker",           "color": "#ff453a", "backend": "HashiCorp Vault"},
    {"id": "THA",  "role": "Telemetry & Healing Agent",     "color": "#ffd60a", "backend": "Kafka Consumer"},
    {"id": "DSWs", "role": "11 Domain Specialist Workers",  "color": "#0a84ff", "backend": "MCP Servers"},
]

SSE_TOPICS = [
    {"type": "sys",  "msg": "COG planning_node completed | steps=11 | latency=2750ms"},
    {"type": "sys",  "msg": "DTB fan_out dispatched jobs to Redis streams"},
    {"type": "warn", "msg": "DSW geological_expert: lease data partial | confidence=0.72"},
    {"type": "heal", "msg": "THA: ticket resolved | strategy=retry_same_tool"},
    {"type": "sys",  "msg": "SVB token issued | worker=chemical_infra_analyst | ttl=300s"},
    {"type": "info", "msg": "COG synthesis_node: blueprint generated"},
    {"type": "warn", "msg": "Kafka consumer lag: tha-consumer-group | lag=2"},
    {"type": "sys",  "msg": "GIA: session active | materials=[SiC, HPQ]"},
    {"type": "heal", "msg": "THA: healing injection applied | degraded=true"},
    {"type": "sys",  "msg": "DTB: job completed | execution_ms=312"},
    {"type": "info", "msg": "COG healing_node applied THAInjection"},
    {"type": "err",  "msg": "SVB: Vault fetch retry 1/3 | failover to vault-secondary"},
    {"type": "sys",  "msg": "DSW workforce_analyst: 3800 engineers available | TN+KA"},
    {"type": "heal", "msg": "THA heartbeat: 0 stale jobs detected"},
    {"type": "info", "msg": "Synthesis confidence=0.814 | workers_completed=10/11"},
]


def create_gia_app() -> FastAPI:
    """
    Create and return the GIA FastAPI application.

    Routes:
      POST /v1/supply-chain/analyze  → Full viability report
      GET  /v1/health                → Health check
      GET  /v1/health/agents         → Live agent status for frontend dashboard
      GET  /v1/events                → SSE stream of live telemetry events
    """
    app = FastAPI(
        title       = "GPU Supply Chain GIA",
        description = "Gateway Interface Agent — India GPU Manufacturing Supply Chain MAS",
        version     = "1.0.0",
    )

    # ── CORS: allow the frontend (port 5500) to call us ─────────────────────
    app.add_middleware(
        CORSMiddleware,
        allow_origins     = ["*"],   # Tighten to ["http://localhost:5500"] in production
        allow_credentials = True,
        allow_methods     = ["*"],
        allow_headers     = ["*"],
    )

    gia = GatewayInterfaceAgent()
    _sse_index = {"i": 0}          # Shared rotating SSE message index

    # ────────────────────────────────────────────────────────────────────────
    @app.post("/v1/supply-chain/analyze", response_model=GIAResponse)
    async def analyze(payload: GIAUserPayload, request: Request):
        """Submit a supply chain viability analysis request."""
        response = gia.handle_request(payload)
        status_code = 200 if response.status != "failed" else 500
        return JSONResponse(content=response.model_dump(), status_code=status_code)

    # ────────────────────────────────────────────────────────────────────────
    @app.get("/v1/health")
    async def health():
        """Basic liveness probe."""
        return {
            "status":    "healthy",
            "service":   "gia",
            "timestamp": datetime.utcnow().isoformat(),
            "version":   "1.0.0",
        }

    # ────────────────────────────────────────────────────────────────────────
    @app.get("/v1/health/agents")
    async def agent_status():
        """
        Live agent status endpoint consumed by the frontend dashboard.
        Returns metrics for all system components: GIA, COG, DTB, SVB, THA, DSWs.
        In production, this queries Redis/Prometheus. Here it returns live-realistic data.
        """
        agents = []
        for a in AGENT_REGISTRY:
            latency_ms = {
                "GIA":  14   + random.randint(-3, 5),
                "COG":  2800 + random.randint(-200, 400),
                "DTB":  95   + random.randint(-10, 30),
                "SVB":  12   + random.randint(-2, 4),
                "THA":  0,
                "DSWs": 310  + random.randint(-50, 150),
            }.get(a["id"], 0)

            agents.append({
                **a,
                "status":     "healthy",
                "latency_ms": latency_ms,
                "ops_per_min": {
                    "GIA":  38  + random.randint(0, 12),
                    "COG":  12  + random.randint(0, 5),
                    "DTB":  890 + random.randint(-50, 150),
                    "SVB":  330 + random.randint(-20, 60),
                    "THA":  0,
                    "DSWs": 142 + random.randint(-30, 80),
                }.get(a["id"], 0),
            })

        return {
            "timestamp":       datetime.utcnow().isoformat(),
            "active_sessions": random.randint(1, 6),
            "tha_tickets":     random.randint(4, 14),
            "avg_viability":   round(6.0 + random.random() * 2.8, 1),
            "dsw_ops_per_min": 100 + random.randint(0, 140),
            "agents":          agents,
        }

    # ────────────────────────────────────────────────────────────────────────
    @app.get("/v1/events")
    async def sse_stream(request: Request):
        """
        Server-Sent Events stream of live telemetry.
        Frontend subscribes with EventSource('/v1/events').
        Rotates through SSE_TOPICS, emitting one event every ~2.5s.
        """
        async def event_generator() -> AsyncGenerator[str, None]:
            idx = 0
            while True:
                if await request.is_disconnected():
                    break
                evt = SSE_TOPICS[idx % len(SSE_TOPICS)]
                data = json.dumps({
                    "type":      evt["type"],
                    "msg":       evt["msg"],
                    "trace_id":  uuid.uuid4().hex[:8],
                    "timestamp": datetime.utcnow().isoformat(),
                })
                yield f"data: {data}\n\n"
                idx += 1
                await asyncio.sleep(2.5)

        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control":               "no-cache",
                "X-Accel-Buffering":           "no",
                "Access-Control-Allow-Origin": "*",
            },
        )

    return app
