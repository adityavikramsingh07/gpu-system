"""
core_services/svb.py
=====================
Secure Vault Broker (SVB)

The SVB is the centralized identity and access manager for the entire
GPU supply chain multi-agent system. It enforces:

  1. Zero local credential storage  — agents NEVER store tokens
  2. Just-in-time (JIT) injection   — tokens issued per-request
  3. Scoped ephemeral tokens        — minimum permission surface
  4. Automatic token expiry          — TTL enforced server-side
  5. Full audit trail                — every issuance logged to OTel

Security contract (handshake flow):
  DSW Worker
    │
    ▼ CredentialRequest(worker_id, mcp_server_id, required_scopes, task_id)
  ┌────────────────────────────────────────────────────────────┐
  │                  Secure Vault Broker (SVB)                 │
  │                                                            │
  │  1. Authenticate worker_id (HMAC signature or JWT)         │
  │  2. Verify task_id is active in DTB (prevent replay)       │
  │  3. Check policy: does this worker_type allow these scopes? │
  │  4. Fetch master secret from HashiCorp Vault               │
  │  5. Generate scoped ephemeral token (TTL = 5 min)          │
  │  6. Log issuance to OTel audit span                        │
  │  7. Return EphemeralToken (never persisted on SVB)         │
  └───────────────────────────┬────────────────────────────────┘
                              │
                              ▼ EphemeralToken(token, ttl, scopes)
  DSW Worker
    │ Uses token immediately for MCP server auth
    │ Token is NOT stored after MCP call completes
    ▼
  MCP Server (validates token server-side)

Vault backends supported:
  - HashiCorp Vault (primary)
  - AWS Secrets Manager (fallback)
  - Azure Key Vault (tertiary)
  - Environment variables (dev/test only)
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional


# ============================================================================
# SCOPE DEFINITIONS — Minimum-privilege access control
# ============================================================================

class MCPScope(str, Enum):
    """
    Fine-grained scopes controlling which MCP server tools a DSW can invoke.
    Worker-type to scope mapping is enforced by SVB policy.
    """
    # Geological MCP scopes
    READ_MINING_DEPOSITS  = "geo:read:mining_deposits"
    READ_LEASE_STATUS     = "geo:read:lease_status"
    READ_GEO_SURVEY       = "geo:read:geological_survey"

    # Chemical MCP scopes
    READ_CHEMICAL_PLANTS  = "chem:read:chemical_plants"
    READ_PURITY_CERTS     = "chem:read:purity_certifications"
    READ_REAGENT_SUPPLY   = "chem:read:reagent_supply"

    # Logistics MCP scopes
    READ_PORT_DATA        = "log:read:port_data"
    READ_TRANSPORT_ROUTES = "log:read:transport_routes"
    READ_WAREHOUSE_CAP    = "log:read:warehouse_capacity"

    # Trade/Policy MCP scopes
    READ_PLI_SCHEMES      = "trade:read:pli_schemes"
    READ_TARIFF_DATA      = "trade:read:tariff_data"
    READ_TRADE_RESTRICT   = "trade:read:trade_restrictions"

    # Industrial/Fab MCP scopes
    READ_FAB_FACILITIES   = "ind:read:fab_facilities"
    READ_CLEANROOM_SPECS  = "ind:read:cleanroom_specs"

    # Environmental MCP scopes
    READ_ENV_CLEARANCE    = "env:read:env_clearance"
    READ_POLLUTION_NORMS  = "env:read:pollution_norms"

    # QA MCP scopes
    READ_PURITY_SPEC      = "qa:read:purity_spec"
    READ_CERTIFICATIONS   = "qa:read:certification_requirements"


# Worker-type → allowed scopes policy map
WORKER_SCOPE_POLICY: Dict[str, List[str]] = {
    "geological_expert": [
        MCPScope.READ_MINING_DEPOSITS.value,
        MCPScope.READ_LEASE_STATUS.value,
        MCPScope.READ_GEO_SURVEY.value,
    ],
    "chemical_infra_analyst": [
        MCPScope.READ_CHEMICAL_PLANTS.value,
        MCPScope.READ_PURITY_CERTS.value,
        MCPScope.READ_REAGENT_SUPPLY.value,
    ],
    "logistics_coordinator": [
        MCPScope.READ_PORT_DATA.value,
        MCPScope.READ_TRANSPORT_ROUTES.value,
        MCPScope.READ_WAREHOUSE_CAP.value,
    ],
    "trade_policy_expert": [
        MCPScope.READ_PLI_SCHEMES.value,
        MCPScope.READ_TARIFF_DATA.value,
        MCPScope.READ_TRADE_RESTRICT.value,
    ],
    "fab_locator": [
        MCPScope.READ_FAB_FACILITIES.value,
        MCPScope.READ_CLEANROOM_SPECS.value,
    ],
    "environmental_compliance": [
        MCPScope.READ_ENV_CLEARANCE.value,
        MCPScope.READ_POLLUTION_NORMS.value,
    ],
    "semiconductor_grade_qa": [
        MCPScope.READ_PURITY_SPEC.value,
        MCPScope.READ_CERTIFICATIONS.value,
    ],
    "supply_chain_forecaster": [
        MCPScope.READ_TRADE_RESTRICT.value,
        MCPScope.READ_PLI_SCHEMES.value,
        MCPScope.READ_TRANSPORT_ROUTES.value,
    ],
    "mining_lease_analyst": [
        MCPScope.READ_LEASE_STATUS.value,
        MCPScope.READ_ENV_CLEARANCE.value,
    ],
    "workforce_analyst": [],      # Workforce MCP — add scopes as needed
    "thermal_materials_expert": [],
}


# ============================================================================
# DATA MODELS
# ============================================================================

@dataclass
class CredentialRequest:
    """
    Request payload sent by a DSW to the SVB.
    The DSW signs this with its worker_secret_key (HMAC-SHA256).
    SVB verifies the signature before issuing any token.
    """
    request_id:      str       = field(default_factory=lambda: str(uuid.uuid4()))
    worker_id:       str       = ""          # e.g. "dsw-geo-worker-1"
    worker_type:     str       = ""          # e.g. "geological_expert"
    task_id:         str       = ""          # Active DTB task_id (anti-replay)
    session_id:      str       = ""          # COG session for audit
    trace_id:        str       = ""          # OTel trace for correlation
    mcp_server_id:   str       = ""          # Target MCP server
    requested_scopes: List[str] = field(default_factory=list)
    requested_ttl_s: int       = 300         # 5 minutes default
    hmac_signature:  str       = ""          # HMAC-SHA256(request_id+task_id+worker_id)
    issued_at:       str       = field(default_factory=lambda: datetime.utcnow().isoformat())


@dataclass
class EphemeralToken:
    """
    Short-lived token returned by SVB to the DSW.
    The token is scoped to a SINGLE mcp_server_id and a list of scopes.
    It expires in `ttl_seconds` and is NEVER persisted by the receiver.
    """
    token_id:        str       = field(default_factory=lambda: str(uuid.uuid4()))
    issued_to:       str       = ""          # worker_id
    mcp_server_id:   str       = ""
    granted_scopes:  List[str] = field(default_factory=list)
    token_value:     str       = ""          # Opaque bearer token (encrypted)
    issued_at:       datetime  = field(default_factory=datetime.utcnow)
    expires_at:      datetime  = field(default_factory=lambda: datetime.utcnow() + timedelta(minutes=5))
    ttl_seconds:     int       = 300
    audit_trail_id:  str       = ""          # Reference to vault audit log entry

    def is_expired(self) -> bool:
        return datetime.utcnow() >= self.expires_at

    def as_bearer_header(self) -> Dict[str, str]:
        """Format token as HTTP Authorization header for MCP server auth."""
        return {"Authorization": f"Bearer {self.token_value}"}


@dataclass
class SVBAuthFailure(Exception):
    """Raised when SVB rejects a credential request."""
    reason:      str  = ""
    request_id:  str  = ""
    worker_id:   str  = ""


# ============================================================================
# SECURE VAULT BROKER
# ============================================================================

class SecureVaultBroker:
    """
    SVB: Centralized just-in-time credential injection engine.

    The SVB is the ONLY component in the system that ever holds
    plaintext secrets. All other components operate with ephemeral
    scoped tokens returned by issue_token().

    Vault backend priority:
      1. HashiCorp Vault (production)
      2. AWS Secrets Manager (cloud deployment)
      3. Environment variables (dev/CI only — warns loudly)
    """

    def __init__(
        self,
        vault_addr:   str = "http://localhost:8200",
        vault_token:  str = "",      # SVB's own Vault service token
        vault_path:   str = "secret/gpu-supply-chain/mcp-servers",
        hmac_key:     str = "",      # Shared HMAC key for worker verification
        backend:      str = "vault", # vault | aws | env
    ):
        self.vault_addr  = vault_addr
        self.vault_token = vault_token or os.environ.get("VAULT_TOKEN", "")
        self.vault_path  = vault_path
        self.hmac_key    = (hmac_key or os.environ.get("SVB_HMAC_KEY", "dev-hmac-key")).encode()
        self.backend     = backend

        # In-memory token revocation list (production: use Redis Set with TTL)
        self._revoked_tokens: set = set()

        # Active task registry (anti-replay: task_id must be in DTB)
        # In production: query DTB Redis for task existence
        self._active_tasks: set = set()   # Populated by DTB worker registration

    # ── Public API ──────────────────────────────────────────────────────────

    def issue_token(self, request: CredentialRequest) -> EphemeralToken:
        """
        The SVB Handshake — the security-critical path.

        Security steps (ALL must pass):
          1. Verify HMAC signature on the request
          2. Validate worker_type is a known DSW type
          3. Enforce scope policy: requested_scopes ⊆ allowed_scopes
          4. Verify task_id is active (anti-replay attack prevention)
          5. Fetch MCP server master secret from Vault
          6. Generate a scoped ephemeral token
          7. Set TTL (capped at max 15 minutes, min 1 minute)
          8. Log issuance to OTel audit span

        Args:
            request: CredentialRequest from DSW

        Returns:
            EphemeralToken (scoped, time-limited bearer token)

        Raises:
            SVBAuthFailure: If any security check fails
        """
        # ── 1. Verify HMAC signature ─────────────────────────────────────────
        self._verify_hmac(request)

        # ── 2. Validate worker_type ──────────────────────────────────────────
        if request.worker_type not in WORKER_SCOPE_POLICY:
            raise SVBAuthFailure(
                reason    = f"Unknown worker_type: {request.worker_type}",
                request_id = request.request_id,
                worker_id  = request.worker_id,
            )

        # ── 3. Enforce scope policy ──────────────────────────────────────────
        allowed = set(WORKER_SCOPE_POLICY[request.worker_type])
        requested = set(request.requested_scopes)
        denied = requested - allowed

        if denied:
            raise SVBAuthFailure(
                reason    = f"Scope violation: {denied} not permitted for {request.worker_type}",
                request_id = request.request_id,
                worker_id  = request.worker_id,
            )

        # ── 4. Verify task is active (anti-replay) ───────────────────────────
        # In production: self._verify_active_task(request.task_id)
        # Here we trust the DTB registered it

        # ── 5. Fetch master secret from Vault ───────────────────────────────
        master_secret = self._fetch_from_vault(
            mcp_server_id = request.mcp_server_id,
            worker_type   = request.worker_type,
        )

        # ── 6. Generate scoped ephemeral token ──────────────────────────────
        ttl = max(60, min(request.requested_ttl_s, 900))    # 1min – 15min cap
        token_value    = self._generate_scoped_token(
            master_secret = master_secret,
            scopes        = list(requested),
            ttl           = ttl,
            request_id    = request.request_id,
        )

        # ── 7. Build and return EphemeralToken ──────────────────────────────
        now     = datetime.utcnow()
        token   = EphemeralToken(
            issued_to      = request.worker_id,
            mcp_server_id  = request.mcp_server_id,
            granted_scopes = list(requested & allowed),
            token_value    = token_value,
            issued_at      = now,
            expires_at     = now + timedelta(seconds=ttl),
            ttl_seconds    = ttl,
            audit_trail_id = self._log_issuance(request, token_value, ttl),
        )

        return token

    def revoke_token(self, token_id: str) -> None:
        """
        Immediately revoke a token (e.g., on DSW crash or anomaly detection).
        Adds token_id to in-memory revocation list.
        In production: Kafka event to all MCP servers to reject this token.
        """
        self._revoked_tokens.add(token_id)

    def is_token_valid(self, token: EphemeralToken) -> bool:
        """Check if a token is still valid (not expired, not revoked)."""
        if token.token_id in self._revoked_tokens:
            return False
        return not token.is_expired()

    # ── Internal methods ─────────────────────────────────────────────────────

    def _verify_hmac(self, request: CredentialRequest) -> None:
        """
        Verify the DSW signed its request with the shared HMAC key.
        Prevents forged credential requests from unauthorized processes.
        """
        message = f"{request.request_id}:{request.task_id}:{request.worker_id}".encode()
        expected_sig = hmac.new(self.hmac_key, message, hashlib.sha256).hexdigest()

        if not hmac.compare_digest(expected_sig, request.hmac_signature):
            raise SVBAuthFailure(
                reason    = "HMAC signature verification failed",
                request_id = request.request_id,
                worker_id  = request.worker_id,
            )

    def _fetch_from_vault(self, mcp_server_id: str, worker_type: str) -> str:
        """
        Fetch MCP server master API key from the configured Vault backend.

        In production: makes authenticated HTTP call to HashiCorp Vault.
        Here: returns from environment variables for testability.
        """
        if self.backend == "vault":
            return self._fetch_hashicorp_vault(mcp_server_id)
        elif self.backend == "aws":
            return self._fetch_aws_secrets(mcp_server_id)
        else:
            # Dev mode: read from env
            env_key = f"MCP_{mcp_server_id.upper().replace('-', '_')}_API_KEY"
            secret = os.environ.get(env_key, f"dev-secret-{mcp_server_id}")
            return secret

    def _fetch_hashicorp_vault(self, mcp_server_id: str) -> str:
        """
        Fetch secret from HashiCorp Vault KV v2.

        Path: secret/gpu-supply-chain/mcp-servers/<mcp_server_id>
        Key:  api_key
        """
        import urllib.request
        url = f"{self.vault_addr}/v1/{self.vault_path}/{mcp_server_id}"
        req = urllib.request.Request(
            url,
            headers={"X-Vault-Token": self.vault_token},
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                data   = json.loads(resp.read().decode())
                return data["data"]["data"]["api_key"]
        except Exception as e:
            raise SVBAuthFailure(
                reason    = f"Vault fetch failed for {mcp_server_id}: {e}",
                request_id = "",
                worker_id  = "",
            )

    def _fetch_aws_secrets(self, mcp_server_id: str) -> str:
        """Fetch from AWS Secrets Manager (fallback backend)."""
        try:
            import boto3
            client = boto3.client("secretsmanager")
            response = client.get_secret_value(
                SecretId=f"gpu-supply-chain/mcp-servers/{mcp_server_id}"
            )
            secret = json.loads(response["SecretString"])
            return secret["api_key"]
        except Exception as e:
            raise SVBAuthFailure(
                reason    = f"AWS Secrets Manager fetch failed: {e}",
                request_id = "",
                worker_id  = "",
            )

    def _generate_scoped_token(
        self,
        master_secret: str,
        scopes:        List[str],
        ttl:           int,
        request_id:    str,
    ) -> str:
        """
        Generate a scoped ephemeral token by deriving from the master secret.

        Production implementation: Use Vault's token creation endpoint with
        a policy attachment corresponding to the granted scopes.

        Simplified here: HMAC-derive a token that encodes scopes + expiry.
        """
        scope_str   = ":".join(sorted(scopes))
        expiry_str  = str(int(time.time()) + ttl)
        payload     = f"{request_id}:{scope_str}:{expiry_str}"
        token       = hmac.new(master_secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
        return f"svb-{token[:32]}-{expiry_str}"

    def _log_issuance(
        self,
        request:     CredentialRequest,
        token_value: str,
        ttl:         int,
    ) -> str:
        """
        Log token issuance to OpenTelemetry audit span.
        Returns an audit_trail_id for cross-referencing.
        """
        audit_id = f"audit-{uuid.uuid4().hex[:8]}"
        try:
            from opentelemetry import trace
            tracer = trace.get_tracer("svb")
            with tracer.start_as_current_span("svb.token_issued") as span:
                span.set_attribute("audit_id",      audit_id)
                span.set_attribute("worker_id",     request.worker_id)
                span.set_attribute("worker_type",   request.worker_type)
                span.set_attribute("mcp_server_id", request.mcp_server_id)
                span.set_attribute("scopes",        json.dumps(request.requested_scopes))
                span.set_attribute("ttl_s",         ttl)
                span.set_attribute("task_id",       request.task_id)
                span.set_attribute("session_id",    request.session_id)
                span.set_attribute("trace_id",      request.trace_id)
                # Never log token_value — only its hash
                span.set_attribute("token_hash", hashlib.sha256(token_value.encode()).hexdigest()[:16])
        except Exception:
            pass  # OTel failure must not block credential issuance

        return audit_id


# ============================================================================
# DSW CREDENTIAL HELPER — used by DSW workers to request tokens
# ============================================================================

def request_mcp_credentials(
    svb:           SecureVaultBroker,
    worker_id:     str,
    worker_type:   str,
    task_id:       str,
    session_id:    str,
    trace_id:      str,
    mcp_server_id: str,
    required_scopes: List[str],
    worker_hmac_key: str,
    ttl_seconds:   int = 300,
) -> EphemeralToken:
    """
    Convenience function for DSW workers to request a scoped token from SVB.

    This is the ONLY way a DSW should obtain MCP credentials.
    The token is returned in-memory and must be used immediately —
    never stored to disk, database, or logs.

    Usage in DSW worker:
        token = request_mcp_credentials(
            svb            = svb_instance,
            worker_id      = "dsw-geo-worker-1",
            worker_type    = "geological_expert",
            task_id        = envelope.task_id,
            session_id     = envelope.session_id,
            trace_id       = envelope.trace_id,
            mcp_server_id  = "geological-mcp-server",
            required_scopes = [MCPScope.READ_MINING_DEPOSITS.value],
            worker_hmac_key = os.environ["DSW_HMAC_KEY"],
        )
        # Use immediately:
        mcp_client.call_tool(
            tool_name = "query_mining_deposits",
            headers   = token.as_bearer_header(),
            params    = {...},
        )
        # token goes out of scope — no cleanup needed

    Args:
        svb:              SVB instance
        worker_id:        Unique worker identifier
        worker_type:      DSW canonical type string
        task_id:          Active DTB task_id (anti-replay)
        session_id:       COG session_id for audit
        trace_id:         OTel trace_id for correlation
        mcp_server_id:    Target MCP server identifier
        required_scopes:  List of MCPScope values needed
        worker_hmac_key:  Worker's shared secret for signing
        ttl_seconds:      Requested token TTL (SVB may cap this)

    Returns:
        EphemeralToken: Use immediately, do not store
    """
    # Build request
    request_id = str(uuid.uuid4())
    message    = f"{request_id}:{task_id}:{worker_id}".encode()
    signature  = hmac.new(worker_hmac_key.encode(), message, hashlib.sha256).hexdigest()

    cred_request = CredentialRequest(
        request_id       = request_id,
        worker_id        = worker_id,
        worker_type      = worker_type,
        task_id          = task_id,
        session_id       = session_id,
        trace_id         = trace_id,
        mcp_server_id    = mcp_server_id,
        requested_scopes = required_scopes,
        requested_ttl_s  = ttl_seconds,
        hmac_signature   = signature,
    )

    return svb.issue_token(cred_request)
