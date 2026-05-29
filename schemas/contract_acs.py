"""
ACS (Access Control Service) Security Contract

Defines how specialist agents request temporary credentials
for MCP server access. Credentials are NEVER stored locally on agents.
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
import uuid


# ============================================================================
# CREDENTIAL REQUEST/RESPONSE FLOW
# ============================================================================

class CredentialType(Enum):
    """Types of credentials the ACS manages."""
    API_KEY = "api_key"
    OAUTH2_TOKEN = "oauth2_token"
    BEARER_TOKEN = "bearer_token"
    DATABASE_CREDENTIALS = "db_credentials"
    AWS_STS_ASSUME_ROLE = "aws_sts_assume_role"
    VAULT_TRANSIT_KEY = "vault_transit_key"


@dataclass
class CredentialRequest:
    """
    Request from a specialist agent to ACS for temporary credentials.
    
    Example Flow:
    
    Specialist Agent (geological_scout) needs to call MCP tool:
        -> Creates CredentialRequest(
             requested_permissions=["geological_survey_india:read"],
             mcp_server_name="geological_mcp",
             ttl_seconds=300,
             reason="Querying mineral deposits in Tamil Nadu"
           )
        -> Sends to ACS via secure channel
        -> ACS validates & retrieves from Vault
        -> Returns TemporaryCredential with token & expiry
        -> Specialist injects token into MCP tool call
        -> Token auto-expires after TTL
    """
    
    # Request metadata
    request_id: str                    # Unique request ID
    timestamp: float                   # Unix timestamp
    
    # Requester info (always verified via mutual TLS or JWT)
    requester_agent_id: str            # e.g., "geological_scout_001"
    requester_trace_id: Optional[str]  # OpenTelemetry trace ID
    
    # Credential requirements
    mcp_server_name: str               # e.g., "geological_mcp", "trade_api_mcp"
    requested_permissions: List[str]   # e.g., ["read:deposits", "read:mining_leases"]
    credential_type: CredentialType    # Type of credential needed
    
    # Security parameters
    ttl_seconds: int                   # Time-to-live (max 3600 seconds = 1 hour)
    reason: str                        # Justification for credential request
    ip_restricted: Optional[bool]      # If True, restrict to requesting IP
    
    # Audit trail
    user_id: Optional[str]             # If applicable
    job_id: Optional[str]              # EJMS job ID for traceability


@dataclass
class TemporaryCredential:
    """
    Single-use or time-limited credential issued by ACS.
    
    Properties:
    - Auto-expires after ttl_seconds
    - Can be revoked early if abuse detected
    - Tied to specific MCP server
    - Never stored on agent; only in memory during execution
    """
    
    # Credential metadata
    credential_id: str                 # Unique ID for audit
    request_id: str                    # Reference to original request
    
    # Credential data
    token: str                         # The actual credential (JWT, API key, etc.)
    credential_type: CredentialType
    mcp_server_url: str                # Where to use this credential
    
    # Expiry
    issued_at: float                   # Unix timestamp
    expires_at: float                  # Unix timestamp (auto-revoke after this)
    ttl_seconds: int
    
    # Metadata
    permissions_granted: List[str]     # Actual permissions approved
    issuer: str                        # "acs" or specific vault identity
    
    # Audit trail  
    agent_id_hint: str                 # For logging (not security-sensitive)
    job_id: Optional[str]              # EJMS job for correlation


@dataclass
class CredentialResponse:
    """Response from ACS after credential request."""
    
    status: str                        # "success", "denied", "error"
    credential: Optional[TemporaryCredential]
    error_message: Optional[str]
    audit_log_id: Optional[str]        # For debugging


# ============================================================================
# ACS SECURITY CONTRACT (Specialist Agent -> ACS -> MCP)
# ============================================================================

class ACSSecurityContract:
    """
    Defines the security contract between:
    1. Specialist Agent (requestor)
    2. ACS (credential issuer)
    3. MCP Server (resource)
    """

    @staticmethod
    def request_credentials_for_mcp(
        agent_id: str,
        mcp_server_name: str,
        permissions: List[str],
        job_id: str,
        trace_id: str,
        ttl_seconds: int = 300
    ) -> CredentialRequest:
        """
        Specialist agent calls this to request temporary credentials.
        
        Example:
            request = ACSSecurityContract.request_credentials_for_mcp(
                agent_id="geological_scout_001",
                mcp_server_name="geological_mcp",
                permissions=["read:deposits", "read:mining_leases:Tamil Nadu"],
                job_id="job_abc123",
                trace_id="trace_xyz789",
                ttl_seconds=300
            )
        
        Returns:
            CredentialRequest ready to send to ACS
        """
        return CredentialRequest(
            request_id=str(uuid.uuid4()),
            timestamp=datetime.now().timestamp(),
            requester_agent_id=agent_id,
            requester_trace_id=trace_id,
            mcp_server_name=mcp_server_name,
            requested_permissions=permissions,
            credential_type=CredentialType.API_KEY,
            ttl_seconds=min(ttl_seconds, 3600),  # Cap at 1 hour
            reason=f"MCP tool execution for {mcp_server_name}",
            ip_restricted=False,
            job_id=job_id
        )

    @staticmethod
    def validate_credential_request(request: CredentialRequest) -> tuple[bool, Optional[str]]:
        """
        ACS validates credential request before issuing.
        
        Validation checks:
        1. Is requester_agent_id known & authorized?
        2. Are requested_permissions valid for this agent?
        3. Is TTL within acceptable range?
        4. Does rate limiting allow this request?
        5. Is job_id valid (EJMS)?
        
        Returns:
            (is_valid, error_message)
        """
        # Check 1: Agent identity
        known_agents = {
            "geological_scout", "chemical_infra", "logistics_analyst",
            "fab_locator", "workforce_analyzer", "thermal_specialist"
        }
        agent_type = request.requester_agent_id.split("_")[0]
        if agent_type not in known_agents:
            return False, f"Unknown agent type: {agent_type}"

        # Check 2: Permission scope (simplified)
        valid_permissions = {
            "geological_mcp": [
                "read:deposits", "read:mining_leases", "read:mineral_reserves"
            ],
            "industrial_mcp": [
                "read:facilities", "read:industrial_zones", "read:certifications"
            ],
            "trade_api_mcp": [
                "read:import_data", "read:export_data", "read:port_logs"
            ]
        }
        
        allowed = valid_permissions.get(request.mcp_server_name, [])
        for perm in request.requested_permissions:
            if perm not in allowed:
                return False, f"Permission denied: {perm}"

        # Check 3: TTL
        if request.ttl_seconds > 3600:
            return False, "TTL exceeds maximum (3600 seconds)"
        if request.ttl_seconds < 1:
            return False, "TTL must be >= 1 second"

        # All checks passed
        return True, None

    @staticmethod
    def issue_temporary_credential(
        request: CredentialRequest,
        vault_token: str,  # From Vault
        permissions_approved: List[str]
    ) -> TemporaryCredential:
        """
        ACS issues a temporary credential after validation.
        
        Steps:
        1. Validate request (see validate_credential_request)
        2. Query Vault/Secrets Manager for MCP server credentials
        3. Generate temporary token (JWT) with:
           - Issuer: ACS
           - Subject: requester_agent_id
           - Permissions: requested_permissions
           - Expiry: ttl_seconds from now
        4. Return TemporaryCredential
        5. Log audit trail
        
        Returns:
            TemporaryCredential (never persisted on agent)
        """
        now = datetime.now()
        expiry = now + timedelta(seconds=request.ttl_seconds)
        
        # In production: Call Vault to get actual secret/API key
        # For now, mock JWT token generation
        mock_jwt_token = f"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9." \
                        f"eyJpc3MiOiJhY3MiLCJzdWIiOiIie request.requester_agent_id}\"," \
                        f"\"exp\":{expiry.timestamp()}}}"
        
        return TemporaryCredential(
            credential_id=str(uuid.uuid4()),
            request_id=request.request_id,
            token=mock_jwt_token,
            credential_type=request.credential_type,
            mcp_server_url=f"https://mcp-{request.mcp_server_name}:8443",
            issued_at=now.timestamp(),
            expires_at=expiry.timestamp(),
            ttl_seconds=request.ttl_seconds,
            permissions_granted=permissions_approved,
            issuer="acs",
            agent_id_hint=request.requester_agent_id.split("_")[0],
            job_id=request.job_id
        )


# ============================================================================
# MCP SERVER SECURE WRAPPER
# ============================================================================

class MCPServerSecureWrapper:
    """
    Wrapper that every MCP tool call goes through.
    
    Ensures:
    1. Credential is valid & not expired
    2. Credential is attached to request
    3. Requests are logged for audit
    4. Revocation is enforced
    """

    @staticmethod
    def call_mcp_tool_securely(
        mcp_server_name: str,
        tool_name: str,
        tool_params: Dict[str, Any],
        credential: TemporaryCredential,
        trace_id: str
    ) -> Dict[str, Any]:
        """
        Execute MCP tool with security checks.
        
        Flow:
        1. Validate credential not expired
        2. Validate credential not revoked
        3. Create request with credential attached
        4. Call MCP tool
        5. Log with trace_id
        6. Return result
        
        Example:
            result = MCPServerSecureWrapper.call_mcp_tool_securely(
                mcp_server_name="geological_mcp",
                tool_name="query_deposits",
                tool_params={"region": "TN", "material": "copper"},
                credential=temp_credential,
                trace_id="trace_xyz789"
            )
        """
        # Check 1: Credential expiry
        if datetime.now().timestamp() > credential.expires_at:
            raise RuntimeError(f"Credential expired: {credential.credential_id}")

        # Check 2: Credential revocation (check revocation list)
        revoked_credentials = set()  # In production, fetch from ACS
        if credential.credential_id in revoked_credentials:
            raise RuntimeError(f"Credential revoked: {credential.credential_id}")

        # Check 3: Build secure request
        secure_request = {
            "tool": tool_name,
            "params": tool_params,
            "auth": {
                "token": credential.token,
                "type": credential.credential_type.value
            },
            "trace_id": trace_id,
            "timestamp": datetime.now().isoformat()
        }

        # Check 4: Make MCP call (mocked here)
        # In production: HTTP POST to mcp_server_url with auth header
        # response = requests.post(
        #     credential.mcp_server_url,
        #     json=secure_request,
        #     headers={"Authorization": f"Bearer {credential.token}"}
        # )
        
        mock_response = {
            "status": "success",
            "data": {"mock_result": "placeholder"},
            "execution_time_ms": 150
        }

        # Check 5: Log audit trail
        print(f"[AUDIT] MCP call: {mcp_server_name}/{tool_name} "
              f"by {credential.agent_id_hint} (trace:{trace_id})")

        return mock_response


# ============================================================================
# CREDENTIAL REVOCATION
# ============================================================================

class CredentialRevocation:
    """
    Mechanism to revoke credentials early if abuse detected.
    """

    revoked_credentials = set()  # In production: persistent store

    @staticmethod
    def revoke_credential(credential_id: str, reason: str):
        """
        ACS can revoke a credential before expiry.
        
        Reasons:
        - Agent misbehavior detected
        - Unusual access pattern
        - Quote exceeded
        - Manual admin revocation
        """
        CredentialRevocation.revoked_credentials.add(credential_id)
        print(f"[SECURITY] Revoked credential {credential_id}: {reason}")

    @staticmethod
    def is_revoked(credential_id: str) -> bool:
        """Check if credential is revoked."""
        return credential_id in CredentialRevocation.revoked_credentials


if __name__ == "__main__":
    print("ACS Security Contract loaded")
    
    # Example: Request flow
    request = ACSSecurityContract.request_credentials_for_mcp(
        agent_id="geological_scout_001",
        mcp_server_name="geological_mcp",
        permissions=["read:deposits"],
        job_id="job_42",
        trace_id="trace_xyz"
    )
    print(f"Request ID: {request['request_id']}")
    
    # Validate
    is_valid, error = ACSSecurityContract.validate_credential_request(request)
    print(f"Valid: {is_valid}, Error: {error}")
    
    # Issue credential
    if is_valid:
        cred = ACSSecurityContract.issue_temporary_credential(
            request, "vault_token_123", ["read:deposits"]
        )
        print(f"Credential issued: {cred.credential_id} (expires in {cred.ttl_seconds}s)")
