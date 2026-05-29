"""
core_services/dtb.py
=====================
Distributed Task Broker (DTB)

The DTB is the asynchronous execution engine backed by Redis (queue/result store)
and Kafka (event bus for fault signalling). The COG's fan_out_node publishes
TaskEnvelopes here; DTB workers pull jobs, execute DSW agents, and write results back.

Architecture:
  COG fan_out_node
      │
      ▼ publish(TaskEnvelope)
  ┌───────────────────────────────┐
  │        Redis Stream           │  ← per-worker-type stream keys
  │  queue:geological_expert      │
  │  queue:chemical_infra_analyst │
  │  ...                         │
  └─────────────┬─────────────────┘
                │ XREADGROUP (consumer groups)
                ▼
       DTB Worker Processes
       (one per SpecialistType)
                │
                ▼
       DSW Specialist Agent
                │
                ▼ write result
  ┌───────────────────────────────┐
  │        Redis Hash             │  ← result:job_id
  │  status, result_data, etc.   │
  └───────────────────────────────┘
                │
                ▼ publish fault events
  ┌───────────────────────────────┐
  │        Kafka                  │  ← sys-events, agent-faults topics
  └───────────────────────────────┘

Dead-Letter Queue (DLQ):
  After max_retries exhausted, job is moved to `dlq:<worker_type>` stream
  and a CRITICAL telemetry event is published to Kafka `agent-faults`.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

import redis


# ============================================================================
# ENUMS & DATA MODELS
# ============================================================================

class TaskPriority(int, Enum):
    """Task execution priority (lower = higher urgency)."""
    CRITICAL = 1
    HIGH     = 2
    MEDIUM   = 3
    LOW      = 4
    BULK     = 5


class TaskStatus(str, Enum):
    """DTB job lifecycle states."""
    QUEUED     = "queued"      # Published to Redis stream
    CLAIMED    = "claimed"     # Worker picked up via XREADGROUP
    EXECUTING  = "executing"   # DSW agent running
    COMPLETED  = "completed"   # Result written to Redis
    FAILED     = "failed"      # Error, may retry
    RETRYING   = "retrying"    # Backoff, re-queued
    DLQ        = "dlq"         # Exhausted retries → Dead Letter Queue
    TIMEOUT    = "timeout"     # Exceeded deadline


@dataclass
class TaskEnvelope:
    """
    Payload published by COG fan_out_node to the DTB.
    Immutable once submitted — workers clone and annotate internally.
    """
    # Identity
    task_id:           str                = field(default_factory=lambda: f"task-{uuid.uuid4().hex[:10]}")
    parent_request_id: str                = ""
    session_id:        str                = ""
    trace_id:          str                = ""

    # Routing
    worker_type:       str                = ""    # DSWWorkerType value
    priority:          TaskPriority       = TaskPriority.MEDIUM

    # Task payload
    query:             str                = ""
    region_focus:      str                = "Southern India"
    material_focus:    str                = ""
    required_tools:    List[str]          = field(default_factory=list)
    mcp_server_id:     str                = ""

    # Execution constraints
    timeout_seconds:   int                = 45
    max_retries:       int                = 3
    prerequisite_job_ids: List[str]       = field(default_factory=list)

    # Timestamps
    created_at:        str                = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_redis_fields(self) -> Dict[str, str]:
        """Serialize to flat dict for Redis XADD."""
        d = asdict(self)
        d["required_tools"]        = json.dumps(d["required_tools"])
        d["prerequisite_job_ids"]  = json.dumps(d["prerequisite_job_ids"])
        d["priority"]              = str(d["priority"])
        return {k: str(v) for k, v in d.items()}

    @classmethod
    def from_redis_fields(cls, fields: Dict[str, str]) -> "TaskEnvelope":
        """Deserialize from Redis XREADGROUP message."""
        return cls(
            task_id           = fields["task_id"],
            parent_request_id = fields["parent_request_id"],
            session_id        = fields["session_id"],
            trace_id          = fields["trace_id"],
            worker_type       = fields["worker_type"],
            priority          = TaskPriority(int(fields["priority"])),
            query             = fields["query"],
            region_focus      = fields["region_focus"],
            material_focus    = fields["material_focus"],
            required_tools    = json.loads(fields["required_tools"]),
            mcp_server_id     = fields["mcp_server_id"],
            timeout_seconds   = int(fields["timeout_seconds"]),
            max_retries       = int(fields["max_retries"]),
            prerequisite_job_ids = json.loads(fields["prerequisite_job_ids"]),
            created_at        = fields["created_at"],
        )


@dataclass
class DTBJobRecord:
    """Internal job tracking record stored in Redis Hash."""
    job_id:       str
    task_id:      str
    worker_type:  str
    stream_id:    str               # Redis stream message ID for XACK
    status:       TaskStatus        = TaskStatus.QUEUED
    retry_count:  int               = 0
    created_at:   str               = field(default_factory=lambda: datetime.utcnow().isoformat())
    claimed_at:   Optional[str]     = None
    completed_at: Optional[str]     = None
    result_data:  Optional[str]     = None   # JSON string
    error_msg:    Optional[str]     = None
    trace_id:     str               = ""


# ============================================================================
# DISTRIBUTED TASK BROKER
# ============================================================================

class DistributedTaskBroker:
    """
    DTB: Asynchronous task routing engine.

    Implements a Redis Streams-based job queue with:
      - Consumer groups for worker fault tolerance
      - Exponential backoff retries (2^n seconds, capped at 60s)
      - Dead-letter queue for exhausted jobs
      - Kafka fault event publishing for THA interception
    """

    # Redis key namespaces
    QUEUE_PREFIX  = "dtb:queue:"       # Redis stream per worker_type
    JOB_PREFIX    = "dtb:job:"         # Redis hash per job
    RESULT_PREFIX = "dtb:result:"      # Redis hash for job results
    DLQ_PREFIX    = "dtb:dlq:"         # Dead-letter stream per worker_type
    CONSUMER_GROUP = "dsw-workers"     # Consumer group name

    def __init__(
        self,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db:   int  = 1,           # Separate DB from EJMS
    ):
        self.redis = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            decode_responses=True,
        )
        self._ensure_consumer_groups()

    def _ensure_consumer_groups(self) -> None:
        """Create consumer groups for all known worker types."""
        from orchestration.cog.state_schema import DSWWorkerType
        for worker_type in DSWWorkerType:
            stream_key = f"{self.QUEUE_PREFIX}{worker_type.value}"
            try:
                self.redis.xgroup_create(
                    stream_key, self.CONSUMER_GROUP,
                    id="0", mkstream=True
                )
            except redis.exceptions.ResponseError:
                pass  # Group already exists

    def publish(self, envelope: TaskEnvelope) -> str:
        """
        Publish a TaskEnvelope to the appropriate worker queue.

        Uses Redis XADD with MAXLEN to bound queue size.
        Returns a unique job_id for tracking.

        Args:
            envelope: TaskEnvelope from COG fan_out_node

        Returns:
            job_id: UUID string for result polling
        """
        job_id     = f"job-{uuid.uuid4().hex}"
        stream_key = f"{self.QUEUE_PREFIX}{envelope.worker_type}"

        # Add to stream
        fields        = envelope.to_redis_fields()
        fields["job_id"] = job_id
        stream_msg_id = self.redis.xadd(stream_key, fields, maxlen=10000)

        # Create job tracking record
        job_record = {
            "job_id":      job_id,
            "task_id":     envelope.task_id,
            "worker_type": envelope.worker_type,
            "stream_id":   stream_msg_id,
            "status":      TaskStatus.QUEUED.value,
            "retry_count": 0,
            "created_at":  datetime.utcnow().isoformat(),
            "trace_id":    envelope.trace_id,
            "session_id":  envelope.session_id,
        }
        self.redis.hset(f"{self.JOB_PREFIX}{job_id}", mapping=job_record)
        self.redis.expire(f"{self.JOB_PREFIX}{job_id}", 3600)  # 1h TTL

        return job_id

    def claim_next_job(
        self,
        worker_type: str,
        consumer_id: str,
        block_ms:    int = 1000,
    ) -> Optional[tuple[str, TaskEnvelope]]:
        """
        Claim next available job from the queue (worker call).

        Uses Redis XREADGROUP for at-least-once delivery.
        Worker must call ack_job() after completion.

        Args:
            worker_type:  Worker type string (e.g. "geological_expert")
            consumer_id:  Unique consumer ID (e.g. "worker-geo-1")
            block_ms:     How long to block waiting for messages

        Returns:
            Tuple of (job_id, TaskEnvelope) or None if queue empty
        """
        stream_key = f"{self.QUEUE_PREFIX}{worker_type}"

        messages = self.redis.xreadgroup(
            groupname  = self.CONSUMER_GROUP,
            consumername = consumer_id,
            streams    = {stream_key: ">"},
            count      = 1,
            block      = block_ms,
        )

        if not messages:
            return None

        _, msg_list = messages[0]
        if not msg_list:
            return None

        stream_msg_id, fields = msg_list[0]
        job_id   = fields.pop("job_id", None)
        envelope = TaskEnvelope.from_redis_fields(fields)

        # Mark as claimed
        job_key = f"{self.JOB_PREFIX}{job_id}"
        self.redis.hset(job_key, mapping={
            "status":     TaskStatus.CLAIMED.value,
            "claimed_at": datetime.utcnow().isoformat(),
            "stream_id":  stream_msg_id,
        })

        return job_id, envelope

    def complete_job(
        self,
        job_id:        str,
        worker_type:   str,
        result_data:   Dict[str, Any],
        execution_ms:  float,
        stream_msg_id: str,
    ) -> None:
        """
        Mark job as completed and store result.

        Called by DTB worker after DSW agent finishes successfully.
        Calls XACK to remove from PEL (Pending Entry List).
        """
        result_payload = {
            "job_id":       job_id,
            "status":       TaskStatus.COMPLETED.value,
            "result_data":  json.dumps(result_data),
            "execution_ms": str(execution_ms),
            "completed_at": datetime.utcnow().isoformat(),
        }
        self.redis.hset(f"{self.RESULT_PREFIX}{job_id}", mapping=result_payload)
        self.redis.expire(f"{self.RESULT_PREFIX}{job_id}", 3600)

        self.redis.hset(f"{self.JOB_PREFIX}{job_id}", mapping={
            "status":       TaskStatus.COMPLETED.value,
            "completed_at": datetime.utcnow().isoformat(),
        })

        # Acknowledge to remove from PEL
        stream_key = f"{self.QUEUE_PREFIX}{worker_type}"
        self.redis.xack(stream_key, self.CONSUMER_GROUP, stream_msg_id)

    def fail_job(
        self,
        job_id:        str,
        worker_type:   str,
        error_msg:     str,
        stream_msg_id: str,
        retry:         bool = True,
    ) -> bool:
        """
        Mark job as failed; apply retry or route to DLQ.

        Args:
            job_id:        Job identifier
            worker_type:   DSW type for queue routing
            error_msg:     Error description from DSW
            stream_msg_id: Redis stream message ID for XACK
            retry:         Whether to attempt retry

        Returns:
            True if will retry, False if sent to DLQ
        """
        job_key     = f"{self.JOB_PREFIX}{job_id}"
        retry_count = int(self.redis.hget(job_key, "retry_count") or 0)
        max_retries = int(self.redis.hget(job_key, "max_retries") or 3)

        # Always ACK to prevent re-delivery by the same consumer
        stream_key = f"{self.QUEUE_PREFIX}{worker_type}"
        self.redis.xack(stream_key, self.CONSUMER_GROUP, stream_msg_id)

        if retry and retry_count < max_retries:
            backoff_s   = min(2 ** (retry_count + 1), 60)
            retry_count += 1

            self.redis.hset(job_key, mapping={
                "status":      TaskStatus.RETRYING.value,
                "retry_count": retry_count,
                "error_msg":   error_msg,
            })

            # Re-enqueue after backoff (simplified — in prod use delayed queue)
            time.sleep(backoff_s)
            task_fields = self.redis.hgetall(job_key)
            # Re-publish to stream (would reconstruct envelope in production)
            self.redis.xadd(stream_key, task_fields, maxlen=10000)
            return True

        else:
            # Send to Dead-Letter Queue
            dlq_key = f"{self.DLQ_PREFIX}{worker_type}"
            dlq_fields = {
                "job_id":       job_id,
                "worker_type":  worker_type,
                "error_msg":    error_msg,
                "retry_count":  str(retry_count),
                "dlq_at":       datetime.utcnow().isoformat(),
            }
            self.redis.xadd(dlq_key, dlq_fields, maxlen=1000)

            self.redis.hset(job_key, mapping={
                "status":       TaskStatus.DLQ.value,
                "completed_at": datetime.utcnow().isoformat(),
                "error_msg":    error_msg,
            })

            # Publish CRITICAL fault to Kafka for THA
            self._publish_dlq_fault(job_id, worker_type, error_msg, retry_count)
            return False

    def get_job_result(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Poll for job result.

        Returns:
            Dict with status, result_data, execution_ms, error if available.
            None if job not yet in result store.
        """
        result_key = f"{self.RESULT_PREFIX}{job_id}"
        raw = self.redis.hgetall(result_key)
        if not raw:
            # Check job status
            job_raw = self.redis.hgetall(f"{self.JOB_PREFIX}{job_id}")
            if job_raw:
                return {"status": job_raw.get("status", "unknown"), "job_id": job_id}
            return None

        result_data = None
        if raw.get("result_data"):
            try:
                result_data = json.loads(raw["result_data"])
            except json.JSONDecodeError:
                result_data = {}

        return {
            "job_id":       job_id,
            "status":       raw.get("status", TaskStatus.COMPLETED.value),
            "result_data":  result_data,
            "execution_ms": float(raw.get("execution_ms", 0)),
            "completed_at": raw.get("completed_at"),
        }

    def _publish_dlq_fault(
        self,
        job_id:       str,
        worker_type:  str,
        error_msg:    str,
        retry_count:  int,
    ) -> None:
        """
        Publish a CRITICAL fault event to Kafka `agent-faults` topic.
        THA subscribes to this topic and triggers healing.
        """
        try:
            from kafka import KafkaProducer
            producer = KafkaProducer(
                bootstrap_servers=["localhost:9092"],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            fault_event = {
                "event_type":   "job_dlq",
                "severity":     "critical",
                "job_id":       job_id,
                "worker_type":  worker_type,
                "error_msg":    error_msg,
                "retry_count":  retry_count,
                "timestamp":    datetime.utcnow().isoformat(),
            }
            producer.send("agent-faults", fault_event)
            producer.flush()
        except Exception:
            pass  # Best-effort Kafka publish
