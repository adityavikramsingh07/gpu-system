"""
Enterprise Job Management Service (EJMS)

Handles async execution of specialist agent jobs via Redis queue.
Manages job lifecycle, timeout handling, result aggregation, and retries.

Flow:
  1. dispatcher_node calls EJMS.submit_job(specialist_type, task)
  2. EJMS creates Job, stores in Redis, queues on worker
  3. Worker picks up job, fetches ACS credentials, runs specialist agent
  4. specialist_agent yields result or error back to job
  5. MRA calls EJMS.wait_all_jobs(job_ids, timeout) to fan-in
  6. Returns partial/full results depending on completion status
"""

import json
import uuid
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import time
from enum import Enum

import redis
from pydantic import BaseModel, Field


# ============================================================================
# ENUMS & DATA MODELS
# ============================================================================

class JobStatus(str, Enum):
    """Job lifecycle states."""
    PENDING = "pending"        # Created, awaiting worker pickup
    EXECUTING = "executing"    # Worker running specialist agent
    COMPLETED = "completed"    # Finished with result
    FAILED = "failed"          # Error occurred (not retryable)
    TIMEOUT = "timeout"        # Exceeded deadline
    CANCELLED = "cancelled"    # Manual cancellation


class SpecialistType(str, Enum):
    """Available specialist agent types."""
    GEOLOGICAL_SCOUT = "geological_scout"
    CHEMICAL_INFRA = "chemical_infra"
    LOGISTICS_ANALYST = "logistics_analyst"
    FAB_LOCATOR = "fab_locator"
    WORKFORCE_ANALYZER = "workforce_analyzer"
    THERMAL_SPECIALIST = "thermal_specialist"


@dataclass
class JobTask:
    """Task assigned to a specialist agent."""
    task_id: str
    specialist_type: SpecialistType
    query: str
    context: Dict[str, Any]  # Region, material, etc.
    required_permissions: List[str]  # MCP tool permissions needed
    
    def to_dict(self) -> Dict:
        return {
            "task_id": self.task_id,
            "specialist_type": self.specialist_type.value,
            "query": self.query,
            "context": self.context,
            "required_permissions": self.required_permissions
        }


class JobResult(BaseModel):
    """Result returned by completed job."""
    job_id: str
    task_id: str
    specialist_type: str
    status: JobStatus
    result_data: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    execution_time_ms: Optional[float] = None
    completed_at: Optional[datetime] = None
    retry_count: int = 0


class EJMSJobDefinition(BaseModel):
    """Internal job definition stored in Redis."""
    job_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    task: JobTask
    status: JobStatus = JobStatus.PENDING
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    deadline: datetime  # Absolute timeout
    timeout_seconds: int  # Original timeout value
    retry_count: int = 0
    max_retries: int = 3
    result: Optional[JobResult] = None
    trace_id: str
    job_parent_id: Optional[str] = None  # Links to MRA request


# ============================================================================
# EJMS SERVICE
# ============================================================================

class EnterpriseJobManagementService:
    """
    Centralized job dispatcher & result aggregator.
    
    Responsibilities:
      - Submit jobs to Redis queue
      - Poll job status / wait for completion
      - Handle timeouts (escalate to ECA)
      - Retry failed jobs with exponential backoff
      - Aggregate results for fan-in
    """

    def __init__(
        self,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 0,
        default_timeout_seconds: int = 30
    ):
        """
        Initialize EJMS.
        
        Args:
            redis_host: Redis server host
            redis_port: Redis server port
            redis_db: Redis database number
            default_timeout_seconds: Default job timeout
        """
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            decode_responses=True
        )
        self.default_timeout_seconds = default_timeout_seconds
        self.job_prefix = "job:"
        self.queue_prefix = "queue:"
        self.result_prefix = "result:"

    def submit_job(
        self,
        specialist_type: SpecialistType,
        query: str,
        context: Dict[str, Any],
        required_permissions: List[str],
        timeout_seconds: Optional[int] = None,
        trace_id: Optional[str] = None,
        parent_request_id: Optional[str] = None
    ) -> str:
        """
        Submit a new job to the EJMS queue.
        
        Args:
            specialist_type: Type of specialist to execute
            query: Query/instruction for specialist
            context: Additional context (region, material, etc.)
            required_permissions: List of MCP permissions needed
            timeout_seconds: Job timeout (uses default if None)
            trace_id: Trace ID for observability
            parent_request_id: Links to parent MRA request
        
        Returns:
            job_id: Unique identifier for tracking
        
        Example:
            job_id = ejms.submit_job(
                specialist_type=SpecialistType.GEOLOGICAL_SCOUT,
                query="Find HPQ deposits in Tamil Nadu",
                context={"region": "Tamil Nadu", "material": "HPQ"},
                required_permissions=["read:deposits", "read:mining_leases"],
                timeout_seconds=30,
                trace_id="trace_abc123"
            )
        """
        timeout_seconds = timeout_seconds or self.default_timeout_seconds
        deadline = datetime.utcnow() + timedelta(seconds=timeout_seconds)
        
        # Create job task
        task = JobTask(
            task_id=str(uuid.uuid4()),
            specialist_type=specialist_type,
            query=query,
            context=context,
            required_permissions=required_permissions
        )
        
        # Create job definition
        job = EJMSJobDefinition(
            task=task,
            deadline=deadline,
            timeout_seconds=timeout_seconds,
            trace_id=trace_id or str(uuid.uuid4()),
            job_parent_id=parent_request_id
        )
        
        # Store job in Redis
        job_key = f"{self.job_prefix}{job.job_id}"
        job_data = {
            "job_id": job.job_id,
            "task": json.dumps(task.to_dict()),
            "status": job.status.value,
            "created_at": job.created_at.isoformat(),
            "deadline": job.deadline.isoformat(),
            "timeout_seconds": job.timeout_seconds,
            "trace_id": job.trace_id,
            "parent_id": parent_request_id or "N/A",
            "retry_count": 0,
            "max_retries": job.max_retries
        }
        self.redis_client.hset(job_key, mapping=job_data)
        self.redis_client.expire(job_key, timeout_seconds + 300)  # TTL: timeout + 5min
        
        # Enqueue for worker processing
        queue_key = f"{self.queue_prefix}{specialist_type.value}"
        self.redis_client.lpush(queue_key, job.job_id)
        
        return job.job_id

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """
        Retrieve current status of a job.
        
        Returns:
            Dictionary with:
              - job_id
              - status: (pending|executing|completed|failed|timeout|cancelled)
              - result_data: (if completed successfully)
              - error_message: (if failed)
              - execution_time_ms: (if completed)
              - retry_count
              - deadline
        """
        job_key = f"{self.job_prefix}{job_id}"
        job_data = self.redis_client.hgetall(job_key)
        
        if not job_data:
            return {"error": f"Job {job_id} not found"}
        
        # Check for timeout
        deadline = datetime.fromisoformat(job_data["deadline"])
        if datetime.utcnow() > deadline and job_data["status"] == JobStatus.EXECUTING.value:
            # Mark as timeout
            self.redis_client.hset(job_key, "status", JobStatus.TIMEOUT.value)
            job_data["status"] = JobStatus.TIMEOUT.value
        
        # Retrieve result if completed
        result_data = None
        error_msg = None
        execution_time = None
        
        if job_data["status"] in [JobStatus.COMPLETED.value, JobStatus.FAILED.value]:
            result_key = f"{self.result_prefix}{job_id}"
            result_json = self.redis_client.get(result_key)
            if result_json:
                result = json.loads(result_json)
                result_data = result.get("result_data")
                error_msg = result.get("error_message")
                execution_time = result.get("execution_time_ms")
        
        return {
            "job_id": job_id,
            "status": job_data["status"],
            "result_data": result_data,
            "error_message": error_msg,
            "execution_time_ms": execution_time,
            "retry_count": int(job_data.get("retry_count", 0)),
            "deadline": job_data["deadline"],
            "trace_id": job_data.get("trace_id")
        }

    def wait_all_jobs(
        self,
        job_ids: List[str],
        timeout_seconds: Optional[int] = None,
        poll_interval_ms: int = 100
    ) -> Tuple[Dict[str, JobResult], bool]:
        """
        Fan-in: Wait for all jobs to complete or timeout.
        
        Args:
            job_ids: List of job IDs to wait for
            timeout_seconds: Max time to wait (uses longest job deadline if None)
            poll_interval_ms: How often to check job status
        
        Returns:
            Tuple of:
              - Dictionary mapping job_id -> JobResult
              - Boolean: True if all completed, False if timeout
        
        Example:
            results, all_complete = ejms.wait_all_jobs(
                job_ids=["job_1", "job_2", "job_3"],
                timeout_seconds=60
            )
            if all_complete:
                print("All results ready")
            else:
                print(f"Timeout: {len([r for r in results if r.status != 'completed'])} incomplete")
        """
        deadline = datetime.utcnow() + timedelta(
            seconds=timeout_seconds or self.default_timeout_seconds
        )
        
        results = {}
        poll_count = 0
        
        while True:
            poll_count += 1
            all_complete = True
            
            for job_id in job_ids:
                if job_id in results:
                    continue  # Already have result
                
                status = self.get_job_status(job_id)
                job_status = status.get("status")
                
                if job_status in [
                    JobStatus.COMPLETED.value,
                    JobStatus.FAILED.value,
                    JobStatus.TIMEOUT.value,
                    JobStatus.CANCELLED.value
                ]:
                    # Job finished
                    results[job_id] = JobResult(
                        job_id=job_id,
                        task_id="",  # Would retrieve from job data
                        specialist_type="",
                        status=JobStatus(job_status),
                        result_data=status.get("result_data"),
                        error_message=status.get("error_message"),
                        execution_time_ms=status.get("execution_time_ms"),
                        retry_count=status.get("retry_count", 0)
                    )
                else:
                    all_complete = False
            
            # Check conditions
            if all_complete:
                return results, True
            
            if datetime.utcnow() > deadline:
                return results, False  # Timeout
            
            # Sleep before next poll
            time.sleep(poll_interval_ms / 1000.0)

    def complete_job(
        self,
        job_id: str,
        result_data: Dict[str, Any],
        execution_time_ms: float
    ):
        """
        Mark job as completed with results (called by worker).
        
        Args:
            job_id: Job ID
            result_data: Dictionary of results from specialist agent
            execution_time_ms: How long execution took
        """
        job_key = f"{self.job_prefix}{job_id}"
        
        # Update job status
        self.redis_client.hset(
            job_key,
            mapping={
                "status": JobStatus.COMPLETED.value,
                "completed_at": datetime.utcnow().isoformat()
            }
        )
        
        # Store result
        result_key = f"{self.result_prefix}{job_id}"
        result_data_json = json.dumps({
            "result_data": result_data,
            "execution_time_ms": execution_time_ms,
            "completed_at": datetime.utcnow().isoformat()
        })
        self.redis_client.set(result_key, result_data_json)

    def fail_job(
        self,
        job_id: str,
        error_message: str,
        execution_time_ms: float,
        retryable: bool = True
    ):
        """
        Mark job as failed (called by worker on exception).
        
        Args:
            job_id: Job ID
            error_message: Error description
            execution_time_ms: Execution time before failure
            retryable: Whether to retry
        """
        job_key = f"{self.job_prefix}{job_id}"
        
        # Check if we should retry
        retry_count = int(self.redis_client.hget(job_key, "retry_count") or 0)
        max_retries = int(self.redis_client.hget(job_key, "max_retries") or 3)
        
        if retryable and retry_count < max_retries:
            # Requeue with exponential backoff
            retry_count += 1
            backoff_seconds = min(2 ** retry_count, 60)  # Cap at 60s
            
            self.redis_client.hset(
                job_key,
                mapping={
                    "status": JobStatus.PENDING.value,
                    "retry_count": retry_count,
                    "next_retry_at": (
                        datetime.utcnow() + timedelta(seconds=backoff_seconds)
                    ).isoformat()
                }
            )
            
            # Re-enqueue
            task_data = json.loads(self.redis_client.hget(job_key, "task"))
            queue_key = f"{self.queue_prefix}{task_data['specialist_type']}"
            self.redis_client.lpush(queue_key, job_id)
        else:
            # Mark as failed permanently
            self.redis_client.hset(
                job_key,
                mapping={
                    "status": JobStatus.FAILED.value,
                    "completed_at": datetime.utcnow().isoformat()
                }
            )
        
        # Store error result
        result_key = f"{self.result_prefix}{job_id}"
        result_data_json = json.dumps({
            "error_message": error_message,
            "execution_time_ms": execution_time_ms,
            "retry_count": retry_count,
            "completed_at": datetime.utcnow().isoformat(),
            "will_retry": retryable and retry_count < max_retries
        })
        self.redis_client.set(result_key, result_data_json)

    def get_job_for_queue(self, specialist_type: SpecialistType) -> Optional[str]:
        """
        Get next job from queue for a specialist type (worker call).
        
        Returns:
            job_id or None if queue empty
        """
        queue_key = f"{self.queue_prefix}{specialist_type.value}"
        job_id = self.redis_client.rpop(queue_key)
        
        if job_id:
            # Mark as executing
            job_key = f"{self.job_prefix}{job_id}"
            self.redis_client.hset(
                job_key,
                mapping={
                    "status": JobStatus.EXECUTING.value,
                    "started_at": datetime.utcnow().isoformat()
                }
            )
        
        return job_id

    def get_job_task(self, job_id: str) -> Optional[JobTask]:
        """
        Retrieve full job task (worker call).
        
        Returns:
            JobTask or None if not found
        """
        job_key = f"{self.job_prefix}{job_id}"
        task_json = self.redis_client.hget(job_key, "task")
        
        if not task_json:
            return None
        
        task_data = json.loads(task_json)
        return JobTask(
            task_id=task_data["task_id"],
            specialist_type=SpecialistType(task_data["specialist_type"]),
            query=task_data["query"],
            context=task_data["context"],
            required_permissions=task_data["required_permissions"]
        )

    def list_job_queue(self, specialist_type: SpecialistType) -> List[str]:
        """
        List all pending jobs for a specialist type.
        
        Returns:
            List of job IDs in queue order
        """
        queue_key = f"{self.queue_prefix}{specialist_type.value}"
        job_ids = self.redis_client.lrange(queue_key, 0, -1)
        return job_ids


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    # Initialize EJMS
    ejms = EnterpriseJobManagementService(
        redis_host="localhost",
        redis_port=6379,
        default_timeout_seconds=30
    )
    
    print("=== EJMS Usage Example ===\n")
    
    # Submit 3 jobs (simulating dispatcher fan-out)
    job_ids = []
    
    job_id_1 = ejms.submit_job(
        specialist_type=SpecialistType.GEOLOGICAL_SCOUT,
        query="Find HPQ deposits in Tamil Nadu",
        context={"region": "Tamil Nadu", "material": "HPQ"},
        required_permissions=["read:deposits", "read:mining_leases"],
        timeout_seconds=30,
        trace_id="trace_001",
        parent_request_id="req_hpq_copper"
    )
    job_ids.append(job_id_1)
    print(f"✓ Submitted job: {job_id_1}")
    
    job_id_2 = ejms.submit_job(
        specialist_type=SpecialistType.LOGISTICS_ANALYST,
        query="Find copper foil suppliers in South India",
        context={"region": "South India", "material": "Copper Foil"},
        required_permissions=["read:trade_data", "read:suppliers"],
        timeout_seconds=45,
        trace_id="trace_001",
        parent_request_id="req_hpq_copper"
    )
    job_ids.append(job_id_2)
    print(f"✓ Submitted job: {job_id_2}\n")
    
    # Check status (simulating polling)
    for job_id in job_ids:
        status = ejms.get_job_status(job_id)
        print(f"Job {job_id}: status={status['status']}, trace_id={status['trace_id']}")
    
    print("\n✓ EJMS initialized and ready for dispatcher integration")
