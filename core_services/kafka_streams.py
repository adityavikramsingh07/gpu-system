"""
core_services/kafka_streams.py
================================
Kafka Topic & Stream Manager

Initializes Apache Kafka topics, manages producer/consumer lifecycle,
and provides type-safe topic constants used across all agents.

Topic architecture:
  sys-events       → General system lifecycle events (planning, dispatch, completion)
  agent-faults     → DSW errors, timeouts, MCP failures — THA primary subscription
  tha-remediations → THA healing directives pushed back to COG
  data-updates     → MCP data freshness and cache invalidation signals
  audit-trail      → Immutable audit log (write-only; consumed by SIEM)

Producer: All agents (COG nodes, DSW workers, THA)
Consumer groups:
  tha-consumer-group  → THA subscribes to sys-events + agent-faults
  cog-consumer-group  → COG subscribes to tha-remediations
  audit-consumer-group → Audit service consumes audit-trail
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


# ============================================================================
# KAFKA TOPIC CONSTANTS
# ============================================================================

class KafkaTopic(str, Enum):
    """Canonical Kafka topic names. Use these constants everywhere."""
    SYS_EVENTS      = "sys-events"
    AGENT_FAULTS    = "agent-faults"
    THA_REMEDIATION = "tha-remediations"
    DATA_UPDATES    = "data-updates"
    AUDIT_TRAIL     = "audit-trail"


# Topic configurations (partitions, replication, retention)
TOPIC_CONFIGS: Dict[str, Dict[str, Any]] = {
    KafkaTopic.SYS_EVENTS: {
        "num_partitions":   8,
        "replication_factor": 3,
        "retention_ms":     86_400_000,   # 24h
        "cleanup_policy":   "delete",
    },
    KafkaTopic.AGENT_FAULTS: {
        "num_partitions":   4,
        "replication_factor": 3,
        "retention_ms":     604_800_000,  # 7 days (for post-mortem analysis)
        "cleanup_policy":   "delete",
    },
    KafkaTopic.THA_REMEDIATION: {
        "num_partitions":   2,
        "replication_factor": 3,
        "retention_ms":     3_600_000,    # 1h (fast-moving healing events)
        "cleanup_policy":   "delete",
    },
    KafkaTopic.DATA_UPDATES: {
        "num_partitions":   4,
        "replication_factor": 2,
        "retention_ms":     43_200_000,   # 12h
        "cleanup_policy":   "delete",
    },
    KafkaTopic.AUDIT_TRAIL: {
        "num_partitions":   2,
        "replication_factor": 3,
        "retention_ms":     -1,            # Infinite retention (compliance)
        "cleanup_policy":   "compact",
    },
}


# ============================================================================
# KAFKA STREAM MANAGER
# ============================================================================

@dataclass
class KafkaConfig:
    """Kafka broker configuration."""
    bootstrap_servers: List[str] = None
    security_protocol: str       = "PLAINTEXT"
    sasl_mechanism:    str       = ""
    sasl_username:     str       = ""
    sasl_password:     str       = ""
    ssl_cafile:        str       = ""

    def __post_init__(self):
        if self.bootstrap_servers is None:
            self.bootstrap_servers = ["localhost:9092"]

    def producer_config(self) -> Dict[str, Any]:
        """Return kafka-python KafkaProducer config dict."""
        cfg = {
            "bootstrap_servers":  self.bootstrap_servers,
            "value_serializer":   lambda v: json.dumps(v).encode("utf-8"),
            "key_serializer":     lambda k: k.encode("utf-8") if k else None,
            "acks":               "all",       # Wait for all ISR replicas
            "retries":            5,
            "retry_backoff_ms":   500,
            "compression_type":   "gzip",
            "linger_ms":          10,          # Micro-batching
        }
        if self.security_protocol != "PLAINTEXT":
            cfg["security_protocol"] = self.security_protocol
        return cfg

    def consumer_config(self, group_id: str) -> Dict[str, Any]:
        """Return kafka-python KafkaConsumer config dict."""
        return {
            "bootstrap_servers":      self.bootstrap_servers,
            "group_id":               group_id,
            "value_deserializer":     lambda v: json.loads(v.decode("utf-8")),
            "auto_offset_reset":      "earliest",
            "enable_auto_commit":     False,   # Manual commit for at-least-once
            "max_poll_records":       50,
            "session_timeout_ms":     30_000,
            "heartbeat_interval_ms":  10_000,
        }


class KafkaStreamManager:
    """
    Manages Kafka producer/consumer lifecycle and topic initialization.

    Usage:
        manager = KafkaStreamManager(config)
        manager.initialize_topics()
        manager.publish(KafkaTopic.SYS_EVENTS, {"event": "planning_complete"})

        # For consumers (blocking):
        manager.consume(
            topic=KafkaTopic.AGENT_FAULTS,
            group_id="tha-consumer-group",
            handler=my_fault_handler,
        )
    """

    def __init__(self, config: Optional[KafkaConfig] = None):
        self.config   = config or KafkaConfig()
        self._producer = None

    def initialize_topics(self) -> None:
        """
        Idempotently create all system Kafka topics with configured settings.
        Should be called during system bootstrap.
        """
        try:
            from kafka.admin import KafkaAdminClient, NewTopic

            admin = KafkaAdminClient(
                bootstrap_servers=self.config.bootstrap_servers,
                client_id="gpu-supply-chain-admin",
            )

            existing = set(admin.list_topics())
            topics_to_create = []

            for topic_enum, cfg in TOPIC_CONFIGS.items():
                if topic_enum.value not in existing:
                    topics_to_create.append(
                        NewTopic(
                            name               = topic_enum.value,
                            num_partitions     = cfg["num_partitions"],
                            replication_factor = cfg["replication_factor"],
                            topic_configs      = {
                                "retention.ms":     str(cfg["retention_ms"]),
                                "cleanup.policy":   cfg["cleanup_policy"],
                            }
                        )
                    )

            if topics_to_create:
                admin.create_topics(topics_to_create)
                logger.info(f"Created {len(topics_to_create)} Kafka topics")
            else:
                logger.info("All Kafka topics already exist")

            admin.close()

        except Exception as e:
            logger.warning(f"Kafka topic initialization failed (will retry): {e}")

    def publish(
        self,
        topic:   KafkaTopic,
        payload: Dict[str, Any],
        key:     Optional[str] = None,
    ) -> None:
        """
        Publish a JSON payload to a Kafka topic.

        Args:
            topic:   KafkaTopic enum value
            payload: Dict to serialize as JSON
            key:     Optional partition key (e.g., session_id for ordering)
        """
        try:
            from kafka import KafkaProducer
            if self._producer is None:
                self._producer = KafkaProducer(**self.config.producer_config())

            self._producer.send(
                topic.value,
                value = payload,
                key   = key,
            )
        except Exception as e:
            logger.error(f"Failed to publish to {topic.value}: {e}")

    def consume(
        self,
        topic:    KafkaTopic,
        group_id: str,
        handler:  Callable[[Dict[str, Any]], None],
        timeout_ms: int = 5000,
        max_messages: Optional[int] = None,
    ) -> None:
        """
        Blocking consumer loop with manual offset commit.

        Args:
            topic:        Topic to consume from
            group_id:     Consumer group ID
            handler:      Callback function for each message
            timeout_ms:   Poll timeout in milliseconds
            max_messages: Stop after N messages (None = run forever)
        """
        from kafka import KafkaConsumer

        consumer = KafkaConsumer(
            topic.value,
            **self.config.consumer_config(group_id),
        )

        count = 0
        try:
            while True:
                msgs = consumer.poll(timeout_ms=timeout_ms)
                for tp, records in msgs.items():
                    for record in records:
                        try:
                            handler(record.value)
                            consumer.commit()
                            count += 1
                        except Exception as e:
                            logger.error(f"Handler error on {topic.value}: {e}")

                if max_messages and count >= max_messages:
                    break

        finally:
            consumer.close()

    def flush(self) -> None:
        """Flush pending producer messages."""
        if self._producer:
            self._producer.flush()

    def close(self) -> None:
        """Gracefully close producer."""
        if self._producer:
            self._producer.close()
            self._producer = None


# ============================================================================
# SINGLETON — shared stream manager instance
# ============================================================================

_stream_manager: Optional[KafkaStreamManager] = None


def get_stream_manager() -> KafkaStreamManager:
    """Get or create the global KafkaStreamManager singleton."""
    global _stream_manager
    if _stream_manager is None:
        _stream_manager = KafkaStreamManager()
    return _stream_manager
