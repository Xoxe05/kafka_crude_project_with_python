import os
from confluent_kafka.admin import AdminClient, NewTopic
from src.core.config import settings
from src.utils.logging import logger


class KafkaAdmin:
    def __init__(self):
        self.conf = {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "sasl.mechanism": settings.kafka_sasl_mechanism,
            "security.protocol": settings.kafka_security_protocol,
            "sasl.username": settings.kafka_client_username,
            "sasl.password": settings.kafka_client_password,
        }

        self.admin_client = AdminClient(self.conf)

        # self.params = params
        # self.logger = params["logger"]

    def create_topic(
        self,
        topic_name: str,
        num_partitions: int = 3,
        replication_factor: int = 2,
        min_insync_replicas: int = None,
    ):

        if min_insync_replicas is None:
            min_insync_replicas = (
                max(1, replication_factor - 1) if replication_factor > 1 else 1
            )

        if min_insync_replicas >= replication_factor:
            logger.warning(
                f"min.insync.replicas ({min_insync_replicas}) should be less than replication_factor ({replication_factor})"
            )
            min_insync_replicas = max(1, replication_factor - 1)

        topic_config = {
            "cleanup.policy": "delete",
            "retention.ms": "604800000",
            "segment.ms": "86400000",
            "compression.type": "snappy",
            "min.insync.replicas": str(min_insync_replicas),
            "max.message.bytes": "1000000",
            "flush.messages": "10000",
            "flush.ms": "1000",
        }

        topic = NewTopic(
            topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            config=topic_config,
        )

        futures = self.admin_client.create_topics([topic])

        for topic_name, future in futures.items():
            try:
                future.result()
                logger.info(f"Topic {topic_name} created successfully")
            except Exception as e:
                logger.info(f"Failed to create topic {topic_name}: {e}")

    def delete_topic(self, topic_name: str):
        futures = self.admin_client.delete_topics([topic_name])

        for topic_name, future in futures.items():
            try:
                future.result()
                logger.info(f"Topic {topic_name} deleted successfully")
            except Exception as e:
                logger.info(f"Failed to delete topic {topic_name}: {e}")

    def list_topics(self):
        metadata = self.admin_client.list_topics(timeout=10)
        return list(metadata.topics.keys())
