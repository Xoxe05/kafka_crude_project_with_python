from src.utils.logging import logger
from src.core.config import settings
from confluent_kafka import Consumer


class Consumers:
    consumer = None

    def __init__(self):
        pass

    def create_consumers(self, topic=None):
        try:

            conf = {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "security.protocol": settings.kafka_security_protocol,
                "sasl.mechanism": settings.kafka_sasl_mechanism,
                "sasl.username": settings.kafka_consumer_username,
                "sasl.password": settings.kafka_consumer_password,
                "group.id": "secure-consumer-group",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
                "auto.commit.interval.ms": 5000,
                "max.poll.interval.ms": 300000,
                "session.timeout.ms": 45000,
                "heartbeat.interval.ms": 15000,
            }

            self.consumer = Consumer(conf)

        except Exception as e:
            logger.exception("Couldn't create the consumer...\n", e)
            raise

    def subscribe_topic(self, topic: list[str] = None):

        if topic:
            self.consumer.subscribe(topic)
            logger.info(f"Subscribed to topic/s: {topic}")

        else:
            logger.error(f"No topic/s given to Subscribe to!!!")

    def consume_message(self):
        return self.consumer.poll(timeout=1)

    def cleanup(self):

        self.consumer.close()
