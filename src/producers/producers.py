from src.utils.logging import logger
from src.core.config import settings
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer


class Producers:
    producer = None
    messages_sent = 0
    messages_failed = 0

    def __init__(self):
        self.string_serializer = StringSerializer("utf_8")
        # self.params = params
        # self.logger = params["logger"]

    def create_producers(self):
        try:

            conf = {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "sasl.mechanism": settings.kafka_sasl_mechanism,
                "security.protocol": settings.kafka_security_protocol,
                "sasl.username": settings.kafka_producer_username,
                "sasl.password": settings.kafka_producer_password,
                "enable.idempotence": True,
                "acks": "all",
                "retries": 5,
                "delivery.timeout.ms": 30000,
                "request.timeout.ms": 25000,
                "retry.backoff.ms": 1000,
                "socket.timeout.ms": 10000,
                "compression.type": "snappy",
                "linger.ms": 20,
                "batch.num.messages": 1000,
            }

            self.producer = Producer(conf)

        except Exception as e:
            logger.exception("Couldn't create the producer...\n", e)
            raise

    def callback(self, err, msg):
        if err:
            logger.error(f"Message delivery failed: {err}")
            self.messages_failed += 1
        else:
            self.messages_sent += 1
            if self.messages_sent % 100 == 0: 
                logger.info(
                    f"Messages sent: {self.messages_sent}, failed: {self.messages_failed}"
                )

    def produce_message(self, key, data, topic=None):
        try:
            self.producer.produce(
                topic=topic, key=key, value=data, on_delivery=self.callback
            )
            return True

        except Exception as e:
            logger.error(f"Error producing message: {e}")
            return False

    def poll(self):
        self.producer.poll(0)

    def cleanup(self):
        logger.info("Cleaning up producer...")
        try:
            self.producer.flush(timeout=10)
            logger.info(
                f"Final stats - Sent: {self.messages_sent}, Failed: {self.messages_failed}"
            )

        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
        finally:
            logger.info("Producer stopped")
