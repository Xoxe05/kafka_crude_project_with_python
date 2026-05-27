from src.consumers.consumers import Consumers
from src.utils.kafka_utils import KafkaAdmin
from src.utils.es_utils import ElasticSearch

from src.utils.logging import logger
import traceback

from confluent_kafka import KafkaError, KafkaException

import json


class WikiMediaConsumer:

    consumer = Consumers
    kafka_utils = KafkaAdmin

    def __init__(self, params):

        self.consumer = params["consumer"]
        self.kafka_utils = params["kafka_utils"]

        self.es_client = ElasticSearch(params=params)
        self.batch_buffer = []
        self.batch_size = 1000
        self.shutdown_event = params["shutdown_event"]

    def consume_data(self, topic=["WikiMedia-Changes"], index_name="wikimedia-changes"):
        logger.info("Starting OpenSearch consumer...")
        logger.info(f"Kafka topic: {topic}")
        logger.info(f"OpenSearch index: {index_name}")

        message_count = 0

        try:
            if topic:
                self.consumer.subscribe_topic(topic=[topic])
                logger.info(f"Subscribed to topic/s: {topic}")

            while not self.shutdown_event.is_set():
                try:
                    msg = self.consumer.consume_message()

                    if msg is None:
                        continue

                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            logger.debug(
                                f"End of partition reached {msg.topic()}/{msg.partition()}"
                            )
                        else:
                            logger.error(f"Kafka error: {msg.error()}")
                        continue

                    if self.shutdown_event.is_set():
                        logger.info(
                            f"Shutdown detected during message processing. Processed {message_count} messages."
                        )
                        break

                    self.process_message(msg, index_name=index_name)
                    message_count += 1

                    if message_count % 100 == 0:
                        logger.info(f"Processed {message_count} messages")

                except KafkaException as e:
                    if not self.shutdown_event.is_set():
                        logger.error(f"Kafka exception: {e}")
                        break 
                    else:
                        logger.info("Kafka exception during shutdown, exiting...")
                        break

                except Exception as e:
                    if not self.shutdown_event.is_set():
                        logger.error(f"Error during data consumption: {e}")
                        logger.exception(traceback.format_exc())
                        break 
                    else:
                        logger.info("Exception during shutdown, exiting...")
                        break

        except KeyboardInterrupt:
            logger.info(
                f"KeyboardInterrupt in consume_data. Processed {message_count} messages."
            )
            raise 
        finally:
            logger.info(f"Consumer finished. Total messages processed: {message_count}")

    def process_message(self, message, index_name):
        try:
            data = json.loads(message.value().decode("utf-8"))
            self.batch_buffer.append(data)

            if len(self.batch_buffer) >= self.batch_size:
                self.es_client.bulk_index_streaming(
                    self.batch_buffer, index_name=index_name
                )
                self.batch_buffer.clear()

        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON message: {e}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def cleanup(self, index_name):
        logger.info("Cleaning up consumer...")

        try:
            if self.batch_buffer:
                logger.info(f"Indexing remaining {len(self.batch_buffer)} documents")
                self.es_client.bulk_index_streaming(
                    self.batch_buffer, index_name=index_name
                )
                self.batch_buffer.clear()

            self.consumer.cleanup()

        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
        finally:
            logger.info("Consumer stopped")

    def run_wikimedia_consumer(self, index_name="wikimedia-changes"):

        topic = "WikiMedia-Changes"
        index_mapping = {
            "mappings": {
                "properties": {
                    "id": {"type": "long"},
                    "type": {"type": "keyword"},
                    "title": {"type": "text", "analyzer": "standard"},
                    "user": {"type": "keyword"},
                    "bot": {"type": "boolean"},
                    "minor": {"type": "boolean"},
                    "timestamp": {"type": "date"},
                    "processed_timestamp": {"type": "date"},
                    "comment": {"type": "text", "analyzer": "standard"},
                    "server_name": {"type": "keyword"},
                    "wiki": {"type": "keyword"},
                    "namespace": {"type": "integer"},
                    "length": {
                        "properties": {
                            "old": {"type": "integer"},
                            "new": {"type": "integer"},
                        }
                    },
                    "revision": {
                        "properties": {"old": {"type": "long"}, "new": {"type": "long"}}
                    },
                    "source": {"type": "keyword"},
                    "pipeline_version": {"type": "keyword"},
                    "@timestamp": {"type": "date"},
                }
            },
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 1,
                "refresh_interval": "5s",
            },
        }

        try:
            topics_list = self.kafka_utils.list_topics()
            if topic in topics_list:
                logger.info("Topic Exists...")
            else:
                logger.info("Topic Doesn't Exist!!!")
                raise
            self.es_client.create_index(index_name=index_name, mapping=index_mapping)
            self.consumer.create_consumers(topic=topic)
            self.consume_data(topic=topic, index_name=index_name)
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        finally:
            self.cleanup(index_name=index_name)
