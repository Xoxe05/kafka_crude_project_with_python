import time
import requests

from typing import Generator, Dict, Any, Optional
from datetime import datetime

import json
from src.utils.logging import logger


from src.producers.producers import Producers
from src.utils.kafka_utils import KafkaAdmin


class WikiMediaProducer:

    producer = Producers
    kafka_utils = KafkaAdmin

    def __init__(self, params):
        self.producer = params["producer"]
        self.kafka_utils = params["kafka_utils"]
        self.running = True
        self.stream_url = "https://stream.wikimedia.org/v2/stream/recentchange"
        # self.logger = params["logger"]
        self.stream_parser = WikimediaStreamParser(params=params)
        self.shutdown_event = params["shutdown_event"]

    def enhance_message(self, data: dict):
        enhanced_data = data.copy()
        enhanced_data.update(
            {
                "processed_timestamp": datetime.utcnow().isoformat(),
                "source": "wikimedia-stream",
                "pipeline_version": "1.0",
            }
        )
        return enhanced_data

    def stream_data(self, topic="WikiMedia-Changes"):

        logger.info(f"Starting Wikimedia stream producer...")
        logger.info(f"Stream URL: {self.stream_url}")
        logger.info(f"Kafka topic: {topic}")

        events_processed = 0

        while not self.shutdown_event.is_set():

            try:

                for event in self.stream_parser.parse_events():
                    if self.shutdown_event.is_set():
                        logger.info(
                            f"Shutdown detected. Stopping at event #{events_processed}"
                        )
                        return
                    key = f"{event.get('wiki', 'unknown')}:{event.get('id', '')}"
                    event_with_ts = self.enhance_message(data=event)
                    success = self.producer.produce_message(
                        key,
                        data=json.dumps(event_with_ts, ensure_ascii=False),
                        topic=topic,
                    )

                    if success:
                        events_processed += 1

                        if events_processed <= 5 or events_processed % 50 == 0:
                            logger.info(
                                f"Event #{events_processed}: {event_with_ts.get('wiki')} - "
                                f"{event_with_ts.get('type')} - {event_with_ts.get('title', '')[:50]}"
                            )
                    self.producer.poll()

            except KeyboardInterrupt:
                logger.info(f"Interrupted. Processed {events_processed} events.")
                raise
            except Exception as e:
                logger.error(f"Unexpected error: {e}")

    def run_wikimedia_producer(self):
        topic = "WikiMedia-Changes"

        try:
            topics_list = self.kafka_utils.list_topics()
            if topic in topics_list:
                logger.info("Topic Exists...")
            else:
                logger.info("Creating Topic...")
                self.kafka_utils.create_topic(topic_name=topic)
            self.producer.create_producers()
            self.stream_data()
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        finally:
            self.producer.cleanup()


class WikimediaStreamParser:
    def __init__(self, params):
        self.stream_url = "https://stream.wikimedia.org/v2/stream/recentchange"
        self.params = params
        logger = params["logger"]

    def connect_to_stream(self) -> requests.Response:
        try:
            response = requests.get(
                self.stream_url,
                stream=True,
                timeout=(10, 60),
                headers={
                    "Accept": "text/event-stream",
                    "Cache-Control": "no-cache",
                    "User-Agent": "WikimediaKafkaConnector/1.0",
                },
            )
            response.raise_for_status()
            logger.info("Connected to Wikimedia stream")
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to connect to stream: {e}")
            raise

    def parse_events(self) -> Generator[Dict[str, Any], None, None]:
        response = self.connect_to_stream()

        buffer = ""

        for chunk in response.iter_content(chunk_size=1024, decode_unicode=True):

            if not chunk:
                continue

            buffer += chunk

            while "\n\n" in buffer:
                event_block, buffer = buffer.split("\n\n", 1)

                event = self._parse_sse_event(event_block)
                if event:
                    yield event

    def _parse_sse_event(self, event_block: str) -> Optional[Dict[str, Any]]:
        lines = event_block.strip().split("\n")
        event_data = {"event": None, "id": None, "data": None}

        current_field = None

        for line in lines:
            line = line.strip()
            if not line:
                continue

            if ":" in line:
                field, value = line.split(":", 1)
                field = field.strip()
                value = value.strip()

                if field in ["event", "id", "data"]:
                    event_data[field] = value
                    current_field = field
            else:
                if current_field and event_data[current_field]:
                    event_data[current_field] += "\n" + line

        if event_data["event"] == "message" and event_data["data"]:
            try:
                parsed_data = json.loads(event_data["data"])

                parsed_data["_event_id"] = event_data["id"]
                parsed_data["_parsed_at"] = time.time()

                return parsed_data

            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON data: {e}")
                logger.debug(f"Raw data: {event_data['data'][:200]}...")
                return None

        return None
