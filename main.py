from src.producers.producers import Producers
from src.consumers.consumers import Consumers
from wikimedia.wikimedia_producer import WikiMediaProducer
from wikimedia.wikimedia_consumer import WikiMediaConsumer
from src.utils.argparse import arg_parser
from src.utils.kafka_utils import KafkaAdmin
from src.utils.logging import logger

import time
import threading
import sys


class WikiMediaRunner:
    def __init__(self):

        self.shutdown_event = threading.Event()
        self.params = {
            "logger": logger,
            "producer": Producers(),
            "consumer": Consumers(),
            "kafka_utils": KafkaAdmin(),
            "shutdown_event": self.shutdown_event,
        }
        self.producer_thread = None
        self.consumer_thread = None

    def run_producer(self):
        try:
            producer = WikiMediaProducer(params=self.params)
            producer.run_wikimedia_producer()
        except Exception as e:
            logger.error(f"Producer error: {e}")
            self.shutdown_event.set()

    def run_consumer(self):
        try:
            consumer = WikiMediaConsumer(params=self.params)
            consumer.run_wikimedia_consumer()
        except Exception as e:
            logger.error(f"Consumer error: {e}")
            self.shutdown_event.set()

    def start_producer_mode(self):
        logger.info("Starting producer mode...")
        self.run_producer()

    def start_consumer_mode(self):
        logger.info("Starting consumer mode...")
        self.run_consumer()

    def start_full_mode(self):
        logger.info("Starting full mode (producer + consumer)...")

        self.producer_thread = threading.Thread(
            target=self.run_producer, name="WikiMediaProducer"
        )

        self.consumer_thread = threading.Thread(
            target=self.run_consumer, name="WikiMediaConsumer"
        )

        self.producer_thread.start()
        self.consumer_thread.start()

        try:
            while not self.shutdown_event.is_set():
                time.sleep(0.5)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
            self.shutdown_event.set()
        finally:
            self.cleanup()

    def cleanup(self):
        logger.info("Cleaning up resources...")

        self.shutdown_event.set()

        if self.producer_thread and self.producer_thread.is_alive():
            self.producer_thread.join(timeout=15.0)
            if self.producer_thread.is_alive():
                logger.warning("Producer thread did not finish within timeout")
        if self.consumer_thread and self.consumer_thread.is_alive():
            self.consumer_thread.join(timeout=15.0)
            if self.consumer_thread.is_alive():
                logger.warning("Consumer thread did not finish within timeout")
        logger.info("Cleanup completed")


def main():
    args = arg_parser()
    runner = WikiMediaRunner()

    try:
        if args.mode == "producer":
            runner.start_producer_mode()
        elif args.mode == "consumer":
            runner.start_consumer_mode()
        elif args.mode == "full":
            runner.start_full_mode()
        else:
            logger.error(f"Invalid mode: {args.mode}")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Application error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
