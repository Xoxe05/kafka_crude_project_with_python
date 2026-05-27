from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

from src.core.config import settings
from src.utils.logging import logger
import traceback
from typing import List
from datetime import datetime


class ElasticSearch:

    es_client = Elasticsearch

    def __init__(self, params=None):
        self.params = params
        self.es_client_config()

    def es_client_config(self):

        try:
            logger.info("Connecting to the ElasticSearch Client")
            es_config = {
                "hosts": settings.es_host,
                "basic_auth": (settings.es_username, settings.es_password),
                "verify_certs": False,
                "request_timeout": 30,
                "max_retries": 3,
                "retry_on_timeout": True,
            }

            self.es_client = Elasticsearch(**es_config)

        except Exception as e:
            logger.exception(f"Exception Occurred: {e}")
            logger.exception(traceback.format_exc())
            raise

    def create_index(self, index_name=None, mapping=None):

        try:
            if not self.es_client.indices.exists(index=index_name):
                if mapping == None:
                    logger.info("Creating index without mapping")
                    self.es_client.indices.create(index=index_name)

                else:
                    self.es_client.indices.create(index=index_name, body=mapping)

                logger.info(f"Created Index with index name: {index_name}")

            else:
                logger.info(f"Index {index_name} already exists.")
        except Exception as e:
            logger.exception(f"Exception Occurred while creating index: {e}")
            logger.exception(traceback.format_exc())
            raise

    def bulk_index_streaming(
        self, documents: List[dict], batch_size: int = 1000, index_name: str = None
    ):

        def document_generator():
            for doc in documents:
                yield {
                    "_index": index_name,
                    "_source": self.transform_document(doc),
                    "_id": doc.get("meta", {}).get("id"),
                }

        total_success = 0
        total_failed = 0
        all_failures = []

        try:

            for success, info in streaming_bulk(
                client=self.es_client,
                actions=document_generator(),
                chunk_size=batch_size,
                max_chunk_bytes=10 * 1024 * 1024,
                request_timeout=60,
                max_retries=3,
                initial_backoff=2,
                max_backoff=600,
                raise_on_error=False,
                raise_on_exception=True,
            ):
                if success:
                    total_success += 1
                else:
                    total_failed += 1
                    all_failures.append(info)

                    if "index" in info:
                        error_info = info["index"]
                        doc_id = error_info.get("_id", "unknown")
                        error_details = error_info.get("error", {})
                        error_type = error_details.get("type", "unknown")
                        error_reason = error_details.get("reason", "unknown")

                        logger.error(
                            f"Failed document ID: {doc_id}, Error: {error_type} - {error_reason}"
                        )

            logger.info(
                f"Streaming bulk indexing completed. Success: {total_success}, Failed: {total_failed}"
            )

            return {
                "success_count": total_success,
                "failed_count": total_failed,
                "failed_docs": all_failures,
            }

        except Exception as e:
            logger.error(f"Unexpected error during streaming bulk indexing: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    def transform_document(self, data: dict) -> dict:
        doc = data.copy()

        doc["@timestamp"] = datetime.utcnow().isoformat()

        if "timestamp" in doc:
            try:
                timestamp = datetime.fromtimestamp(doc["timestamp"])
                doc["timestamp"] = timestamp.isoformat()
            except (ValueError, TypeError):
                pass

        return doc
