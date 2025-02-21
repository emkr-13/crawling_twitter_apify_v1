from .logging import logger
from kafka import KafkaProducer, KafkaClient
from kafka.cluster import ClusterMetadata
import kafka.errors as kafkaErrors
import asyncio
import json

class KafkaProducer:
    def __init__(self, configs: dict, init_topic=None, max_retries=3, retry_interval=5):
        assert configs.get("bootstrap_servers") is not None, "configs must have bootstrap_servers"
        bootstrap_servers = configs.get("bootstrap_servers")
        
        retry_count = 0
        while retry_count < max_retries:
            try:
                self._producer = KafkaProducer(**configs)
                if self._producer.bootstrap_connected():
                    logger.debug({"message": f"bootstrap server connected to {bootstrap_servers}"})
                else:
                    logger.debug({"message": "bootstrap server not connected"})
                    
                metadata = ClusterMetadata(bootstrap_servers=bootstrap_servers)
                logger.debug({"message": "available brokers", "content": list(metadata.brokers())})
                logger.debug({"message": "available topics", "content": list(metadata.topics(exclude_internal_topics=False))})
                return
            except kafkaErrors.NoBrokersAvailable:
                retry_count += 1
                logger.error({"message": f"No brokers available. Retrying ({retry_count}/{max_retries})..."})
                asyncio.sleep(retry_interval)
        
        logger.error({"message": f"Failed to connect to Kafka after {max_retries} retries. Check Kafka configuration."})
        raise kafkaErrors.NoBrokersAvailable

    def close(self):
        self._producer.close()

    def on_send_success(self, record_metadata):
        logger.success({
            "message": "sent message successfully for twitter every day",
            "detail":{
                "topic": record_metadata.topic, 
                "partition": record_metadata.partition, 
                "offset": record_metadata.offset
            }
            })

    def on_send_failed(self, excp):
        logger.error({"message": "cannot send", "excp": excp})

    def produce(self, topic, value):
        serialized_value = json.dumps(value).encode('utf-8')  # Serialize the value to bytes
        try:
            metadata = (
                self._producer.send(topic, value=serialized_value)
                .add_callback(self.on_send_success)
                .add_errback(self.on_send_failed)
            )
            self._producer.flush()
            return metadata
        except Exception as e:
            logger.error({"message": "Error producing message to Kafka", "exception": str(e)})
            raise