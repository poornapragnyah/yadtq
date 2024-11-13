from typing import List, Dict
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import json
from utils.logging import setup_logging
import time

# Set up the logger
logger = setup_logging(__name__)

class KafkaMessageBroker:
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        
        # Set up Kafka producer and consumer
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.debug("Kafka producer initialized.")
            
            self.consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda v: json.loads(v.decode('utf-8'))
            )
            logger.debug("Kafka consumer initialized.")
        
        except KafkaError as e:
            logger.error(f"Error initializing Kafka producer/consumer: {e}")
            raise

    def publish_task(self, topic: str, task: Dict):
        try:
            # Send the task to the specified Kafka topic
            logger.debug(f"Attempting to send task {task} to Kafka topic: {topic}")
            self.producer.send(topic, value=task)
            self.producer.flush()  # Ensure the task is sent
            logger.info(f"Successfully sent task {task} to Kafka topic: {topic}")
        except KafkaError as e:
            logger.error(f"Error publishing task to {topic}: {e}")
            raise

    def consume_tasks(self, topic: str,max_wait=2) -> List[Dict]:
        batch_size = 10
        logger.debug(f"Attempting to consume tasks from Kafka topic: {topic}")
        self.consumer.subscribe([topic])  # Subscribe to the topic
        tasks = []
        start_time = time.time()
        try:
            while True:
                messages = self.consumer.poll(timeout_ms=100)
                if isinstance(messages, dict) and messages:
                        for partition, msgs in messages.items():
                            if isinstance(msgs, list):
                                for message in msgs:
                                    if hasattr(message, 'value'):
                                        tasks.append(message.value)
                                        logger.debug(f"Received message from Kafka: {message.value}")

                                        # Check if batch size is reached
                                        if len(tasks) >= batch_size:
                                            logger.debug(f"Consumed {len(tasks)} tasks from {topic}.")
                                            return tasks
                # Check elapsed time for early return
                if time.time() - start_time > max_wait:
                    logger.debug(f"Time elapsed is {time.time() - start_time:.2f} seconds, returning {len(tasks)} tasks.")
                    return tasks
        except KafkaError as e:
                logger.error(f"Error consuming tasks from {topic}: {e}")
                return []

        
