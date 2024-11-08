from typing import List, Dict
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import json
from utils.logging import setup_logging

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

    def consume_tasks(self, topic: str) -> List[Dict]:
        logger.debug(f"Attempting to consume tasks from Kafka topic: {topic}")
        self.consumer.subscribe([topic])  # Subscribe to the topic
        tasks = []
        try:
            # Consume tasks from the Kafka topic
            for message in self.consumer:
                logger.debug(f"Received message from Kafka: {message.value}")
                tasks.append(message.value)
                
                # Optional: Add a break condition or consume a fixed number of messages
                if len(tasks) >= 1:  # Adjust the number of tasks as per your requirement
                    logger.debug(f"Consumed {len(tasks)} tasks from {topic}.")
                    break
            return tasks
        except KafkaError as e:
            logger.error(f"Error consuming tasks from {topic}: {e}")
            return []
        
