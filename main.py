import os
import logging
from client.client import YADTQClient
from worker.worker import YADTQWorker
from broker.broker import KafkaMessageBroker
from result_backend.redis_backend import RedisResultBackend
import sys
from utils.logging import setup_logging

def main():
    setup_logging(__name__)
    logger = logging.getLogger("my_logger")
    
    # Check if an input file path is provided
    if len(sys.argv) < 2:
        logger.error("Input file path not provided.")
        sys.exit(1)
    
    input_file_path = sys.argv[1]
    logger.info(f"Input file set to {input_file_path}")
    
    # Load environment variables
    kafka_hosts = os.getenv("KAFKA_HOSTS", "localhost:9092")
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))

    # Initialize components
    logger.info("Initializing YADTQ components...")
    broker = KafkaMessageBroker(kafka_hosts)
    logger.info("Started Broker")
    result_backend = RedisResultBackend(redis_host, redis_port)
    logger.info("Started Redis")
    worker = YADTQWorker(broker, result_backend)
    logger.info("Started Worker")
    client = YADTQClient(broker, result_backend)
    logger.info("Started Client")

    # Open the input file and process each line
    try:
        with open(input_file_path, 'r') as file:
            for line in file:
                line = line.strip().split(',')
                if len(line) != 3:
                    logger.warning(f"Skipping invalid line: {line}")
                    continue

                task_type, a, b = line[0], line[1], line[2]
                try:
                    # Convert the arguments to integers if they are numeric
                    a, b = int(a), int(b)
                except ValueError:
                    logger.error(f"Invalid task arguments: {a}, {b} for task {task_type}")
                    continue

                logger.info(f"Submitting task {task_type} with arguments {a}, {b}")
                task_id = client.submit_task(task_type, a, b)
                logger.info(f"Task {task_id} has been submitted for task {task_type}")

                result = client.get_task_status(task_id)
                logger.info(f"Task result: {result}")

    except FileNotFoundError:
        logger.error(f"File not found: {input_file_path}")
        sys.exit(1)

if __name__ == "__main__":
    main()
