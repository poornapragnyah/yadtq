from utils.logging import setup_logging
from typing import Dict
from broker.broker import KafkaMessageBroker
from result_backend.redis_backend import RedisResultBackend
from worker.tasks import CodeExecutionWorker
import time

# Initialize the logger
logger = setup_logging(__name__)

class YADTQWorker:
    def __init__(self, broker: KafkaMessageBroker, result_backend: RedisResultBackend):
        self.broker = broker
        self.result_backend = result_backend
        self.task_executor = CodeExecutionWorker()
        self._running = False  # Flag to control the worker's running state
        logger.info("YADTQ Worker initialized.")

    def start(self):
        self._running = True  # Set the worker to running state
        logger.info("YADTQ Worker starting...")
        self.consume_tasks()

    def stop(self):
        self._running = False  # Set the worker to stop consuming tasks
        logger.info("YADTQ Worker has been stopped.")  # Log the stop event

    def consume_tasks(self):
        logger.info("YADTQ Worker is now consuming tasks.")
        while self._running:  # Continue only if the worker is running
            logger.debug("Polling for tasks from Kafka...")
            tasks = self.broker.consume_tasks("task_queue")  # Poll for new tasks

            if not tasks:
                logger.debug("No tasks found in the queue.")
                time.sleep(1)  # Add a small delay to avoid busy waiting
                continue

            for task in tasks:
                if not self._running:  # Check if stop was called while processing tasks
                    logger.info("Worker is stopping while processing tasks.")
                    break
                logger.info(f"Processing task: {task.get('task_id')}")
                self.process_task(task)

    def process_task(self, task: Dict):
        task_id = task["task_id"]
        task_type = task["task_type"]
        task_args = task["args"]

        try:
            logger.info(f"Executing task {task_id} of type {task_type} with arguments {task_args}.")
            result = getattr(self.task_executor, task_type)(**task_args)  # This will call `self.task_executor.add(a=5, b=3)` for an "add" task
            logger.info(f"Task {task_id} completed with result: {result}")
            self.result_backend.store_task_result(task_id, result)
        except Exception as e:
            logger.error(f"Error processing task {task_id}: {str(e)}")
            error_result = {"status": "error", "error": str(e)}
            self.result_backend.store_task_result(task_id, error_result)

