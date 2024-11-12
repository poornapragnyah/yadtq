import json
from utils.logging import setup_logging
import redis
from typing import Dict

logger = setup_logging(__name__)

class RedisResultBackend:
    def __init__(self, host: str, port: int):
        try:
            self.redis = redis.Redis(host=host, port=port)
            # Test connection
            self.redis.ping()
            logger.info(f"Successfully connected to Redis at {host}:{port}")
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis at {host}:{port}. Error: {str(e)}")
            raise e
        
    def store_task_result(self, task_id: str, result: Dict, status: str):
        try:
            logger.info(f"Storing result for task {task_id} with status {status}")
            result['status'] = status
            self.redis.set(f"task:{task_id}", json.dumps(result))
            logger.info(f"Task {task_id} result successfully stored with status {status}.")
        except redis.RedisError as e:
            logger.error(f"Error storing result for task {task_id}. Error: {str(e)}")
            raise e        

    def get_task_result(self, task_id: str) -> Dict:
        try:
            logger.info(f"Fetching result for task {task_id}")
            result_json = self.redis.get(f"task:{task_id}")
            if result_json:
                logger.info(f"Result for task {task_id} found.")
                return json.loads(result_json)
            else:
                logger.warning(f"Result for task {task_id} not found.")
                return {}
        except redis.RedisError as e:
            logger.error(f"Error fetching result for task {task_id}. Error: {str(e)}")
            return {}
        
    def update_task_status(self, task_id: str, status: str):
        try:
            logger.info(f"Updating status for task {task_id} to {status}")
            task_data = self.get_task_result(task_id)
            if task_data:
                task_data['status'] = status
                self.redis.set(f"task:{task_id}", json.dumps(task_data))
                logger.info(f"Task {task_id} status successfully updated to {status}.")
            else:
                logger.warning(f"Cannot update status for task {task_id} as it does not exist.")
        except redis.RedisError as e:
            logger.error(f"Error updating status for task {task_id}. Error: {str(e)}")
            raise e

