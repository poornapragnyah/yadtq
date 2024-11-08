from utils.logging import setup_logging

logger = setup_logging(__name__)

class CodeExecutionWorker:
    def add(self, a, b):
        logger.debug(f"Performing addition: {a} + {b}")
        return a + b

    def subtract(self, a, b):
        logger.debug(f"Performing subtraction: {a} - {b}")
        return a - b

    def multiply(self, a, b):
        logger.debug(f"Performing multiplication: {a} * {b}")
        return a * b
