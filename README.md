# CABBAGE

Cabbage is a distributed task queue system designed to handle task execution in a scalable and fault-tolerant manner. It leverages Kafka as the message broker for task distribution and Redis as the result backend for storing task statuses and results. The system is designed to support multiple task types and provides a simple interface for submitting and processing tasks.

## Features

- **Distributed Architecture**: Tasks are distributed across workers using Kafka, enabling horizontal scalability.
- **Task Result Storage**: Task statuses and results are stored in Redis for easy retrieval and monitoring.
- **Pluggable Task Execution**: Supports custom task types through a modular task execution framework.
- **Fault Tolerance**: Handles errors gracefully during task execution and result storage.
- **Logging**: Comprehensive logging for debugging and monitoring.

## Components

### 1. **Message Broker**
The system uses Kafka as the message broker to publish and consume tasks. Kafka ensures reliable delivery of tasks to workers and supports high throughput for task distribution.

### 2. **Result Backend**
Redis is used as the result backend to store task statuses and results. It provides fast and efficient access to task data, enabling real-time monitoring of task execution.

### 3. **Worker**
The worker component is responsible for consuming tasks from Kafka, executing them, and updating their statuses and results in Redis. It supports multiple task types, such as addition, subtraction, and multiplication.

### 4. **Client**
The client component provides an interface for submitting tasks to the system. It generates unique task IDs, publishes tasks to Kafka, and initializes their statuses in Redis.

### 5. **Task Execution**
The system includes a modular task execution framework that allows defining custom task types. Each task type is implemented as a method in the `CodeExecutionWorker` class.

## How It Works

1. **Task Submission**: 
   - The client submits a task with a unique task ID, task type, and arguments.
   - The task is published to a Kafka topic and its initial status is stored in Redis.

2. **Task Consumption**:
   - Workers consume tasks from the Kafka topic and execute them based on their type.
   - During execution, the task status is updated to "executing" in Redis.

3. **Task Execution**:
   - The worker executes the task using the appropriate method in the `CodeExecutionWorker` class.
   - Upon completion, the result is stored in Redis with a "completed" status. If an error occurs, the status is updated to "error" with the error details.

4. **Result Retrieval**:
   - The client or any other component can retrieve task results from Redis using the task ID.

## Prerequisites

- **Kafka**: Ensure Kafka is installed and running. Configure the `bootstrap_servers` parameter in the code to point to your Kafka instance.
- **Redis**: Ensure Redis is installed and running. Configure the `host` and `port` parameters in the code to point to your Redis instance.

## Running the System

1. **Start Kafka and Redis**:
   - Start your Kafka and Redis instances.

2. **Run the Worker**:
   - Start the worker by running `newworker.py`. The worker will continuously poll Kafka for tasks and process them.

3. **Submit Tasks**:
   - Use the client to submit tasks by running `main.py` with an input CSV file containing tasks. For example:
     ```bash
     python main.py i.csv
     ```

4. **Monitor Results**:
   - Task results can be retrieved from Redis using the task ID.

## Logging

The system uses a custom logging setup to log events to both the console and a file (`app.log`). Logs include detailed information about task submission, execution, and result storage.

## Fault Tolerance

Cabbage incorporates fault tolerance mechanisms to ensure reliable task execution and system stability:

1. **Heartbeat Monitoring**:
   - The worker continuously monitors its running state using a `_running` flag. This allows the system to gracefully handle shutdowns or interruptions (e.g., via a `KeyboardInterrupt`).
   - If the worker is stopped during task processing, it ensures that no new tasks are consumed, and the system can safely resume from the last known state.

2. **Error Handling During Task Execution**:
   - If an error occurs while executing a task, the worker catches the exception and updates the task status in Redis to "error" along with the error details. This ensures that failed tasks are not lost and can be inspected or retried later.

3. **Task Status Updates**:
   - The worker updates the task status in Redis at every stage of the task lifecycle (e.g., "queued," "executing," "completed," or "error"). This ensures that the system can track the progress of each task and recover from failures by reprocessing tasks that are stuck in an incomplete state.

4. **Kafka Polling with Timeout**:
   - The Kafka consumer polls for tasks with a configurable timeout. If no tasks are available within the specified time, the worker avoids busy waiting by introducing a small delay. This prevents resource exhaustion and ensures efficient task consumption.

5. **Redis Connection Resilience**:
   - The `RedisResultBackend` class includes error handling for Redis operations. If a connection error occurs, it logs the issue and raises an exception, allowing the system to handle the failure gracefully.

These mechanisms collectively ensure that Cabbage can handle unexpected failures, maintain task integrity, and provide a reliable distributed task queue system.
