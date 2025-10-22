import argparse
import asyncio
import os

from dotenv import load_dotenv
from temporalio.client import Client
from temporalio.worker import Worker

from utils.failure_logger import log_failure

load_dotenv()


def parse_arguments():
    """Parse command-line arguments with fallback to environment variables."""
    parser = argparse.ArgumentParser(description="Temporal Worker")

    parser.add_argument(
        "--worker-type",
        type=str,
        default=os.environ.get("WORKER_TYPE", "main"),
        choices=["main", "transformation", "destination"],
        help="Type of worker to start (default: main)",
    )

    parser.add_argument(
        "--task-queue",
        type=str,
        default=os.environ.get("TASK_QUEUE", "default-1"),
        help="Temporal task queue name (default: default-1)",
    )

    parser.add_argument(
        "--temporal-host",
        type=str,
        default=os.environ.get("TEMPORAL_HOST", "datanadhi-temporal:7233"),
        help="Temporal server host:port (default: datanadhi-temporal:7233)",
    )

    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    return parser.parse_args()


async def main():
    # Parse command-line arguments
    args = parse_arguments()

    if args.verbose:
        print("Starting worker with configuration:")
        print(f"  Worker Type: {args.worker_type}")
        print(f"  Task Queue: {args.task_queue}")
        print(f"  Temporal Host: {args.temporal_host}")

    workflows = activities = []

    if args.worker_type == "main":
        from temporal_workers.main_worker.activities import (
            fetch_pipeline_config,
            fetch_workflow_config,
        )
        from temporal_workers.main_worker.workflow import MainWorkflow

        workflows = [MainWorkflow]
        activities = [fetch_pipeline_config, fetch_workflow_config, log_failure]

    elif args.worker_type == "transformation":
        from temporal_workers.transformation_worker.activities import filters, transform
        from temporal_workers.transformation_worker.workflow import (
            TransformationWorkflow,
        )

        workflows = [TransformationWorkflow]
        activities = [filters, transform, log_failure]

    elif args.worker_type == "destination":
        from temporal_workers.destination_worker.activities import (
            fetch_integration_connector,
            fetch_integration_target,
            send_to_destination,
        )
        from temporal_workers.destination_worker.workflow import DestinationWorkflow

        workflows = [DestinationWorkflow]
        activities = [
            fetch_integration_target,
            fetch_integration_connector,
            send_to_destination,
            log_failure,
        ]

    else:
        raise ValueError(f"Worker type not supported: {args.worker_type}")

    # Connect to Temporal server
    client = await Client.connect(args.temporal_host)

    # Create worker for a specific task queue
    worker = Worker(
        client,
        task_queue=args.task_queue,  # Task queue name
        workflows=workflows,  # Optional: workflows
        activities=activities,  # Activities this worker will handle
    )

    print(
        f"Worker started on task queue {args.task_queue} with type {args.worker_type}"
    )

    # Run the worker
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
