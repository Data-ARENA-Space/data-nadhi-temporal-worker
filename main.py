import argparse
import asyncio
import os

from dotenv import load_dotenv
from temporalio.client import Client
from temporalio.worker import Worker

from temporal_worker.activities.initial import (
    fetch_pipeline_config,
    fetch_workflow_config,
)
from temporal_worker.activities.transformations import end, filters, transform
from temporal_worker.workflows import MainWorkflow, TransformationWorkflow

load_dotenv()


def parse_arguments():
    """Parse command-line arguments with fallback to environment variables."""
    parser = argparse.ArgumentParser(description="Temporal Worker")

    parser.add_argument(
        "--worker-type",
        type=str,
        default=os.environ.get("WORKER_TYPE", "main"),
        choices=["main", "transformation"],
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


workers = {
    "main": {
        "workflows": [MainWorkflow],
        "activities": [fetch_pipeline_config, fetch_workflow_config],
    },
    "transformation": {
        "workflows": [TransformationWorkflow],
        "activities": [end, filters, transform],
    },
}


async def main():
    # Parse command-line arguments
    args = parse_arguments()

    if args.verbose:
        print("Starting worker with configuration:")
        print(f"  Worker Type: {args.worker_type}")
        print(f"  Task Queue: {args.task_queue}")
        print(f"  Temporal Host: {args.temporal_host}")

    if args.worker_type not in workers:
        raise ValueError(f"Unknown worker type: {args.worker_type}")

    # Connect to Temporal server
    client = await Client.connect(args.temporal_host)

    # Create worker for a specific task queue
    worker = Worker(
        client,
        task_queue=args.task_queue,  # Task queue name
        workflows=workers[args.worker_type]["workflows"],  # Optional: workflows
        activities=workers[args.worker_type][
            "activities"
        ],  # Activities this worker will handle
    )

    print(
        f"Worker started on task queue {args.task_queue} with type {args.worker_type}"
    )

    # Run the worker
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
