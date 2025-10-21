#!/usr/bin/env bash
set -e  # Exit immediately on error

# Parse arguments
TASK_QUEUE="${1:-default}"       # Default value = "default"
WORKER_TYPE="${2:-main}"         # Default = "main"

mkdir -p .venv
# Create and activate virtual environment
python -m venv ".venv/${WORKER_TYPE}-worker"
source ".venv/${WORKER_TYPE}-worker/bin/activate"

# Install dependencies specific to the worker
pip install -e "./[${WORKER_TYPE}]"

# Run the worker
python main.py --worker-type "${WORKER_TYPE}" --task-queue "${TASK_QUEUE}"