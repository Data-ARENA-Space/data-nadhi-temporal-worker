# Data Nadhi Temporal Worker

Temporal worker that executes data pipeline workflows.

## Description

This worker connects to Temporal and executes pipeline workflow activities. It supports three worker types:
- `main` - Handles main pipeline orchestration workflows
- `transformation` - Processes data transformation activities
- `destination` - Manages data delivery to destinations

## Dev Container

This repository includes a dev container configuration with all required dependencies and services pre-configured.

**To use:**
1. Open the repository in VS Code
2. Click "Reopen in Container" when prompted
3. All services (Temporal, MongoDB, Redis) will be available automatically

## Running the Worker

```bash
python main.py --worker-type main --task-queue default-1
```

**Options:**
- `--worker-type`: Choose `main`, `transformation`, or `destination` (default: `main`)
- `--task-queue`: Task queue name (default: `default-1`)
- `--temporal-host`: Temporal server address (default: `datanadhi-temporal:7233`)
- `--verbose` or `-v`: Enable verbose logging

## Environment Variables

Copy `.env.example` to `.env` and configure:

```env
MONGO_URL=mongodb://localhost:27017/datanadhi_dev
MONGO_DATABASE=datanadhi_dev
REDIS_URL=redis://redis:6379
```

## License

This project is licensed under the ISC License - see the [LICENSE](LICENSE) file for details.
