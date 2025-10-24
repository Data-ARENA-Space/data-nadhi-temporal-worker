# 🌊 Data Nadhi

**Data Nadhi** is an open-source platform that helps you manage the flow of data starting from your application logs all the way to your desired destinations — databases, APIs, or alerting systems.

> **Direct. Transform. Deliver.**  
> Flow your logs, trigger your pipelines.

---

## 🧠 What is Data Nadhi?

Data Nadhi provides a unified platform to **ingest, transform, and deliver** data — powered by **Temporal**, **MongoDB**, **Redis**, and **MinIO**.

It connects easily with your applications using the **Data Nadhi SDK**, and gives you full control over how data moves across your system.

### Core Concept
- **Direct** – Collect logs and data from your applications or external sources.  
- **Transform** – Use Temporal workflows to apply filters, enrichments, or custom transformations.  
- **Deliver** – Send the final processed data to any configured destination — all handled reliably and asynchronously.

Data Nadhi is designed to be **modular**, **developer-friendly**, and **ready for production**.

---

## 🏗️ System Overview

The platform is built from multiple services and tools working together:

| Component | Description |
|------------|-------------|
| [**data-nadhi-server**](https://github.com/Data-ARENA-Space/data-nadhi-server) | Handles incoming requests from the SDK and passes them to Temporal. |
| [**data-nadhi-internal-server**](https://github.com/Data-ARENA-Space/data-nadhi-internal-server) | Internal service for managing entities, pipelines, and configurations. |
| [**data-nadhi-temporal-worker**](https://github.com/Data-ARENA-Space/data-nadhi-temporal-worker) | Executes workflow logic and handles transformations and delivery. |
| [**data-nadhi-sdk**](https://github.com/Data-ARENA-Space/data-nadhi-sdk) | Python SDK for logging and sending data from applications. |
| [**data-nadhi-dev**](https://github.com/Data-ARENA-Space/data-nadhi-dev) | Local environment setup using Docker Compose for databases and Temporal. |
| [**data-nadhi-documentation**](https://github.com/Data-ARENA-Space/data-nadhi-documentation) | Documentation site built with Docusaurus (you’re here now). |

All components are connected through a shared Docker network, making local setup and development simple.

---

## ⚙️ Features

- 🧩 **Unified Pipeline** – Move data seamlessly from logs to destinations  
- ⚙️ **Custom Transformations** – Define your own transformations using Temporal  
- 🔄 **Reliable Delivery** – Retries, fault tolerance, and monitoring built in  
- 🧠 **Easy Integration** – Simple SDK-based setup for applications  
- 💡 **Developer Focused** – Dev containers and Docker-first setup for consistency  

---

## 📚 What's Inside this repository

This repository contains the three workers for temporal listening to different task queues

### Tech Stack

- **MongoDB** – Primary datastore for pipeline and entity configurations  
- **Redis** – Used for caching and quick lookups  
- **MinIO** – S3-compatible object storage for failure logs temporarily
- **Temporal** – Workflow orchestration engine to run the data pipelines  
- **Python(temporalio)** - Framework used to create temporal worker
- **Docker** – For consistent local and production deployment 
- **Docker Network (`datanadhi-net`)** – Shared network for connecting all services locally  

---

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose  
- VS Code (with Dev Containers extension)

### Setup Instructions

1. Open [data-nadhi-temporal-worker](https://github.com/Data-ARENA-Space/data-nadhi-temporal-worker) in Dev Container
2. Set this in `.env` file
    ```bash
    # Secret Key for decrypting encrypted Creds
    SEC_DB=my-secret-db-key

    # MongoDB Configuration
    MONGO_URL=mongodb://mongo:27017/datanadhi_dev
    MONGO_DATABASE=datanadhi_dev

    # Redis Configuration (for caching)
    REDIS_URL=redis://redis:6379

    # MinIO
    MINIO_ENDPOINT=datanadhi-minio:9000
    MINIO_ACCESS_KEY=minio
    MINIO_SECRET_KEY=minio123
    MINIO_BUCKET=failure-logs
    ```
3. Give execute permission for the worker script
    ```bash
    chmod +x scripts/run-worker.sh
    ```
4. Run these in separate terminals:
    ```bash
    ./scripts/run-worker.sh default main
    ./scripts/run-worker.sh default-transform transformation
    ./scripts/run-worker.sh default-destination destination
    ```
    
---

## 🔗 Links

- **Main Website**: [https://datanadhi.com](https://datanadhi.com)
- **Documentation**: [https://docs.datanadhi.com](https://docs.datanadhi.com)
- **GitHub Organization**: [Data-ARENA-Space](https://github.com/Data-ARENA-Space)

## 📄 License

This project is open source and available under the [GNU Affero General Public License v3.0](LICENSE).

## 💬 Community

- **GitHub Discussions**: [Coming soon]
- **Discord**: [Data Nadhi Community](https://discord.gg/gMwdfGfnby)
- **Issues**: [GitHub Issues](https://github.com/Data-ARENA-Space/data-nadhi-documentation/issues)
