# Scrapor

Reliable supermarket data ingestion for Israeli retail price transparency.

Scrapor is the data pipeline behind AGALI's supermarket comparison platform. It collects public supermarket files, converts them into structured records, publishes durable dataset snapshots, and serves the latest processed data through an authenticated API.

The project focuses on a hard real-world data problem: turning fragmented, frequently changing retail feeds into dependable infrastructure that can power consumer price comparison, promotion discovery, product search, and long-term market analysis.

## Why Scrapor Matters

Retail price transparency only works when the underlying data is fresh, normalized, and queryable. In practice, supermarket feeds are messy: file formats differ across chains, updates arrive on different schedules, historical data needs to be preserved, and applications need a fast operational store rather than raw dumps.

Scrapor bridges that gap. It is designed as a practical ingestion system that can:

- scrape public supermarket price, store, and promotion files;
- parse raw feed files into structured data using the `il-supermarket-scraper` and `il-supermarket-parser` ecosystem;
- skip already processed files to reduce redundant network and compute work;
- publish long-term dataset snapshots, with Kaggle support out of the box;
- update short-term serving databases such as MongoDB or PostgreSQL;
- expose a FastAPI read API for chains, file types, raw file content, and health checks;
- run locally, in Docker Compose, or as a scheduled processing service.

## Key Features

- **End-to-end ingestion DAG**: scraping, converting, API database update, long-term publishing, and cleanup are modeled as explicit pipeline operations.
- **Configurable chain and file filters**: run the full market or narrow a job to specific chains and file types.
- **Incremental processing**: previously processed files can be fetched from the short-term database or a cache file so the pipeline avoids repeat work.
- **Multiple storage targets**: supports MongoDB, PostgreSQL, Kafka, local file-backed storage, and Kaggle-backed long-term storage interfaces.
- **Serving API**: provides an authenticated FastAPI service with OpenAPI docs and typed Pydantic responses.
- **Operational health checks**: includes service health endpoints and a heartbeat-based Docker health check for the data processor.
- **Test coverage for core components**: includes unit, integration, access-layer, remote-storage, publisher, and system validation tests.

## Architecture

```text
Public supermarket feeds
        |
        v
Scraping task
        |
        v
Raw dumps in app_data/dumps
        |
        v
Converting task
        |
        v
Structured outputs in app_data/outputs
        |
        +--------------------------+
        |                          |
        v                          v
Short-term serving DB        Long-term dataset store
MongoDB / PostgreSQL /       Kaggle / local file storage
Kafka / file DB
        |
        v
FastAPI serving layer
```

The pipeline is intentionally modular. Storage backends implement small uploader interfaces, while publisher classes coordinate the lifecycle of scraping, parsing, publishing, and cleanup.

## Repository Layout

```text
scrapor/
  api.py                         FastAPI serving application
  main.py                        Data processing entry point
  Dockerfile                     Multi-stage Docker build
  docker-compose.yml             MongoDB, API, and processor services
  requirements.txt               Runtime dependencies
  requirements-dev.txt           Test and development dependencies

  access/                        API auth, token validation, telemetry, access layer
  data_models/                   Pydantic API response and raw schema models
  managers/                      Cache, short-term DB, long-term dataset, large-file managers
  publishers/                    DAG publisher and scheduled publisher logic
  remotes/                       Storage backends for MongoDB, PostgreSQL, Kafka, Kaggle, files
  scripts/                       Migration, maintenance, benchmarking, and backfill scripts
  system_tests/                  End-to-end service and data validation
  tests/                         Integration validation tests
```

## Pipeline Operations

Scrapor can execute individual operations or full operation chains through the `OPERATION` environment variable.

Common operations:

| Operation | Purpose |
| --- | --- |
| `scraping` | Download supermarket feed files into the local app data folder. |
| `converting` | Parse downloaded files into structured outputs. |
| `api_update` | Upload converted outputs into the short-term serving database. |
| `publishing` | Compose and upload a long-term dataset snapshot. |
| `clean_dump_files` | Remove raw dump files while preserving status data. |
| `clean_all_source_data` | Remove source, output, status, and cache artifacts. |
| `download_from_long_term` | Download the long-term dataset into local storage. |
| `reload_complete_api` | Force a complete API database reload. |

Example:

```bash
OPERATION="scraping,converting,api_update" python main.py
```

When `OPERATION` is not set, `main.py` starts the scheduled publisher loop.

## Requirements

- Python 3.11 recommended
- Docker and Docker Compose for containerized local runs
- MongoDB or PostgreSQL for serving storage
- Kaggle credentials if long-term dataset publishing is enabled

Install dependencies:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

On Windows PowerShell:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

## Configuration

Scrapor is configured through environment variables.

### Core Runtime

| Variable | Description | Default |
| --- | --- | --- |
| `APP_DATA_PATH` | Working directory for dumps, outputs, status, heartbeat, and cache files. | `./app_data` |
| `NUM_OF_PROCESSES` | Number of scraping and parsing workers. | `5` |
| `LIMIT` | Optional limit for local or test runs. | unset |
| `ENABLED_SCRAPERS` | Comma-separated scraper names to run. | all available scrapers |
| `ENABLED_FILE_TYPES` | Comma-separated file types to process. | all available file types |
| `WHEN` | Optional ISO timestamp for backfills or reproducible runs. | current Jerusalem time |
| `OUTPUT_DESTINATION` | Short-term destination: `mongo`, `postgres`, `postgresql`, `kafka`, or `file`. | `mongo` |
| `LOG_LEVEL` | Logging verbosity. | `WARNING` in Docker Compose |

### Scheduling

| Variable | Description |
| --- | --- |
| `OPERATION` | If set, runs the specified comma-separated operation chain once. |
| `STOP_DAG_CONDITION` | `NEVER` or `ONCE`. |
| `EXEC_FINAL_OPERATIONS_CONDITION` | `EOD`, `ONCE`, or an integer job count. |
| `SECOND_TO_WAIT_BETWEEN_OPERATIONS` | Delay between scheduled operation runs. |
| `SECOND_TO_WAIT_AFTER_FINAL_OPERATIONS` | Delay after final operations complete. |

### Storage and API

| Variable | Description |
| --- | --- |
| `MONGODB_URI` | MongoDB connection string. |
| `POSTGRESQL_URL`, `DATABASE_URL`, or `SUPABASE_DATABASE_URL` | PostgreSQL connection string. |
| `KAGGLE_USERNAME` | Kaggle username. |
| `KAGGLE_KEY` | Kaggle API key. |
| `KAGGLE_DATASET_REMOTE_NAME` | Remote Kaggle dataset name. |
| `API_TOKEN` | Bearer token used by the API auth middleware. |
| `DISABLE_AUTH_MIDDLEWARE` | Set to `true` only for local development or tests. |
| `PROCESSED_FILES_CACHE` | Optional JSON cache of already processed files. |
| `HEALTHCHECK_MAX_AGE_SECONDS` | Maximum allowed heartbeat age for processor health checks. |

## Running Locally

### 1. Run a Small Processing Job

```bash
export ENABLED_SCRAPERS=BAREKET
export LIMIT=10
export OUTPUT_DESTINATION=file
export OPERATION="scraping,converting"
python main.py
```

This is the fastest way to validate the scraper and parser flow without external database credentials.

### 2. Start the API

The API expects configured storage credentials. For local development, auth can be disabled:

```bash
export DISABLE_AUTH_MIDDLEWARE=true
uvicorn api:app --reload --host 0.0.0.0 --port 8000
```

Open the API documentation at:

```text
http://localhost:8000/docs
```

### 3. Run with Docker Compose

Create an environment file with the required variables, then start the services:

```bash
docker compose up --build
```

Docker Compose starts:

- `mongodb`: internal MongoDB service;
- `api`: FastAPI service exposed on port `8080`;
- `data_processor`: pipeline worker running `main.py`.

The API health endpoint is available at:

```text
http://localhost:8080/service_health
```

## API Overview

Scrapor exposes a focused read API for raw processed supermarket data.

| Endpoint | Description |
| --- | --- |
| `GET /list_chains` | List available supermarket chains. |
| `GET /list_file_types` | List available scraped file types. |
| `GET /list_scraped_files` | List processed files with optional chain, type, store, date, and latest-only filters. |
| `GET /raw/file_content` | Retrieve paginated raw file content with cursor support. |
| `GET /service_health` | Service health check. |
| `GET /short_term_health` | Short-term serving database freshness check. |
| `GET /long_term_health` | Long-term dataset freshness check. |

Authenticated endpoints use Bearer token authentication unless `DISABLE_AUTH_MIDDLEWARE=true` is set.

## Testing

Run the Python test suite:

```bash
python -m pytest
```

Run a targeted integration test:

```bash
python -m pytest tests/test_complete_integration.py
```

Run the full Docker-backed local validation:

```bash
./local_test.sh --incremental
```

The Docker validation starts the API and database, runs a limited scrape, publishes, verifies cleanup behavior, and executes system tests.

## Operational Notes

- The default timezone is `Asia/Jerusalem`, and the publisher validates timezone-sensitive execution.
- Incremental runs use processed-file metadata to avoid downloading and converting files already present in the short-term store.
- PostgreSQL mode is persistence-oriented and avoids wiping managed tables during restart.
- The processor writes heartbeat state to `APP_DATA_PATH/heartbeat.json`; Docker health checks use this file to detect stale or failed jobs.
- Long-term publishing is optional. If `KAGGLE_DATASET_REMOTE_NAME` is not configured, Scrapor uses a dummy file-storage target.

## Open Source Program Fit

Scrapor is a strong candidate for open source support because it sits at the intersection of public-interest data access, practical data engineering, and consumer-facing AI applications.

The project is valuable beyond one application:

- it makes public retail data easier to collect, validate, and reuse;
- it creates a foundation for transparent price comparison and affordability tooling;
- it exposes realistic ingestion, normalization, and serving patterns for public datasets;
- it provides a concrete backend for multilingual, AI-assisted shopping and market analysis experiences;
- it is modular enough for contributors to improve storage backends, parser coverage, data quality, API ergonomics, monitoring, and deployment workflows.

In short, Scrapor turns public-but-fragmented data into reliable infrastructure. That is exactly the kind of foundation that helps open ecosystems produce useful, verifiable, user-centered applications.

## Roadmap

- Expand contributor-facing setup docs and sample `.env` templates.
- Add richer API examples and response fixtures.
- Improve observability around per-chain scrape freshness and parser failures.
- Add more PostgreSQL migration documentation.
- Publish reproducible benchmark results for large catalog and promotion imports.
- Add CI workflows for linting, unit tests, and Docker smoke tests.

## Contributing

Contributions are welcome. Good first areas include:

- improving documentation and examples;
- adding tests around pipeline edge cases;
- strengthening API response validation;
- improving storage backend implementations;
- adding monitoring and operational dashboards;
- documenting new chain or file-type behavior.

Before submitting a larger change, please open an issue or discussion describing the proposed direction.

## License

This repository is currently distributed under the custom license in `LICENSE.txt`. Please review it carefully before using, modifying, or redistributing the project, especially for commercial use.
