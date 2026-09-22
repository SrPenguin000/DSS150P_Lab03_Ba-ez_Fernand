# DSS150P Laboratory 3 Starter Repository

This repository supports Module 2: Pipeline Construction, Storage, and Orchestration.
It is intentionally incomplete. Students must implement the marked TODOs and document their decisions.

## Main progression
- Goal 1: reproducible environment, modularization, Git, Docker, configuration
- Goal 2: raw -> staging -> curated transformations; audit/error handling; rerun-safe loading
- Goal 3: CSV/JSON/Parquet/PostgreSQL comparison; partitioning; selected-partition load
- Goal 4: Apache Airflow DAG for extract -> transform -> load -> validate

Start with `DSS150P_Laboratory_Activity_3.pdf`.

## Recommended commands
```bash
cp .env.example .env
python -m venv .venv
# activate .venv then:
pip install -r requirements.txt
python -m src.cli validate-env
```
The provided `.env.example` uses `POSTGRES_HOST=localhost` for host-side commands. Docker Compose overrides the application containers to use the service hostname `postgres`.

Docker/PostgreSQL:
```bash
docker compose up -d postgres
docker compose run --rm pipeline python -m src.cli validate-env
```

Airflow in Goal 4:
```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
```
Airflow UI: http://localhost:8080 (training credentials: admin/admin; change if reused outside the lab).



-----------------------------------------------------------------------------------------------------------------------



DSS150P Pipeline Execution Commands
1. Environment & Infrastructure Setup
Activate the virtual environment and start the required PostgreSQL and Apache Airflow containers:

PowerShell:
# Activate virtual environment (Windows)
.venv\Scripts\activate
# Start PostgreSQL database
docker compose up -d
# Start Airflow orchestration
docker compose -f docker-compose.airflow.yml up -d
# Verify all containers are running
docker ps

2. CLI Pipeline Execution
Run the modular Python pipeline using the custom CLI:

PowerShell:
# Validate environment variables and connections
python -m src.cli validate-env
# Run the complete end-to-end ETL pipeline
python -m src.cli run-all
# Run an idempotent PostgreSQL load independently
python -m src.cli load
# Generate Parquet vs CSV query performance benchmarks
python -m src.cli benchmark
# Execute a parameterized load for a specific partition

python -m src.cli load-partition --year 2026 --month 09
3. Infrastructure Teardown
Safely stop and remove the Docker containers when finished:
PowerShell:
docker compose down
docker compose -f docker-compose.airflow.yml down