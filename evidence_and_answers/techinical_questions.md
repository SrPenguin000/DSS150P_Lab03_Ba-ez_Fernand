1. Why record_hash is useful and exclusions:
- A record_hash provides a reliable, single-value fingerprint of a row's core business data. During a rerun, the pipeline can compare the incoming hash against the existing database hash to determine if an UPSERT genuinely needs to update the row or if it can be skipped, which guarantees idempotency and saves I/O. You should never include metadata generated during the pipeline execution—such as pipeline_run_id, created_at, updated_at, or extraction_timestamp—because these will change on every run, falsely tricking the system into thinking the business data has changed.

2. Preserving raw data:
- Raw data acts as an immutable source of truth. If a bug is discovered in the downstream transformation logic, or if business requirements change to include a previously dropped column, preserving the raw layer allows you to completely rebuild and backfill the staging and curated tables from scratch without losing historical context.

3. Data-quality rejection vs. system exception:
- A data-quality rejection is a controlled, expected business logic violation (e.g., a missing customer ID or a negative price); the pipeline successfully catches it, quarantines the bad row, and continues processing the rest. A system exception is a fatal infrastructure or code failure (e.g., a database connection timeout, missing source directory, or out-of-memory error) that crashes the pipeline task entirely and requires intervention or an Airflow retry.

4. Parquet vs. CSV performance:
- Parquet is a columnar storage format, while CSV is row-based. For analytical workloads that heavily aggregate specific fields (e.g., SUM(sales_amount)), Parquet allows the query engine to read only that specific column (column pruning) rather than scanning the entire dataset. Parquet also stores column-level statistics (min/max values) that allow the engine to skip entire data blocks that don't match the query filters (predicate pushdown), drastically reducing I/O.

5. Direct transformation logic in a DAG:
- Embedding heavy transformation code directly inside DAG files (like massive Pandas operations within a PythonOperator) tightly couples orchestration with business logic. This makes the transformations impossible to test outside of an Airflow environment, creates massive processing bottlenecks for the Airflow scheduler/workers, and violates modularity. DAGs should merely trigger independent, containerized scripts or SQL models.

6. Retries and idempotency:
- Retries rely on idempotency to be safe because a task might fail halfway through execution. If a pipeline is not idempotent (e.g., it uses blind INSERT statements), a network timeout halfway through a data load will cause the task to fail; when Airflow automatically retries it, the rows that were successfully loaded before the crash will be inserted a second time, resulting in double-counted revenue and duplicated records.

7. Aggressive partitioning trade-offs:
- Partitioning too aggressively (e.g., partitioning by day and hour for a very small dataset) creates the "small file problem." This generates massive metadata overhead for the query engine and file system. The database will spend vastly more time and compute resources simply opening, reading, and closing thousands of tiny individual files than it would scanning the actual data.

8. Adapting to an API/Database source:
- Because the pipeline is modular, only the extract module needs to be modified. You would update the extraction script to handle API pagination or execute a database query, then write the resulting output into the exact same raw standard format (e.g., saving the payload to data/raw). The transform, validate, and load tasks would require absolutely zero code changes.