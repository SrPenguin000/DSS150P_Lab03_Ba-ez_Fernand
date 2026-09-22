1. Explain how you would backfill a historical month if the DAG normally runs daily. You may demonstrate an Airflow backfill/test command if supported by your local version, but the required deliverable is a correct explanation of data interval, idempotency, and avoiding double loads.

- To backfill a historical month for a daily DAG, you instruct Airflow to execute the pipeline for those specific past dates. Airflow will sequentially process the DAG for each scheduled interval within that range as if it were running in the past.

If supported by your local environment, you can trigger a backfill across a historical date range using the Airflow CLI:
```bash
airflow dags backfill dss150p_sales_pipeline \
    --start-date 2025-12-01 \
    --end-date 2025-12-31