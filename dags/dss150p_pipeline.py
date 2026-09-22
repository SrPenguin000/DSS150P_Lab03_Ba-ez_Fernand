from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
import logging

def failure_callback(context):
    ti = context['task_instance']
    logging.error(f"FAILURE ALERT: Task '{ti.task_id}' in Run '{ti.run_id}' failed after {ti.try_number - 1} retries.")
    print(f"PIPELINE ALERT: Encountered failure in task {ti.task_id}. Execution halted.")

DEFAULT_ARGS = {
    'owner': 'dss150p',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=10),
    'on_failure_callback': failure_callback,
}

with DAG(
    dag_id='dss150p_sales_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule='0 2 * * *',
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        'run_mode': Param('full', enum=['full', 'partition']),
        'year': Param(2026, type='integer'),
        'month': Param(1, type='integer', minimum=1, maximum=12),
    },
    tags=['DSS150P'],
) as dag:
    
    extract = BashOperator(
        task_id='extract',
        bash_command='cd /opt/airflow/project && PIPELINE_RUN_ID="{{ run_id }}" python -m src.cli extract',
    )
    
    transform = BashOperator(
        task_id='transform',
        bash_command='cd /opt/airflow/project && PIPELINE_RUN_ID="{{ run_id }}" python -m src.cli transform',
    )
    
    load = BashOperator(
        task_id='load',
        bash_command='cd /opt/airflow/project && PIPELINE_RUN_ID="{{ run_id }}" python -m src.cli {% if params.run_mode == "full" %}load{% else %}load-partition --year {{ params.year }} --month {{ params.month }}{% endif %}',
    )
    
    validate = BashOperator(
        task_id='validate',
        bash_command='cd /opt/airflow/project && PIPELINE_RUN_ID="{{ run_id }}" python -m src.cli validate',
    )

    extract >> transform >> load >> validate