## Mandatory Imports
from dateutil import parser
from datetime import datetime, timedelta
from datetime import timezone
from airflow import DAG
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
## Only needed if migrating Oozie Hive Actions
from cloudera.cdp.airflow.operators.cdw_operator import CDWOperator
from airflow.operators.bash_operator import BashOperator

## Needed to declare the DAG
default_args = {
    'owner': 'your_username_here',
    'retry_delay': timedelta(seconds=5),
    'depends_on_past': False,
    'start_date': parser.isoparse('2021-05-25T07:33:37.393Z').replace(tzinfo=timezone.utc)
}

dag = DAG(
    'airflow-pipeline-demo',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    is_paused_upon_creation=False
)

## Spark Job - Declare one operator for each Spark Job

step1 = CDEJobRunOperator(
    task_id='enter_task_id',
    dag=dag,
    job_name='spark_cde_job_name'
)

step2 = CDEJobRunOperator(
    task_id='enter_task_id',
    dag=dag,
    job_name='spark_cde_job_name-claims-job'
)

## Optional: CDW Query

cdw_query = """
show databases;
"""

dw_step = CDWOperator(
    task_id='dataset-etl-cdw',
    dag=dag,
    cli_conn_id='cdw-hive-demo',
    hql=cdw_query,
    schema='default',
    ### CDW related args ###
    use_proxy_user=False,
    query_isolation=True
)


## Optional: Bash operator

bash_task = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    retries=3,
    dag=dag)

from airflow.models.baseoperator import BaseOperator

class HelloOperator(BaseOperator):
    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name

    def execute(self, context):
        message = f"Hello {self.name}"
        print(message)
        return message

hello = HelloOperator(task_id="sample-task", name="foo_bar", dag=dag)

## Mandatory: declare the sequence of execution. Notice we are referencing each Operator from above.
step1 >> step2 >> dw_step >> bash_task >> hello
