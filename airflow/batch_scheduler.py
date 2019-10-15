import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta
from airflow.utils.trigger_rule import TriggerRule

schedule_interval = timedelta(days=1)

default_args = {
    'owner': 'HaoZheng',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['haozheng247@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    #'queue': 'bash_queue',
    #'pool': 'backfill',
    #'priority_weight': 10,
    #'end_date': datetime(2019, 10, 7),
    #'wait_for_downstream': False,
    #'dag': dag,
    #'adhoc': False,
    #'sla': timedelta(hours=2),
    #'execution_timeout': timedelta(seconds=300),
    #'on_failure_callback': callback_function,
    #'on_success_callback': callback_function,
    #'on_retry_callback': callback_function,
    #'trigger_rule': u'all_success'
}

dag = DAG(
    'batch_scheduler',
    default_args=default_args,
    description='DAG for batch processsing',
    schedule_interval=schedule_interval)

task = BashOperator(
    task_id='run_batch_processing',
    bash_command='cd /home/ubuntu/git/smartcargo/ ; ./spark-run.sh',
    dag=dag)

TaskSuccess = EmailOperator(
    dag = dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    task_id='TaskSuccess',
    to=['haozheng247@gmail.com'],
    subject="Batch processing is complete",
    html_content='<h3>Batch processing completed succesfully </h3>')

TaskSuccess.set_upstream([task])

TaskFailed = EmailOperator(
    dag = dag,
    trigger_rule=TriggerRule.ONE_FAILED,
    task_id='TaskFailed',
    to=['haozheng247@gmail.com'],
    subject="Batch processing task failed",
    html_content='<h3>Batch processing task failed </h3>')

TaskFailed.set_upstream([task])
