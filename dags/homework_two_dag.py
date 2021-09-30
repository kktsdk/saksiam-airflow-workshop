import logging

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

def _say_hello():
    logging.info("Hello, SAKSIAM!")

def _print_log_messages():
    logging.info("Log Messages!!!")

default_args = {
    "owner": "kong kktsdk",
    "start_date": timezone.datetime(2021, 9, 27)
}

with DAG(
    "homework_two_dag",
    schedule_interval="*/30 * * * *",
    default_args= default_args,
    catchup=False,
    tags=["saksiam"],
) as dag:

    start = DummyOperator(task_id="start")
    echo_hello = BashOperator(
        task_id="echo_hello",
        bash_command="echo 'hello'",
    )
    say_hello = PythonOperator(
        task_id="say_hello",
        python_callable=_say_hello,
    )
    print_log_messages = PythonOperator(
        task_id="print_log_messages",
        python_callable=_print_log_messages,
    )
    end = DummyOperator(task_id="end")

    start >> echo_hello >> say_hello >> print_log_messages >> end
