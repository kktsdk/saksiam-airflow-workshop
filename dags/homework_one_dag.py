from airflow import DAG
from airflow.utils import timezone
from airflow.operators.dummy import DummyOperator

def _say_hello():
    logging.info("Hello, SAKSIAM!")

default_args = {
    "owner": "kong kktsdk",
    "start_date": timezone.datetime(2021, 9, 27)
}

with DAG(
    "homework_one_dag",
    schedule_interval="*/10 * * * *",
    default_args= default_args,
    catchup=False,
    tags=["saksiam"],
) as dag:

    one = DummyOperator(task_id="1")
    two = DummyOperator(task_id="2")
    three = DummyOperator(task_id="3")
    four = DummyOperator(task_id="4")
    five = DummyOperator(task_id="5")
    six = DummyOperator(task_id="6")
    seven = DummyOperator(task_id="7")
    eight = DummyOperator(task_id="8")
    nine = DummyOperator(task_id="9")

    one >> [two, five]
    two >> [three, six]
    three >> four >> nine
    five >> [six, seven] >> eight >> nine