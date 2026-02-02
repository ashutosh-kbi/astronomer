from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pendulum import datetime

# Simple python functions
def task_1_func():
    print("This is task 1")

def task_2_func():
    print("This is task 2")
from custom_calendar_timetables import BusinessCalendarTimetable
holidays=["2026-01-29", "2026-02-03"]
# Define DAG
with DAG(
    dag_id="test_custom_calendar_catchup_false_run_2",
    start_date=datetime(2026, 1, 20),
    schedule=BusinessCalendarTimetable(holidays=holidays,hour=9, minute=0, second=0, timezone="Europe/London"),
    catchup=False
) as dag:

    task_1 = PythonOperator(
        task_id="task_1",
        python_callable=task_1_func
    )

    task_2 = PythonOperator(
        task_id="task_2",
        python_callable=task_2_func
    )

    # Task dependency
    task_1 >> task_2