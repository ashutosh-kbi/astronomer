from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pendulum import datetime

# Simple python functions  
def task_1_func():
    print("This is task 1")

def task_2_func():
    print("This is task 2")
from custom_calendar_timetables import BusinessCalendarTimetableNextDay

holidays=["2026-01-23", "2026-01-26","2026-12-28"]
# Define DAG
with DAG(
    dag_id="test_cc_catchup_true_savings_run_8",
    start_date=datetime(2026, 1, 15),
    schedule=BusinessCalendarTimetableNextDay(holidays=holidays,hour=9, minute=0, second=0, timezone="Europe/London"),
    catchup=True
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