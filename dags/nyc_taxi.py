from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args={
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'nyc_taxi',
    default_args=default_args,
    description='NYC Taxi Analysis',
    schedule_interval="@monthly",
    start_date=datetime(2023, 11, 1),
    tags=['nyc_taxi'],
) as dag:
    run_intermediate = BashOperator(
        task_id="run_intermediate",
        bash_command="dbt run --project-dir /opt/airflow/dags/dbts/weekly_assignment --profiles-dir /opt/airflow/dags/dbts/weekly_assignment -select int_nyc_taxi_real_trip int_nyc_taxi_real_trip_2021",
    )
    test_intermediate = BashOperator(
        task_id="test_intermediate",
        bash_command="dbt test --project-dir /opt/airflow/dags/dbts/weekly_assignment --profiles-dir /opt/airflow/dags/dbts/weekly_assignment -select int_nyc_taxi_real_trip int_nyc_taxi_real_trip_2021",
    )
    run_mart = BashOperator(
        task_id="run_mart",
        bash_command="dbt run --project-dir /opt/airflow/dags/dbts/weekly_assignment --profiles-dir /opt/airflow/dags/dbts/weekly_assignment -select monthly_total_passengers_2021 monthly_transactions_per_payment_type_2021_ver2 monthly_transactions_per_payment_type_2021 monthly_trip_distance_per_rate_code_2021_ver2 monthly_trip_distance_per_rate_code_2021",
    )
    test_mart = BashOperator(
        task_id="test_mart",
        bash_command="dbt test --project-dir /opt/airflow/dags/dbts/weekly_assignment --profiles-dir /opt/airflow/dags/dbts/weekly_assignment -select monthly_total_passengers_2021 monthly_transactions_per_payment_type_2021_ver2 monthly_transactions_per_payment_type_2021 monthly_trip_distance_per_rate_code_2021_ver2 monthly_trip_distance_per_rate_code_2021",
    )
run_intermediate >> test_intermediate >> run_mart >> test_mart