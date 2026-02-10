"""
Airflow DAG for Flight Price Pipeline
"""
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from src.database import get_mysql_engine, get_postgres_engine
from src.ingestion import load_csv_to_mysql, get_staging_data_for_validation
from src.validation import validate_data_quality
from src.transformation import clean_and_enrich
from src.kpi_calculator import compute_all_kpis, save_kpis_to_postgres


def check_csv_exists(**context):
    csv_path = os.getenv("CSV_INPUT_PATH", "/opt/airflow/data/Flight_Price_Dataset_of_Bangladesh.csv")
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    return csv_path


def ingest_to_staging(**context):
    csv_path = context['ti'].xcom_pull(task_ids="check_csv_exists")
    mysql_engine = get_mysql_engine()
    metadata = load_csv_to_mysql(csv_path, "raw_flight_data", mysql_engine)
    mysql_engine.dispose()
    return metadata


def validate_data(**context):
    mysql_engine = get_mysql_engine()
    df = get_staging_data_for_validation(mysql_engine)
    report = validate_data_quality(df)
    mysql_engine.dispose()
    
    # Persist data quality results to PostgreSQL
    try:
        checks = report.checks_performed
        if checks:
            import pandas as pd
            postgres_engine = get_postgres_engine()
            checks_df = pd.DataFrame(checks)
            checks_df["records_processed"] = report.total_records
            checks_df["records_valid"] = checks_df["records_passed"]
            checks_df["records_invalid"] = checks_df["records_failed"]
            checks_df["execution_timestamp"] = report.validation_timestamp
            checks_df = checks_df[[
                "check_name",
                "check_type",
                "records_processed",
                "records_valid",
                "records_invalid",
                "error_message",
                "execution_timestamp"
            ]]
            checks_df.to_sql(
                "data_quality_metrics",
                con=postgres_engine,
                if_exists="append",
                index=False,
                method="multi"
            )
            postgres_engine.dispose()
    except Exception:
        pass
    return report.to_dict()


def transform_data(**context):
    mysql_engine = get_mysql_engine()
    df = get_staging_data_for_validation(mysql_engine)
    mysql_engine.dispose()

    transformed = clean_and_enrich(df)
    return transformed.to_dict(orient="records")


def compute_kpis(**context):
    records = context['ti'].xcom_pull(task_ids="transform_data")
    if not records:
        return {}
    df = None
    try:
        import pandas as pd
        df = pd.DataFrame(records)
    except Exception:
        return {}

    kpis = compute_all_kpis(df, top_routes=int(os.getenv("TOP_ROUTES_LIMIT", "10")))
    return {k: v.to_dict(orient="records") for k, v in kpis.items()}


def load_to_postgres(**context):
    records = context['ti'].xcom_pull(task_ids="transform_data")
    kpis_dict = context['ti'].xcom_pull(task_ids="compute_kpis")

    if records:
        import pandas as pd
        df = pd.DataFrame(records)
        postgres_engine = get_postgres_engine()
        df.to_sql("flights_enriched", con=postgres_engine, if_exists="append", index=False, method="multi")
        postgres_engine.dispose()

    if kpis_dict:
        import pandas as pd
        postgres_engine = get_postgres_engine()
        kpi_frames = {k: pd.DataFrame(v) for k, v in kpis_dict.items() if v}
        save_kpis_to_postgres(kpi_frames, postgres_engine)
        postgres_engine.dispose()


def generate_report(**context):
    return {"status": "completed", "timestamp": datetime.utcnow().isoformat()}


default_args = {
    "owner": os.getenv("PIPELINE_OWNER", "analytics-team"),
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="flight_price_pipeline",
    default_args=default_args,
    description="End-to-end pipeline for Bangladesh flight prices",
    schedule_interval=os.getenv("SCHEDULE_INTERVAL", "@daily"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["flight", "bangladesh", "kpi"]
) as dag:

    check_csv = PythonOperator(
        task_id="check_csv_exists",
        python_callable=check_csv_exists
    )

    ingest = PythonOperator(
        task_id="ingest_to_staging",
        python_callable=ingest_to_staging
    )

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    compute = PythonOperator(
        task_id="compute_kpis",
        python_callable=compute_kpis
    )

    load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres
    )

    report = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report
    )

    done = EmptyOperator(
        task_id="pipeline_complete"
    )

    check_csv >> ingest >> validate >> transform >> compute >> load >> report >> done
