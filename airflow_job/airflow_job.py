from datetime import datetime, timedelta
import uuid
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# ─── DAG default arguments ───────────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 9, 13),
}


# ─── Task 1: Resolve Airflow Variables at RUNTIME ────────────────────────────
def resolve_variables(**context):
    """
    Fetch all Airflow Variables at task runtime (NOT at DAG parse time).
    This prevents parse-time crashes when variables are not yet imported.
    All resolved values are pushed to XCom for downstream tasks.
    """
    env = Variable.get("env", default_var="dev")
    gcs_bucket = Variable.get("gcs_bucket", default_var="airflow-projects-dev-hr")
    bq_project = Variable.get("bq_project", default_var="ivory-signer-496620-s1")
    bq_dataset = Variable.get("bq_dataset", default_var=f"flight_data_{env}")

    # tables variable — provide defaults to avoid KeyError if import missed
    tables = Variable.get(
        "tables",
        deserialize_json=True,
        default_var={
            "transformed_table": f"transformed_flight_data_{env}",
            "route_insights_table": f"route_insights_{env}",
            "origin_insights_table": f"origin_insights_{env}",
        },
    )

    transformed_table = tables["transformed_table"]
    route_insights_table = tables["route_insights_table"]
    origin_insights_table = tables["origin_insights_table"]

    batch_id = f"flight-booking-batch-{env}-{str(uuid.uuid4())[:8]}"

    ti = context["ti"]
    ti.xcom_push(key="env", value=env)
    ti.xcom_push(key="gcs_bucket", value=gcs_bucket)
    ti.xcom_push(key="bq_project", value=bq_project)
    ti.xcom_push(key="bq_dataset", value=bq_dataset)
    ti.xcom_push(key="transformed_table", value=transformed_table)
    ti.xcom_push(key="route_insights_table", value=route_insights_table)
    ti.xcom_push(key="origin_insights_table", value=origin_insights_table)
    ti.xcom_push(key="batch_id", value=batch_id)

    print(f"[resolve_variables] env={env}, bucket={gcs_bucket}, project={bq_project}")
    print(f"[resolve_variables] batch_id={batch_id}")


# ─── Task 3: Submit Dataproc Serverless batch job ────────────────────────────
def run_dataproc_batch(**context):
    """
    Pull resolved config from XCom and submit a Dataproc Serverless batch
    using the Airflow DataprocCreateBatchOperator hook pattern.
    Waits for job completion before marking task as success.
    """
    from airflow.providers.google.cloud.hooks.dataproc import DataprocHook

    ti = context["ti"]
    env = ti.xcom_pull(task_ids="resolve_variables", key="env")
    gcs_bucket = ti.xcom_pull(task_ids="resolve_variables", key="gcs_bucket")
    bq_project = ti.xcom_pull(task_ids="resolve_variables", key="bq_project")
    bq_dataset = ti.xcom_pull(task_ids="resolve_variables", key="bq_dataset")
    transformed_table = ti.xcom_pull(task_ids="resolve_variables", key="transformed_table")
    route_insights_table = ti.xcom_pull(task_ids="resolve_variables", key="route_insights_table")
    origin_insights_table = ti.xcom_pull(task_ids="resolve_variables", key="origin_insights_table")
    batch_id = ti.xcom_pull(task_ids="resolve_variables", key="batch_id")

    batch = {
        "pyspark_batch": {
            "main_python_file_uri": (
                f"gs://{gcs_bucket}/flight-booking-analysis/spark-job/spark_transformation_job.py"
            ),
            "args": [
                f"--env={env}",
                f"--bq_project={bq_project}",
                f"--bq_dataset={bq_dataset}",
                f"--transformed_table={transformed_table}",
                f"--route_insights_table={route_insights_table}",
                f"--origin_insights_table={origin_insights_table}",
            ],
        },
        "runtime_config": {"version": "2.2"},
        "environment_config": {
            "execution_config": {
                "service_account": "55396527919-compute@developer.gserviceaccount.com",
                "network_uri": f"projects/{bq_project}/global/networks/default",
                "subnetwork_uri": (
                    f"projects/{bq_project}/regions/us-central1/subnetworks/default"
                ),
            }
        },
    }

    hook = DataprocHook(gcp_conn_id="google_cloud_default")
    print(f"[run_dataproc_batch] Submitting batch job: {batch_id}")

    operation = hook.create_batch(
        project_id=bq_project,
        region="us-central1",
        batch=batch,
        batch_id=batch_id,
    )

    # Wait for the Dataproc job to complete
    result = operation.result()
    print(f"[run_dataproc_batch] Batch job completed: {result.name}, state={result.state.name}")


# ─── DAG Definition ──────────────────────────────────────────────────────────
with DAG(
    dag_id="flight_booking_dataproc_bq_dag",
    default_args=default_args,
    description="Flight Booking: GCS sensor → Dataproc Serverless → BigQuery",
    schedule_interval=None,   # Triggered manually or via API
    catchup=False,
    tags=["flight-booking", "dataproc", "bigquery"],
) as dag:

    # Task 1: Resolve all Airflow Variables at runtime
    resolve_vars_task = PythonOperator(
        task_id="resolve_variables",
        python_callable=resolve_variables,
    )

    # Task 2: Wait for CSV to land in GCS before proceeding
    file_sensor = GCSObjectExistenceSensor(
        task_id="check_file_arrival",
        bucket="{{ task_instance.xcom_pull(task_ids='resolve_variables', key='gcs_bucket') }}",
        object="{{ 'flight-booking-analysis/source-' ~ task_instance.xcom_pull(task_ids='resolve_variables', key='env') ~ '/flight_booking.csv' }}",
        google_cloud_conn_id="google_cloud_default",
        timeout=300,
        poke_interval=30,
        mode="poke",
    )

    # Task 3: Submit PySpark job on Dataproc Serverless
    pyspark_task = PythonOperator(
        task_id="run_spark_job_on_dataproc_serverless",
        python_callable=run_dataproc_batch,
    )

    # ── Pipeline ──────────────────────────────────────────────────────────────
    resolve_vars_task >> file_sensor >> pyspark_task