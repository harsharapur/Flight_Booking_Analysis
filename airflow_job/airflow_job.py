from datetime import datetime, timedelta
import uuid
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 9, 13),
}


def resolve_variables(**context):
    """
    Fetch all Airflow Variables at TASK RUNTIME (not DAG parse time).
    This prevents parse-time crashes when variables are not yet imported.
    Pushes config values to XCom for downstream tasks.
    """
    env = Variable.get("env", default_var="dev")
    gcs_bucket = Variable.get("gcs_bucket", default_var="airflow-projects-dev-hr")
    bq_project = Variable.get("bq_project", default_var="ivory-signer-496620-s1")
    bq_dataset = Variable.get("bq_dataset", default_var=f"flight_data_{env}")
    tables = Variable.get("tables", deserialize_json=True)

    transformed_table = tables["transformed_table"]
    route_insights_table = tables["route_insights_table"]
    origin_insights_table = tables["origin_insights_table"]

    batch_id = f"flight-booking-batch-{env}-{str(uuid.uuid4())[:8]}"

    context["ti"].xcom_push(key="env", value=env)
    context["ti"].xcom_push(key="gcs_bucket", value=gcs_bucket)
    context["ti"].xcom_push(key="bq_project", value=bq_project)
    context["ti"].xcom_push(key="bq_dataset", value=bq_dataset)
    context["ti"].xcom_push(key="transformed_table", value=transformed_table)
    context["ti"].xcom_push(key="route_insights_table", value=route_insights_table)
    context["ti"].xcom_push(key="origin_insights_table", value=origin_insights_table)
    context["ti"].xcom_push(key="batch_id", value=batch_id)


def run_dataproc_batch(**context):
    """
    Dynamically build the Dataproc batch config from XCom values at runtime
    and submit the Dataproc Serverless batch job programmatically.
    """
    from google.cloud import dataproc_v1

    ti = context["ti"]
    env = ti.xcom_pull(task_ids="resolve_variables", key="env")
    gcs_bucket = ti.xcom_pull(task_ids="resolve_variables", key="gcs_bucket")
    bq_project = ti.xcom_pull(task_ids="resolve_variables", key="bq_project")
    bq_dataset = ti.xcom_pull(task_ids="resolve_variables", key="bq_dataset")
    transformed_table = ti.xcom_pull(task_ids="resolve_variables", key="transformed_table")
    route_insights_table = ti.xcom_pull(task_ids="resolve_variables", key="route_insights_table")
    origin_insights_table = ti.xcom_pull(task_ids="resolve_variables", key="origin_insights_table")
    batch_id = ti.xcom_pull(task_ids="resolve_variables", key="batch_id")

    client = dataproc_v1.BatchControllerClient(
        client_options={"api_endpoint": "us-central1-dataproc.googleapis.com:443"}
    )

    batch = dataproc_v1.Batch()
    batch.pyspark_batch = dataproc_v1.PySparkBatch(
        main_python_file_uri=f"gs://{gcs_bucket}/flight-booking-analysis/spark-job/spark_transformation_job.py",
        args=[
            f"--env={env}",
            f"--bq_project={bq_project}",
            f"--bq_dataset={bq_dataset}",
            f"--transformed_table={transformed_table}",
            f"--route_insights_table={route_insights_table}",
            f"--origin_insights_table={origin_insights_table}",
        ],
    )
    batch.runtime_config = dataproc_v1.RuntimeConfig(version="2.2")
    batch.environment_config = dataproc_v1.EnvironmentConfig(
        execution_config=dataproc_v1.ExecutionConfig(
            service_account="55396527919-compute@developer.gserviceaccount.com",
            network_uri=f"projects/{bq_project}/global/networks/default",
            subnetwork_uri=f"projects/{bq_project}/regions/us-central1/subnetworks/default",
        )
    )

    request = dataproc_v1.CreateBatchRequest(
        parent=f"projects/{bq_project}/locations/us-central1",
        batch=batch,
        batch_id=batch_id,
    )

    operation = client.create_batch(request=request)
    print(f"Submitted Dataproc batch job: {batch_id}")
    result = operation.result()  # Wait for batch to complete
    print(f"Batch job completed: {result.name}")


# Define the DAG
with DAG(
    dag_id="flight_booking_dataproc_bq_dag",
    default_args=default_args,
    schedule_interval=None,  # Trigger manually or on-demand
    catchup=False,
) as dag:

    # Task 1: Resolve all Airflow Variables at runtime (avoids parse-time failures)
    resolve_vars_task = PythonOperator(
        task_id="resolve_variables",
        python_callable=resolve_variables,
    )

    # Task 2: File Sensor — wait for CSV to land in GCS
    # bucket/object resolved at runtime via Jinja from XCom
    file_sensor = GCSObjectExistenceSensor(
        task_id="check_file_arrival",
        bucket="{{ task_instance.xcom_pull(task_ids='resolve_variables', key='gcs_bucket') }}",
        object="{{ 'flight-booking-analysis/source-' ~ task_instance.xcom_pull(task_ids='resolve_variables', key='env') ~ '/flight_booking.csv' }}",
        google_cloud_conn_id="google_cloud_default",
        timeout=300,
        poke_interval=30,
        mode="poke",
    )

    # Task 3: Submit Dataproc Serverless batch job at runtime
    pyspark_task = PythonOperator(
        task_id="run_spark_job_on_dataproc_serverless",
        python_callable=run_dataproc_batch,
    )

    # Task Dependencies
    resolve_vars_task >> file_sensor >> pyspark_task