from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 5, 1),
    "retries": 0,
}

with DAG(
    dag_id="dag_spark_test_calcula_pi",
    default_args=default_args,
    schedule=None,
    catchup=False,
    description="DAG de prueba para Spark en cluster externo",
    tags=["spark", "test"],
) as dag:

    spark_pi = SparkSubmitOperator(
        task_id="spark_pi",
        application="/opt/airflow/dags/spark_pi.py",  # Ruta dentro del contenedor
        conn_id="spark_default",                       # Usa la conexión Spark (ver abajo)
        application_args=["5"],                         # Número de particiones
        total_executor_cores=2,
        executor_memory="1G",
        name="test_pi",
        verbose=True,
    )

    spark_pi