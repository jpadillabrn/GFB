"""
DAG: ejemplo_dag_4_tareas (Refactorizado con diagnóstico y transformación separados)
=====================================================================================
Flujo:
1. Diagnóstico del CSV en MinIO → retorna contenido crudo.
2. Transformación del CSV (nulos, tipos) → retorna JSON limpio.
3. Escritura del DataFrame a Parquet en bucket bronze.
4. Creación de esquema y tabla externa en Trino.
5. Consulta de validación.
"""

from datetime import datetime, timedelta
import io
import pandas as pd

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import ObjectStoragePath
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# -------------------------------------------------------------------
# Configuración centralizada
# -------------------------------------------------------------------
MINIO_CONN_ID = "minio_storage"       # Connection ID MinIO (S3)
BUCKET_LANDING = "bck-landing"        # Bucket origen
OBJECT_KEY = "data/data_muestra.csv"  # Ruta del CSV

TRINO_CONN_ID = "trino_bronze"        # Connection ID Trino
BUCKET_BRONZE = "bck-bronze"
SCHEMA_NAME = "bronze.prueba"
TABLE_NAME = "bronze.prueba.tbl_data"
OUTPUT_PARQUET_KEY = "master/data_prueba_tecnica.parquet"
OUTPUT_PARQUET_PATH = f"s3://{BUCKET_BRONZE}/{OUTPUT_PARQUET_KEY}"

# -------------------------------------------------------------------
# Argumentos del DAG
# -------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# -------------------------------------------------------------------
# DAG
# -------------------------------------------------------------------
with DAG(
    dag_id="dag_etl_engineer_challenge",
    default_args=default_args,
    description="DAG Prueba Tecnica ETL",
    schedule=None,
    start_date=datetime(2026, 5, 27),
    catchup=False,
    tags=["Prueba Tecnica", "MinIO", "Refactorizado", "Diagnóstico", "Transformación"],
) as dag:

    inicio = EmptyOperator(task_id="inicio")
    fin = EmptyOperator(task_id="fin")

    
    # -------------------------------------------------------------------
    # 1. Diagnóstico del CSV (lectura + reporte)
    # -------------------------------------------------------------------
    @task(task_id="leer_csv")
    def leer_csv(
        conn_id: str = MINIO_CONN_ID,
        bucket_origen: str = BUCKET_LANDING,
        key_origen: str = OBJECT_KEY,
    ) -> str:
        """
        Lee el archivo CSV desde MinIO y emite un diagnóstico completo:
        - Lista de archivos en el bucket.
        - Dimensiones, tipos de datos, nulos y estadísticas básicas.
        Retorna el contenido crudo del CSV (texto) para la siguiente tarea.
        """
        hook = S3Hook(aws_conn_id=conn_id)

        # Listar archivos (informativo)
        keys = hook.list_keys(bucket_name=bucket_origen)
        print(f"Archivos en {bucket_origen}: {keys}")

        # Leer el archivo como texto
        csv_text = hook.read_key(key_origen, bucket_name=bucket_origen)

        # Convertir temporalmente a DataFrame solo para diagnóstico
        df_diag = pd.read_csv(io.StringIO(csv_text))
        filas, columnas = df_diag.shape
        print(f"\n--- DIAGNÓSTICO DEL CSV ---")
        print(f"Dimensiones: {filas} filas x {columnas} columnas")
        print("\nTipos de datos y conteo de no nulos:")
        df_diag.info()
        print("\nValores nulos por columna:")
        print(df_diag.isnull().sum())
        print("\nEstadísticas básicas (numéricas):")
        print(df_diag.describe())
        print("--- FIN DIAGNÓSTICO ---\n")

        # Devolver el texto original para evitar serializaciones pesadas
        return csv_text

    # -------------------------------------------------------------------
    # 2. Transformación del CSV
    # -------------------------------------------------------------------
    @task(task_id="transformar_csv")
    def transformar_csv(csv_text: str) -> str:
        """
        Recibe el contenido crudo del CSV, lo convierte a DataFrame,
        aplica las transformaciones necesarias y retorna el DataFrame
        limpio serializado como JSON (orient='records').
        """
        # Reconstruir DataFrame
        df = pd.read_csv(io.StringIO(csv_text))

        # Aplicar transformaciones 
        df["amount"] = df["amount"].fillna(0)
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
        df["paid_at"] = pd.to_datetime(df["paid_at"], errors="coerce")

        # Limpieza
        valores_raros = ['0xFFFF', '', ' ', None] 
        df['status'] = df['status'].replace(valores_raros, 'desconocido')
        df['status'] = df['status'].fillna('desconocido')

        print("Transformaciones aplicadas. DataFrame final:")
        df.info()

        # Agrupar por name y create_at
        # Agrupar por nombre y fecha exacta
        df_agrupado = df.groupby(['name', 'created_at']).agg(
            total_amount=('amount', 'sum'),       # Suma de montos
            conteo_transacciones=('id', 'count'), # Cantidad de registros
            promedio=('amount', 'mean')           # Promedio (opcional)
            ).reset_index()

        print(df_agrupado)
        df_agrupado.info()


        # Serializar a JSON para la siguiente etapa
        return df_agrupado.to_json(orient="records", date_format="iso")

    # -------------------------------------------------------------------
    # 3. Guardar como Parquet
    # -------------------------------------------------------------------
    @task(task_id="guardar_parquet")
    def guardar_parquet(
        json_data: str,
        conn_id: str = MINIO_CONN_ID,
        ruta_salida: str = OUTPUT_PARQUET_PATH,
    ) -> None:
        """
        Reconstruye un DataFrame a partir del JSON transformado,
        lo convierte a Parquet y lo guarda en la ruta especificada.
        """
        df = pd.read_json(json_data, orient="records")

        # Aseguramos que las fechas mantengan el tipo correcto
        if "created_at" in df.columns:
            df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
        # if "paid_at" in df.columns:
        #     df["paid_at"] = pd.to_datetime(df["paid_at"], errors="coerce")

        print("DataFrame listo para escritura:")
        df.info()

        output_path = ObjectStoragePath(ruta_salida, conn_id=conn_id)
        with output_path.open("wb") as f:
            df.to_parquet(f, index=False)

        print(f"Archivo Parquet guardado en: {output_path}")

    # -------------------------------------------------------------------
    # Operadores SQL para Trino
    # -------------------------------------------------------------------
    crear_esquema = SQLExecuteQueryOperator(
        task_id="crear_esquema_trino",
        conn_id=TRINO_CONN_ID,
        sql=f"""
            CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}
            WITH (location = 's3a://{BUCKET_BRONZE}/');
        """,
        doc="Crea el esquema 'bronze.prueba' en Trino, apuntando al bucket bronze.",
    )

    crear_tabla = SQLExecuteQueryOperator(
        task_id="crear_tabla_parquet_trino",
        conn_id=TRINO_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (                
                name VARCHAR,
                created_at TIMESTAMP,
                total_amount DOUBLE,
                conteo_transacciones INTEGER,
                promedio DOUBLE
            )
            WITH (
                format = 'PARQUET',
                external_location = 's3a://{BUCKET_BRONZE}/master/'
            );
        """,
        doc="Crea la tabla externa 'tbl_data' sobre los Parquet en 'master'.",
    )

    consultar_tabla = SQLExecuteQueryOperator(
        task_id="consultar_tbl_data",
        conn_id=TRINO_CONN_ID,
        sql="""
            SELECT
                name,
                created_at,
                total_amount,
                conteo_transacciones,
                promedio

            FROM bronze.prueba.tbl_data;
        """,
        doc="Consulta de validación: registros únicos por nombre y estado.",
    )

    # -------------------------------------------------------------------
    # Instanciación y dependencias
    # -------------------------------------------------------------------    
    
    t_diag = leer_csv()
    t_trans = transformar_csv(csv_text=t_diag)
    t_parquet = guardar_parquet(json_data=t_trans)

    # Cadena de ejecución
    inicio >> t_diag >> t_trans >> t_parquet >> crear_esquema >> crear_tabla >> consultar_tabla >> fin