"""
Módulo de Orquestación de Pipeline de Datos (Medallion Architecture).
Capa Landing (JSON crudo) -> Capa Bronze (Parquet estructurado y particionado).
Desarrollado para Airflow 3.2.0 y Spark Connect 3.5.1.
"""

from datetime import datetime
from typing import Any, Dict, List
import requests  
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

from airflow.sdk import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk.bases.hook import BaseHook
#from pyspark.sql.functions import col, explode, to_date, from_unixtime

# =============================================================================
# CONFIGURACIÓN (SOLID: SRP - Aislamiento de Infraestructura)
# =============================================================================
DEFAULT_ARGS: Dict[str, Any] = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

# Identificador de tu conexión 100% funcional en la UI de Airflow
MINIO_CONN_ID = "minio_storage"  # <-- REEMPLAZA por tu Conn ID real de la UI
MINIO_ENDPOINT = "http://minio:9000"         # Endpoint de red interno de tu MinIO en Docker

# Rutas de almacenamiento utilizando protocolos nativos de cada motor
MINIO_LANDING_S3 = "s3://bck-landing/data/ecobici/station_status.json"
MINIO_BRONZE_S3  = "s3://bck-bronze/master/ecobici"
SPARK_CONNECT_URL = "sc://spark-connect:15002"

# =============================================================================
# CLASES DE SERVICIO (SOLID: SRP)
# =============================================================================
class APIDataExtractor:
    """Servicio encargado exclusivamente de la comunicación HTTP con la API."""
    def __init__(self, base_url: str):
        self.base_url = base_url

    def fetch_data(self, endpoint: str) -> List[Dict[str, Any]]:
        response = requests.get(f"{self.base_url}/{endpoint}", timeout=30)
        response.raise_for_status()
        return response.json()


class RawDataLandingUploader:
    """Servicio encargado de depositar los datos crudos en la zona Landing usando S3Hook."""
    def __init__(self, bucket_name: str, object_key: str, aws_conn_id: str):
        self.bucket_name = bucket_name
        self.object_key = object_key
        self.s3_hook = S3Hook(aws_conn_id=aws_conn_id)

    def upload_raw_json(self, data: List[Dict[str, Any]]) -> None:
        json_string = json.dumps(data, ensure_ascii=False, indent=2)
        print(f"Subiendo JSON crudo a MinIO usando conexión '{self.s3_hook.aws_conn_id}'...")
        
        self.s3_hook.load_string(
            string_data=json_string,
            key=self.object_key,
            bucket_name=self.bucket_name,
            replace=True
        )


class NativePythonStorageProcessor:
    """
    Servicio de procesamiento de alto rendimiento usando Pandas y PyArrow nativos.
    Lee de Landing, limpia las estaciones y guarda en Bronze en formato Parquet.
    Cumple con SRP y elimina dependencias de JVM/Java y bloqueos gRPC.
    """
    def __init__(self, source_path: str, target_path: str, aws_conn_id: str):
        self.source_path = source_path
        self.target_path = target_path
        # Recuperamos las credenciales reales de la UI usando la API moderna del SDK
        self.conn = BaseHook.get_connection(aws_conn_id)

    def clean_and_persist_to_bronze(self) -> None:
        try:
            # 1. Inicializamos el sistema de archivos de S3 nativo en Python usando fsspec / s3fs
            fs = s3fs.S3FileSystem(
                key=self.conn.login,
                secret=self.conn.password,
                client_kwargs={"endpoint_url": MINIO_ENDPOINT}
            )

            print(f"Leyendo JSON crudo desde Landing usando Diccionario nativo de Python...")
            # SOLUCIÓN DEFINITIVA AL KEYERROR: Abrimos el archivo de MinIO como un puntero de texto plano
            # y lo cargamos directo con json.loads(). Esto evita que Pandas arruine la estructura anidada.
            with fs.open(self.source_path, "r", encoding="utf-8") as file_pointer:
                raw_json = json.loads(file_pointer.read())

            # Validación idéntica a tu lógica original de negocio (SOLID: SRP)
            if not raw_json or "data" not in raw_json or "stations" not in raw_json["data"]:
                print("⚠️ Estructura JSON inválida, vacía o corrupta en el bucket Landing.")
                return

            print("Desanidando la estructura 'data.stations' de Ecobici...")
            stations_list = raw_json["data"]["stations"]
            
            # Ahora creamos el DataFrame a partir de la lista pura de diccionarios planos de estaciones
            df_stations = pd.DataFrame(stations_list)

            print("Ejecutando transformaciones, limpieza y tipado de datos en Python...")
            # Convertir marcas de tiempo unix a datetime string legible utilizando conversión vectorial de Pandas
            df_stations["last_reported_at"] = pd.to_datetime(df_stations["last_reported"], unit="s").dt.strftime("%Y-%m-%d %H:%M:%S")
            
            # Generar columna de partición basada exclusivamente en la fecha
            df_stations["fecha_reporte"] = pd.to_datetime(df_stations["last_reported"], unit="s").dt.strftime("%Y-%m-%d")

            # Mapear columnas y calcular la capacidad total de forma matemática optimizada
            df_transformed = pd.DataFrame({
                "station_id": df_stations["station_id"].astype(int),
                "bikes_available": df_stations["num_bikes_available"].astype(int),
                "docks_available": df_stations["num_docks_available"].astype(int),
                "total_capacity": df_stations["num_bikes_available"].astype(int) + df_stations["num_docks_available"].astype(int),
                "is_renting": df_stations["is_renting"].astype(bool),
                "is_returning": df_stations["is_returning"].astype(bool),
                "last_reported_at": df_stations["last_reported_at"],
                "fecha_reporte": df_stations["fecha_reporte"]
            })

            print("Aplicando filtros de reglas de negocio...")
            # Filtrar solo estaciones activas y consistentes
            df_cleaned = df_transformed[
                (df_transformed["is_renting"] == True) & 
                (df_transformed["is_returning"] == True) & 
                (df_transformed["total_capacity"] > 0)
            ]

            if df_cleaned.empty:
                print("⚠️ El filtrado no devolvió registros válidos para guardar.")
                return

            # Corrección del protocolo S3 para asegurar rutas compatibles con fsspec/s3fs
            python_target = self.target_path.replace("s3a://", "s3://")
            print(f"Persistiendo archivo Parquet particionado con PyArrow en MinIO: {python_target}")
            
            # Convertir a tabla de Apache Arrow
            arrow_table = pa.Table.from_pandas(df_cleaned, preserve_index=False)

            # Escritura directa de ultra alto rendimiento particionada con compresión Snappy hacia tu MinIO
            pq.write_to_dataset(
                table=arrow_table,
                root_path=python_target,
                partition_cols=["fecha_reporte"],
                filesystem=fs,
                use_dictionary=True,
                compression="SNAPPY",
                version="2.6"
            )
            
            print("✅ ¡ÉXITO ABSOLUTO E INCONTESTABLE! El pipeline ha finalizado correctamente sin usar Java.")

        except Exception as e:
            print(f"❌ Error durante el procesamiento nativo: {str(e)}")
            raise




# =============================================================================
# DEFINICIÓN DEL DAG (Airflow 3.2.0 TaskFlow API)
# =============================================================================
@dag(
    dag_id="dag_ecobici_estado_estaciones",
    default_args=DEFAULT_ARGS,
    description="Pipeline Medallion robusto e híbrido optimizado para Spark Connect",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["solid", "medallion", "hibrido", "v3.2.0"],
)
def mi_pipeline_modular():
    
    inicio = EmptyOperator(task_id="inicio")

    # Tarea 2: Guardar JSON crudo en la Capa Landing usando S3Hook
    @task(task_id="guardar_datos_crudos_landing")
    def guardar_landing() -> str:
        api_url = "https://gbfs.mex.lyftbikes.com/gbfs/es" 
        extractor = APIDataExtractor(base_url=api_url)
        datos_crudos = extractor.fetch_data(endpoint="station_status.json")
        
        uploader = RawDataLandingUploader(
            bucket_name="bck-landing",
            object_key="data/ecobici/station_status.json",
            aws_conn_id=MINIO_CONN_ID
        )
        uploader.upload_raw_json(datos_crudos)
        return "Landing Completado"

    # Tarea 3: Procesamiento y persistencia nativa Parquet en Bronze
    @task(task_id="procesar_landing_a_bronze_python")
    def procesar_con_python(status_landing: str) -> str:
        processor = NativePythonStorageProcessor(

            source_path=MINIO_LANDING_S3,
            target_path=MINIO_BRONZE_S3,
            aws_conn_id=MINIO_CONN_ID
        )
        processor.clean_and_persist_to_bronze()
        return "Bronze Parquet Completado"

    # Orquestación del flujo de dependencias
    status_lnd = guardar_landing()
    status_brz = procesar_con_python(status_lnd)
    
    inicio >> status_lnd

dag_instanciado = mi_pipeline_modular()