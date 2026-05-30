"""
Módulo de Orquestación de Pipeline de Datos.

Este script define la estructura base de un DAG en Apache Airflow 3.2.0,
siguiendo principios de diseño SOLID y decoupling de configuración.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List
import requests  
import os

from airflow.sdk import dag , task
from airflow.providers.standard.operators.empty import EmptyOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# =============================================================================
# CONFIGURACIÓN (SOLID: SRP - Aislamiento de Configuración)
# =============================================================================
DEFAULT_ARGS: Dict[str, Any] = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# Configuración de infraestructura (MinIO)
MINIO_BUCKET_URL = "s3a://bck-bronze/ecobici"

os.environ["SPARK_REMOTE"] = "sc://spark-master:7077" # O usar .option("spark.remote", "...")


# =============================================================================
# CLASES DE SERVICIO (SOLID: SRP - Lógica de Negocio Pura fuera de Airflow)
# =============================================================================
class APIDataExtractor:
    """
    Servicio encargado exclusivamente de la comunicación con la API.
    Cumple con SRP: Si la URL o autenticación cambian, solo se modifica esta clase.
    """
    def __init__(self, base_url: str):
        self.base_url = base_url

    def fetch_data(self, endpoint: str) -> List[Dict[str, Any]]:
        """Realiza la petición HTTP y retorna los datos en formato crudo."""
        # Nota: En producción, se recomienda usar HttpHook de Airflow para manejar credenciales
        response = requests.get(f"{self.base_url}/{endpoint}", timeout=30)
        response.raise_for_status()
        return response.json()


class StationDataTransformer:
    """
    Clase responsable exclusivamente de transformar y limpiar el JSON de estaciones.
    No conoce nada de Airflow; es altamente testeable de forma aislada.
    """

    @staticmethod
    def clean_and_transform(raw_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Limpia, tipifica y enriquece los datos crudos de las estaciones.
        """
        # 1. Validación inicial del payload y desanidado
        if not raw_json or "data" not in raw_json or "stations" not in raw_json["data"]:
            return []

        stations = raw_json["data"]["stations"]
        cleaned_stations: List[Dict[str, Any]] = []

        for station in stations:
            # 2. Manejo de registros truncados/incompletos (ej. Estación 34)
            required_keys = {"station_id", "num_bikes_available", "num_docks_available", "last_reported"}
            if not required_keys.issubset(station.keys()):
                continue  # Ignora registros corruptos de la API

            # 3. Conversión de Marcas de Tiempo Unix a Datetime string legible
            try:
                dt_reported = datetime.fromtimestamp(station["last_reported"]).strftime("%Y-%m-%d %H:%M:%S")
            except (ValueError, OSError, OverflowError):
                dt_reported = None

            # 4. Cálculo de Métricas y Normalización Booleana
            total_bikes = station.get("num_bikes_available", 0) + station.get("num_bikes_disabled", 0)
            total_docks_slots = station.get("num_docks_available", 0) + station.get("num_docks_disabled", 0)
            
            cleaned_row = {
                "station_id": int(station["station_id"]),
                "bikes_available": int(station["num_bikes_available"]),
                "docks_available": int(station["num_docks_available"]),
                "total_capacity": total_bikes + total_docks_slots,
                "is_renting": bool(station.get("is_renting", 0)),
                "is_returning": bool(station.get("is_returning", 0)),
                "last_reported_at": dt_reported
            }
            cleaned_stations.append(cleaned_row)

        return cleaned_stations


class SparkParquetWriter:
    """
    Abstracción de almacenamiento en PySpark. 
    Se encarga de interactuar con el ecosistema Spark y escribir hacia MinIO.
    """
    
    def __init__(self, target_path: str):
        self.target_path = target_path
        # Nota: En entornos productivos, los parámetros de S3A se leen 
        # dinámicamente desde el hook de conexión de Airflow.
        self.spark = SparkSession.builder \
            .appName("Airflow-MinIO-Writer") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minio") \
            .config("spark.hadoop.fs.s3a.secret.key", "minio1234") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()

    def write_append_single_file(self, data: List[Dict[str, Any]]) -> None:
        """Convierte los datos a DataFrame, genera la partición y escribe un único archivo."""
        if not data:
            return

        # 1. Crear DataFrame de Spark
        df = self.spark.createDataFrame(data)

        # 2. Crear columna de partición basada exclusivamente en la fecha (sin hora)
        df_with_partition = df.withColumn("fecha_reporte", to_date(col("last_reported_at")))

        # 3. Coalesce(1) asegura un único archivo Parquet por partición física.
        #    Mode("append") añade el archivo a la ruta sin sobreescribir los históricos.
        df_with_partition.coalesce(1) \
            .write \
            .mode("append") \
            .partitionBy("fecha_reporte") \
            .parquet(self.target_path)


# =============================================================================
# DEFINICIÓN DEL DAG
# =============================================================================
@dag(
    dag_id="dag_ecobici_estado_estaciones",
    default_args=DEFAULT_ARGS,
    description="Un DAG modular que implementa principios SOLID en Airflow 3.2.0",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["solid", "template", "v3.2.0"],
)
def mi_pipeline_modular():
    """
    Función constructora del pipeline.
    Contiene la definición y el orden de ejecución de las tareas.
    """
    
    # -------------------------------------------------------------------------
    # Tarea 1: Inicio
    # -------------------------------------------------------------------------
    inicio = EmptyOperator(task_id="inicio")

    # -------------------------------------------------------------------------
    # Tarea 2: Extraer datos desde API (TaskFlow API + SOLID)
    # -------------------------------------------------------------------------
    @task(task_id="extraer_datos_api")
    def extraer_datos() -> List[Dict[str, Any]]:
        """
        Tarea que orquesta la extracción. El uso de @task maneja 
        automáticamente XCom para pasar los datos a la siguiente tarea.
        """
        # Inyección de dependencias / Configuración agnóstica
        api_url = "https://gbfs.mex.lyftbikes.com/gbfs/es" 
        extractor = APIDataExtractor(base_url=api_url)
        
        # Ejecución del servicio aislado
        datos_crudos = extractor.fetch_data(endpoint="station_status.json")
        return datos_crudos
    
    # -------------------------------------------------------------------------
    # Tarea 3: Limpiar y transformar datos (TaskFlow API)
    # -------------------------------------------------------------------------
    @task(task_id="limpiar_datos")
    def limpiar_datos(datos_crudos: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Tarea que actúa de puente entre Airflow (XCom) y nuestra capa lógica puramente SOLID.
        """
        # Inyección implícita del servicio de transformación
        datos_limpios = StationDataTransformer.clean_and_transform(datos_crudos)
        return datos_limpios
    
    # -------------------------------------------------------------------------
    # Tarea 4: Guardar en MinIO con PySpark
    # -------------------------------------------------------------------------
    @task(task_id="guardar_en_minio")
    def guardar_en_minio(datos_limpios: List[Dict[str, Any]]) -> None:
        """Instancia el escritor de Spark y ejecuta la persistencia."""
        writer = SparkParquetWriter(target_path=MINIO_BUCKET_URL)
        writer.write_append_single_file(data=datos_limpios)
    
    # Instanciamos la tarea TaskFlow
    datos_extraidos = extraer_datos()
    payload_limpio = limpiar_datos(datos_crudos=datos_extraidos)
    guardado_final = guardar_en_minio(datos_limpios=payload_limpio)

    # -------------------------------------------------------------------------
    # FLUJO DE DEPENDENCIAS
    # -------------------------------------------------------------------------
    # Conectamos el EmptyOperator con la tarea TaskFlow
    inicio >> datos_extraidos >> payload_limpio >> guardado_final

# Instanciación del flujo
dag_instanciado = mi_pipeline_modular()

