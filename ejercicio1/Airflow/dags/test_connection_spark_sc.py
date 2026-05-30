from pyspark.sql.connect.session import SparkSession
import sys

def test_spark_connect():
    remote_url = "sc://spark-master:7077"
    print(f"Intentando conectar a {remote_url}...")
    
    try:
        # 1. Crear la sesión
        spark = SparkSession.builder.remote(remote_url).getOrCreate()
        
        # 2. Ejecutar una operación simple para validar la comunicación gRPC
        # Si esto funciona, la conexión es exitosa
        df = spark.range(10)
        count = df.count()
        
        print(f"✅ ¡Conexión exitosa! El servidor devolvió el conteo: {count}")
        print(f"   Versión del servidor: {spark.version}")
        return True

    except Exception as e:
        print(f"❌ Error de conexión: {type(e).__name__}")
        print(f"   Detalle: {str(e)}")
        print("\nPosibles causas:")
        print("  - El servidor Spark Connect no está ejecutándose.")
        print("  - El puerto 15002 está bloqueado por un firewall.")
        print("  - La URL es incorrecta.")
        return False

if __name__ == "__main__":
    success = test_spark_connect()
    sys.exit(0 if success else 1)   