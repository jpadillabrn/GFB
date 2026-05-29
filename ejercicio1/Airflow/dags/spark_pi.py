# Archivo: spark_pi.py (dentro de Airflow/dags/)
import sys
from random import random
from operator import add
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PythonPi").getOrCreate()
    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    n = 100000 * partitions

    def f(_):
        x, y = random(), random()
        return 1 if x * x + y * y <= 1 else 0

    count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
    pi_val = 4.0 * count / n
    print(f"Pi is roughly {pi_val}")
    spark.stop()