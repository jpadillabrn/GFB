# Ejercicio Adiconal

## SPARK en en el Entorno

Para no cargar los workers de Airflow se incluye un contenedor de SPARK en el entorno, se agrgaron los siguientes servicios, volúmenes y la variable de entorno en el archivo docker-compose.yaml.  

```yaml
  # Servicio
  spark-master:
    image: bitnami/spark:3.5
    container_name: spark-master
    environment:
      - SPARK_MODE=master
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
      - SPARK_MASTER_PORT=7077
      - SPARK_MASTER_WEBUI_PORT=8080
    ports:
      - "8081:8080"   # UI de Spark Master (mapeado a 8081 para no chocar con el API server)
      - "7077:7077"   # Puerto de comunicación del master
    volumes:
      - spark-data:/bitnami/spark/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/"]
      interval: 10s
      timeout: 5s
      retries: 5
    restart: unless-stopped

  spark-worker:
    image: bitnami/spark:3.5
    container_name: spark-worker
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=2G          # Ajusta según tus recursos
      - SPARK_WORKER_CORES=2
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
      - SPARK_WORKER_WEBUI_PORT=8081
    ports:
      - "8082:8081"   # UI del Worker (mapeado a 8082)
    depends_on:
      - spark-master
    volumes:
      - spark-data:/bitnami/spark/data
    restart: unless-stopped
```

Variable de Entorno  

```yaml
# Variable de entorno en environment de x-airflow-common
SPARK_MASTER: spark://spark-master:7077
```  
Volumen 
```yaml
# Agrega al final del archivo, en la sección volumes
spark-data:
```