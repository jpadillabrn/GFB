FROM apache/airflow:3.2.0

# Cambiar a root para instalar paquetes del sistema
USER root
RUN apt-get update && apt-get install -y openjdk-17-jdk-headless && rm -rf /var/lib/apt/lists/*

# Volver al usuario airflow
USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt