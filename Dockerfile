FROM apache/airflow:2.8.4-python3.10

# Instala compiladores y dependencias del sistema necesarias
USER root
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev \
    libjpeg-dev \
    zlib1g-dev \
    libfreetype6-dev \
    libpng-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Cambia al usuario airflow
USER airflow

# Copia e instala las dependencias de Python
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user -r /requirements.txt