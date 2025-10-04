FROM apache/airflow:2.10.3

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
    && apt-get clean

# Copia tus dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

USER airflow
