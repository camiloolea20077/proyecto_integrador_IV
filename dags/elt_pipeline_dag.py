from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

# Agrega la ruta src al path de Python
BASE_DIR = "/opt/airflow/src"
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# Importa las funciones
try:
    from extract import extract
    from transform import run_queries
    from load import load
    from config import (
        DATASET_ROOT_PATH,
        PUBLIC_HOLIDAYS_URL,
        SQLITE_BD_ABSOLUTE_PATH,
        get_csv_to_table_mapping
    )
    from sqlalchemy import create_engine
except ImportError as e:
    raise ImportError(f"Error importando módulos desde {BASE_DIR}: {str(e)}")

default_args = {
    "owner": "camilo",
    "start_date": datetime(2025, 10, 1),
    "retries": 1,
}

def extract_task():
    """Ejecuta la extracción de datos desde CSVs"""
    csv_table_mapping = get_csv_to_table_mapping()
    result = extract(
        csv_folder=DATASET_ROOT_PATH,
        csv_table_mapping=csv_table_mapping,
        public_holidays_url=PUBLIC_HOLIDAYS_URL
    )
    print(f"Extraídas {len(result)} tablas: {list(result.keys())}")
    return result

def load_to_database_task(**context):
    """Carga los DataFrames extraídos a la base de datos SQLite"""
    ti = context['ti']
    dataframes = ti.xcom_pull(task_ids='extract_data')
    database = create_engine(f"sqlite:///{SQLITE_BD_ABSOLUTE_PATH}")
    for table_name, df in dataframes.items():
        df.to_sql(table_name, database, if_exists='replace', index=False)
        print(f"Tabla '{table_name}' cargada con {len(df)} registros")
    
    print(f"Total de {len(dataframes)} tablas cargadas en la base de datos")
    return True

def transform_task():
    """Ejecuta las transformaciones SQL sobre los datos"""
    database = create_engine(f"sqlite:///{SQLITE_BD_ABSOLUTE_PATH}")
    result = run_queries(database)
    print(f"Ejecutadas {len(result)} queries: {list(result.keys())}")
    return result

def load_results_task(**context):
    """Guarda los resultados de las transformaciones"""
    ti = context['ti']
    query_results = ti.xcom_pull(task_ids='transform_data')
    database= create_engine(f"sqlite:///{SQLITE_BD_ABSOLUTE_PATH}")
    result = load(data_frames=query_results, database=database)
    print("Resultados guardados exitosamente")
    return result

with DAG(
    "elt_pipeline_dag",
    default_args=default_args,
    description="Pipeline ELT para extraer, transformar y cargar datos de Olist",
    schedule_interval=None,
    catchup=False,
    tags=["elt", "pipeline", "olist"],
) as dag:

    extract_op = PythonOperator(
        task_id="extract_data",
        python_callable=extract_task,
    )

    load_to_db_op = PythonOperator(
        task_id="load_to_database",
        python_callable=load_to_database_task,
        provide_context=True,
    )

    transform_op = PythonOperator(
        task_id="transform_data",
        python_callable=transform_task,
    )

    load_results_op = PythonOperator(
        task_id="load_results",
        python_callable=load_results_task,
        provide_context=True,
    )

    extract_op >> load_to_db_op >> transform_op >> load_results_op