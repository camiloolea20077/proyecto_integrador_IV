# --- load.py (o donde tengas load) ---
from typing import Dict
from pandas import DataFrame
from sqlalchemy.engine.base import Engine

def load(data_frames: Dict[str, DataFrame], database: Engine):
    """Carga cada DataFrame del diccionario como una tabla en la base de datos.

    Para el nombre de tabla, se usa la clave del diccionario.
    Si la tabla existe, se reemplaza.
    """
    for table_name, df in data_frames.items():
        df.to_sql(
            name=table_name,
            con=database,
            if_exists="replace",
            index=False
        )
