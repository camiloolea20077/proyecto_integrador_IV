from typing import Dict
import requests
from pandas import DataFrame, read_csv, to_datetime

def temp() -> DataFrame:
    """Get the temperature data."""
    return read_csv("data/temperature.csv")


def get_public_holidays(public_holidays_url: str, year: str) -> DataFrame:
    """Obtiene los festivos públicos de Brasil para el año dado desde la API.

    Args:
        public_holidays_url (str): URL base de la API de festivos (sin la parte final /{year}/BR).
        year (str): Año a consultar, por ejemplo "2017".

    Raises:
        SystemExit: Si la solicitud HTTP falla (código 4xx/5xx o error de red).

    Returns:
        DataFrame: DataFrame con los festivos. Se eliminan columnas 'types' y 'counties'
                   y la columna 'date' se convierte a datetime64[ns].
    """
    try:
        resp = requests.get(f"{public_holidays_url}/{year}/BR", timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        # Cubre errores de conexión, timeout y códigos HTTP no exitosos
        raise SystemExit(e) from e

    df = DataFrame(resp.json())

    # Eliminar columnas no requeridas si existen
    cols_to_drop = [c for c in ("types", "counties") if c in df.columns]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)

    # Convertir fecha a datetime64[ns] (naive, sin timezone)
    df["date"] = to_datetime(df["date"], errors="coerce")

    return df


def extract(
    csv_folder: str, csv_table_mapping: Dict[str, str], public_holidays_url: str
) -> Dict[str, DataFrame]:
    """Extract the data from the csv files and load them into the dataframes."""
    dataframes = {
        table_name: read_csv(f"{csv_folder}/{csv_file}")
        for csv_file, table_name in csv_table_mapping.items()
    }

    holidays = get_public_holidays(public_holidays_url, "2017")
    dataframes["public_holidays"] = holidays

    return dataframes
