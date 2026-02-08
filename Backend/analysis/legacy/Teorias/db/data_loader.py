"""
Módulo para cargar datos desde Supabase
"""
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()


def conexion_DB():
    """Crea conexión con Supabase"""
    from supabase import create_client, Client
    URL = os.getenv("SUPABASE_URL")
    KEY = os.getenv("SUPABASE_ANON_KEY")
    supabase: Client = create_client(URL, KEY)
    return supabase


def obtener_tabla(supabase, table, page_size=1000):
    """
    Obtiene todos los datos de una tabla con paginación
    
    Args:
        supabase: Cliente de Supabase
        table: Nombre de la tabla
        page_size: Tamaño de página para paginación
        
    Returns:
        Lista con todos los registros
    """
    all_data = []
    start = 0

    while True:
        response = (
            supabase.table(table)
            .select("*")
            .range(start, start + page_size - 1)
            .execute()
        )

        data = response.data
        if not data:
            break

        all_data.extend(data)
        start += page_size

    return all_data


def cargar_datos(tabla="v_partidos_completo"):
    """
    Función principal para cargar datos en DataFrame
    
    Args:
        tabla: Nombre de la tabla en Supabase
        
    Returns:
        DataFrame con los datos de partidos
    """
    supabase = conexion_DB()
    data = obtener_tabla(supabase, tabla)
    df = pd.DataFrame(data)
    
    # Convertir fecha a datetime
    if "fecha_partido" in df.columns:
        df["fecha_partido"] = pd.to_datetime(df["fecha_partido"])
        df = df.sort_values("fecha_partido").reset_index(drop=True)
    
    return df