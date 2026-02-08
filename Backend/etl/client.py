import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()


def get_supabase_client() -> Client:
    """
    Retorna un cliente Supabase configurado con credenciales de variables de entorno
    """
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_ANON_KEY")

    if not url or not key:
        raise EnvironmentError(
            "Variables de entorno faltantes: se requieren SUPABASE_URL y SUPABASE_ANON_KEY"
        )

    # Crear cliente una vez y retornarlo
    return create_client(url, key)
