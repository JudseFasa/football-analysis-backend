from typing import List, Optional
import logging
from datetime import datetime, timezone
from scrapers.flashscore_adapter import PartidoScraped
from etl.client import get_supabase_client

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)


def parse_flashscore_fecha(fecha_raw: str) -> Optional[str]:
    """
    Convierte fecha Flashscore "DD.MM. HH:MM" a ISO 8601 UTC datetime string.
    Usa año actual si no viene en el string.
    Devuelve None si no puede convertir (tolerante a errores).
    """
    if not fecha_raw or not isinstance(fecha_raw, str):
        return None
    
    try:
        fecha_raw = fecha_raw.strip()
        
        # Formato esperado: "02.02. 18:20" → "02.02.YYYY 18:20"
        # Remover espacios múltiples
        fecha_raw = ' '.join(fecha_raw.split())
        
        # Obtener año actual
        year = datetime.now(timezone.utc).year
        
        # Parsear "DD.MM. HH:MM"
        # Puede ser: "02.02. 18:20" o "2.2. 18:20" o "02.02. 18:20:00"
        parts = fecha_raw.split()
        if len(parts) < 2:
            return None
        
        fecha_parte = parts[0]  # "DD.MM." o "DD.MM"
        hora_parte = parts[1]   # "HH:MM" o "HH:MM:SS"
        
        # Limpiar punto final de fecha_parte si existe
        fecha_parte = fecha_parte.rstrip('.')
        
        # Parsear fecha
        fecha_split = fecha_parte.split('.')
        if len(fecha_split) != 2:
            return None
        
        day = int(fecha_split[0])
        month = int(fecha_split[1])
        
        # Parsear hora
        hora_split = hora_parte.split(':')
        hour = int(hora_split[0])
        minute = int(hora_split[1]) if len(hora_split) > 1 else 0
        second = int(hora_split[2]) if len(hora_split) > 2 else 0
        
        # Crear datetime en UTC
        dt = datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
        
        # Devolver en formato ISO 8601
        return dt.isoformat()
        
    except Exception as e:
        logger.debug(f"⚠️  Error parseando fecha '{fecha_raw}': {e}")
        return None


def upsert_partidos(partidos: List[PartidoScraped]) -> int:
    """
    Inserta partidos en Supabase si no existen (usando link como llave lógica)
    Resuelve/crea paises, ligas, temporadas, equipos antes de insertar
    
    Requiere que cada PartidoScraped incluya 'pais_slug' (no puede ser None).
    
    Retorna cantidad de partidos insertados
    Raises:
        ValueError: Si algún partido no tiene pais_slug válido
        EnvironmentError: Si faltan variables de entorno Supabase
    """
    if not partidos:
        return 0
    
    try:
        supabase = get_supabase_client()
        insertados = 0
        
        for partido in partidos:
            try:
                # Validar que pais_slug está presente (crítico para NO NULL en DB)
                if not partido.pais_slug:
                    raise ValueError(
                        f"pais_slug faltante en partido {partido.link}. "
                        f"El slug debe extraerse de la URL de Flashscore, no del nombre del país."
                    )
                
                # Verificar si el partido ya existe por link
                resultado = supabase.table("partidos").select("id").eq("link", partido.link).execute()
                
                if resultado.data:
                    # Partido ya existe, saltar
                    continue
                
                # Resolver o crear país USANDO EL SLUG (no el nombre)
                pais_res = supabase.table("paises").select("id").eq("slug_flashscore", partido.pais_slug).execute()
                if pais_res.data:
                    pais_id = pais_res.data[0]["id"]
                else:
                    pais_insert = supabase.table("paises").insert({
                        "nombre": partido.pais,
                        "slug_flashscore": partido.pais_slug
                    }).execute()
                    pais_id = pais_insert.data[0]["id"]
                
                # Resolver o crear liga (usar slug_flashscore como clave)
                liga_slug = partido.liga.lower().strip().replace(" ", "-")
                liga_res = supabase.table("ligas").select("id").eq("slug_flashscore", liga_slug).eq("pais_id", pais_id).execute()
                if liga_res.data:
                    liga_id = liga_res.data[0]["id"]
                else:
                    liga_insert = supabase.table("ligas").insert({
                        "pais_id": pais_id,
                        "slug_flashscore": liga_slug,
                        "nombre_display": partido.liga
                    }).execute()
                    liga_id = liga_insert.data[0]["id"]
                
                # Resolver o crear temporada (usar nombre_flashscore como clave)
                temporada_res = supabase.table("temporadas").select("id").eq("nombre_flashscore", partido.temporada).eq("liga_id", liga_id).execute()
                if temporada_res.data:
                    temporada_id = temporada_res.data[0]["id"]
                else:
                    # Inferir tipo y fechas
                    tipo = "anual" if "/" in partido.temporada else "calendario"
                    if "/" in partido.temporada:
                        anos = partido.temporada.split("/")
                        fecha_inicio = f"{anos[0]}-07-01"
                        fecha_fin = f"{anos[1]}-06-30"
                    else:
                        fecha_inicio = f"{partido.temporada}-01-01"
                        fecha_fin = f"{partido.temporada}-12-31"
                    temporada_insert = supabase.table("temporadas").insert({
                        "liga_id": liga_id,
                        "nombre_flashscore": partido.temporada,
                        "nombre_display": partido.temporada,
                        "tipo": tipo,
                        "fecha_inicio": fecha_inicio,
                        "fecha_fin": fecha_fin
                    }).execute()
                    temporada_id = temporada_insert.data[0]["id"]
                
                # Resolver o crear equipo local (usar slug_flashscore como clave)
                local_slug = partido.local.lower().strip().replace(" ", "-")
                local_res = supabase.table("equipos").select("id").eq("slug_flashscore", local_slug).execute()
                if local_res.data:
                    local_id = local_res.data[0]["id"]
                else:
                    local_insert = supabase.table("equipos").insert({
                        "nombre": partido.local,
                        "slug_flashscore": local_slug
                    }).execute()
                    local_id = local_insert.data[0]["id"]
                
                # Resolver o crear equipo visitante (usar slug_flashscore como clave)
                visitante_slug = partido.visitante.lower().strip().replace(" ", "-")
                visitante_res = supabase.table("equipos").select("id").eq("slug_flashscore", visitante_slug).execute()
                if visitante_res.data:
                    visitante_id = visitante_res.data[0]["id"]
                else:
                    visitante_insert = supabase.table("equipos").insert({
                        "nombre": partido.visitante,
                        "slug_flashscore": visitante_slug
                    }).execute()
                    visitante_id = visitante_insert.data[0]["id"]
                
                # Resolver estado (Finalizado si hay goles, Programado si no)
                estado_nombre = "Finalizado" if (partido.goles_local is not None and partido.goles_visitante is not None) else "Programado"
                estado_res = supabase.table("estados").select("id").eq("nombre", estado_nombre).execute()
                if estado_res.data:
                    estado_id = estado_res.data[0]["id"]
                else:
                    estado_insert = supabase.table("estados").insert({"nombre": estado_nombre}).execute()
                    estado_id = estado_insert.data[0]["id"]
                
                # Resolver fase (opcional)
                fase_id = None
                if partido.fase:
                    fase_slug = partido.fase.lower().strip().replace(" ", "-")
                    fase_res = supabase.table("fase").select("id").eq("slug_flashscore", fase_slug).eq("temporada_id", temporada_id).execute()
                    if fase_res.data:
                        fase_id = fase_res.data[0]["id"]
                    else:
                        fase_insert = supabase.table("fase").insert({
                            "temporada_id": temporada_id,
                            "nombre": partido.fase,
                            "slug_flashscore": fase_slug
                        }).execute()
                        fase_id = fase_insert.data[0]["id"]
                
                # Resolver jornada (opcional)
                jornada_id = None
                if partido.jornada:
                    jornada_slug = partido.jornada.lower().strip().replace(" ", "-")
                    jornada_res = supabase.table("jornada").select("id").eq("slug_flashscore", jornada_slug).eq("temporada_id", temporada_id).execute()
                    if jornada_res.data:
                        jornada_id = jornada_res.data[0]["id"]
                    else:
                        jornada_insert = supabase.table("jornada").insert({
                            "temporada_id": temporada_id,
                            "nombre": partido.jornada,
                            "slug_flashscore": jornada_slug
                        }).execute()
                        jornada_id = jornada_insert.data[0]["id"]
                
                # Resolver especial (opcional)
                especial_id = None
                if partido.especial:
                    especial_res = supabase.table("especial").select("id").eq("nombre", partido.especial).execute()
                    if especial_res.data:
                        especial_id = especial_res.data[0]["id"]
                    else:
                        especial_insert = supabase.table("especial").insert({"nombre": partido.especial}).execute()
                        especial_id = especial_insert.data[0]["id"]
                
                # Insertar partido (usar nombres exactos de columnas: equipo_local_id, equipo_visitante_id, fecha_partido)
                # Parsear fecha_raw a timestamp válido
                fecha_partido_parsed = parse_flashscore_fecha(partido.fecha_raw)
                
                supabase.table("partidos").insert({
                    "temporada_id": temporada_id,
                    "estado_id": estado_id,
                    "especial_id": especial_id,
                    "fase_id": fase_id,
                    "jornada_id": jornada_id,
                    "link": partido.link,
                    "equipo_local_id": local_id,
                    "equipo_visitante_id": visitante_id,
                    "fecha_raw": partido.fecha_raw,
                    "fecha_partido": fecha_partido_parsed,
                    "goles_local": partido.goles_local,
                    "goles_visitante": partido.goles_visitante,
                    "scraped_at": partido.scraped_at.isoformat()
                }).execute()
                
                insertados += 1
                
            except ValueError as e:
                # Re-lanzar errores de validación (pais_slug faltante)
                logger.error(f"Error de validación en partido: {e}")
                raise
            except Exception as e:
                logger.error(f"Error insertando partido {partido.link}: {e}")
                continue
        
        return insertados
        
    except EnvironmentError:
        # Propagar errores de configuración
        raise
    except Exception as e:
        logger.error(f"Error en upsert_partidos: {e}")
        return 0
