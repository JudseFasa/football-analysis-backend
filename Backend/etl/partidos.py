from typing import List, Optional, Dict, Tuple, Iterable
import logging
from datetime import datetime, timezone
import re
import time
from scrapers.flashscore_adapter import PartidoScraped
from etl.client import get_supabase_client

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)


def _infer_year_from_temporada(month: int, temporada: Optional[str], fallback_year: int) -> int:
    """
    Infere el año correcto usando la temporada.
    - Para temporadas interanuales "2024/2025":
      meses Jul-Dic -> 2024, meses Ene-Jun -> 2025.
    - Para temporadas de un solo año "2025": usa 2025.
    - Si no se puede inferir, devuelve fallback_year.
    """
    if not temporada:
        return fallback_year

    temporada = temporada.strip()
    if not temporada:
        return fallback_year

    # 2024/2025 o 2024/25
    match = re.search(r"(\d{4})\s*/\s*(\d{2,4})", temporada)
    if match:
        start_year = int(match.group(1))
        end_raw = match.group(2)
        if len(end_raw) == 2:
            end_year = (start_year // 100) * 100 + int(end_raw)
            if end_year < start_year:
                end_year += 100
        else:
            end_year = int(end_raw)

        return start_year if month >= 7 else end_year

    # Temporada con año único (e.g. "2025")
    match = re.search(r"(\d{4})", temporada)
    if match:
        return int(match.group(1))

    return fallback_year


def parse_flashscore_fecha(fecha_raw: str, temporada: Optional[str] = None) -> Optional[str]:
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
        
        # Año por defecto (fallback)
        year = datetime.now(timezone.utc).year
        
        # Parsear "DD.MM. HH:MM"
        # Puede ser: "02.02. 18:20" o "2.2. 18:20" o "02.02. 18:20:00"
        # También puede incluir año: "02.02.2025 18:20"
        parts = fecha_raw.split()
        if len(parts) < 2:
            return None
        
        fecha_parte = parts[0]  # "DD.MM." o "DD.MM"
        hora_parte = parts[1]   # "HH:MM" o "HH:MM:SS"
        
        # Limpiar punto final de fecha_parte si existe
        fecha_parte = fecha_parte.rstrip('.')
        
        # Parsear fecha
        fecha_split = fecha_parte.split('.')
        if len(fecha_split) < 2:
            return None

        day = int(fecha_split[0])
        month = int(fecha_split[1])
        if len(fecha_split) >= 3 and fecha_split[2].isdigit():
            year = int(fecha_split[2])
        else:
            year = _infer_year_from_temporada(month, temporada, year)
        
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

    def _chunked(seq: Iterable, size: int):
        buf = []
        for item in seq:
            buf.append(item)
            if len(buf) >= size:
                yield buf
                buf = []
        if buf:
            yield buf

    try:
        start = time.perf_counter()
        supabase = get_supabase_client()
        insertados = 0
        actualizados = 0

        # Validación mínima local
        valid_partidos: List[PartidoScraped] = []
        for partido in partidos:
            if not partido.link:
                logger.error("Partido sin link, se omite.")
                continue
            if not partido.pais_slug:
                raise ValueError(
                    f"pais_slug faltante en partido {partido.link}. "
                    f"El slug debe extraerse de la URL de Flashscore, no del nombre del país."
                )
            if not partido.liga or not partido.temporada or not partido.local or not partido.visitante:
                logger.error(f"Partido con datos incompletos, se omite: {partido.link}")
                continue
            valid_partidos.append(partido)

        if not valid_partidos:
            return 0

        # Cache local de IDs
        pais_cache: Dict[str, int] = {}
        liga_cache: Dict[Tuple[int, str], int] = {}
        temporada_cache: Dict[Tuple[int, str], int] = {}
        equipo_cache: Dict[str, int] = {}
        estado_cache: Dict[str, int] = {}
        fase_cache: Dict[Tuple[int, str], int] = {}
        jornada_cache: Dict[Tuple[int, str], int] = {}
        especial_cache: Dict[str, int] = {}

        # Pre-cargar estados si existen
        for nombre in ("Finalizado", "Programado"):
            res = supabase.table("estados").select("id").eq("nombre", nombre).limit(1).execute()
            if res.data:
                estado_cache[nombre] = res.data[0]["id"]

        # Detectar links existentes en batch
        existing_links = set()
        links = [p.link for p in valid_partidos]
        for chunk in _chunked(links, 400):
            res = supabase.table("partidos").select("link").in_("link", chunk).execute()
            for row in res.data or []:
                existing_links.add(row["link"])

        def get_pais_id(pais_slug: str, pais_nombre: str) -> int:
            if pais_slug in pais_cache:
                return pais_cache[pais_slug]
            res = supabase.table("paises").select("id").eq("slug_flashscore", pais_slug).limit(1).execute()
            if res.data:
                pid = res.data[0]["id"]
            else:
                ins = supabase.table("paises").insert({
                    "nombre": pais_nombre,
                    "slug_flashscore": pais_slug
                }).execute()
                pid = ins.data[0]["id"]
            pais_cache[pais_slug] = pid
            return pid

        def get_liga_id(pais_id: int, liga_nombre: str) -> int:
            liga_slug = liga_nombre.lower().strip().replace(" ", "-")
            key = (pais_id, liga_slug)
            if key in liga_cache:
                return liga_cache[key]
            res = supabase.table("ligas").select("id").eq("slug_flashscore", liga_slug).eq("pais_id", pais_id).limit(1).execute()
            if res.data:
                lid = res.data[0]["id"]
            else:
                ins = supabase.table("ligas").insert({
                    "pais_id": pais_id,
                    "slug_flashscore": liga_slug,
                    "nombre_display": liga_nombre
                }).execute()
                lid = ins.data[0]["id"]
            liga_cache[key] = lid
            return lid

        def get_temporada_id(liga_id: int, temporada_nombre: str) -> int:
            key = (liga_id, temporada_nombre)
            if key in temporada_cache:
                return temporada_cache[key]
            res = supabase.table("temporadas").select("id").eq("nombre_flashscore", temporada_nombre).eq("liga_id", liga_id).limit(1).execute()
            if res.data:
                tid = res.data[0]["id"]
            else:
                tipo = "anual" if "/" in temporada_nombre else "calendario"
                if "/" in temporada_nombre:
                    anos = temporada_nombre.split("/")
                    fecha_inicio = f"{anos[0]}-07-01"
                    fecha_fin = f"{anos[1]}-06-30"
                else:
                    fecha_inicio = f"{temporada_nombre}-01-01"
                    fecha_fin = f"{temporada_nombre}-12-31"
                ins = supabase.table("temporadas").insert({
                    "liga_id": liga_id,
                    "nombre_flashscore": temporada_nombre,
                    "nombre_display": temporada_nombre,
                    "tipo": tipo,
                    "fecha_inicio": fecha_inicio,
                    "fecha_fin": fecha_fin
                }).execute()
                tid = ins.data[0]["id"]
            temporada_cache[key] = tid
            return tid

        def get_equipo_id(nombre_equipo: str) -> int:
            slug = nombre_equipo.lower().strip().replace(" ", "-")
            if slug in equipo_cache:
                return equipo_cache[slug]
            res = supabase.table("equipos").select("id").eq("slug_flashscore", slug).limit(1).execute()
            if res.data:
                eid = res.data[0]["id"]
            else:
                ins = supabase.table("equipos").insert({
                    "nombre": nombre_equipo,
                    "slug_flashscore": slug
                }).execute()
                eid = ins.data[0]["id"]
            equipo_cache[slug] = eid
            return eid

        def get_estado_id(nombre: str) -> int:
            if nombre in estado_cache:
                return estado_cache[nombre]
            res = supabase.table("estados").select("id").eq("nombre", nombre).limit(1).execute()
            if res.data:
                eid = res.data[0]["id"]
            else:
                ins = supabase.table("estados").insert({"nombre": nombre}).execute()
                eid = ins.data[0]["id"]
            estado_cache[nombre] = eid
            return eid

        def get_fase_id(temporada_id: int, fase_nombre: Optional[str]) -> Optional[int]:
            if not fase_nombre:
                return None
            fase_slug = fase_nombre.lower().strip().replace(" ", "-")
            key = (temporada_id, fase_slug)
            if key in fase_cache:
                return fase_cache[key]
            res = supabase.table("fase").select("id").eq("slug_flashscore", fase_slug).eq("temporada_id", temporada_id).limit(1).execute()
            if res.data:
                fid = res.data[0]["id"]
            else:
                ins = supabase.table("fase").insert({
                    "temporada_id": temporada_id,
                    "nombre": fase_nombre,
                    "slug_flashscore": fase_slug
                }).execute()
                fid = ins.data[0]["id"]
            fase_cache[key] = fid
            return fid

        def get_jornada_id(temporada_id: int, jornada_nombre: Optional[str]) -> Optional[int]:
            if not jornada_nombre:
                return None
            jornada_slug = jornada_nombre.lower().strip().replace(" ", "-")
            key = (temporada_id, jornada_slug)
            if key in jornada_cache:
                return jornada_cache[key]
            res = supabase.table("jornada").select("id").eq("slug_flashscore", jornada_slug).eq("temporada_id", temporada_id).limit(1).execute()
            if res.data:
                jid = res.data[0]["id"]
            else:
                ins = supabase.table("jornada").insert({
                    "temporada_id": temporada_id,
                    "nombre": jornada_nombre,
                    "slug_flashscore": jornada_slug
                }).execute()
                jid = ins.data[0]["id"]
            jornada_cache[key] = jid
            return jid

        def get_especial_id(especial_nombre: Optional[str]) -> Optional[int]:
            if not especial_nombre:
                return None
            if especial_nombre in especial_cache:
                return especial_cache[especial_nombre]
            res = supabase.table("especial").select("id").eq("nombre", especial_nombre).limit(1).execute()
            if res.data:
                eid = res.data[0]["id"]
            else:
                ins = supabase.table("especial").insert({"nombre": especial_nombre}).execute()
                eid = ins.data[0]["id"]
            especial_cache[especial_nombre] = eid
            return eid

        rows_insert = []
        rows_upsert = []

        for partido in valid_partidos:
            try:
                pais_id = get_pais_id(partido.pais_slug, partido.pais)
                liga_id = get_liga_id(pais_id, partido.liga)
                temporada_id = get_temporada_id(liga_id, partido.temporada)
                local_id = get_equipo_id(partido.local)
                visitante_id = get_equipo_id(partido.visitante)

                is_final = partido.goles_local is not None and partido.goles_visitante is not None
                estado_nombre = "Finalizado" if is_final else "Programado"
                estado_id = get_estado_id(estado_nombre)

                fase_id = get_fase_id(temporada_id, partido.fase)
                jornada_id = get_jornada_id(temporada_id, partido.jornada)
                especial_id = get_especial_id(partido.especial)

                fecha_partido_parsed = parse_flashscore_fecha(partido.fecha_raw, partido.temporada)

                row = {
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
                    "scraped_at": partido.scraped_at.isoformat(),
                }

                exists = partido.link in existing_links
                if exists and not is_final:
                    # No sobrescribir partidos existentes si aún no están finalizados
                    continue

                if exists:
                    rows_upsert.append(row)
                else:
                    rows_insert.append(row)

            except Exception as e:
                logger.error(f"Error preparando partido {partido.link}: {e}")
                continue

        # Insertar nuevos en batches
        for chunk in _chunked(rows_insert, 100):
            supabase.table("partidos").insert(chunk).execute()
            insertados += len(chunk)

        # Upsert de finalizados existentes en batches
        for chunk in _chunked(rows_upsert, 100):
            supabase.table("partidos").upsert(chunk, on_conflict="link").execute()
            actualizados += len(chunk)

        elapsed = time.perf_counter() - start
        logger.info(f"ETL partidos: insertados={insertados}, actualizados={actualizados}, total={len(valid_partidos)}, tiempo={elapsed:.2f}s")

        return insertados

    except EnvironmentError:
        raise
    except Exception as e:
        logger.error(f"Error en upsert_partidos: {e}")
        return 0
