from fastapi import APIRouter, HTTPException, BackgroundTasks
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel
import logging
from typing import Any, Dict, Optional
import pandas as pd
from models.api import (
    ScrapeLeagueRequest,
    ScrapeResult,
    ScrapingJob,
    AnalysisRequest,
    AnalysisResult,
    PoissonRequest,
    PoissonResult,
    PoissonBasicResult,
)
from scrapers.flashscore_adapter import scrape_partidos_liga_sync, scrape_goles_partido
from etl.partidos import upsert_partidos
from app.jobs import manager as job_manager
from analysis.legacy.Teorias.db.data_loader import cargar_datos, conexion_DB
from analysis.legacy.Teorias.analysis.partido import predecir_partido

router = APIRouter()
logger = logging.getLogger(__name__)


class ScrapeMatchRequest(BaseModel):
    link: str


@router.post("/scraping/league", response_model=ScrapingJob)
async def scrape_league(data: ScrapeLeagueRequest, background_tasks: BackgroundTasks):
    """
    Inicia un job de scraping de liga.
    Devuelve el job con ID para consultar su estado.
    """
    # Crear job con estado pending
    job_id = job_manager.create_job("league", data.url)
    # Agregar tarea en background que ejecutará el scraping
    background_tasks.add_task(run_scraping_league_job, job_id, data.url)

    # Devolver inmediatamente el job (estado pending)
    job_data = job_manager.get_job(job_id)
    return ScrapingJob(**job_data)


@router.post("/scraping/match", response_model=ScrapeResult)
async def scrape_match(data: ScrapeMatchRequest):
    """
    Scraping de goles de un partido específico.
    Retorna directamente las métricas (sin job).
    """
    try:
        eventos = await run_in_threadpool(scrape_goles_partido, data.link)
        return ScrapeResult(
            partidos_nuevos=0,
            eventos_nuevos=len(eventos),
            errores=0,
        )
    except Exception as e:
        logger.exception("Error en /scraping/match")
        return ScrapeResult(
            partidos_nuevos=0,
            eventos_nuevos=0,
            errores=1,
        )


@router.get("/scraping/status/{job_id}", response_model=ScrapingJob)
def scraping_status(job_id: str):
    """
    Obtiene el estado actual de un job de scraping.
    Devuelve el job completo con métricas y estado.
    """
    job = job_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} no encontrado")
    
    return ScrapingJob(**job)


def _apply_b10b_filters(df, filtros: Dict[str, Any]):
    if not filtros:
        return df

    if filtros.get("pais"):
        if "pais" not in df.columns:
            raise ValueError("El campo 'pais' no existe en los datos")
        df = df[df["pais"] == filtros["pais"]]

    if filtros.get("liga"):
        if "liga" not in df.columns:
            raise ValueError("El campo 'liga' no existe en los datos")
        df = df[df["liga"] == filtros["liga"]]

    if filtros.get("temporada"):
        if "temporada" not in df.columns:
            raise ValueError("El campo 'temporada' no existe en los datos")
        df = df[df["temporada"] == filtros["temporada"]]

    if filtros.get("fase"):
        if "fase" not in df.columns:
            raise ValueError("El campo 'fase' no existe en los datos")
        df = df[df["fase"] == filtros["fase"]]

    equipo = filtros.get("equipo")
    if equipo:
        if "local" not in df.columns or "visitante" not in df.columns:
            raise ValueError("Los campos 'local' y 'visitante' no existen en los datos")
        df = df[(df["local"] == equipo) | (df["visitante"] == equipo)]

    fecha_desde = filtros.get("fecha_desde")
    if fecha_desde and "fecha_partido" in df.columns:
        fecha_desde = pd.to_datetime(fecha_desde, errors="coerce")
        if pd.isna(fecha_desde):
            raise ValueError("filtros.fecha_desde no es una fecha válida")
        df = df[df["fecha_partido"] >= fecha_desde]

    fecha_hasta = filtros.get("fecha_hasta")
    if fecha_hasta and "fecha_partido" in df.columns:
        fecha_hasta = pd.to_datetime(fecha_hasta, errors="coerce")
        if pd.isna(fecha_hasta):
            raise ValueError("filtros.fecha_hasta no es una fecha válida")
        df = df[df["fecha_partido"] <= fecha_hasta]

    return df


def _apply_catalog_filters(
    df,
    pais: Optional[str] = None,
    liga: Optional[str] = None,
    temporada: Optional[str] = None,
    fase: Optional[str] = None,
):
    if pais:
        if "pais" not in df.columns:
            raise ValueError("El campo 'pais' no existe en los datos")
        df = df[df["pais"] == pais]

    if liga:
        if "liga" not in df.columns:
            raise ValueError("El campo 'liga' no existe en los datos")
        df = df[df["liga"] == liga]

    if temporada:
        if "temporada" not in df.columns:
            raise ValueError("El campo 'temporada' no existe en los datos")
        df = df[df["temporada"] == temporada]

    if fase:
        if "fase" not in df.columns:
            raise ValueError("El campo 'fase' no existe en los datos")
        df = df[df["fase"] == fase]

    return df


def _unique_str_list(df, column: str):
    if column not in df.columns:
        raise ValueError(f"El campo '{column}' no existe en los datos")
    values = df[column].dropna().astype(str).unique().tolist()
    values = sorted(values, key=lambda v: v.lower())
    return values


def _fetch_liga_id(liga: Optional[str]):
    if not liga:
        return None
    supabase = conexion_DB()
    res = (
        supabase.table("ligas")
        .select("id")
        .eq("nombre_display", liga)
        .limit(1)
        .execute()
    )
    if res.data:
        return res.data[0]["id"]
    return None


def _fetch_temporadas_map(nombres, liga_id: Optional[int] = None):
    supabase = conexion_DB()
    query = supabase.table("temporadas").select(
        "id,nombre_display,nombre_flashscore,liga_id"
    )
    if liga_id is not None:
        query = query.eq("liga_id", liga_id)
    res = query.execute()

    mapa = {}
    for row in res.data or []:
        nombre_display = row.get("nombre_display")
        nombre_flashscore = row.get("nombre_flashscore")
        if nombre_display in nombres:
            mapa[nombre_display] = row.get("id")
        if nombre_flashscore in nombres:
            mapa[nombre_flashscore] = row.get("id")
    return mapa


def _fetch_equipos_map(nombres):
    if not nombres:
        return {}
    supabase = conexion_DB()
    res = supabase.table("equipos").select("id,nombre").in_("nombre", list(nombres)).execute()
    return {row["nombre"]: row["id"] for row in (res.data or [])}


def _build_temporadas(df, liga: Optional[str] = None):
    if "temporada" not in df.columns:
        raise ValueError("El campo 'temporada' no existe en los datos")

    if "temporada_id" not in df.columns:
        nombres = _unique_str_list(df, "temporada")
        liga_id = _fetch_liga_id(liga)
        mapa_ids = _fetch_temporadas_map(set(nombres), liga_id)
        items = [{"id": mapa_ids.get(t), "nombre": t} for t in nombres]
        return items

    temp = df[["temporada", "temporada_id"]].dropna(subset=["temporada"]).copy()
    temp["temporada_id"] = pd.to_numeric(temp["temporada_id"], errors="coerce")

    items = {}
    for _, row in temp.iterrows():
        nombre = str(row["temporada"])
        temporada_id = row["temporada_id"]
        if nombre not in items or (items[nombre] is None and pd.notna(temporada_id)):
            items[nombre] = int(temporada_id) if pd.notna(temporada_id) else None

    salida = [{"id": items[nombre], "nombre": nombre} for nombre in items]
    salida.sort(key=lambda x: x["nombre"].lower())
    return salida


def _build_equipos(df):
    if "local" not in df.columns or "visitante" not in df.columns:
        raise ValueError("Los campos 'local' y 'visitante' no existen en los datos")

    items = {}

    if "equipo_local_id" in df.columns:
        temp_local = df[["equipo_local_id", "local"]].dropna(subset=["local"]).copy()
        temp_local["equipo_local_id"] = pd.to_numeric(temp_local["equipo_local_id"], errors="coerce")
        for _, row in temp_local.iterrows():
            nombre = str(row["local"])
            equipo_id = row["equipo_local_id"]
            if nombre not in items or (items[nombre] is None and pd.notna(equipo_id)):
                items[nombre] = int(equipo_id) if pd.notna(equipo_id) else None
    else:
        for nombre in df["local"].dropna().astype(str).unique():
            items[nombre] = None

    if "equipo_visitante_id" in df.columns:
        temp_visit = df[["equipo_visitante_id", "visitante"]].dropna(subset=["visitante"]).copy()
        temp_visit["equipo_visitante_id"] = pd.to_numeric(temp_visit["equipo_visitante_id"], errors="coerce")
        for _, row in temp_visit.iterrows():
            nombre = str(row["visitante"])
            equipo_id = row["equipo_visitante_id"]
            if nombre not in items or (items[nombre] is None and pd.notna(equipo_id)):
                items[nombre] = int(equipo_id) if pd.notna(equipo_id) else None
    else:
        for nombre in df["visitante"].dropna().astype(str).unique():
            items.setdefault(nombre, None)

    if "equipo_local_id" not in df.columns and "equipo_visitante_id" not in df.columns:
        nombres = set(items.keys())
        mapa_ids = _fetch_equipos_map(nombres)
        for nombre in items:
            items[nombre] = mapa_ids.get(nombre)

    salida = [{"id": items[nombre], "nombre": nombre} for nombre in items]
    salida.sort(key=lambda x: x["nombre"].lower())
    return salida


def _require_columns(df, columnas, contexto):
    faltantes = [col for col in columnas if col not in df.columns]
    if faltantes:
        raise ValueError(
            f"Faltan columnas en datos para {contexto}: {', '.join(faltantes)}"
        )


def _filter_by_temporada_id(df, temporada_id: int):
    if "temporada_id" not in df.columns:
        supabase = conexion_DB()
        res = (
            supabase.table("temporadas")
            .select("nombre_display,nombre_flashscore")
            .eq("id", temporada_id)
            .limit(1)
            .execute()
        )
        if not res.data:
            raise ValueError("temporada_id no existe en la base de datos")
        nombres = {res.data[0].get("nombre_display"), res.data[0].get("nombre_flashscore")}
        nombres = {n for n in nombres if n}
        filtrado = df[df["temporada"].isin(nombres)]
        return filtrado

    filtrado = df[df["temporada_id"] == temporada_id]
    if filtrado.empty:
        temporada_str = str(temporada_id)
        filtrado = df[df["temporada_id"].astype(str) == temporada_str]

    return filtrado


def _resolve_team_name(df, equipo_id: int):
    has_local = "equipo_local_id" in df.columns and "local" in df.columns
    has_visit = "equipo_visitante_id" in df.columns and "visitante" in df.columns

    if not (has_local or has_visit):
        supabase = conexion_DB()
        res = (
            supabase.table("equipos")
            .select("nombre")
            .eq("id", equipo_id)
            .limit(1)
            .execute()
        )
        if res.data:
            return res.data[0].get("nombre")
        return None

    equipo_id_str = str(equipo_id)

    if has_local:
        match = df[df["equipo_local_id"] == equipo_id]
        if match.empty:
            match = df[df["equipo_local_id"].astype(str) == equipo_id_str]
        if not match.empty:
            return match["local"].iloc[0]

    if has_visit:
        match = df[df["equipo_visitante_id"] == equipo_id]
        if match.empty:
            match = df[df["equipo_visitante_id"].astype(str) == equipo_id_str]
        if not match.empty:
            return match["visitante"].iloc[0]

    return None


@router.post("/analysis/b10b", response_model=AnalysisResult)
async def analysis_b10b(data: AnalysisRequest):
    """
    Análisis B10B usando el modelo Poisson legacy.

    Espera:
    - filtros: pais, liga, temporada, fase, equipo, fecha_desde, fecha_hasta
    - parametros: equipo_local/equipo_visitante o equipo_local_id/equipo_visitante_id, umbral (opcional)
    """
    filtros = data.filtros or {}
    parametros = data.parametros or {}

    equipo_local_id = parametros.get("equipo_local_id")
    equipo_visitante_id = parametros.get("equipo_visitante_id")

    if equipo_local_id is not None or equipo_visitante_id is not None:
        if equipo_local_id is None or equipo_visitante_id is None:
            raise HTTPException(
                status_code=422,
                detail="parametros.equipo_local_id y parametros.equipo_visitante_id son requeridos",
            )
        try:
            equipo_local_id = int(equipo_local_id)
            equipo_visitante_id = int(equipo_visitante_id)
        except (TypeError, ValueError):
            raise HTTPException(
                status_code=422,
                detail="parametros.equipo_local_id y parametros.equipo_visitante_id deben ser numéricos",
            )
        if equipo_local_id == equipo_visitante_id:
            raise HTTPException(
                status_code=422,
                detail="equipo_local_id y equipo_visitante_id deben ser distintos",
            )
        equipo_local = None
        equipo_visitante = None
    else:
        equipo_local = parametros.get("equipo_local") or parametros.get("local")
        equipo_visitante = parametros.get("equipo_visitante") or parametros.get("visitante")

        if not equipo_local or not equipo_visitante:
            raise HTTPException(
                status_code=422,
                detail="parametros.equipo_local y parametros.equipo_visitante son requeridos",
            )

        if equipo_local == equipo_visitante:
            raise HTTPException(
                status_code=422,
                detail="equipo_local y equipo_visitante deben ser distintos",
            )

    umbral = parametros.get("umbral", parametros.get("umbral_goles", 2.5))
    try:
        umbral = float(umbral)
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail="parametros.umbral debe ser numérico")

    try:
        df = await run_in_threadpool(cargar_datos)
        df = _apply_b10b_filters(df, filtros)

        if df.empty:
            raise HTTPException(
                status_code=404,
                detail="No hay partidos para los filtros entregados",
            )

        if equipo_local_id is not None and equipo_visitante_id is not None:
            equipo_local = _resolve_team_name(df, equipo_local_id)
            equipo_visitante = _resolve_team_name(df, equipo_visitante_id)

            if not equipo_local or not equipo_visitante:
                raise HTTPException(
                    status_code=404,
                    detail="No se encontraron equipos para los IDs entregados",
                )

        prediccion = await run_in_threadpool(
            predecir_partido, df, equipo_local, equipo_visitante, umbral
        )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("Error en /analysis/b10b")
        raise HTTPException(status_code=500, detail="Error interno en análisis B10B")

    resumen = {
        "recomendacion": prediccion.get("recomendacion"),
        "confianza": prediccion.get("confianza"),
        "goles_esperados_total": prediccion.get("goles_esperados_total"),
        "goles_esperados_local": prediccion.get("goles_esperados_local"),
        "goles_esperados_visitante": prediccion.get("goles_esperados_visitante"),
    }

    detalles = {
        "filtros": filtros,
        "parametros": {
            "equipo_local": equipo_local,
            "equipo_visitante": equipo_visitante,
            "umbral": umbral,
        },
        "prediccion": prediccion,
    }

    if equipo_local_id is not None and equipo_visitante_id is not None:
        detalles["parametros"]["equipo_local_id"] = equipo_local_id
        detalles["parametros"]["equipo_visitante_id"] = equipo_visitante_id

    return AnalysisResult(tipo="B10B", resumen=resumen, detalles=detalles)


@router.post("/analysis/poisson", response_model=PoissonResult)
async def analysis_poisson(data: PoissonRequest):
    """
    Análisis Poisson puro usando el modelo legacy.
    """
    if data.equipo_local_id == data.equipo_visitante_id:
        raise HTTPException(
            status_code=422,
            detail="equipo_local_id y equipo_visitante_id deben ser distintos",
        )

    try:
        df = await run_in_threadpool(cargar_datos)
        df = _filter_by_temporada_id(df, data.temporada_id)

        if df.empty:
            raise HTTPException(
                status_code=404,
                detail=f"No hay partidos para temporada_id={data.temporada_id}",
            )

        _require_columns(
            df,
            ["local", "visitante", "goles_local", "goles_visitante"],
            "Poisson",
        )

        equipo_local = _resolve_team_name(df, data.equipo_local_id)
        equipo_visitante = _resolve_team_name(df, data.equipo_visitante_id)

        if not equipo_local or not equipo_visitante:
            raise HTTPException(
                status_code=404,
                detail="No se encontraron equipos para los IDs entregados",
            )

        prediccion = await run_in_threadpool(
            predecir_partido,
            df,
            equipo_local,
            equipo_visitante,
            data.umbral_goles,
        )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("Error en /analysis/poisson")
        raise HTTPException(
            status_code=500,
            detail="Error interno en análisis Poisson",
        )

    goles_esperados = {
        "local": prediccion.get("goles_esperados_local"),
        "visitante": prediccion.get("goles_esperados_visitante"),
        "total": prediccion.get("goles_esperados_total"),
    }

    probabilidades = {
        "recomendacion": prediccion.get("recomendacion"),
        "confianza": prediccion.get("confianza"),
    }

    return PoissonResult(
        temporada_id=data.temporada_id,
        equipo_local_id=data.equipo_local_id,
        equipo_visitante_id=data.equipo_visitante_id,
        equipo_local=equipo_local,
        equipo_visitante=equipo_visitante,
        umbral_goles=data.umbral_goles,
        goles_esperados=goles_esperados,
        total_goles_esperados=prediccion.get("goles_esperados_total"),
        probabilidades=probabilidades,
    )


@router.get("/catalog/paises")
async def catalog_paises():
    try:
        df = await run_in_threadpool(cargar_datos)
        items = _unique_str_list(df, "pais")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("Error en /catalog/paises")
        raise HTTPException(status_code=500, detail="Error interno en catálogo")

    return {"items": items, "total": len(items)}


@router.get("/catalog/ligas")
async def catalog_ligas(pais: Optional[str] = None):
    try:
        df = await run_in_threadpool(cargar_datos)
        df = _apply_catalog_filters(df, pais=pais)
        items = _unique_str_list(df, "liga")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("Error en /catalog/ligas")
        raise HTTPException(status_code=500, detail="Error interno en catálogo")

    return {"items": items, "total": len(items)}


@router.get("/catalog/temporadas")
async def catalog_temporadas(
    pais: Optional[str] = None,
    liga: Optional[str] = None,
):
    try:
        df = await run_in_threadpool(cargar_datos)
        df = _apply_catalog_filters(df, pais=pais, liga=liga)
        items = _build_temporadas(df, liga=liga)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("Error en /catalog/temporadas")
        raise HTTPException(status_code=500, detail="Error interno en catálogo")

    return {"items": items, "total": len(items)}


@router.get("/catalog/fases")
async def catalog_fases(
    pais: Optional[str] = None,
    liga: Optional[str] = None,
    temporada: Optional[str] = None,
):
    try:
        df = await run_in_threadpool(cargar_datos)
        df = _apply_catalog_filters(df, pais=pais, liga=liga, temporada=temporada)
        items = _unique_str_list(df, "fase")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("Error en /catalog/fases")
        raise HTTPException(status_code=500, detail="Error interno en catálogo")

    return {"items": items, "total": len(items)}


@router.get("/catalog/equipos")
async def catalog_equipos(
    pais: Optional[str] = None,
    liga: Optional[str] = None,
    temporada: Optional[str] = None,
    fase: Optional[str] = None,
):
    try:
        df = await run_in_threadpool(cargar_datos)
        df = _apply_catalog_filters(
            df, pais=pais, liga=liga, temporada=temporada, fase=fase
        )
        items = _build_equipos(df)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("Error en /catalog/equipos")
        raise HTTPException(status_code=500, detail="Error interno en catálogo")

    return {"items": items, "total": len(items)}


async def run_scraping_league_job(job_id: str, url: str):
    """
    Ejecuta el scraping de liga en background.
    - marca el job como running
    - ejecuta las funciones de scraping/etl en threadpool
    - calcula métricas
    - marca done o failed en el manager
    """
    try:
        logger.info("Iniciando job de scraping en background %s", job_id)
        job_manager.start_job(job_id)

        # Ejecutar scraping de partidos (función sync) en threadpool
        partidos = await run_in_threadpool(scrape_partidos_liga_sync, url)

        # Persistir/actualizar partidos (función sync)
        partidos_nuevos = upsert_partidos(partidos)

        # Por ahora no se scrapan eventos en este flujo
        eventos_nuevos = 0

        metrics = {
            "partidos_nuevos": partidos_nuevos,
            "eventos_nuevos": eventos_nuevos,
            "errores": 0,
        }

        job_manager.finish_job(job_id, metrics)
        logger.info("Job %s finalizado correctamente", job_id)

    except Exception as e:
        logger.exception("Error en job de scraping %s", job_id)
        job_manager.fail_job(job_id, str(e))


@router.get("/status")
def status():
    return {"status": "ok"}
