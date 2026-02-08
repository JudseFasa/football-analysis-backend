"""
Wrapper síncrono para permitir su uso desde:
- scripts CLI
- endpoints FastAPI sync

FastAPI async debe llamar directamente a:
scrape_goles_partido_async
"""

from playwright.async_api import async_playwright
from playwright.sync_api import sync_playwright
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime, timezone, timedelta
import logging
import sys
import asyncio
import os

# Preferir navegadores instalados globalmente si existen; la detección
# se realiza en `scrapers.browser.get_launch_kwargs()`.
from scrapers.browser import get_launch_kwargs
import re
from urllib.parse import urlsplit, urlunsplit

# ===== LOGGING MÍNIMO =====
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

# ===== FUNCIÓN PARA EXTRAER SLUG DEL PAÍS =====
def extract_country_slug_from_url(url: str) -> str:
    """
    Extrae el slug del país de la URL de Flashscore.
    Ejemplos:
    - https://www.flashscore.co/futbol/colombia/primera-a/ → "colombia"
    - https://www.flashscore.co/futbol/argentina/primera-division/ → "argentina"
    - https://www.flashscore.co/futbol/españa/laliga/ → "españa"
    
    Raises:
        ValueError: Si no se puede extraer el slug de la URL
    """
    if not url or not isinstance(url, str):
        raise ValueError(f"URL inválida: {url}")
    
    # Patrón: /futbol/<slug>/ donde slug es el país
    match = re.search(r'/futbol/([a-z0-9-]+)/', url.lower())
    if not match:
        raise ValueError(f"No se pudo extraer slug del país de: {url}")
    
    slug = match.group(1)
    
    # Validar que el slug no esté vacío
    if not slug or slug in ('resultados', 'estadisticas'):
        raise ValueError(f"Slug inválido extraído de URL: {slug}")
    
    return slug


def _normalize_results_url(url: str) -> str:
    """
    Normaliza una URL de liga para apuntar a la sección de resultados.
    Ejemplos:
    - /partidos/ -> /resultados/
    - /futbol/<pais>/<liga>/ -> /futbol/<pais>/<liga>/resultados/
    Respeta el dominio y el esquema.
    """
    if not url or not isinstance(url, str):
        return url

    cleaned = url.strip()
    if not cleaned:
        return cleaned

    # Asegurar que haya una barra final para simplificar el parseo.
    if not cleaned.endswith("/"):
        cleaned += "/"

    parts = urlsplit(cleaned)
    path = parts.path or ""

    # Normaliza rutas conocidas
    if "/partidos/" in path:
        path = path.replace("/partidos/", "/resultados/")
    else:
        # Si la URL apunta directamente a la liga, agrega resultados.
        m = re.match(r"^(/futbol/[^/]+/[^/]+/)(.*)$", path)
        if m:
            base, tail = m.groups()
            if tail == "" or tail == "resultados/" or tail == "partidos/":
                path = base + "resultados/"

    return urlunsplit((parts.scheme, parts.netloc, path, parts.query, parts.fragment))


def _maybe_force_results_url(current_url: str) -> str:
    """
    Si la página terminó en /partidos/, fuerza a /resultados/.
    """
    if not current_url:
        return current_url
    if "/partidos/" in current_url:
        return current_url.replace("/partidos/", "/resultados/")
    return current_url


def _format_country_from_slug(slug: str) -> str:
    """Convierte un slug tipo 'estados-unidos' en 'Estados Unidos'."""
    if not slug:
        return "Pais Desconocido"
    return " ".join(part.capitalize() for part in slug.split("-") if part)

# ===== MODELOS PYDANTIC =====
class PartidoScraped(BaseModel):
    link: str
    pais: str
    pais_slug: str  # slug extraído de la URL, NOT NULL en DB
    liga: str
    temporada: str
    fase: Optional[str] = None
    jornada: Optional[str] = None
    fecha_partido: Optional[datetime] = None
    fecha_raw: Optional[str] = None
    local: str
    visitante: str
    goles_local: Optional[int] = None
    goles_visitante: Optional[int] = None
    especial: Optional[str] = None
    scraped_at: datetime

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class EventoScraped(BaseModel):
    partido_link: str
    equipo: str
    es_local: bool
    tipo: str = "gol"
    minuto: int
    minuto_raw: Optional[str] = None
    tiempo: Optional[str] = None
    jugador: Optional[str] = None
    scraped_at: datetime

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


# ===== FUNCIONES HELPER =====
async def safe_text(locator, default=""):
    try:
        if await locator.count() > 0:
            # Prefer evaluate to avoid waiting issues with inner_text
            try:
                return (await locator.evaluate("el => el.textContent.trim()")) or default
            except:
                return (await locator.first.inner_text()).strip()
        return default
    except:
        return default


async def safe_attr(locator, attr, default=None):
    try:
        return await locator.get_attribute(attr) if await locator.count() > 0 else default
    except:
        return default


async def safe_evaluate(locator, script, default=""):
    try:
        return await locator.evaluate(script) if await locator.count() > 0 else default
    except:
        return default


# ===== SYNC HELPERS =====
def safe_text_sync(locator, default=""):
    try:
        if locator.count() > 0:
            try:
                return locator.evaluate("el => el.textContent.trim()") or default
            except:
                return locator.first.inner_text().strip()
        return default
    except:
        return default


def safe_attr_sync(locator, attr, default=None):
    try:
        return locator.get_attribute(attr) if locator.count() > 0 else default
    except:
        return default


def safe_evaluate_sync(locator, script, default=""):
    try:
        return locator.evaluate(script) if locator.count() > 0 else default
    except:
        return default


async def _resolve_country_from_page(page, pais_slug: str) -> str:
    if pais_slug:
        selector = f"a.breadcrumb__link[href^='/futbol/{pais_slug}/']"
        pais = await safe_text(page.locator(selector), "")
        if pais:
            return pais

    # Fallback: tomar el segundo breadcrumb bajo /futbol/ (si existe)
    pais = await safe_text(page.locator("a.breadcrumb__link[href^='/futbol/']").nth(1), "")
    return pais or _format_country_from_slug(pais_slug)


def _resolve_country_from_page_sync(page, pais_slug: str) -> str:
    if pais_slug:
        selector = f"a.breadcrumb__link[href^='/futbol/{pais_slug}/']"
        pais = safe_text_sync(page.locator(selector), "")
        if pais:
            return pais

    # Fallback: tomar el segundo breadcrumb bajo /futbol/ (si existe)
    pais = safe_text_sync(page.locator("a.breadcrumb__link[href^='/futbol/']").nth(1), "")
    return pais or _format_country_from_slug(pais_slug)


def _parse_minuto_int(minuto_str: str) -> int:
    """Extrae el número del minuto de strings como '45+2' o '90'"""
    if not minuto_str:
        return 0
    try:
        return int(minuto_str.split('+')[0].split("'")[0].strip())
    except:
        return 0


def _is_final_score(home_score: str, away_score: str) -> bool:
    """
    Retorna True si ambos marcadores son numéricos.
    Flashscore muestra '-' cuando el partido no está finalizado.
    """
    if not home_score or not away_score:
        return False
    home = home_score.strip()
    away = away_score.strip()
    if "-" in home or "-" in away:
        return False
    return home.isdigit() and away.isdigit()


# ===== FUNCIÓN PRINCIPAL: SCRAPING DE PARTIDOS =====
async def scrape_partidos_liga(url: str, only_finished: bool = True) -> List[PartidoScraped]:
    """
    Recibe una URL de liga de Flashscore
    Devuelve una lista de partidos scrapeados
    """
    partidos: List[PartidoScraped] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(**get_launch_kwargs(headless=True))

        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = await context.new_page()

        try:
            # Extraer pais_slug de la URL PRIMERO (obligatorio para PartidoScraped)
            pais_slug = extract_country_slug_from_url(url)
            
            # Cargar página (forzar resultados si se requieren finalizados)
            target_url = _normalize_results_url(url) if only_finished else url
            await page.goto(target_url, timeout=60000, wait_until="domcontentloaded")

            if only_finished:
                redirected = _maybe_force_results_url(page.url)
                if redirected != page.url:
                    await page.goto(redirected, timeout=60000, wait_until="domcontentloaded")

            # Extraer metadata
            pais = await _resolve_country_from_page(page, pais_slug)
            liga = await safe_text(page.locator("div.heading__name"), "Liga Desconocida")
            temporada = await safe_text(page.locator("div.heading__info"), "Temporada Desconocida")

            # Clickear "Mostrar más"
            boton_count = 0
            max_intentos = 100

            while boton_count < max_intentos:
                selector = 'span[data-testid="wcl-scores-caption-05"]'
                boton = page.locator(selector)

                if await boton.count() == 0:
                    break

                try:
                    partidos_antes = await page.locator(".event__match").count()
                    await boton.first.click()
                    boton_count += 1

                    await page.wait_for_function(
                        f"document.querySelectorAll('.event__match').length > {partidos_antes}",
                        timeout=10000
                    )
                except Exception as e:
                    logger.error(f"Error en click: {e}")
                    break

            # Procesar partidos
            root = page.locator("div.sportName.soccer")
            items = root.locator(":scope > div")

            competicion = None
            jornada = None

            total = await items.count()
            for i in range(total):
                item = items.nth(i)
                clases = (await item.get_attribute("class")) or ""

                if "headerLeague__wrapper" in clases:
                    titulo = item.locator(".headerLeague__title-text")
                    competicion = (await titulo.inner_text()).strip()
                    jornada = None

                elif "event__round" in clases:
                    jornada = await safe_text(item, "")

                elif "event__match" in clases:
                    try:
                        hora = await safe_evaluate(
                            item.locator(".event__time"),
                            "el => el.childNodes[0].textContent.trim()",
                            ""
                        )
                        link_partido = await safe_attr(item.locator(".eventRowLink"), "href", "")
                        especial = await safe_text(item.locator(".event__stage--block"))

                        local = (await item.locator(".event__homeParticipant [data-testid='wcl-scores-simple-text-01']").inner_text())
                        visitante = (await item.locator(".event__awayParticipant [data-testid='wcl-scores-simple-text-01']").inner_text())

                        goles_local_str = await safe_text(item.locator(".event__score--home"), "")
                        goles_visitante_str = await safe_text(item.locator(".event__score--away"), "")

                        # Filtrar: Solo guardar partidos finalizados
                        if not _is_final_score(goles_local_str, goles_visitante_str):
                            continue

                        # Convertir goles a int
                        try:
                            goles_local = int(goles_local_str)
                            goles_visitante = int(goles_visitante_str)
                        except:
                            goles_local = None
                            goles_visitante = None

                        partido = PartidoScraped(
                            link=link_partido,
                            pais=pais,
                            pais_slug=pais_slug,
                            liga=liga,
                            temporada=temporada,
                            fase=competicion,
                            jornada=jornada,
                            fecha_partido=None,
                            fecha_raw=hora,
                            local=local,
                            visitante=visitante,
                            goles_local=goles_local,
                            goles_visitante=goles_visitante,
                            especial=especial if especial else None,
                            scraped_at=datetime.now(timezone.utc)
                        )

                        partidos.append(partido)

                    except Exception as e:
                        logger.error(f"Error en partido #{i}: {e}")
                        continue

            return partidos

        except Exception as e:
            logger.error(f"Error crítico en {url}: {e}")
            return []

        finally:
            try:
                await context.close()
            except Exception:
                pass
            try:
                await browser.close()
            except Exception:
                pass


# ===== FUNCIÓN HELPER ASYNC: SCRAPING DE GOLES (ASÍNCRONA INTERNA) =====
async def _scrape_goles_partido_async(partido_link: str) -> List[EventoScraped]:
    """
    Versión async interna para scrapear goles
    """
    eventos: List[EventoScraped] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(**get_launch_kwargs(headless=True))

        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = await context.new_page()

        try:
            await page.goto(partido_link, timeout=30000, wait_until="domcontentloaded")

            await page.wait_for_selector("div.smv__verticalSections.section", timeout=10000)

            root = page.locator("div.smv__verticalSections.section")
            items_incidentes = root.locator(":scope > div")

            tiempo = None

            count = await items_incidentes.count()
            for i in range(count):
                bloque = items_incidentes.nth(i)

                if await bloque.locator(".wcl-cell_1y2-p").count() > 0:
                    tiempo = (await bloque.locator(".wcl-cell_1y2-p").first.inner_text()).strip()

                elif await bloque.locator('[data-testid="wcl-icon-incidents-goal-soccer"]').count() > 0:
                    try:
                        if await bloque.locator(".smv__incidentHomeScore").count() > 0:
                            es_local = True
                        elif await bloque.locator(".smv__incidentAwayScore").count() > 0:
                            es_local = False
                        else:
                            es_local = None

                        if es_local is None:
                            continue

                        minuto_elem = bloque.locator(".smv__timeBox")
                        if await minuto_elem.count() > 0:
                            minuto_raw = (await minuto_elem.inner_text()).strip()
                            minuto_int = _parse_minuto_int(minuto_raw)
                        else:
                            minuto_raw = ""
                            minuto_int = 0

                        jugador_elem = bloque.locator(".smv__participantName")
                        if await jugador_elem.count() > 0:
                            jugador = (await jugador_elem.inner_text()).strip()
                        else:
                            jugador = None

                        evento = EventoScraped(
                            partido_link=partido_link,
                            equipo="",
                            es_local=es_local,
                            tipo="gol",
                            minuto=minuto_int,
                            minuto_raw=minuto_raw if minuto_raw else None,
                            tiempo=tiempo,
                            jugador=jugador,
                            scraped_at=datetime.now(timezone.utc)
                        )

                        eventos.append(evento)

                    except Exception as e:
                        logger.error(f"Error extrayendo gol: {e}")
                        continue

            return eventos

        except Exception as e:
            logger.error(f"Error en {partido_link}: {e}")
            return []

        finally:
            try:
                await page.close()
            except Exception:
                pass
            try:
                await context.close()
            except Exception:
                pass
            try:
                await browser.close()
            except Exception:
                pass


# ===== FUNCIÓN PRINCIPAL: SCRAPING DE GOLES (WRAPPER PARA THREADPOOL) =====
def scrape_goles_partido(partido_link: str) -> List[EventoScraped]:
    """
    Wrapper que ejecuta async_playwright en un nuevo event loop
    Con ProactorEventLoop en Windows para soportar subprocesos
    Diseñado para ejecutarse desde FastAPI run_in_threadpool
    """
    if sys.platform == 'win32':
        # En Windows, crear ProactorEventLoop para soportar subprocesos
        loop = asyncio.ProactorEventLoop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(_scrape_goles_partido_async(partido_link))
        finally:
            loop.close()
    else:
        # En otros sistemas, usar asyncio.run
        return asyncio.run(_scrape_goles_partido_async(partido_link))


# ===== FUNCIÓN PRINCIPAL: SCRAPING DE PARTIDOS (SYNC) =====
def scrape_partidos_liga_sync(url: str, only_finished: bool = True) -> List[PartidoScraped]:
    """
    Versión síncrona de scrape_partidos_liga para compatibilidad con entornos sync.
    """
    partidos: List[PartidoScraped] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(**get_launch_kwargs(headless=True))

        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = context.new_page()

        try:
            # Extraer pais_slug de la URL PRIMERO (obligatorio para PartidoScraped)
            pais_slug = extract_country_slug_from_url(url)
            
            # Cargar página (forzar resultados si se requieren finalizados)
            target_url = _normalize_results_url(url) if only_finished else url
            page.goto(target_url, timeout=60000, wait_until="domcontentloaded")

            if only_finished:
                redirected = _maybe_force_results_url(page.url)
                if redirected != page.url:
                    page.goto(redirected, timeout=60000, wait_until="domcontentloaded")

            # Extraer metadata
            pais = _resolve_country_from_page_sync(page, pais_slug)
            liga = safe_text_sync(page.locator("div.heading__name"), "Liga Desconocida")
            temporada = safe_text_sync(page.locator("div.heading__info"), "Temporada Desconocida")

            # Clickear "Mostrar más"
            boton_count = 0
            max_intentos = 100

            while boton_count < max_intentos:
                selector = 'span[data-testid="wcl-scores-caption-05"]'
                boton = page.locator(selector)

                if boton.count() == 0:
                    break

                try:
                    partidos_antes = page.locator(".event__match").count()
                    boton.first.click()
                    boton_count += 1

                    page.wait_for_function(
                        f"document.querySelectorAll('.event__match').length > {partidos_antes}",
                        timeout=10000
                    )
                except Exception as e:
                    logger.error(f"Error en click: {e}")
                    break

            # Procesar partidos
            root = page.locator("div.sportName.soccer")
            items = root.locator(":scope > div")

            competicion = None
            jornada = None

            total = items.count()
            for i in range(total):
                item = items.nth(i)
                clases = item.get_attribute("class") or ""

                if "headerLeague__wrapper" in clases:
                    titulo = item.locator(".headerLeague__title-text")
                    competicion = safe_text_sync(titulo, "")
                    jornada = None

                elif "event__round" in clases:
                    jornada = safe_text_sync(item, "")

                elif "event__match" in clases:
                    try:
                        hora = safe_evaluate_sync(item.locator(".event__time"), "el => el.childNodes[0].textContent.trim()", "") or ""
                        link_partido = safe_attr_sync(item.locator(".eventRowLink"), "href", "") or ""
                        especial = safe_text_sync(item.locator(".event__stage--block"), "") or ""

                        local = safe_text_sync(item.locator(".event__homeParticipant [data-testid='wcl-scores-simple-text-01']"), "")
                        visitante = safe_text_sync(item.locator(".event__awayParticipant [data-testid='wcl-scores-simple-text-01']"), "")

                        goles_local_str = safe_text_sync(item.locator(".event__score--home"), "") or ""
                        goles_visitante_str = safe_text_sync(item.locator(".event__score--away"), "") or ""

                        # Filtrar: Solo guardar partidos finalizados
                        if not _is_final_score(goles_local_str, goles_visitante_str):
                            continue

                        # Convertir goles a int
                        try:
                            goles_local = int(goles_local_str)
                            goles_visitante = int(goles_visitante_str)
                        except:
                            goles_local = None
                            goles_visitante = None

                        partido = PartidoScraped(
                            link=link_partido,
                            pais=pais,
                            pais_slug=pais_slug,
                            liga=liga,
                            temporada=temporada,
                            fase=competicion,
                            jornada=jornada,
                            fecha_partido=None,
                            fecha_raw=hora,
                            local=local,
                            visitante=visitante,
                            goles_local=goles_local,
                            goles_visitante=goles_visitante,
                            especial=especial if especial else None,
                            scraped_at=datetime.now(timezone.utc)
                        )

                        partidos.append(partido)

                    except Exception as e:
                        logger.error(f"Error en partido #{i}: {e}")
                        continue

            return partidos

        except Exception as e:
            logger.error(f"Error crítico en {url}: {e}")
            return []

        finally:
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass
