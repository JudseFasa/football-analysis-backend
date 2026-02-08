from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.concurrency import run_in_threadpool
from pathlib import Path
import logging
from scrapers.flashscore_adapter import scrape_partidos_liga_sync, scrape_goles_partido
from app.jobs import manager as job_manager

logger = logging.getLogger(__name__)

# Configuración de plantillas
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Crear el router
router = APIRouter()

# Ruta para el dashboard
@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})

# Ruta para API playground
@router.get("/api-test", response_class=HTMLResponse)
async def api_test(request: Request):
    return templates.TemplateResponse("api_test.html", {"request": request})

# Ruta para formulario simple de scraping
@router.get("/scraping-form", response_class=HTMLResponse)
async def scraping_form(request: Request):
    return templates.TemplateResponse("scraping_form.html", {"request": request})

# Ruta para ejecutar scraping manual
@router.post("/scraping", response_class=HTMLResponse)
async def manual_scraping(request: Request, url: str = Form(...)):
    """
    Ejecuta scraping de partidos y goles con sistema de jobs.
    - Crea un job
    - Scrapea partidos de la liga
    - Para cada partido, scrapea goles
    - Actualiza el job con métricas
    """
    job_id = job_manager.create_job("league", url)
    
    try:
        job_manager.start_job(job_id)
        
        partidos_nuevos = 0
        eventos_nuevos = 0
        errores = 0

        # Scraping de partidos
        partidos = await run_in_threadpool(scrape_partidos_liga_sync, url)
        partidos_nuevos = len(partidos) if partidos is not None else 0

        # Scraping de goles para cada partido
        if partidos:
            for partido in partidos:
                try:
                    eventos = await run_in_threadpool(scrape_goles_partido, partido.link)
                    eventos_nuevos += len(eventos) if eventos else 0
                except Exception as e:
                    logger.error(f"Error scraping goles para {partido.link}: {e}")
                    errores += 1

        metrics = {
            "partidos_nuevos": partidos_nuevos,
            "eventos_nuevos": eventos_nuevos,
            "errores": errores,
        }
        
        job_manager.finish_job(job_id, metrics)
        
        job = job_manager.get_job(job_id)
        
    except Exception as e:
        logger.error(f"Error en manual_scraping: {e}")
        job_manager.fail_job(job_id, str(e))
        job = job_manager.get_job(job_id)

    return templates.TemplateResponse("scraping.html", {"request": request, "job": job})


# Ruta para ver el estado de un job
@router.get("/scraping/{job_id}", response_class=HTMLResponse)
async def view_scraping_status(request: Request, job_id: str):
    """
    Muestra el estado actual de un job de scraping.
    Permite refrescar manualmente para ver cambios.
    """
    job = job_manager.get_job(job_id)
    
    if not job:
        return templates.TemplateResponse(
            "scraping.html", 
            {
                "request": request, 
                "job": None,
                "error": f"Job {job_id} no encontrado"
            }
        )
    
    return templates.TemplateResponse("scraping.html", {"request": request, "job": job})


@router.post("/scraping/league", response_class=JSONResponse)
def scraping_league(url: str = Form(...)):
    """
    Endpoint para ejecutar scraping de una liga de Flashscore.
    Recibe una URL y devuelve los partidos scrapeados.
    """
    try:
        # Usar la versión síncrona del scraping
        partidos = scrape_partidos_liga_sync(url)
        return {"success": True, "data": [p.dict() for p in partidos]}
    except Exception as e:
        return {"success": False, "error": str(e)}
