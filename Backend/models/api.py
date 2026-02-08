from pydantic import BaseModel
from typing import Dict, Any, Optional, Literal
from datetime import datetime


class ScrapeLeagueRequest(BaseModel):
    url: str
    force: bool = False
    dry_run: bool = False


class ScrapeResult(BaseModel):
    partidos_nuevos: int
    eventos_nuevos: int
    errores: int


class ScrapingJobMetrics(BaseModel):
    """Métricas del job de scraping"""
    partidos_nuevos: int = 0
    eventos_nuevos: int = 0
    errores: int = 0


class ScrapingJob(BaseModel):
    """Modelo de un job de scraping con estado y métricas"""
    id: str
    tipo: Literal["league", "match"]
    url: str
    status: Literal["pending", "running", "done", "failed"]
    created_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    metrics: ScrapingJobMetrics
    error_message: Optional[str] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class AnalysisRequest(BaseModel):
    tipo: str
    filtros: Dict[str, Any]
    parametros: Dict[str, Any]


class AnalysisResult(BaseModel):
    tipo: str
    resumen: Dict[str, Any]
    detalles: Dict[str, Any] | None = None


class PoissonRequest(BaseModel):
    temporada_id: int
    equipo_local_id: int
    equipo_visitante_id: int
    umbral_goles: float


class PoissonResult(BaseModel):
    temporada_id: int
    equipo_local_id: int
    equipo_visitante_id: int
    equipo_local: str
    equipo_visitante: str
    umbral_goles: float
    goles_esperados: Dict[str, Any]
    total_goles_esperados: Optional[float]
    probabilidades: Dict[str, Any]


class PoissonBasicTeam(BaseModel):
    id: int
    nombre: str
    xg: float


class PoissonBasicOver(BaseModel):
    linea: float
    probabilidad: float
    recomendacion: Literal["OVER", "NO OVER"]


class PoissonBasicResult(BaseModel):
    local: PoissonBasicTeam
    visitante: PoissonBasicTeam
    total_xg: float
    over: PoissonBasicOver
