from typing import Dict, Any, Optional, List
import threading
import uuid
from datetime import datetime, timezone


class JobManager:
    """
    Gestor de jobs en memoria para scraping.
    Thread-safe usando locks.
    """
    def __init__(self):
        self._jobs: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

    def create_job(self, tipo: str, url: str) -> str:
        """Crea un nuevo job con estado pending"""
        job_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        job = {
            "id": job_id,
            "tipo": tipo,
            "url": url,
            "status": "pending",
            "created_at": now,
            "started_at": None,
            "finished_at": None,
            "metrics": {
                "partidos_nuevos": 0,
                "eventos_nuevos": 0,
                "errores": 0,
            },
            "error_message": None,
        }
        with self._lock:
            self._jobs[job_id] = job
        return job_id

    def start_job(self, job_id: str) -> None:
        """Transiciona un job de pending a running"""
        now = datetime.now(timezone.utc)
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                raise KeyError(f"Job {job_id} no encontrado")
            job["status"] = "running"
            job["started_at"] = now

    def finish_job(self, job_id: str, metrics: Dict[str, Any]) -> None:
        """Transiciona un job a done con métricas"""
        now = datetime.now(timezone.utc)
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                raise KeyError(f"Job {job_id} no encontrado")
            job["status"] = "done"
            job["finished_at"] = now
            job["metrics"] = metrics

    def fail_job(self, job_id: str, error: str) -> None:
        """Transiciona un job a failed con mensaje de error"""
        now = datetime.now(timezone.utc)
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                raise KeyError(f"Job {job_id} no encontrado")
            job["status"] = "failed"
            job["finished_at"] = now
            job["error_message"] = error

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Obtiene el estado actual de un job"""
        with self._lock:
            job = self._jobs.get(job_id)
            # Retornar una copia para evitar cambios externos
            return dict(job) if job else None

    def list_jobs(self) -> List[Dict[str, Any]]:
        """Lista todos los jobs"""
        with self._lock:
            return [dict(job) for job in self._jobs.values()]

    def clear_jobs(self) -> None:
        """Limpia todos los jobs (útil para testing)"""
        with self._lock:
            self._jobs.clear()


# Singleton manager para la app
manager = JobManager()
