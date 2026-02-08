import asyncio
import sys

# En Windows, usar ProactorEventLoopPolicy antes de crear/importar cualquier
# componente async que pueda crear un event loop o usar subprocessos.
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from fastapi import FastAPI
from app.router import router
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="Football Scraping & Analysis API",
    version="0.1.0",
)

app.include_router(router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from app.web import router as web_router

app.include_router(web_router)