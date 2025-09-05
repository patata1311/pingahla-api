# app/main.py
from fastapi import FastAPI
from app.core.config import get_settings
from app.routers import system
from app.routers import ingestion
from fastapi.responses import RedirectResponse

tags_metadata = [
    {"name": "System", "description": "Salud del servicio y metadatos."},
    {"name": "Ingestion", "description": "Carga de CSV y batch (1–1000 filas)."},
    {"name": "Metrics", "description": "Consultas SQL 2021."},
]

def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title=settings.APP_NAME,
        version=settings.APP_VERSION,
        description=settings.APP_DESCRIPTION,
        openapi_tags=tags_metadata,
    )
    
    # Redirige "/" -> "/docs"
    @app.get("/", include_in_schema=False)
    def root():
        return RedirectResponse(url="/docs")
    app.include_router(system.router, prefix=settings.API_PREFIX)
    app.include_router(ingestion.router, prefix=settings.API_PREFIX)
    for r in app.routes:
        try:
            print("ROUTE:", r.path, list(getattr(r, "methods", [])))
        except Exception:
            pass
    return app

app = create_app()
