# app/routers/system.py
from fastapi import APIRouter, Depends, status
from app.core.config import get_settings, Settings
from app.core.security import validate_api_key

router = APIRouter()

@router.get("/health", tags=["System"], summary="Health check",
            responses={200: {"description": "Service healthy"}})
def health():
    return {"status": "ok"}

@router.get("/info", tags=["System"], summary="Información de la app",
            status_code=status.HTTP_200_OK)
def info(
    settings: Settings = Depends(get_settings),
    _: bool = Depends(validate_api_key),
):
    return {
        "name": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "db": settings.MYSQL_DB,
        "engine": "SQLAlchemy + MySQL",
    }
