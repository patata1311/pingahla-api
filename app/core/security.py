# app/core/security.py
from fastapi import Depends, HTTPException, Security, status
from fastapi.security.api_key import APIKeyHeader
from app.core.config import get_settings, Settings

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

def validate_api_key(
    api_key: str = Security(api_key_header),
    settings: Settings = Depends(get_settings),
):
    if not api_key or api_key != settings.API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )
    return True
