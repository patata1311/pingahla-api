# app/core/config.py
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path

class Settings(BaseSettings):
    APP_NAME: str = "Pingahla Data Engineering API"
    APP_VERSION: str = "0.1.0"
    APP_DESCRIPTION: str = "API base para ingesta y métricas."
    API_PREFIX: str = "/api/v1"

    API_KEY: str = "changeme"
    

    # MySQL
    MYSQL_HOST: str = "localhost"
    MYSQL_PORT: int = 3306
    MYSQL_DB: str = "pingahla"
    MYSQL_USER: str = "root"
    MYSQL_PASSWORD: str = "2023"
    MYSQL_CHARSET: str = "utf8mb4"

    DATA_DIR: str = "./app/data/inbox"
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @property
    def data_path(self) -> Path: 
        return Path(self.DATA_DIR).expanduser().resolve()
    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"mysql+pymysql://{self.MYSQL_USER}:{self.MYSQL_PASSWORD}"
            f"@{self.MYSQL_HOST}:{self.MYSQL_PORT}/{self.MYSQL_DB}"
            f"?charset={self.MYSQL_CHARSET}"
        )

@lru_cache
def get_settings() -> Settings:
    return Settings()
