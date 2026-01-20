import os
import logging
import sys
from typing import Literal
from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


APP_ENV = os.environ.get("APP_ENV", "local")
BASE_DIR = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------------------
# 로그 초기 설정 (파일 Handler는 현재 사용안함)
# ---------------------------------------------------------------------------------------
def setup_logging():
    """ global logging setup """

    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, settings.LOG_LEVEL))

    if not root_logger.handlers:
        formatter = logging.Formatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Console Handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

# ---------------------------------------------------------------------------------------
# 환경 설정
# ---------------------------------------------------------------------------------------
class Settings(BaseSettings):
    CHROMADB_PATH: str = Field(alias="CHROMADB_PATH", description="Chroma SQLlite DB path")
    ES_URL: str = Field(alias="ES_HOST", description="Elasticsearch URL")
    DEVICE: Literal["cpu", "cuda"] = Field(alias="DEVICE", description="Device Name")
    CHROMA_COLLECTION: str = "m6_retrieval"
    CHROMA_COLLECTION_KO: str = "m6_retrieval_ko"
    ES_INDEX: str = "wiki_titlekor"
    LOG_LEVEL: str = "INFO"

    # ================================================================
    # 데이터베이스 설정
    # ================================================================
    DB_HOST: str = Field(alias="DB_HOST", description="DB host")
    DB_PORT: int = Field(alias="DB_PORT", description="DB port")
    DB_NAME: str = Field(alias="DB_NAME", description="DB name")
    DB_USER: str = Field(alias="DB_USER", description="DB user")
    DB_PWD: str = Field(alias="DB_PWD", description="DB password")

    model_config = SettingsConfigDict(
        env_file=BASE_DIR / f'.env.{APP_ENV}',
        env_file_encoding="utf-8",
        case_sensitive=True,
    )

    def print(self):
        fields = self.__dict__
        logger.info("settings ========================================")
        for field, value in fields.items():
            logger.info(f"{field} = {value}")
        logger.info("=================================================")

# 로그 초기화
settings = Settings()
setup_logging()
logger = logging.getLogger(__name__)
# 설정 초기화

