"""
app/core/config.py
Todas as configs via env vars. Nunca hardcodar segredos.
Render: Settings > Environment Variables.

Variáveis obrigatórias (6):
  JWT_SECRET              — gerar: openssl rand -hex 32
  GOOGLE_CLIENT_ID        — Google Cloud Console > OAuth 2.0 Client ID
  GOOGLE_CREDENTIALS_JSON — JSON da service account (string completa)
  SPREADSHEET_ID          — ID da Planilha-Mãe
  ALLOWED_ORIGINS         — URL exata do GitHub Pages (ou CSV de origens)
  MAKE_DOC_WEBHOOK        — opcional: periférico de geração de docs
"""

from pydantic_settings import BaseSettings
from pydantic import field_validator
from typing import List


class Settings(BaseSettings):
    # Auth — defaults vazios para permitir build/start no Render
    # Endpoints protegidos falham com erro claro se não configuradas
    JWT_SECRET: str = ""
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRE_MINUTES: int = 480          # 8h

    # Google OAuth (validação do id_token)
    GOOGLE_CLIENT_ID: str = ""

    # Google Sheets
    GOOGLE_CREDENTIALS_JSON: str = ""
    SPREADSHEET_ID: str = ""

    # CORS — aceita string CSV ou lista
    ALLOWED_ORIGINS: List[str] = [
        "https://josejuniorsaraiva-collab.github.io"
    ]

    # Make.com periférico (opcional — pode ficar vazio no primeiro shadow)
    MAKE_DOC_WEBHOOK: str = ""
    MAKE_BILLING_WEBHOOK: str = ""

    @field_validator("ALLOWED_ORIGINS", mode="before")
    @classmethod
    def parse_origins(cls, v):
        if isinstance(v, str):
            return [item.strip() for item in v.split(",") if item.strip()]
        return v

    class Config:
        env_file = ".env"


settings = Settings()
