"""
app/core/config.py
Todas as configs via env vars. Nunca hardcodar segredos.
Render: Settings > Environment Variables + Secret Files.

Variaveis obrigatorias (6):
 JWT_SECRET — gerar: openssl rand -hex 32
 GOOGLE_CLIENT_ID — Google Cloud Console > OAuth 2.0 Client ID
 GOOGLE_APPLICATION_CREDENTIALS — path para JSON da service account
   Render: Secret Files > /etc/secrets/gcp-sa.json
 SPREADSHEET_ID — ID da Planilha-Mae
 ALLOWED_ORIGINS — URL exata do GitHub Pages (ou CSV de origens)
 MAKE_WEBHOOK_PROFILE — webhook Make.com para perfil
"""

from pydantic_settings import BaseSettings
from pydantic import field_validator
from typing import List


class Settings(BaseSettings):
    # Auth — defaults vazios para permitir build/start no Render
    # Endpoints protegidos falham com erro claro se nao configuradas
    JWT_SECRET: str = ""
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRE_MINUTES: int = 480  # 8h

    # Google OAuth (validacao do id_token)
    GOOGLE_CLIENT_ID: str = ""

    # Google Sheets — path to service-account JSON file
    # Render: Settings > Secret Files > /etc/secrets/gcp-sa.json
    GOOGLE_APPLICATION_CREDENTIALS: str = ""
    SPREADSHEET_ID: str = ""

    # CORS — stored as str to avoid pydantic-settings JSON parse on List[str]
    ALLOWED_ORIGINS: str = "https://josejuniorsaraiva-collab.github.io,https://neuroauth.com.br"

    # Make.com webhooks (proxy seguro via /api/make-proxy)
    MAKE_WEBHOOK_PROFILE: str = ""
    MAKE_WEBHOOK_GENERAL: str = ""

    # Make.com periferico (opcional)
    MAKE_DOC_WEBHOOK: str = ""
    MAKE_BILLING_WEBHOOK: str = ""

    @property
    def allowed_origins_list(self) -> List[str]:
        """Parse CSV string into list for CORS middleware."""
        return [item.strip() for item in self.ALLOWED_ORIGINS.split(",") if item.strip()]

    model_config = {"env_file": ".env"}


settings = Settings()
