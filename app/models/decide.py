"""
NEUROAUTH v3.0.0 — Pydantic models for /decide endpoint.
"""
from __future__ import annotations

from typing import Any, Optional
from pydantic import BaseModel, Field


class DecideRequest(BaseModel):
    """Payload enviado pelo frontend ou API para solicitar decisao."""
    profile_id: str = Field(..., description="ID do procedimento no dado mestre")
    convenio_id: str = Field("", alias="convenio", description="ID do convenio/operadora")
    nome_paciente: str = Field("", description="Nome do paciente")
    hospital: str = Field("", description="Hospital onde sera realizado")
    carater: str = Field("", alias="carater_cod", description="Carater: ELETIVO/URGENCIA")
    cid_principal: str = Field("", alias="cid", description="CID principal")
    cid2: str = Field("", description="CID secundario")
    qtd_niveis: Optional[int] = Field(None, alias="niveis", description="Numero de niveis")
    lateralidade: str = Field("", description="DIREITA/ESQUERDA/BILATERAL")
    cbo_executor: str = Field("", alias="cbo", description="CBO do executor")
    indicacao_clinica: str = Field("", description="Indicacao clinica textual")
    justificativa_opme: str = Field("", description="Justificativa para OPME")
    procedimento: str = Field("", description="Nome/descricao do procedimento")
    medico_solicitante: str = Field("", description="Nome do medico solicitante")
    opmes_selecionados: list[dict[str, Any]] = Field(default_factory=list, description="Lista de OPMEs")
    form_version: str = Field("", description="Versao do formulario")
    source: str = Field("", description="Origem: form/api/test")

    # Campos opcionais para calendar
    status_agendamento: str = Field("", description="Status de agendamento")
    google_event_id: str = Field("", description="ID do evento Google Calendar")
    google_calendar_id: str = Field("primary", description="ID do calendario")

    class Config:
        populate_by_name = True
        extra = "allow"  # aceitar campos extras sem erro


class DecideResponse(BaseModel):
    """Resposta do motor de decisao."""
    decision_status: str
    go_class: str = ""
    confidence_global: float = 0.0
    can_send: bool = False
    can_autofill: bool = False
    resumo_operacional: str = ""
    bloqueios: list[dict[str, Any]] = Field(default_factory=list)
    pendencias: list[dict[str, Any]] = Field(default_factory=list)
    alertas: list[dict[str, Any]] = Field(default_factory=list)
    campos_ok: list[str] = Field(default_factory=list)
    campos_inferidos: list[dict[str, Any]] = Field(default_factory=list)
    autopreenchimentos: list[dict[str, Any]] = Field(default_factory=list)
    proxima_acao_sugerida: str = ""
    engine_version: str = ""
    episodio_id: str = ""
    _run_id: str = ""
    precheck: Optional[dict[str, Any]] = None

    class Config:
        extra = "allow"


class AuthGoogleRequest(BaseModel):
    """Payload para autenticacao via Google."""
    id_token: str = Field(..., description="Google ID token do frontend")


class AuthGoogleResponse(BaseModel):
    """Resposta de autenticacao Google."""
    ok: bool
    email: str = ""
    name: str = ""
    sub: str = ""
    api_key: str = Field("", description="API key para chamadas subsequentes")
