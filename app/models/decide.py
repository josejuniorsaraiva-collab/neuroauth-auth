"""
app/models/decide.py
Schemas de entrada e saída do endpoint /decide.
"""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict
from datetime import datetime
import uuid


class OpmeItem(BaseModel):
    descricao: str
    qtd: int = 1
    fabricante: Optional[str] = ""
    codigo: Optional[str] = ""
    anvisa: Optional[str] = ""


class DecideRequest(BaseModel):
    episodio_id: Optional[str] = Field(
        default_factory=lambda: (
            f"EP-{datetime.utcnow().strftime('%Y%m%d')}-{str(uuid.uuid4())[:6].upper()}"
        )
    )
    cid_principal: str
    procedimento: str
    cod_cbhpm: Optional[str] = ""
    convenio: str
    indicacao_clinica: str
    achados_resumo: Optional[str] = ""
    tto_conservador: Optional[str] = ""
    necessita_opme: str = "Não"
    opme_items: Optional[List[OpmeItem]] = []
    crm: str
    cbo: str
    medico_solicitante: str
    justificativas_opme: Optional[Dict[str, str]] = Field(
        default_factory=dict,
        description=(
            "Justificativas clínicas por item OPME. "
            "Chave = descricao do OpmeItem, Valor = justificativa textual. "
            "Exemplo: {'Cola biológica Tisseel': 'Vedação dural em base de crânio'}"
        )
    )


class DecideResponse(BaseModel):
    decision_run_id: str
    episodio_id: str
    classification: str
    # GO | GO_COM_RESSALVAS | NO_GO | PRE_ANALISE_APENAS
    decision_status: str
    # APROVADO | PENDENTE | NEGADO | PRE_ANALISE
    score: Optional[int] = None
    # None quando PRE_ANALISE_APENAS (score não é definitivo)
    justificativa: str
    pendencias: List[str] = []
    bloqueios: List[str] = []
    risco_glosa: str = "indeterminado"
    pontos_frageis: List[str] = []
    proximos_passos: List[str] = []
    tuss_normalizado: Optional[str] = None
    timestamp: str
