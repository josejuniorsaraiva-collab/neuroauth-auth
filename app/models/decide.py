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
    model_config = {"extra": "ignore"}

    episodio_id: Optional[str] = Field(
        default_factory=lambda: (
            f"EP-{datetime.utcnow().strftime('%Y%m%d')}-{str(uuid.uuid4())[:6].upper()}"
        )
    )
    trace_id: Optional[str] = None
    cid_principal: str
    procedimento: str
    cod_cbhpm: Optional[str] = ""
    convenio: str
    indicacao_clinica: str
    achados_resumo: Optional[str] = ""
    tto_conservador: Optional[str] = ""
    necessita_opme: str = "Não"
    opme_items: Optional[List[OpmeItem]] = []
    crm: Optional[str] = ""
    cbo: Optional[str] = ""
    medico_solicitante: Optional[str] = ""
    justificativas_opme: Optional[Dict[str, str]] = Field(
        default_factory=dict,
        description=(
            "Justificativas clínicas por item OPME. "
            "Chave = descricao do OpmeItem, Valor = justificativa textual. "
            "Exemplo: {'Cola biológica Tisseel': 'Vedação dural em base de crânio'}"
        )
    )


class DecideResponse(BaseModel):
    # Campos originais (manter retrocompat)
    decision_run_id: str
    episodio_id: str
    classification: str
    # GO | GO_COM_RESSALVAS | NO_GO | PRE_ANALISE_APENAS
    decision_status: str
    # APROVADO | AUTORIZADO_COM_RESSALVAS | NEGADO | PRE_ANALISE
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
    # Campos adicionais para frontend v3
    ok: bool = True
    decision: str = ""         # espelho de classification para frontend
    trace_id: str = ""
    document_url: Optional[str] = None
    motor_version: str = "1.3"
    ts: str = ""               # espelho de timestamp para frontend
    # v1.3 — Motor 3 camadas: output estruturado
    falhas: List[str] = []             # anti-glosa falhas
    correcoes: List[str] = []          # sugestões de correção
    score_clinico: Optional[float] = None  # 0.0-1.0
    camada1: str = ""                  # PASS | FAIL
    camada2_score: Optional[float] = None  # score clínico 0-1
    camada3_risco: str = ""            # baixo|moderado|alto|critico
    gate_reason: str = ""              # razão do gate binário
    tempo_execucao_ms: Optional[int] = None  # tempo de execução do motor
    # v2.0: trace estruturado completo (para persistência e hub explicável)
    v2_trace: Optional[dict] = None
    # v2.1: idempotência explícita
    idempotent_replay: bool = False
