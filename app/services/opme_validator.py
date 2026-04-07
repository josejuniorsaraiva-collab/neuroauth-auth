"""
app/services/opme_validator.py
NEUROAUTH — OPME Validator v2.0

Módulo standalone de validação de OPME.
Separação total de dados, lógica e output.

Melhorias sobre input_hardening v1:
  - PROCEDURE_RULES por perfil (proibidos + sugestões)
  - Pendências ordenadas por severidade (critica > alta > media > baixa)
  - Sugestões automáticas por item incompatível
  - Acumulação total — não para no primeiro problema
  - risco_glosa = "crítico" quando incompatibilidade detectada
"""

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class OpmeItem:
    descricao: str
    qtd: int = 1
    fabricante: Optional[str] = None
    codigo: Optional[str] = None


@dataclass
class OpmePendencia:
    tipo: str
    severidade: str  # critica | alta | media | baixa
    item: str
    mensagem: str
    regra_id: Optional[str] = None
    sugestoes: List[str] = field(default_factory=list)


@dataclass
class OpmeValidationResult:
    opme_generico_detectado: bool = False
    opme_incompativel_detectado: bool = False
    itens_incompativeis: List[str] = field(default_factory=list)
    pendencias: List[OpmePendencia] = field(default_factory=list)
    logs: List[str] = field(default_factory=list)
    risco_glosa: str = "baixo"


# ── TERMOS GENÉRICOS ─────────────────────────────────────────────────────────

GENERIC_TERMS = [
    "kit",
    "opme padrão", "opme padrao",
    "materiais habituais",
    "material padrão", "material padrao",
    "material cirúrgico", "material cirurgico",
    "instrumental padrão", "instrumental padrao",
]

# ── REGRAS POR PERFIL DE PROCEDIMENTO ────────────────────────────────────────

PROCEDURE_RULES = {
    "microdiscectomia": {
        "proibidos": [
            "cage",
            "parafuso pedicular", "parafusos pediculares",
            "parafuso transpedicular", "parafusos transpediculares",
            "transpedicular",
            "haste longitudinal", "haste de conexão",
            "crosslink",
            "fixador pedicular", "sistema pedicular", "implante pedicular",
            "artrodese", "fusão intersomática",
            "placa cervical",
        ],
        "sugestoes": {
            "cage":                  ["barreira hemostática", "dreno se justificado", "instrumental microcirúrgico compatível"],
            "parafuso pedicular":    ["sem implante de fixação", "material hemostático", "instrumental microcirúrgico"],
            "parafusos pediculares": ["sem implante de fixação", "material hemostático", "instrumental microcirúrgico"],
            "transpedicular":        ["sem fixação pedicular", "rever se há indicação de artrodese associada"],
            "artrodese":             ["se há instabilidade, solicitar artrodese como procedimento separado"],
        },
    },
    "artrodese lombar": {
        "proibidos": [],
        "sugestoes": {},
    },
    "laminectomia": {
        "proibidos": [],
        "sugestoes": {},
    },
    "craniotomia": {
        "proibidos": [],
        "sugestoes": {},
    },
}


# ── FUNÇÕES ──────────────────────────────────────────────────────────────────

def normalize_text(text: str) -> str:
    return " ".join(text.lower().strip().split())


def detect_generic_item(item_desc: str) -> bool:
    texto = normalize_text(item_desc)
    return any(term in texto for term in GENERIC_TERMS)


def detect_procedure_profile(procedimento: str) -> str:
    texto = normalize_text(procedimento)
    if "microdiscectomia" in texto or "discectomia simples" in texto:
        return "microdiscectomia"
    if "artrodese" in texto:
        return "artrodese lombar"
    if "laminectomia" in texto or "laminotomia" in texto:
        return "laminectomia"
    if "craniotomia" in texto or "craniectomia" in texto:
        return "craniotomia"
    return "desconhecido"


def validate_opme_items(procedimento: str, opme_items) -> OpmeValidationResult:
    """
    Valida lista de itens OPME contra o perfil do procedimento.
    Retorna OpmeValidationResult com pendências ordenadas por severidade.
    Acumula TODOS os problemas — não para no primeiro.
    """
    result = OpmeValidationResult()
    perfil = detect_procedure_profile(procedimento)
    regras = PROCEDURE_RULES.get(perfil, {"proibidos": [], "sugestoes": {}})

    for item in opme_items:
        if not item.descricao:
            continue

        desc_norm = normalize_text(item.descricao)

        # ── Check 1: OPME genérico ──────────────────────────────────────────
        if detect_generic_item(item.descricao):
            result.opme_generico_detectado = True
            result.pendencias.append(
                OpmePendencia(
                    tipo="OPME_GENERICO",
                    severidade="alta",
                    item=item.descricao,
                    mensagem=(
                        f"OPME genérico detectado: '{item.descricao}' — "
                        "especificar por item (descrição, quantidade, fabricante). "
                        "Declaração como 'kit' gera glosa na fatura hospitalar."
                    ),
                    regra_id="RGL_OPME_GENERIC",
                )
            )
            result.logs.append(f"OPME_GENERIC_BLOCK: '{item.descricao}'")

        # ── Check 2: OPME incompatível com perfil ───────────────────────────
        incompatibilidade_encontrada = False
        regra_ativada = None

        for proibido in regras["proibidos"]:
            if proibido in desc_norm:
                incompatibilidade_encontrada = True
                regra_ativada = proibido
                result.opme_incompativel_detectado = True
                result.itens_incompativeis.append(item.descricao)
                result.logs.append(
                    f"OPME_INCOMPATIVEL: item='{item.descricao}' "
                    f"procedimento='{procedimento}' regra='{proibido}'"
                )
                break  # um match por item é suficiente

        if incompatibilidade_encontrada:
            sugestoes = regras["sugestoes"].get(regra_ativada, [])
            result.pendencias.append(
                OpmePendencia(
                    tipo="OPME_INCOMPATIVEL",
                    severidade="critica",
                    item=item.descricao,
                    mensagem=(
                        f"INCONSISTÊNCIA OPME: '{item.descricao}' não é compatível "
                        f"com {perfil} (sem artrodese/fixação). "
                        "Rever indicação ou documentar justificativa de exceção."
                    ),
                    regra_id="RGL_OPME_PROFILE_MISMATCH",
                    sugestoes=sugestoes,
                )
            )

    # ── Ordenar pendências: critica > alta > media > baixa ──────────────────
    ORDEM = {"critica": 0, "alta": 1, "media": 2, "baixa": 3}
    result.pendencias.sort(key=lambda p: ORDEM.get(p.severidade, 9))

    # ── risco_glosa final ───────────────────────────────────────────────────
    if result.opme_incompativel_detectado:
        result.risco_glosa = "crítico"
    elif result.opme_generico_detectado:
        result.risco_glosa = "alto"

    return result
