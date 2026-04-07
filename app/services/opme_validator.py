"""
app/services/opme_validator.py
NEUROAUTH — OPME Validator v2.1

Expansão sobre v2.0:
  - 4 perfis clínicos: microdiscectomia, artrodese_lombar,
    craniotomia_tumoral, aneurisma_endovascular
  - aliases por perfil (detecção robusta de nomes alternativos)
  - obrigatorios por perfil (ex: parafuso pedicular em artrodese)
  - opcionais_com_justificativa (cola biológica, dreno, stent...)
  - justificativas_opme como parâmetro opcional
  - apply_opme_caps() standalone
  - risco_glosa: critico | alto | medio | baixo
  - perfil_procedimento exposto no resultado
  - perfil desconhecido tratado com lógica própria
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any


@dataclass
class OpmeItem:
    descricao: str
    qtd: int = 1
    fabricante: Optional[str] = None
    codigo: Optional[str] = None


@dataclass
class OpmePendencia:
    tipo: str
    severidade: str   # critica | alta | media | baixa
    item: str
    mensagem: str
    regra_id: Optional[str] = None
    sugestoes: List[str] = field(default_factory=list)


@dataclass
class OpmeValidationResult:
    perfil_procedimento: str = "desconhecido"
    opme_generico_detectado: bool = False
    opme_incompativel_detectado: bool = False
    itens_incompativeis: List[str] = field(default_factory=list)
    itens_obrigatorios_faltantes: List[str] = field(default_factory=list)
    itens_que_exigem_justificativa: List[str] = field(default_factory=list)
    pendencias: List[OpmePendencia] = field(default_factory=list)
    logs: List[str] = field(default_factory=list)
    risco_glosa: str = "baixo"


# ── TERMOS GENÉRICOS ──────────────────────────────────────────────────────────

GENERIC_TERMS = [
    "kit",
    "opme padrão", "opme padrao",
    "materiais habituais",
    "material padrão", "material padrao",
    "materiais diversos",
]

# ── REGRAS POR PERFIL ─────────────────────────────────────────────────────────

PROFILE_RULES: Dict[str, Dict[str, Any]] = {
    "microdiscectomia": {
        "aliases": ["microdiscectomia", "discectomia lombar", "microdiscectomia lombar"],
        "proibidos": [
            "cage",
            "parafuso pedicular", "parafusos pediculares",
            "parafuso transpedicular", "parafusos transpediculares",
            "transpedicular",
            "haste longitudinal", "haste de conexão",
            "crosslink",
            "fixador pedicular", "sistema pedicular", "implante pedicular",
            "artrodese", "fusão intersomática",
            "barra longitudinal",
            "dispositivo intersomático", "dispositivo intersomatico",
            "placa cervical",
        ],
        "obrigatorios": [],
        "opcionais_com_justificativa": ["dreno"],
        "sugestoes": {
            "cage":                  ["barreira hemostática", "dreno se houver justificativa", "instrumental microcirúrgico compatível"],
            "parafuso pedicular":    ["sem implantes", "hemostático", "afastador tubular se aplicável"],
            "parafusos pediculares": ["sem implantes", "hemostático", "afastador tubular se aplicável"],
            "transpedicular":        ["sem fixação pedicular", "rever se há indicação de artrodese associada"],
            "artrodese":             ["se há instabilidade, solicitar artrodese como procedimento separado"],
        },
    },
    "artrodese_lombar": {
        "aliases": ["artrodese lombar", "tlif", "plif", "artrodese posterior lombar"],
        "proibidos": [],
        "obrigatorios": ["parafuso pedicular"],
        "opcionais_com_justificativa": ["dreno", "cage", "substituto ósseo", "substituto osseo"],
        "sugestoes": {},
    },
    "craniotomia_tumoral": {
        "aliases": [
            "craniotomia tumoral", "craniotomia para tumor",
            "ressecção tumoral", "resseccao tumoral",
            "exérese de tumor", "exerese de tumor",
            "craniotomia para exérese",
        ],
        "proibidos": [
            "stent intracraniano",
            "coils",
            "desviador de fluxo",
            "parafuso pedicular",
            "cage",
        ],
        "obrigatorios": [],
        "opcionais_com_justificativa": [
            "cola biológica", "cola biologica",
            "substituto de dura", "substituto de dura-máter", "substituto de dura mater",
            "neuronavegação", "neuronavegacao",
            "monitorização neurofisiológica", "monitorizacao neurofisiologica",
            "dreno",
        ],
        "sugestoes": {
            "stent intracraniano": ["cola biológica se justificada", "substituto de dura se justificado"],
            "coils": ["cola biológica se justificada", "hemostático compatível"],
        },
    },
    "aneurisma_endovascular": {
        "aliases": [
            "embolização aneurisma", "embolizacao aneurisma",
            "aneurisma endovascular", "tratamento endovascular aneurisma",
            "embolização endovascular",
        ],
        "proibidos": [
            "parafuso pedicular", "parafusos pediculares",
            "parafuso transpedicular", "parafusos transpediculares",
            "cage",
            "haste",
            "substituto de dura",
            "placa cervical",
        ],
        "obrigatorios": [],
        "opcionais_com_justificativa": [
            "stent", "balão", "balao",
            "desviador de fluxo",
            "microcateter",
        ],
        "sugestoes": {
            "parafuso pedicular": ["coils", "microcateter", "stent se indicado", "balão se indicado"],
            "cage": ["coils", "microcateter", "stent se indicado", "balão se indicado"],
        },
    },
}

SEVERIDADE_RANK = {"critica": 0, "alta": 1, "media": 2, "baixa": 3}


# ── UTILITÁRIOS ───────────────────────────────────────────────────────────────

def normalize_text(text: Optional[str]) -> str:
    if not text:
        return ""
    return " ".join(text.lower().strip().split())


def contains_any(text: str, terms: List[str]) -> bool:
    return any(normalize_text(t) in text for t in terms)


def detect_generic_item(item_desc: str) -> bool:
    return contains_any(normalize_text(item_desc), GENERIC_TERMS)


def detect_profile(procedimento: str) -> str:
    proc = normalize_text(procedimento)
    for profile_name, cfg in PROFILE_RULES.items():
        if contains_any(proc, cfg["aliases"]):
            return profile_name
    return "desconhecido"


def item_matches_term(item_desc: str, term: str) -> bool:
    return normalize_text(term) in normalize_text(item_desc)


def _add_pendencia(result: OpmeValidationResult, *, tipo: str, severidade: str,
                   item: str, mensagem: str, regra_id: Optional[str] = None,
                   sugestoes: Optional[List[str]] = None) -> None:
    result.pendencias.append(OpmePendencia(
        tipo=tipo, severidade=severidade, item=item,
        mensagem=mensagem, regra_id=regra_id, sugestoes=sugestoes or [],
    ))


# ── VALIDAÇÃO PRINCIPAL ───────────────────────────────────────────────────────

def validate_opme_items(
    procedimento: str,
    opme_items: List[OpmeItem],
    justificativas_opme: Optional[Dict[str, str]] = None,
) -> OpmeValidationResult:
    """
    Valida lista OPME contra perfil do procedimento.
    justificativas_opme: dict {descricao_item: "justificativa textual"}
    """
    justificativas_opme = justificativas_opme or {}
    result = OpmeValidationResult()
    perfil = detect_profile(procedimento)
    result.perfil_procedimento = perfil

    # Perfil desconhecido — valida apenas genérico
    if perfil == "desconhecido":
        result.logs.append("OPME_PROFILE_UNKNOWN")
        for item in opme_items:
            if not item.descricao:
                continue
            if detect_generic_item(item.descricao):
                result.opme_generico_detectado = True
                _add_pendencia(result, tipo="OPME_GENERICO", severidade="alta",
                    item=item.descricao, regra_id="RGL_OPME_GENERIC",
                    mensagem=f"OPME genérico detectado: '{item.descricao}' — especificar por item.")
                result.logs.append(f"OPME_GENERIC_BLOCK: {item.descricao}")
        result.risco_glosa = "alto" if result.opme_generico_detectado else "medio"
        return result

    cfg = PROFILE_RULES[perfil]
    normalized_items = [normalize_text(i.descricao) for i in opme_items if i.descricao]

    for item in opme_items:
        if not item.descricao:
            continue
        desc_norm = normalize_text(item.descricao)

        # Check 1 — OPME genérico
        if detect_generic_item(item.descricao):
            result.opme_generico_detectado = True
            _add_pendencia(result, tipo="OPME_GENERICO", severidade="alta",
                item=item.descricao, regra_id="RGL_OPME_GENERIC",
                mensagem=(f"OPME genérico detectado: '{item.descricao}' — "
                    "especificar por item (descrição, quantidade, fabricante). "
                    "Declaração como 'kit' gera glosa na fatura hospitalar."))
            result.logs.append(f"OPME_GENERIC_BLOCK: {item.descricao}")

        # Check 2 — OPME incompatível
        incompatibilidades: List[str] = []
        sugestoes_agg: List[str] = []
        for proibido in cfg["proibidos"]:
            if item_matches_term(desc_norm, proibido):
                incompatibilidades.append(proibido)
                sugestoes_agg.extend(cfg["sugestoes"].get(proibido, []))

        if incompatibilidades:
            result.opme_incompativel_detectado = True
            result.itens_incompativeis.append(item.descricao)
            _add_pendencia(result, tipo="OPME_INCOMPATIVEL", severidade="critica",
                item=item.descricao, regra_id="RGL_OPME_PROFILE_MISMATCH",
                sugestoes=sorted(set(sugestoes_agg)),
                mensagem=(
                    f"OPME incompatível com o procedimento '{procedimento}': "
                    f"{item.descricao}. "
                    f"Termos incompatíveis: {', '.join(incompatibilidades)}."
                ))
            result.logs.append(
                f"OPME_INCOMPATIVEL: item='{item.descricao}' "
                f"procedimento='{procedimento}' termos='{incompatibilidades}'"
            )

        # Check 3 — Opcional com justificativa
        for opcional in cfg["opcionais_com_justificativa"]:
            if item_matches_term(desc_norm, opcional):
                justificativa = justificativas_opme.get(item.descricao, "").strip()
                if not justificativa and not (item.fabricante or item.codigo):
                    result.itens_que_exigem_justificativa.append(item.descricao)
                    _add_pendencia(result, tipo="OPME_SEM_JUSTIFICATIVA", severidade="media",
                        item=item.descricao, regra_id="RGL_OPME_JUSTIFICATIVA",
                        mensagem=f"Item OPME exige justificativa específica: {item.descricao}.")
                    result.logs.append(f"OPME_JUSTIFICATIVA_MISSING: {item.descricao}")
                break  # um match por item é suficiente

    # Check 4 — Obrigatórios ausentes
    # Estratégia: qualquer palavra-chave do obrigatório encontrada nos items satisfaz
    # Ex: "parafuso pedicular" → tokens ["parafuso", "pedicular"]
    #     "parafusos pediculares transpediculares" contém "pedicular" → encontrado
    for obrigatorio in cfg["obrigatorios"]:
        ob_tokens = normalize_text(obrigatorio).split()
        encontrado = any(
            all(token in item_norm for token in ob_tokens)
            or any(token in item_norm for token in ob_tokens if len(token) >= 6)
            for item_norm in normalized_items
        )
        if not encontrado:
            result.itens_obrigatorios_faltantes.append(obrigatorio)
            _add_pendencia(result, tipo="OPME_OBRIGATORIO_FALTANTE", severidade="alta",
                item=obrigatorio, regra_id="RGL_OPME_REQUIRED",
                mensagem=f"Item OPME obrigatório ausente para '{perfil}': {obrigatorio}.")
            result.logs.append(f"OPME_REQUIRED_MISSING: {obrigatorio}")

    # Ordenar: critica > alta > media > baixa
    result.pendencias.sort(key=lambda p: SEVERIDADE_RANK.get(p.severidade, 9))

    # risco_glosa final
    if result.opme_incompativel_detectado:
        result.risco_glosa = "crítico"
    elif result.opme_generico_detectado:
        result.risco_glosa = "alto"
    elif result.itens_obrigatorios_faltantes or result.itens_que_exigem_justificativa:
        result.risco_glosa = "medio"
    else:
        result.risco_glosa = "baixo"

    return result


# ── CAP STANDALONE ────────────────────────────────────────────────────────────

def apply_opme_caps(score: int, validation: OpmeValidationResult) -> int:
    """Aplica cap de score baseado no resultado da validação OPME."""
    if validation.opme_incompativel_detectado:
        return min(score, 60)
    if validation.opme_generico_detectado:
        return min(score, 74)
    return score
