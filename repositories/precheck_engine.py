from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Set

VALID_CARATER_VALUES: Set[str] = {
    "ELETIVO", "URGENTE", "URGENCIA", "URGÊNCIA", "ELE", "URG",
}


@dataclass
class PrecheckResult:
    allow_submit: bool
    rigor_level: str  # "STANDARD" | "ELEVATED" | "HARD"
    warnings: List[str]
    blocking_issues: List[str]
    metadata: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _norm(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def _get(payload: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload and payload[key] not in (None, ""):
            return payload[key]
    return None


def _profile_requires_laterality(profile_id: str) -> bool:
    profiles = {
        "MICRODISCECTOMIA_LOMBAR",
        "HERNIA_DISCAL_LOMBAR",
        "HERNIA_DISCAL_CERVICAL",
    }
    return _norm(profile_id) in profiles


def _should_warn_generic_unimed(convenio_id: str) -> bool:
    return _norm(convenio_id) == "UNIMED"


def _known_rgl005_sensitive_profiles(profile_id: str) -> bool:
    profiles = {
        "MICRODISCECTOMIA_LOMBAR",
        "HERNIA_DISCAL_LOMBAR",
        "DVP_HIDROCEFALIA",
    }
    return _norm(profile_id) in profiles


def run_precheck(payload: Dict[str, Any]) -> PrecheckResult:
    """
    Bloco 3 — Decision Engine 2.0 (Rigor Adaptativo Leve)
    Roda ANTES do motor. Em shadow mode: só loga, não bloqueia.
    Para ativar bloqueio real, descomentar em decision_routes.py.
    """
    profile_id = _norm(_get(payload, "profile_id", "procedimento"))
    convenio_id = _norm(_get(payload, "convenio_id", "convenio"))
    carater_cod = _norm(
        _get(payload, "carater_cod", "carater", "carater_atendimento")
    )
    lateralidade = _norm(_get(payload, "lateralidade", "lado"))
    opmes = _get(payload, "opmes_selecionados", "opme_items", "opme_context_json")

    warnings: List[str] = []
    blocking_issues: List[str] = []

    # Regra 1 — caráter obrigatório e válido
    if carater_cod and carater_cod not in VALID_CARATER_VALUES:
        blocking_issues.append(
            f"CARATER_INVALIDO: use um dos valores aceitos {sorted(VALID_CARATER_VALUES)}"
        )
    elif not carater_cod:
        blocking_issues.append("CARATER_AUSENTE")

    # Regra 2 — lateralidade obrigatória para perfis específicos
    if _profile_requires_laterality(profile_id) and not lateralidade:
        blocking_issues.append("LATERALIDADE_OBRIGATORIA")

    # Regra 3 — convênio genérico UNIMED
    if _should_warn_generic_unimed(convenio_id):
        warnings.append(
            "CONVENIO_GENERICO_UNIMED: prefira UNIMED_CARIRI ou variante cadastrada"
        )

    # Regra 4 — perfis sensíveis a RGL005
    if _known_rgl005_sensitive_profiles(profile_id):
        warnings.append(
            "PROFILE_SENSIVEL_RGL005: revise caráter e campos regulatórios antes do envio"
        )

    # Regra 5 — OPME estrutura malformada
    if opmes is not None and not isinstance(opmes, (list, dict)):
        warnings.append("OPME_ESTRUTURA_NAO_PADRONIZADA")

    # Determinar nível de rigor
    rigor_level = "STANDARD"
    if warnings:
        rigor_level = "ELEVATED"
    if blocking_issues:
        rigor_level = "HARD"

    return PrecheckResult(
        allow_submit=len(blocking_issues) == 0,
        rigor_level=rigor_level,
        warnings=warnings,
        blocking_issues=blocking_issues,
        metadata={
            "profile_id": profile_id,
            "convenio_id": convenio_id,
            "carater_cod": carater_cod,
            "lateralidade": lateralidade,
        },
    )
