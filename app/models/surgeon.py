"""
app/models/surgeon.py
NEUROAUTH — Schemas Pydantic para atribuição de cirurgião principal + auxiliares.

Regras de negócio embutidas nos validators:
  - Máximo 3 auxiliares (CBHPM — produção paralela limitada)
  - Ordens únicas e 1-3
  - Cirurgião principal não pode aparecer como auxiliar
  - Principal obrigatório
"""
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class AuxiliarCirurgiao(BaseModel):
    """Um cirurgião auxiliar com posição ordenada."""

    id: str = Field(..., description="ID do cirurgião auxiliar (ex: CIR_002)")
    ordem: int = Field(..., ge=1, le=3, description="Posição do auxiliar: 1, 2 ou 3")

    @field_validator("id")
    @classmethod
    def id_nao_vazio(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("id do auxiliar não pode ser vazio")
        return v


class CirurgiaoPayload(BaseModel):
    """Atribuição de equipe cirúrgica: um principal + até 3 auxiliares ordenados."""

    cirurgiao_principal: str = Field(
        ...,
        description="ID do cirurgião principal (ex: CIR_001) — obrigatório",
    )
    cirurgioes_auxiliares: Optional[List[AuxiliarCirurgiao]] = Field(
        default=[],
        description="Lista ordenada de auxiliares — máximo 3 (regra CBHPM)",
    )

    @field_validator("cirurgiao_principal")
    @classmethod
    def principal_nao_vazio(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("cirurgiao_principal é obrigatório e não pode ser vazio")
        return v

    @field_validator("cirurgioes_auxiliares")
    @classmethod
    def validar_auxiliares(cls, auxiliares: Optional[List[AuxiliarCirurgiao]]) -> List[AuxiliarCirurgiao]:
        if not auxiliares:
            return []

        # Máximo 3 auxiliares (regra CBHPM)
        if len(auxiliares) > 3:
            raise ValueError(
                f"Máximo de 3 auxiliares permitidos pela regra CBHPM (recebido: {len(auxiliares)})"
            )

        # Ordens únicas
        ordens = [a.ordem for a in auxiliares]
        if len(set(ordens)) != len(ordens):
            duplicadas = [o for o in ordens if ordens.count(o) > 1]
            raise ValueError(
                f"Ordem dos auxiliares deve ser única. Duplicadas: {list(set(duplicadas))}"
            )

        return auxiliares

    @model_validator(mode="after")
    def principal_nao_e_auxiliar(self) -> "CirurgiaoPayload":
        """Principal não pode aparecer como auxiliar."""
        aux_ids = [a.id for a in (self.cirurgioes_auxiliares or [])]
        if self.cirurgiao_principal and self.cirurgiao_principal in aux_ids:
            raise ValueError(
                f"Cirurgião principal '{self.cirurgiao_principal}' "
                f"não pode constar também como auxiliar"
            )
        return self

    def to_dict(self) -> dict:
        """Serializa para persistência no Sheets."""
        return {
            "cirurgiao_principal": self.cirurgiao_principal,
            "cirurgioes_auxiliares": [
                {"id": a.id, "ordem": a.ordem}
                for a in (self.cirurgioes_auxiliares or [])
            ],
        }
