"""
NEUROAUTH - decision_engine.py
==============================
Motor decisorio v2.3 - production-grade blindado.

Mudancas v2.2 -> v2.3 (blindagem final):
  [1] Removido _check_override (overrides agora sao exclusivamente regras FLEX
      com FORCE_GO, conforme arquitetura ja existente). Bug de assinatura
      eliminado por simplificacao.
  [2] Fail-safe ativo: excecao em qualquer regra retorna RESSALVA com
      final_risk=CRITICO_ERRO_SISTEMA, jamais GO silencioso.
  [3] _auto_defense_template recebe o case de novo, com CID, procedimento
      e flags clinicas no texto da defesa.
  [4] Sanitizacao de input (_sanitize_case) aplicada na entrada do evaluate.
  [5] Risco BAIXO_COM_ATENCAO para GO com score < 80 (priorizacao da secretaria).

Uso:
  from decision_engine import DecisionEngine
  engine = DecisionEngine.from_file("rules_v2_1.json", context={
      "rol_ans_465": [...]
  })
  resultado = engine.evaluate(case_dict)

Autor: NEUROAUTH / HSA-INEC
Versao: 2.3.1
"""

from __future__ import annotations
import json
import logging
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Optional


logger = logging.getLogger("neuroauth.decision_engine")


# ============================================================
# CONDITION EVALUATOR
# ============================================================
class ConditionEvaluator:
    """
    Avalia uma condicao estruturada (clause unica ou AND/OR) contra um caso.
    Aceita contexto externo (tabelas globais como rol_ans_465).
    """

    OPERATORS = {
        "equals":                 lambda a, b: a == b,
        "not_equals":             lambda a, b: a != b,
        "in":                     lambda a, b: a in b if b is not None else False,
        "not_in":                 lambda a, b: a not in b if b is not None else True,
        "greater_than":           lambda a, b: a is not None and a > b,
        "greater_than_or_equal":  lambda a, b: a is not None and a >= b,
        "less_than":              lambda a, b: a is not None and a < b,
        "less_than_or_equal":     lambda a, b: a is not None and a <= b,
        "is_empty":               lambda a, b: a is None or a == "" or a == [],
        "is_not_empty":           lambda a, b: a is not None and a != "" and a != [],
    }

    def __init__(self, perfil: dict, context: Optional[dict] = None):
        self.perfil = perfil
        self.context = context or {}

    def evaluate(self, condition: dict, case: dict) -> bool:
        # Compound (AND/OR)
        if "operator" in condition and "clauses" in condition:
            results = [self.evaluate(c, case) for c in condition["clauses"]]
            return all(results) if condition["operator"] == "AND" else any(results)

        # Clause unica
        field_name = condition["field"]
        op = condition["op"]
        value = condition["value"]

        # Resolver placeholder de perfil
        if isinstance(value, str) and value.startswith("perfil."):
            value = self.perfil.get(value.split(".", 1)[1])

        # Resolver value para operadores de colecao.
        # Prioridade: context global -> case -> ERRO EXPLICITO.
        # Em producao, melhor quebrar (fail-safe) do que assumir errado.
        if op in ("in", "not_in") and isinstance(value, str):
            if value in self.context:
                value = self.context[value]
            else:
                resolved = self._get_field(case, value)
                if resolved is not None:
                    value = resolved
                else:
                    raise ValueError(
                        f"Contexto nao resolvido para '{value}' "
                        f"(operador '{op}', field '{field_name}'). "
                        f"Verifique se '{value}' esta no context global ou no case."
                    )

        actual = self._get_field(case, field_name)

        operator_fn = self.OPERATORS.get(op)
        if not operator_fn:
            raise ValueError(f"Operador desconhecido: {op}")

        return operator_fn(actual, value)

    @staticmethod
    def _get_field(case: dict, path: str) -> Any:
        # Primeiro tenta chave plana exata
        if path in case:
            return case[path]
        # Fallback: lookup aninhado por dot notation
        parts = path.split(".")
        value = case
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None
            if value is None:
                return None
        return value


# ============================================================
# RESULT DATACLASSES
# ============================================================
@dataclass
class LayerResult:
    score: int = 100
    rules_fired: list = field(default_factory=list)
    violacoes_duras: list = field(default_factory=list)
    violacoes_moderadas: list = field(default_factory=list)


@dataclass
class DecisionResult:
    trace_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    final_gate: str = ""
    final_score: int = 0
    final_risk: str = ""
    layer_ans: dict = field(default_factory=dict)
    layer_evidencia: dict = field(default_factory=dict)
    layer_operadora: dict = field(default_factory=dict)
    pending_items: list = field(default_factory=list)
    recommended_action: str = ""
    defense_ready: Optional[str] = None
    summary: str = ""
    judicial_risk_score: float = 0.0
    cost_vs_judicialization_ratio: float = 1.0
    rules_fired: list = field(default_factory=list)
    perfil_operadora_aplicado: str = "DEFAULT"
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    engine_version: str = "2.3.1"
    error: Optional[str] = None  # populado apenas em fail-safe

    def to_dict(self) -> dict:
        return asdict(self)


# ============================================================
# RISK CLASSIFIER (granular, 5 niveis)
# ============================================================
def classify_risk(score: int) -> str:
    if score >= 85:
        return "MUITO_BAIXO"
    elif score >= 70:
        return "BAIXO"
    elif score >= 50:
        return "MODERADO"
    elif score >= 30:
        return "ALTO"
    else:
        return "CRITICO"


# ============================================================
# DECISION ENGINE
# ============================================================
class DecisionEngine:
    """
    Motor decisorio v2.3 - production-grade blindado.
    """

    NO_GO_PENALTY_MULTIPLIER = 1.5

    def __init__(self, ruleset: dict, context: Optional[dict] = None):
        self.ruleset = ruleset
        self.rules = ruleset["rules"]
        self.camadas = ruleset["camadas"]
        self.perfis = ruleset["perfis_operadora"]
        self.consolidacao = ruleset["consolidacao"]
        self.context = context or {}

        # Indexar por camada
        self.rules_by_layer = {"ANS": [], "EVIDENCIA": [], "OPERADORA": []}
        for rule in self.rules:
            camada = rule["camada"]
            if camada in self.rules_by_layer:
                self.rules_by_layer[camada].append(rule)

        logger.info(
            "Motor v%s carregado: %d regras (%d ANS, %d EV, %d OP), %d perfis",
            ruleset.get("schema_version", "?"),
            len(self.rules),
            len(self.rules_by_layer["ANS"]),
            len(self.rules_by_layer["EVIDENCIA"]),
            len(self.rules_by_layer["OPERADORA"]),
            len(self.perfis)
        )

        # Validar context no startup: detecta referencias quebradas antes
        # do primeiro request, em vez de descobrir em producao.
        self._validate_context_references()

    def _validate_context_references(self) -> None:
        """
        Inspeciona todas as regras procurando referencias a nomes de tabela
        em operadores 'in'/'not_in' que sao strings (nao listas literais).
        Loga warning para cada referencia que nao esta no context.
        Nao quebra o startup; apenas alerta para o operador fixar antes do
        primeiro request real.
        """
        referencias_faltando = set()

        def inspect(condition: dict, rule_id: str) -> None:
            if "operator" in condition and "clauses" in condition:
                for c in condition["clauses"]:
                    inspect(c, rule_id)
                return
            op = condition.get("op")
            value = condition.get("value")
            if (op in ("in", "not_in")
                    and isinstance(value, str)
                    and not value.startswith("perfil.")
                    and value not in self.context):
                referencias_faltando.add((rule_id, value))

        for rule in self.rules:
            inspect(rule["condition"], rule["id"])

        if referencias_faltando:
            for rule_id, value in sorted(referencias_faltando):
                logger.warning(
                    "Regra %s referencia '%s' que nao esta no context. "
                    "Sera resolvido via case[] em runtime, ou levantara ValueError "
                    "se ausente no case (fail-safe).",
                    rule_id, value
                )

    @classmethod
    def from_file(cls, path: str, context: Optional[dict] = None) -> "DecisionEngine":
        with open(path, "r", encoding="utf-8") as f:
            return cls(json.load(f), context=context)

    # ---------- SANITIZACAO DE INPUT ----------
    @staticmethod
    def _sanitize_case(case: dict) -> dict:
        """
        Limpa strings (trim) e normaliza None/NaN. Aplicado antes de qualquer
        avaliacao de regra para evitar problemas com input vindo da secretaria.
        """
        if not isinstance(case, dict):
            raise ValueError(f"case deve ser dict, recebido {type(case).__name__}")

        sanitized = {}
        for k, v in case.items():
            if isinstance(v, str):
                v_clean = v.strip()
                sanitized[k] = v_clean if v_clean else None
            elif isinstance(v, float) and v != v:  # NaN check
                sanitized[k] = None
            else:
                sanitized[k] = v
        return sanitized

    # ---------- API PUBLICA ----------
    def evaluate(self, case: dict) -> dict:
        """
        Avalia um caso. Em caso de erro inesperado, retorna fail-safe
        (RESSALVA + CRITICO_ERRO_SISTEMA) JAMAIS GO silencioso.
        """
        try:
            return self._evaluate_internal(case)
        except Exception as e:
            logger.error("Erro fatal na avaliacao: %s", e, exc_info=True)
            return self._fail_safe(case, e)

    def _evaluate_internal(self, case: dict) -> dict:
        # Sanitiza input antes de tudo
        case = self._sanitize_case(case)

        result = DecisionResult()

        # 1. Resolver perfil
        convenio = case.get("convenio_perfil", "DEFAULT")
        perfil = self.perfis.get(convenio, self.perfis["DEFAULT"])
        result.perfil_operadora_aplicado = convenio

        evaluator = ConditionEvaluator(perfil, context=self.context)

        # 2. Inicializar camadas
        layers = {
            "ANS":       LayerResult(),
            "EVIDENCIA": LayerResult(),
            "OPERADORA": LayerResult(),
        }

        force_go = False
        promote_ressalva = False
        defense_text = None
        score_boosts = 0

        # 3. Iterar regras por camada
        for camada, layer_rules in self.rules_by_layer.items():
            for rule in layer_rules:
                try:
                    fired = evaluator.evaluate(rule["condition"], case)
                except Exception as e:
                    logger.error(
                        "Regra %s falhou na avaliacao para trace_id=%s: %s",
                        rule["id"], result.trace_id, e
                    )
                    # Em produção, regra que falha deve disparar fail-safe
                    # para não passar como "não disparou" silenciosamente.
                    raise

                if not fired:
                    continue

                self._apply_rule(rule, layers[camada], perfil, result)

                # Capturar flags
                action = rule["action"]
                if action == "FORCE_GO":
                    force_go = True
                elif action == "PROMOTE_RESSALVA_TO_GO":
                    promote_ressalva = True
                elif action == "GENERATE_DEFENSE":
                    defense_text = rule.get("texto_defesa")
                elif action == "BOOST_SCORE":
                    score_boosts += rule.get("score_boost", 0)

                result.rules_fired.append({
                    "id": rule["id"],
                    "camada": camada,
                    "tipo": rule["tipo"],
                    "action": action,
                    "mensagem": rule["mensagem"]
                })

        # 4. Aplicar boosts
        for layer in layers.values():
            layer.score = max(0, min(100, layer.score + score_boosts))

        # 5. Score final ponderado
        score_final = (
            layers["ANS"].score       * self.camadas["ANS"]["weight_global"] +
            layers["EVIDENCIA"].score * self.camadas["EVIDENCIA"]["weight_global"] +
            layers["OPERADORA"].score * self.camadas["OPERADORA"]["weight_global"]
        )
        result.final_score = int(round(score_final))

        # 6. Inputs preditivos
        result.judicial_risk_score = case.get("judicial_risk_score", 0.0)
        result.cost_vs_judicialization_ratio = case.get("cost_vs_judicialization_ratio", 1.0)

        # 7. Decisao final
        any_dura = any(layer.violacoes_duras for layer in layers.values())

        if force_go:
            result.final_gate = "GO"
            result.final_risk = "BAIXO_OVERRIDE_URGENCIA"
        elif any_dura:
            result.final_gate = "NO_GO"
            n_duras = sum(len(layer.violacoes_duras) for layer in layers.values())
            result.final_risk = "CRITICO" if n_duras >= 2 else "ALTO"
        else:
            limiares = self.consolidacao["limiares_score"]
            if result.final_score >= limiares["GO"]["min"]:
                result.final_gate = "GO"
                result.final_risk = classify_risk(result.final_score)
            elif result.final_score >= limiares["RESSALVA"]["min"]:
                result.final_gate = "RESSALVA"
                if promote_ressalva:
                    result.final_gate = "GO"
                    result.final_risk = "BAIXO_PROMOCAO_JUDICIAL"
                else:
                    result.final_risk = classify_risk(result.final_score)
            else:
                result.final_gate = "NO_GO"
                result.final_risk = classify_risk(result.final_score)

        # GO com score < 80 vira BAIXO_COM_ATENCAO (priorizacao secretaria)
        if (result.final_gate == "GO" and result.final_score < 80
                and result.final_risk not in (
                    "BAIXO_OVERRIDE_URGENCIA", "BAIXO_PROMOCAO_JUDICIAL"
                )):
            result.final_risk = "BAIXO_COM_ATENCAO"

        # 8. Defesa: combina texto da regra (se houver) com contexto clinico do caso
        if defense_text:
            # FLEX_003 forneceu argumento juridico estatico; enriquecemos com clinica
            result.defense_ready = self._compose_defense(case, result, defense_text)
        elif result.final_gate == "NO_GO":
            result.defense_ready = self._auto_defense_template(case, result)

        # 9. Acao recomendada
        result.recommended_action = self._build_recommendation(result)

        # 10. Summary
        result.summary = self._build_summary(case, result, layers)

        # 11. Serializar layers
        result.layer_ans       = self._serialize_layer(layers["ANS"])
        result.layer_evidencia = self._serialize_layer(layers["EVIDENCIA"])
        result.layer_operadora = self._serialize_layer(layers["OPERADORA"])

        logger.info(
            "Decisao trace_id=%s gate=%s score=%d risco=%s pendencias=%d",
            result.trace_id, result.final_gate, result.final_score,
            result.final_risk, len(result.pending_items)
        )

        return result.to_dict()

    # ---------- FAIL-SAFE ----------
    def _fail_safe(self, case: dict, error: Exception) -> dict:
        """
        Resposta segura quando o motor falha. JAMAIS retorna GO silencioso.
        Todo erro vira RESSALVA + CRITICO_ERRO_SISTEMA, exigindo revisao manual.
        """
        return {
            "trace_id": str(uuid.uuid4()),
            "engine_version": "2.3.1",
            "final_gate": "RESSALVA",
            "final_score": 0,
            "final_risk": "CRITICO_ERRO_SISTEMA",
            "layer_ans": {},
            "layer_evidencia": {},
            "layer_operadora": {},
            "pending_items": [{
                "rule_id": "FAIL_SAFE",
                "severidade": "CRITICA",
                "descricao": "Erro interno do motor. Revisar manualmente antes do envio."
            }],
            "recommended_action": (
                "Erro interno do sistema na avaliacao. NAO submeter automaticamente. "
                "Revisar caso manualmente e contactar suporte tecnico."
            ),
            "defense_ready": None,
            "summary": f"Falha do motor: {type(error).__name__}",
            "judicial_risk_score": 0.0,
            "cost_vs_judicialization_ratio": 1.0,
            "rules_fired": [],
            "perfil_operadora_aplicado": case.get("convenio_perfil", "DEFAULT") if isinstance(case, dict) else "DEFAULT",
            "timestamp": datetime.utcnow().isoformat(),
            "error": f"{type(error).__name__}: {str(error)}"
        }

    # ---------- INTERNAL ----------
    def _apply_rule(self, rule: dict, layer: LayerResult, perfil: dict,
                    result: DecisionResult) -> None:
        """
        Aplica uma regra disparada na camada correspondente.

        Note: nao ha mais _check_override. Overrides sao exclusivamente regras
        FLEX com action FORCE_GO ou PROMOTE_RESSALVA_TO_GO, conforme arquitetura.
        """
        action = rule["action"]
        layer.rules_fired.append(rule["id"])

        if action == "NO_GO":
            layer.violacoes_duras.append(rule["id"])
            penalty = rule["weight"] * self.NO_GO_PENALTY_MULTIPLIER
            layer.score = max(0, layer.score - int(penalty))
            self._add_pending(result, {
                "rule_id": rule["id"],
                "severidade": "CRITICA",
                "descricao": rule.get("correcao", rule["mensagem"])
            })

        elif action == "RESSALVA":
            layer.violacoes_moderadas.append(rule["id"])
            weight_ajustado = rule["weight"]
            if rule["camada"] == "OPERADORA" and "opme" in rule["id"].lower():
                weight_ajustado *= perfil.get("rigor_opme", 1.0)
            elif rule["camada"] == "EVIDENCIA":
                weight_ajustado *= perfil.get("rigor_documental", 1.0)
            layer.score = max(0, layer.score - int(weight_ajustado))
            self._add_pending(result, {
                "rule_id": rule["id"],
                "severidade": "MODERADA",
                "descricao": rule.get("correcao", rule["mensagem"])
            })

    @staticmethod
    def _add_pending(result: DecisionResult, item: dict) -> None:
        """Deduplicacao por rule_id."""
        if any(p.get("rule_id") == item.get("rule_id") for p in result.pending_items):
            return
        result.pending_items.append(item)

    @staticmethod
    def _serialize_layer(layer: LayerResult) -> dict:
        return {
            "score": int(round(layer.score)),
            "rules_fired": layer.rules_fired,
            "violacoes_duras": layer.violacoes_duras,
            "violacoes_moderadas": layer.violacoes_moderadas,
        }

    @staticmethod
    def _build_recommendation(result: DecisionResult) -> str:
        if result.final_gate == "GO":
            if result.pending_items:
                return f"Aprovacao recomendada. Atentar a {len(result.pending_items)} ressalvas menores."
            return "Aprovacao recomendada. Submeter diretamente a operadora."
        elif result.final_gate == "RESSALVA":
            n = len(result.pending_items)
            return (
                f"Completar {n} pendencias documentais e reenviar. "
                f"Probabilidade estimada de aprovacao apos correcao: alta."
            )
        else:
            return (
                "Negativa provavel. Revisar bloqueios criticos antes de submeter. "
                "Defesa pre-montada disponivel para eventual recurso."
            )

    @staticmethod
    def _auto_defense_template(case: dict, result: DecisionResult) -> str:
        """
        Defesa pre-montada usando contexto clinico real do caso.
        """
        cid = case.get("cid", "CID nao informado")
        proc = case.get("procedimento_descricao", "procedimento")
        urgencia = case.get("urgencia_caracterizada", False)
        deficit = case.get("deficit_motor_mencionado", False)
        evidencia = case.get("evidencia_robusta", False)

        flags = []
        if urgencia:
            flags.append("urgencia clinica documentada")
        if deficit:
            flags.append("deficit neurologico")
        if evidencia:
            flags.append("evidencia robusta disponivel")
        flags_str = " | ".join(flags) if flags else "sem flags clinicas adicionais"

        return (
            f"Defesa SCQA pre-montada para recurso administrativo. "
            f"Caso: {proc} (CID {cid}). Flags: {flags_str}. "
            f"Estruturar argumentacao em 3 camadas: clinica (guideline + correlacao "
            f"imagem-clinica), regulatoria (DUT cumprida ou Lei 14.454/2022 quando "
            f"aplicavel) e juridica (Sumula 102 TJSP + jurisprudencia STJ sobre "
            f"negativa abusiva). Razao custo-negativa/autorizacao: "
            f"{result.cost_vs_judicialization_ratio:.2f}x. "
            f"Score residual do motor: {result.final_score}/100."
        )

    @staticmethod
    def _compose_defense(case: dict, result: DecisionResult, static_text: str) -> str:
        """
        Compoe defesa final concatenando o argumento estatico da regra (ex: FLEX_003)
        com o contexto clinico do caso (CID, procedimento, flags). Garante que
        defesas geradas por FLEX_* nunca percam o contexto clinico.
        """
        cid = case.get("cid", "")
        proc = case.get("procedimento_descricao", "")
        urgencia = case.get("urgencia_caracterizada", False)
        deficit = case.get("deficit_motor_mencionado", False)

        contexto_parts = []
        if proc:
            contexto_parts.append(f"Caso: {proc}")
        if cid:
            contexto_parts.append(f"CID {cid}")

        flags = []
        if urgencia:
            flags.append("urgencia clinica documentada")
        if deficit:
            flags.append("deficit neurologico documentado")

        contexto = ". ".join(contexto_parts) + "."
        flags_str = f" Flags clinicas: {' | '.join(flags)}." if flags else ""

        return f"{contexto}{flags_str} {static_text}"

    @staticmethod
    def _build_summary(case: dict, result: DecisionResult, layers: dict) -> str:
        proc = case.get("procedimento_descricao", "Procedimento")
        cid = case.get("cid", "CID nao informado")
        return (
            f"{proc} (CID {cid}). Decisao: {result.final_gate} "
            f"(score {result.final_score}/100, risco {result.final_risk}). "
            f"Camadas: ANS={layers['ANS'].score} | "
            f"Evidencia={layers['EVIDENCIA'].score} | "
            f"Operadora={layers['OPERADORA'].score}. "
            f"{len(result.pending_items)} pendencias listadas."
        )


# ============================================================
# OUTCOME RECORDER (idempotente)
# ============================================================
class OutcomeRecorder:
    def __init__(self, sheets_client=None):
        self.sheets_client = sheets_client
        self._recorded_traces = set()

    def record(self, trace_id: str, motor_result: dict, real_outcome: dict) -> bool:
        if trace_id in self._recorded_traces:
            logger.info("Outcome para trace_id=%s ja registrado, ignorando", trace_id)
            return False

        record = {
            "trace_id": trace_id,
            "data_decisao": motor_result.get("timestamp"),
            "decisao_motor": motor_result.get("final_gate"),
            "score_motor": motor_result.get("final_score"),
            "risco_motor": motor_result.get("final_risk"),
            "decisao_real_operadora": real_outcome.get("decisao"),
            "tempo_resposta_dias": real_outcome.get("tempo_resposta_dias"),
            "glosa_motivo": real_outcome.get("glosa_motivo"),
            "valor_glosado": real_outcome.get("valor_glosado", 0),
            "recurso_administrativo": real_outcome.get("recurso", False),
            "recurso_provido": real_outcome.get("recurso_provido", False),
            "rules_fired": [r["id"] for r in motor_result.get("rules_fired", [])],
            "perfil_operadora": motor_result.get("perfil_operadora_aplicado"),
        }

        if self.sheets_client:
            try:
                self.sheets_client.append("22_DECISION_OUTCOMES", record)
            except Exception as e:
                logger.error("Falha ao gravar outcome no Sheets: %s", e)
                return False
        else:
            print(f"[OUTCOME RECORDED] {json.dumps(record, ensure_ascii=False)}")

        self._recorded_traces.add(trace_id)
        return True


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    engine = DecisionEngine.from_file("rules_v2_1.json")
    print(f"Motor v{engine.ruleset['schema_version']} carregado.")
    print(f"Engine version: 2.3.0")
    print(f"Regras por camada:")
    for camada, regras in engine.rules_by_layer.items():
        print(f"  {camada}: {len(regras)} regras")
