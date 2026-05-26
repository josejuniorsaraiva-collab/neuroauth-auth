/* ============================================================
 * DTC Interpretive Engine
 * Camada de interpretação hemodinâmica narrativa
 * ============================================================
 * Recebe o snapshot determinístico do engine principal e gera
 * narrativa interpretativa contextual sofisticada.
 *
 * REGRAS ABSOLUTAS:
 *   - NÃO modifica estados VS/PR/EM do engine determinístico
 *   - NÃO modifica alarmes
 *   - NÃO modifica thresholds
 *   - NÃO emite diagnóstico definitivo
 *   - NÃO recomenda intervenção específica
 *   - Linguagem sempre qualificada: "padrão compatível com",
 *     "favorece", "sugere", "aumenta probabilidade",
 *     "merece correlação", "em contexto de", "dinâmica temporal"
 *   - Boundary obrigatório no final
 *   - Sempre declara grau de confiança interpretativa
 *
 * Subordinada a runtime_boundary_conditions.md (BC-01..10) do
 * kernel DTC_HSA_KERNEL_v1.
 * ============================================================ */

(function (global) {
  'use strict';

  // -------- Helpers --------
  function present(x) { return x !== null && x !== undefined && !Number.isNaN(x); }
  function fmt(x, d) { return present(x) ? Number(x).toFixed(d == null ? 1 : d) : '—'; }
  function pct(num, den) { return (den && den !== 0) ? (num / den) * 100 : null; }
  function nz(arr) { return arr.filter(present); }
  function avg(arr) { const a = nz(arr); return a.length ? a.reduce((s, x) => s + x, 0) / a.length : null; }
  function maxBy(arr, key) {
    let best = null;
    for (const it of arr) { if (!best || it[key] > best[key]) best = it; }
    return best;
  }
  function safe(v, fallback) { return (v === null || v === undefined || v === '') ? fallback : v; }

  // ============================================================
  // CAMADA 1 — Análise hemodinâmica
  // ============================================================
  function analyzeHemodynamics(s) {
    const ves = s.vessels || {};
    const der = s.derived || {};

    const mcaR = ves.MCA_right && ves.MCA_right.Vm_cms;
    const mcaL = ves.MCA_left && ves.MCA_left.Vm_cms;
    const acaR = ves.ACA_right && ves.ACA_right.Vm_cms;
    const acaL = ves.ACA_left && ves.ACA_left.Vm_cms;
    const pcaR = ves.PCA_right && ves.PCA_right.Vm_cms;
    const pcaL = ves.PCA_left && ves.PCA_left.Vm_cms;
    const bas = ves.basilar && ves.basilar.Vm_cms;
    const vertR = ves.vertebral_right && ves.vertebral_right.Vm_cms;
    const vertL = ves.vertebral_left && ves.vertebral_left.Vm_cms;
    const ipR = ves.MCA_right && ves.MCA_right.IP;
    const ipL = ves.MCA_left && ves.MCA_left.IP;
    const ipMean = avg([ipR, ipL]);

    const lrD = der.lindegaard_right;
    const lrE = der.lindegaard_left;
    const dR = der.delta_vm_mca_right_24h;
    const dL = der.delta_vm_mca_left_24h;

    // ---- Lateralidade ----
    let asymmetryPct = null;
    let dominantSide = null;
    if (present(mcaR) && present(mcaL)) {
      const max = Math.max(mcaR, mcaL);
      const min = Math.min(mcaR, mcaL);
      if (max > 0) {
        asymmetryPct = ((max - min) / max) * 100;
        if (asymmetryPct > 30) dominantSide = mcaR > mcaL ? 'direita' : 'esquerda';
      }
    }

    // ---- Focal vs difuso ----
    const anteriorVms = nz([mcaR, mcaL, acaR, acaL]);
    const posteriorVms = nz([pcaR, pcaL, bas, vertR, vertL]);
    const anteriorElev = anteriorVms.length && anteriorVms.every(v => v >= 100);
    const posteriorElev = posteriorVms.length >= 2 && posteriorVms.filter(v => v >= 60).length >= 2;
    const mcaElev = (present(mcaR) && mcaR >= 120) || (present(mcaL) && mcaL >= 120);
    const mcaSevere = (present(mcaR) && mcaR >= 200) || (present(mcaL) && mcaL >= 200);
    const focalPattern = dominantSide && asymmetryPct > 40;
    const diffusePattern = anteriorElev && !focalPattern;

    // ---- Hiperemia vs vasoespasmo modulators ----
    const hyperemiaContext = [];
    if (s.PaCO2_mmHg && s.PaCO2_mmHg > 45) hyperemiaContext.push('hipercapnia (PaCO₂ ' + s.PaCO2_mmHg + ' mmHg)');
    if (s.hematocrit_pct && s.hematocrit_pct < 30) hyperemiaContext.push('anemia (Ht ' + s.hematocrit_pct + '%)');
    if (s.HR && s.HR > 100) hyperemiaContext.push('estado hiperdinâmico clínico (FC ' + s.HR + ' bpm)');
    if (s.context === 'pos_op_neurovascular') hyperemiaContext.push('contexto pós-revascularização');

    // ---- IP context modulators ----
    const ipContext = [];
    if (present(ipMean) && ipMean > 1.2) {
      if (s.PaCO2_mmHg && s.PaCO2_mmHg < 32) ipContext.push('hipocapnia (PaCO₂ ' + s.PaCO2_mmHg + ' mmHg) pode contribuir');
      if (s.sedation && /propofol|midaz|fent|barbi|tiopent|dexme/i.test(s.sedation)) ipContext.push('sedação profunda ativa pode contribuir');
      if (s.MAP_mmHg && s.MAP_mmHg < 65) ipContext.push('PAM baixa (' + s.MAP_mmHg + ' mmHg) pode reduzir VDF');
    }

    // ---- Lindegaard concordance ----
    let lrConcordance = null;
    if (mcaSevere && present(lrD) && present(lrE)) {
      const lrMax = Math.max(lrD, lrE);
      if (lrMax > 6) lrConcordance = 'concordante (LR > 6)';
      else if (lrMax >= 3) lrConcordance = 'parcialmente concordante (LR 3-6 com Vm grave)';
      else lrConcordance = 'discordante (LR < 3 com Vm grave)';
    } else if (mcaElev && (present(lrD) || present(lrE))) {
      const lrMax = Math.max(lrD || 0, lrE || 0);
      if (lrMax >= 3) lrConcordance = 'concordante (LR ≥ 3)';
      else lrConcordance = 'discordante (LR < 3)';
    }

    // ---- Sloan posterior ----
    let posteriorSloanFlag = null;
    if ((present(bas) && bas >= 95) || (present(vertR) && vertR >= 80) || (present(vertL) && vertL >= 80)) {
      posteriorSloanFlag = 'limiares de Sloan cruzados em circulação posterior — alta especificidade, baixa sensibilidade (BC-05)';
    }

    return {
      mcaR, mcaL, acaR, acaL, pcaR, pcaL, bas, vertR, vertL,
      ipR, ipL, ipMean, lrD, lrE, dR, dL,
      asymmetryPct, dominantSide,
      mcaElev, mcaSevere, focalPattern, diffusePattern,
      anteriorElev, posteriorElev,
      hyperemiaContext, ipContext, lrConcordance,
      posteriorSloanFlag,
    };
  }

  // ============================================================
  // CAMADA 2 — Análise temporal
  // ============================================================
  function analyzeTemporal(s, h) {
    const out = {
      hasPrevious: !!(s.previous_exam_datetime_iso || s.prev_vm_mca_right || s.prev_vm_mca_left),
      techniqueIdentical: s.technique_identical !== false,
      narrative: null,
      flags: [],
    };

    if (!out.hasPrevious) {
      out.narrative = 'Sem exame prévio registrado — interpretação atual configura potencial baseline; tendência temporal só será mensurável a partir do próximo exame seriado.';
      return out;
    }

    if (!out.techniqueIdentical) {
      out.flags.push('comparação serial frágil — técnica não declarada como idêntica (BC-08)');
    }

    const deltas = [];
    if (present(h.dR)) deltas.push({ side: 'direita', delta: h.dR });
    if (present(h.dL)) deltas.push({ side: 'esquerda', delta: h.dL });

    if (deltas.length === 0) {
      out.narrative = 'Exame anterior registrado, porém sem dados quantitativos comparáveis informados nesta sessão.';
      return out;
    }

    const maxDelta = maxBy(deltas, 'delta');
    const minDelta = deltas.reduce((m, d) => (m === null || d.delta < m.delta) ? d.delta : m, null);

    const parts = [];
    deltas.forEach(d => {
      const sign = d.delta > 0 ? '+' : '';
      if (Math.abs(d.delta) < 15) {
        parts.push('ACM ' + d.side + ' estável (Δ ' + sign + d.delta.toFixed(1) + ' cm/s)');
      } else if (d.delta >= 50) {
        parts.push('ACM ' + d.side + ' com aceleração significativa (Δ ' + sign + d.delta.toFixed(1) + ' cm/s/24h) — peso prognóstico independente do valor absoluto (BC-08)');
        out.flags.push('aceleração significativa ACM ' + d.side);
      } else if (d.delta >= 30) {
        parts.push('ACM ' + d.side + ' com tendência ascendente (Δ +' + d.delta.toFixed(1) + ' cm/s/24h)');
      } else if (d.delta <= -30) {
        parts.push('ACM ' + d.side + ' com queda relevante (Δ ' + d.delta.toFixed(1) + ' cm/s/24h) — possível resposta a intervenção ou normalização de variável sistêmica');
      } else {
        parts.push('ACM ' + d.side + ' com variação discreta (Δ ' + sign + d.delta.toFixed(1) + ' cm/s/24h)');
      }
    });

    out.narrative = 'Em comparação com o exame anterior: ' + parts.join('; ') + '.';

    // Asymmetric progression
    if (deltas.length === 2 && Math.abs(deltas[0].delta - deltas[1].delta) > 30) {
      out.narrative += ' A progressão é assimétrica entre os hemisférios, o que pode favorecer fenômeno focal em detrimento de modulação sistêmica difusa.';
      out.flags.push('progressão assimétrica');
    }

    return out;
  }

  // ============================================================
  // CAMADA 3 — Significado clínico provável (qualificado)
  // ============================================================
  function analyzeClinicalSignificance(s, h, t) {
    const ctx = s.context;
    const vs = (s.states && s.states.vasospasm_axis) || {};
    const pr = (s.states && s.states.pressure_axis) || {};
    const lines = [];

    // --- HSA context ---
    if (ctx === 'HSA') {
      const day = s.post_bleed_day;
      const inWindow = present(day) && day >= 3 && day <= 14;
      const peak = present(day) && day >= 7 && day <= 10;

      if (peak) lines.push('Paciente em **pico da janela de risco vasoespástico** pós-HSA (dia ' + day + '), o que aumenta a relevância pré-teste de qualquer aceleração hemodinâmica detectada.');
      else if (inWindow) lines.push('Paciente dentro da **janela de risco vasoespástico** pós-HSA (dia ' + day + ' de 3–14), contexto que sensibiliza interpretação de tendência.');
      else if (present(day) && day < 3) lines.push('Paciente em fase **pré-janela** de vasoespasmo (dia ' + day + '), achados atuais úteis sobretudo como referência de baseline.');
      else if (present(day) && day > 14) lines.push('Paciente além da janela clássica de vasoespasmo (dia ' + day + '), porém vigilância prolongada pode ser apropriada em alto risco.');

      const highRisk = (s.fisher === '3' || s.fisher === '4' || s.fisher === 3 || s.fisher === 4 ||
                        ['III', 'IV', 'V'].includes(s.hunt_hess));
      if (highRisk) lines.push('Estratificação **Fisher ' + (s.fisher || '?') + ' / Hunt-Hess ' + (s.hunt_hess || '?') + '** caracteriza risco elevado de isquemia tardia, recomendando cadência intensificada e baixo limiar de complementação por imagem.');

      // VS axis narrative
      if (vs.state === 'VS-3') {
        const where = h.focalPattern
          ? ('com predomínio em circulação anterior ' + h.dominantSide)
          : 'sem lateralização clara';
        const lrPhrase = h.lrConcordance ? ' Razão de Lindegaard ' + h.lrConcordance + '.' : '';
        lines.push('Achados ' + where + ' configuram **padrão compatível com vasoespasmo grave** (BC-02).' + lrPhrase + ' Em ausência de critérios isolados que definam DCI, eleva-se substancialmente a probabilidade pré-teste de evolução isquêmica tardia, justificando correlação clínica imediata e baixo limiar para neuroimagem vascular complementar.');
      } else if (vs.state === 'VS-2_with_discordance') {
        lines.push('Detecta-se **discordância hemodinâmica relevante**: velocidades em território de gravidade na ACM, sem corroboração proporcional da razão de Lindegaard. Cenário que pode favorecer hiperemia regional, contribuição sistêmica (anemia/hipercapnia/estado hiperdinâmico) ou — alternativamente — vasoespasmo proximal coexistente com vasoespasmo do sifão carotídeo mascarando o denominador (BC-05). Investigação direcionada é apropriada.');
      } else if (vs.state === 'VS-2') {
        lines.push('Achados configuram **padrão compatível com vasoespasmo leve a moderado** em ACM. Interpretação probabilística favorece evolução vasoespástica em contexto adequado, mas exige correlação clínica e seguimento seriado antes de qualquer ativação de protocolo institucional de DCI (BC-02).');
      } else if (vs.state === 'VS-1') {
        if (h.hyperemiaContext.length) {
          lines.push('Velocidades elevadas em ACM com Lindegaard preservado, **em contexto de ' + h.hyperemiaContext.join(' + ') + '**, favorecem hiperemia em detrimento de vasoespasmo focal. Recomenda-se normalização das variáveis sistêmicas e reavaliação seriada antes de inferir fenômeno vascular cerebral primário.');
        } else {
          lines.push('Padrão inicial sugestivo de hiperemia ou vasoespasmo incipiente, **a diferenciar** com correlação sistêmica e tendência temporal (BC-02).');
        }
      } else if (vs.state === 'VS-0') {
        if (s.clinical_status === 'piorando' || s.clinical_status === 'deficit_novo' || s.clinical_status === 'deteriorando_sem_definicao') {
          lines.push('**Discordância clínico-hemodinâmica relevante**: déficit ou piora clínica reportada com TCD em parâmetros normais para o segmento avaliado. A clínica prevalece (BC-07); deve-se considerar vasoespasmo distal não detectável ao TCD (BC-05) e investigar com neuroimagem vascular.');
        } else {
          lines.push('Sem evidência hemodinâmica de vasoespasmo proximal nos segmentos avaliados. Em paciente HSA em janela de risco, vigilância seriada permanece indicada — TCD normal **não exclui** vasoespasmo distal (BC-05).');
        }
      } else if (vs.state === 'VS-INDETERMINATE') {
        lines.push('Eixo vasoespástico **não classificável** nesta avaliação por limitação técnica (janela inadequada ou ausência de Lindegaard computável). Em paciente HSA em janela de risco, complementação por neuroimagem vascular ganha relevância — repetir TCD não resolve limitação anatômica primária do paciente (BC-06).');
      }

      if (h.posteriorSloanFlag) {
        lines.push('Em circulação posterior, ' + h.posteriorSloanFlag + ', achado que **aumenta probabilidade** de vasoespasmo posterior; sensibilidade limitada do método justifica considerar imagem complementar mesmo se demais segmentos forem inocentes.');
      }
    }

    // --- TCE / HIC ---
    if (ctx === 'TCE' || ctx === 'HIC_suspeita') {
      if (pr.state === 'PR-0') {
        lines.push('Eixo de pressão dentro de faixa fisiológica nesta janela do exame. Em paciente neurocrítico com PIC invasiva, TCD compõe avaliação multimodal e documenta tendência (BC-01).');
      } else if (pr.state === 'PR-1') {
        const ctxNotes = h.ipContext.length ? ' Observa-se que ' + h.ipContext.join(' e ') + ', o que recomenda cautela antes de inferir HIC isoladamente.' : '';
        lines.push('Pulsatilidade sustentadamente elevada em circulação anterior **favorece** aumento de resistência distal. Diferenciais incluem HIC, edema, vasoconstrição farmacológica e vasoespasmo a jusante.' + ctxNotes + ' TCD informa tendência, não estima PIC absoluta (BC-01).');
      } else if (pr.state === 'PR-2') {
        lines.push('VDF ausente bilateralmente, configurando padrão de **perfusão apenas sistólica** — assinatura compatível com elevação crítica de resistência distal. Cenário exige reavaliação imediata de PaCO₂, PPC e medidas neuroproteção, sob cuidado da equipe assistencial. TCD não substitui PIC invasiva (BC-01).');
      } else if (pr.state === 'PR-3') {
        lines.push('Componente diastólico reverso configura **progressão hemodinâmica preocupante** em direção a falência de perfusão. Em contexto neurocrítico, padrão demanda avaliação imediata de protocolo institucional de HIC refratária e correlação clínica integrada (BC-03).');
      } else if (['PR-4', 'PR-5', 'PR-6'].includes(pr.state)) {
        const cfm = s.CFM_2173_2017_activated;
        const pp = s.previously_demonstrated_patent;
        let phrase = 'Padrão espectral compatível com **parada circulatória cerebral em evolução**';
        if (pr.state === 'PR-6' && !pp) {
          phrase = 'Ausência de sinal observada **sem documentação prévia de janela patente** neste paciente; achado **NÃO interpretável** como ausência de fluxo (BC-06) — pode representar limitação técnica.';
        } else if (!cfm) {
          phrase += '. ⚠ Protocolo CFM 2.173/2017 **não está ativado**; achado é registrado como observação ancilar, **sem autorização para inferência diagnóstica de morte encefálica** (BC-03).';
        } else {
          phrase += '. Dentro do protocolo CFM 2.173/2017 ativo, achado compõe avaliação ancilar — diagnóstico de morte encefálica é clínico-legal e exige bilateralidade, janela previamente patente, exame clínico e teste de apneia integrais (BC-03).';
        }
        lines.push(phrase);
      }
    }

    // --- Protocolo ME ativo ---
    if (ctx === 'protocolo_ME_ativo') {
      const cfm = s.CFM_2173_2017_activated;
      const pp = s.previously_demonstrated_patent;
      const ce = s.clinical_exam_concordant;
      const at = s.apnea_test_done;
      const bi = s.bilateral_assessment_done;
      const oc = s.operator_certified_for_ME;

      if (['PR-4','PR-5','PR-6'].includes(pr.state)) {
        if (!cfm) {
          lines.push('⚠ Padrão espectral compatível com parada circulatória cerebral em evolução, **porém o protocolo CFM 2.173/2017 NÃO está formalmente ativado**. Achado deve ser registrado como observação ancilar; **NÃO autoriza inferência diagnóstica de morte encefálica** (BC-03). Conduzir como referência hemodinâmica para a equipe assistencial responsável.');
        } else if (pr.state === 'PR-6' && !pp) {
          lines.push('⚠ Ausência de sinal observada, porém **janela previamente patente NÃO documentada** neste paciente. Achado **NÃO é interpretável** como ausência hemodinâmica de fluxo (BC-06) — pode representar limitação técnica anatômica.');
        } else if (cfm) {
          const missing = [];
          if (!ce) missing.push('exame clínico concordante');
          if (!at) missing.push('teste de apneia');
          if (!bi) missing.push('avaliação bilateral');
          if (!oc) missing.push('certificação do operador para ME');
          if (missing.length) {
            lines.push('Padrão espectral compatível com parada circulatória cerebral, dentro do protocolo CFM 2.173/2017 ativado. **Pré-requisitos pendentes:** ' + missing.join(', ') + '. Achado compõe avaliação ancilar somente quando o conjunto dos pré-requisitos legais e técnicos estiver íntegro (BC-03).');
          } else {
            lines.push('Padrão espectral compatível com parada circulatória cerebral, em paciente com protocolo CFM 2.173/2017 ativado e pré-requisitos técnicos preenchidos. Achado compõe **avaliação ancilar**; o diagnóstico de morte encefálica permanece clínico-legal e exige integração completa do protocolo (BC-03).');
          }
        }
      } else {
        lines.push('Contexto de protocolo ME ativo, porém eixo de pressão atual (' + pr.state + ') não configura padrão de parada circulatória cerebral. Achados registrados como observação evolutiva.');
      }
    }

    // --- AVC oclusão ---
    if (ctx === 'AVC_suspeita_oclusao') {
      lines.push('Em contexto de suspeita de oclusão arterial aguda, achados hemodinâmicos do TCD **compõem** avaliação funcional do segmento, mas **não substituem** caracterização anatômica por angio-TC/angio-RM ou angiografia (BC-04). Decisão de trombólise ou trombectomia segue protocolo institucional de AVC e correlação multidisciplinar.');
    }

    // --- Pós-op ---
    if (ctx === 'pos_op_neurovascular') {
      lines.push('No pós-operatório neurovascular, achados servem prioritariamente a (i) documentação de baseline imediato, (ii) detecção precoce de hiperperfusão pós-revascularização e (iii) vigilância de vasoespasmo em HSA tratada. Comparação seriada com técnica idêntica é essencial (BC-08).');
    }

    // --- Discordância clínica-TCD genérica ---
    if (!lines.some(l => /BC-07/.test(l)) &&
        (s.clinical_status === 'piorando' || s.clinical_status === 'deficit_novo' || s.clinical_status === 'deteriorando_sem_definicao')) {
      const stableState = ['VS-0', 'VS-1'].includes(vs.state) && ['PR-0', 'PR-1'].includes(pr.state);
      if (stableState) {
        lines.push('Há **discordância clínico-hemodinâmica** a registrar: piora clínica reportada com TCD em parâmetros estáveis. **A clínica prevalece** (BC-07); recomenda-se investigação complementar e considerar vasoespasmo distal não acessível ao método.');
      }
    }

    if (lines.length === 0) {
      lines.push('Sem achados hemodinâmicos que modifiquem substancialmente a interpretação clínica nesta janela. Manutenção de vigilância e cadência conforme contexto.');
    }

    return lines;
  }

  // ============================================================
  // CAMADA 4 — Coerência / discordâncias
  // ============================================================
  function detectDiscordances(s, h) {
    const out = [];

    // Vm extrema sem LR confirmatório
    if (h.mcaSevere && h.lrConcordance && /discord/i.test(h.lrConcordance)) {
      out.push({
        type: 'vm-lr-discordance',
        text: 'Vm em território extremo com Lindegaard não-concordante. Considerar (a) hiperemia regional, (b) vasoespasmo proximal com vasoespasmo do sifão coexistente mascarando o denominador (BC-05), ou (c) erro técnico na medida da ACI extracraniana. Investigação direcionada apropriada.',
      });
    }

    // IP alto + modulador sistêmico
    if (h.ipElev && h.ipContext.length) {
      out.push({
        type: 'ip-systemic',
        text: 'Pulsatilidade elevada coexiste com ' + h.ipContext.join(' + ') + ' — fatores que podem contribuir ao achado independentemente de HIC. Interpretação isolada como HIC merece cautela (BC-01).',
      });
    }

    // Difuso bilateral simétrico sem contexto HSA
    if (h.diffusePattern && !h.focalPattern && s.context !== 'HSA' && h.hyperemiaContext.length) {
      out.push({
        type: 'diffuse-hyperemia',
        text: 'Elevação simultânea bilateral difusa em circulação anterior, em contexto de ' + h.hyperemiaContext.join(' + ') + ', favorece estado hiperêmico sistêmico em detrimento de vasoespasmo focal clássico.',
      });
    }

    // Topografia déficit ↔ vaso
    const def = s.deficit_side;
    if (def && def !== 'nenhum' && def !== 'indefinido' && h.dominantSide) {
      // Hemiparesia esquerda = ACM direita; hemiparesia direita = ACM esquerda
      const expectedSide = def === 'esquerdo' ? 'direita' : (def === 'direito' ? 'esquerda' : null);
      if (expectedSide && expectedSide === h.dominantSide) {
        out.push({
          type: 'topographic-concordance',
          text: 'Há **concordância topográfica** entre o déficit clínico (' + def + ') e o predomínio hemodinâmico (ACM ' + h.dominantSide + '), o que aumenta a relevância pré-teste de comprometimento vascular hemisférico contralateral ao déficit.',
        });
      } else if (expectedSide && expectedSide !== h.dominantSide) {
        out.push({
          type: 'topographic-discordance',
          text: 'Padrão hemodinâmico predominante (ACM ' + h.dominantSide + ') **não** corresponde topograficamente ao déficit relatado (' + def + '). Considerar causa não vasoespástica, vasoespasmo distal contralateral (BC-05) ou outra etiologia.',
        });
      }
    }

    return out;
  }

  // ============================================================
  // CAMADA 5 — Sugestão de seguimento
  // ============================================================
  function buildMonitoringSuggestion(s, h, t) {
    const ctx = s.context;
    const vs = (s.states && s.states.vasospasm_axis) || {};
    const pr = (s.states && s.states.pressure_axis) || {};
    const alarms = s.alarms || [];

    const highestPrio = alarms.reduce((p, a) => {
      const ranks = { info: 0, info_with_caveat: 1, low: 2, medium: 3, high: 4, critical: 5 };
      return (ranks[a.priority] || 0) > (ranks[p] || 0) ? a.priority : p;
    }, 'info');

    const items = [];

    if (vs.state === 'VS-3' || pr.state === 'PR-2' || pr.state === 'PR-3') {
      items.push('reavaliação em **janela curta** (sugestão operacional: ~6h), conforme pactuação institucional (BC-09)');
      items.push('correlação clínica integrada (exame neurológico, neuroimagem vascular) antes de qualquer decisão terapêutica');
    } else if (vs.state === 'VS-2' || vs.state === 'VS-2_with_discordance' || pr.state === 'PR-1') {
      items.push('intensificar cadência (sugestão: ~12h) com técnica idêntica (BC-08)');
      items.push('manter correlação seriada com variáveis sistêmicas (PaCO₂, sedação, Ht, PAM)');
    } else if (vs.state === 'VS-INDETERMINATE' && (ctx === 'HSA' && (s.fisher === '3' || s.fisher === '4' || ['III', 'IV', 'V'].includes(s.hunt_hess)))) {
      items.push('**complementação por neuroimagem vascular** (angio-TC/angio-RM) — limitação anatômica de janela em paciente de alto risco não se resolve com repetição de TCD');
    } else if (ctx === 'HSA') {
      items.push('manter cadência basal de vigilância na janela de risco (dias 3–14) conforme pactuação institucional');
    }

    if (['PR-4', 'PR-5', 'PR-6'].includes(pr.state)) {
      items.push('integração ao **protocolo CFM 2.173/2017** somente se já ativado pela equipe assistencial responsável');
    }

    if (h.posteriorSloanFlag) {
      items.push('considerar avaliação dirigida da circulação posterior por imagem');
    }

    if (t.flags.includes('aceleração significativa ACM direita') || t.flags.includes('aceleração significativa ACM esquerda')) {
      items.push('reforço imediato de comunicação ao intensivista — Δ > 50 cm/s/24h carrega peso prognóstico independente (BC-08)');
    }

    if (items.length === 0) items.push('manter cadência conforme protocolo institucional e contexto clínico');

    return { items, highestPrio };
  }

  // ============================================================
  // CAMADA 6 — Confiança interpretativa
  // ============================================================
  function assessConfidence(s, h, t) {
    let score = 0;
    let factors = [];

    // Janela patente bilateral
    if (s.transtemporal_right === 'patente' && s.transtemporal_left === 'patente') {
      score += 2; factors.push('janela transtemporal bilateral patente');
    } else if (s.transtemporal_right !== 'ausente' && s.transtemporal_left !== 'ausente') {
      score += 1; factors.push('janela transtemporal acessível ao menos unilateralmente');
    } else {
      score -= 2; factors.push('janela transtemporal ausente bilateralmente (limitação técnica importante)');
    }

    // Lindegaard computável
    if (present(h.lrD) && present(h.lrE)) { score += 1; factors.push('Lindegaard bilateral computável'); }
    else if (present(h.lrD) || present(h.lrE)) { score += 0; factors.push('Lindegaard unilateral'); }
    else if (h.mcaElev) { score -= 1; factors.push('Lindegaard não computável com Vm elevada — diferencial hiperemia/vasoespasmo prejudicado (BC-05)'); }

    // Tendência temporal disponível
    if (t.hasPrevious && t.techniqueIdentical) { score += 2; factors.push('comparação serial com técnica reprodutível disponível'); }
    else if (t.hasPrevious && !t.techniqueIdentical) { score -= 1; factors.push('comparação serial com técnica não reprodutível (BC-08)'); }
    else { score += 0; factors.push('sem comparação serial nesta sessão'); }

    // Variáveis sistêmicas registradas
    const sysVars = [s.PaCO2_mmHg, s.hematocrit_pct, s.sedation, s.head_elevation_degrees].filter(x => present(x) || (x && x.length));
    if (sysVars.length >= 3) { score += 1; factors.push('variáveis sistêmicas registradas'); }
    else { score -= 1; factors.push('variáveis sistêmicas incompletas (' + sysVars.length + '/4)'); }

    // Estado nos eixos definido?
    const vs = (s.states && s.states.vasospasm_axis) || {};
    const pr = (s.states && s.states.pressure_axis) || {};
    if (vs.state === 'VS-INDETERMINATE' || pr.state === 'PR-INDETERMINATE') {
      score -= 1; factors.push('ao menos um eixo INDETERMINATE');
    }

    // Discordâncias clínicas
    if (s.clinical_status === 'piorando' || s.clinical_status === 'deficit_novo' || s.clinical_status === 'deteriorando_sem_definicao') {
      score += 0; factors.push('clínica em piora — peso adicional à correlação clínica');
    }

    let level;
    if (score >= 4) level = 'ALTO';
    else if (score >= 1) level = 'MODERADO';
    else level = 'BAIXO';

    return { level, score, factors };
  }

  // ============================================================
  // CAMADA 7 — Síntese e montagem da narrativa final
  // ============================================================
  function buildNarrative(s, h, t, sig, disc, mon, conf) {
    const sep = '═══════════════════════════════════════════════════════════════════';
    const dash = '─────────────────────────────────────────────────────────────────';
    const lines = [];

    lines.push(sep);
    lines.push('INTERPRETAÇÃO HEMODINÂMICA NEUROVASCULAR — síntese narrativa');
    lines.push('(camada interpretativa subordinada ao engine determinístico)');
    lines.push(sep);
    lines.push('');

    // -- Resumo executivo --
    lines.push('▌ RESUMO EXECUTIVO');
    lines.push(executiveSummary(s, h, sig));
    lines.push('');

    // -- Interpretação hemodinâmica principal --
    lines.push('▌ INTERPRETAÇÃO HEMODINÂMICA');
    lines.push(hemodynamicSynthesis(s, h));
    lines.push('');

    // -- Comparação temporal --
    lines.push('▌ COMPARAÇÃO TEMPORAL');
    lines.push(t.narrative || '—');
    if (t.flags.length) lines.push('Sinalizações temporais: ' + t.flags.join(' · '));
    lines.push('');

    // -- Significado clínico provável --
    lines.push('▌ SIGNIFICADO CLÍNICO PROVÁVEL');
    sig.forEach(l => lines.push('· ' + l));
    lines.push('');

    // -- Alertas hemodinâmicos (discordâncias, padrões críticos) --
    if (disc.length) {
      lines.push('▌ ALERTAS HEMODINÂMICOS');
      disc.forEach(d => lines.push('⚠ ' + d.text));
      lines.push('');
    }

    // -- Sugestão de seguimento --
    lines.push('▌ SUGESTÃO DE MONITORIZAÇÃO');
    mon.items.forEach(it => lines.push('· ' + it));
    lines.push('Prioridade global de comunicação (do engine determinístico): ' + mon.highestPrio);
    lines.push('');

    // -- Limitações --
    lines.push('▌ LIMITAÇÕES INTERPRETATIVAS');
    lines.push(buildLimitations(s, h, conf));
    lines.push('');

    // -- Confiança --
    lines.push('▌ GRAU DE CONFIANÇA INTERPRETATIVA: ' + conf.level + ' (score ' + conf.score + ')');
    conf.factors.forEach(f => lines.push('  · ' + f));
    lines.push('');

    // -- Boundary obrigatório --
    lines.push(dash);
    lines.push('O Doppler Transcraniano compõe avaliação hemodinâmica multimodal e não substitui exame neurológico, neuroimagem vascular/estrutural, monitorização invasiva ou decisão clínica especializada.');
    lines.push('');
    lines.push('TCD informa dinâmica hemodinâmica; a equipe assistencial decide interpretação clínica integrada e conduta.');
    lines.push(dash);
    lines.push('[Camada interpretativa v0.1 · subordinada a DTC_HSA_KERNEL_v1 · rascunho técnico, exige homologação médica]');

    return lines.join('\n');
  }

  // ---- Sub-narrativas ----
  function executiveSummary(s, h, sig) {
    const vs = (s.states && s.states.vasospasm_axis) || {};
    const pr = (s.states && s.states.pressure_axis) || {};
    const ctx = s.context || 'contexto não especificado';
    const phrases = [];

    let vsPhrase = '';
    if (vs.state === 'VS-0') vsPhrase = 'sem evidência de vasoespasmo proximal nos segmentos avaliados';
    else if (vs.state === 'VS-1') vsPhrase = 'padrão hemodinâmico inicial — diferencial hiperemia vs vasoespasmo incipiente';
    else if (vs.state === 'VS-2') vsPhrase = 'padrão compatível com vasoespasmo leve a moderado em ACM';
    else if (vs.state === 'VS-2_with_discordance') vsPhrase = 'velocidades em território de gravidade com discordância de Lindegaard — diferencial probabilístico exigido';
    else if (vs.state === 'VS-3') vsPhrase = 'padrão compatível com vasoespasmo grave em ACM' + (h.dominantSide ? (' (predomínio à ' + h.dominantSide + ')') : '');
    else if (vs.state === 'VS-INDETERMINATE') vsPhrase = 'eixo vasoespástico não classificável por limitação técnica';

    let prPhrase = '';
    if (pr.state === 'PR-0') prPhrase = 'pulsatilidade preservada';
    else if (pr.state === 'PR-1') prPhrase = 'pulsatilidade sustentadamente elevada (BC-01)';
    else if (pr.state === 'PR-2') prPhrase = 'perfusão apenas sistólica (VDF ausente)';
    else if (pr.state === 'PR-3') prPhrase = 'componente diastólico reverso';
    else if (['PR-4','PR-5','PR-6'].includes(pr.state)) prPhrase = 'padrão compatível com parada circulatória cerebral em evolução (BC-03)';
    else if (pr.state === 'PR-INDETERMINATE') prPhrase = 'eixo de pressão não classificável';

    phrases.push('Em contexto de ' + ctx + ', exame caracteriza ' + vsPhrase + ', com ' + prPhrase + '.');
    if (h.posteriorSloanFlag) phrases.push('Adicionalmente, observa-se padrão posterior provável por critérios de Sloan (especificidade ~100%, sensibilidade limitada).');

    return phrases.join(' ');
  }

  function hemodynamicSynthesis(s, h) {
    const parts = [];

    // Anterior
    const anteriorParts = [];
    if (present(h.mcaR)) anteriorParts.push('ACM direita Vm=' + fmt(h.mcaR, 0) + ' cm/s' + (present(h.ipR) ? ', IP=' + fmt(h.ipR, 2) : ''));
    if (present(h.mcaL)) anteriorParts.push('ACM esquerda Vm=' + fmt(h.mcaL, 0) + ' cm/s' + (present(h.ipL) ? ', IP=' + fmt(h.ipL, 2) : ''));
    if (anteriorParts.length) {
      let line = 'Circulação anterior: ' + anteriorParts.join('; ');
      if (present(h.lrD) || present(h.lrE)) {
        const lrParts = [];
        if (present(h.lrD)) lrParts.push('LR D=' + fmt(h.lrD, 2));
        if (present(h.lrE)) lrParts.push('LR E=' + fmt(h.lrE, 2));
        line += '. ' + lrParts.join(', ');
      } else if (h.mcaElev) {
        line += '. Lindegaard NÃO COMPUTÁVEL — diferencial hiperemia/vasoespasmo prejudicado (BC-05)';
      }
      parts.push(line + '.');
    }

    // Posterior
    const postParts = [];
    if (present(h.bas)) postParts.push('basilar Vm=' + fmt(h.bas, 0));
    if (present(h.vertR)) postParts.push('vertebral D Vm=' + fmt(h.vertR, 0));
    if (present(h.vertL)) postParts.push('vertebral E Vm=' + fmt(h.vertL, 0));
    if (postParts.length) {
      parts.push('Circulação posterior: ' + postParts.join('; ') + ' cm/s' + (h.posteriorSloanFlag ? ' — limiares Sloan cruzados.' : '.'));
    }

    // Pattern characterization
    if (h.focalPattern) {
      parts.push('O padrão observado é **focal**, com predomínio à ' + h.dominantSide + ' (assimetria ' + fmt(h.asymmetryPct, 0) + '%), o que favorece fenômeno hemisférico em detrimento de modulação sistêmica difusa.');
    } else if (h.diffusePattern) {
      parts.push('A distribuição é **bilateralmente difusa** sem lateralização clara, padrão que em contexto sistêmico apropriado pode favorecer estado hiperêmico ou modulação fisiológica em detrimento de vasoespasmo focal clássico.');
    }

    // Pulsatilidade contextualizada
    if (h.ipElev) {
      let line = 'Pulsatilidade média anterior elevada (IP médio ' + fmt(h.ipMean, 2) + ')';
      if (h.ipContext.length) line += ', porém ' + h.ipContext.join(' e ') + ' — recomenda-se contextualizar antes de inferir aumento de resistência distal isolado (BC-01)';
      parts.push(line + '.');
    }

    // Hyperemia context
    if (h.mcaElev && h.hyperemiaContext.length && !h.focalPattern) {
      parts.push('Achados de velocidades elevadas em **contexto sistêmico de ' + h.hyperemiaContext.join(' + ') + '** favorecem hiperemia, sustentando interpretação probabilística — não diagnóstica.');
    }

    return parts.join(' ');
  }

  function buildLimitations(s, h, conf) {
    const items = [];
    items.push('Toda inferência é probabilística e contextual; nenhum achado isolado autoriza diagnóstico definitivo.');
    if (h.mcaElev && !present(h.lrD) && !present(h.lrE)) items.push('Lindegaard não computável — diferenciação hiperemia × vasoespasmo permanece INDETERMINADA por limitação anatômica/técnica (BC-05).');
    if (s.transtemporal_right === 'ausente' || s.transtemporal_left === 'ausente') items.push('Janela transtemporal ausente em ao menos um lado limita interpretação segmentar e impede inferência sobre territórios não avaliáveis (BC-06).');
    if (s.context === 'protocolo_ME_ativo' && !s.previously_demonstrated_patent) items.push('Em contexto ME, ausência de documentação de janela previamente patente invalida inferência de ausência de fluxo (BC-06).');
    if (!s.PaCO2_mmHg) items.push('PaCO₂ não informado — interpretação de IP/Vm sem ancoragem fisiológica plena.');
    if (conf.level === 'BAIXO') items.push('Confiança interpretativa BAIXA — desfecho desta sessão deve ser tratado como referência preliminar, não como caracterização hemodinâmica robusta.');
    items.push('TCD não substitui exame neurológico, neuroimagem vascular/estrutural, monitorização invasiva, nem decisão clínica especializada.');
    return items.map(x => '· ' + x).join('\n');
  }

  // ============================================================
  // ENTRY POINT
  // ============================================================
  function generate(snapshot) {
    if (!snapshot || typeof snapshot !== 'object') {
      return { error: 'snapshot inválido' };
    }
    const h = analyzeHemodynamics(snapshot);
    const t = analyzeTemporal(snapshot, h);
    const sig = analyzeClinicalSignificance(snapshot, h, t);
    const disc = detectDiscordances(snapshot, h);
    const mon = buildMonitoringSuggestion(snapshot, h, t);
    const conf = assessConfidence(snapshot, h, t);
    const fullNarrative = buildNarrative(snapshot, h, t, sig, disc, mon, conf);

    return {
      executiveSummary: executiveSummary(snapshot, h, sig),
      hemodynamicSynthesis: hemodynamicSynthesis(snapshot, h),
      temporalComparison: t.narrative,
      clinicalSignificance: sig,
      hemodynamicAlerts: disc.map(d => d.text),
      monitoringSuggestion: mon.items,
      confidenceLevel: conf.level,
      confidenceScore: conf.score,
      confidenceFactors: conf.factors,
      fullNarrative: fullNarrative,
      _internal: { h, t },
    };
  }

  // Expose
  global.DTCInterpretive = {
    generate: generate,
    version: '0.1',
  };

})(typeof window !== 'undefined' ? window : globalThis);
