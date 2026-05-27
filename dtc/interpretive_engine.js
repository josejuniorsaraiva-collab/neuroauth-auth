/* ============================================================
 * Interpretação Hemodinâmica Neurovascular — DTC HSA
 * Camada narrativa avançada (v0.5 — coerência fisiopatológica P5)
 * ============================================================
 * Produz raciocínio hemodinâmico integrado, contextual e
 * temporalmente sofisticado, no estilo de laudos neurointensivos
 * de centros terciários.
 *
 * REGRAS:
 *   - NÃO modifica estados VS/PR/EM
 *   - NÃO modifica alarmes ou thresholds
 *   - NÃO emite diagnóstico definitivo, conduta ou intervenção
 *   - Linguagem sempre qualificada: "padrão compatível com",
 *     "favorece", "sugere", "aumenta probabilidade pré-teste",
 *     "merece correlação", "não exclui"
 *   - Boundaries obrigatórios em todo laudo
 *   - Sempre declara confiança multidimensional
 *
 * ============================================================ */

(function (global) {
  'use strict';

  // ======== Helpers ========
  function present(x) { return x !== null && x !== undefined && !Number.isNaN(x); }
  function fmt(x, d) { return present(x) ? Number(x).toFixed(d == null ? 1 : d) : '—'; }
  function nz(arr) { return arr.filter(present); }
  function avg(arr) { const a = nz(arr); return a.length ? a.reduce((s, x) => s + x, 0) / a.length : null; }
  function maxBy(arr, key) { let best = null; for (const it of arr) { if (!best || it[key] > best[key]) best = it; } return best; }
  function cap(s) { return s ? s.charAt(0).toUpperCase() + s.slice(1) : s; }
  function joinList(arr, sep, last) {
    if (!arr || !arr.length) return '';
    if (arr.length === 1) return arr[0];
    if (arr.length === 2) return arr[0] + (last || ' e ') + arr[1];
    return arr.slice(0, -1).join(sep || ', ') + (last || ' e ') + arr[arr.length - 1];
  }

  // ============================================================
  // CAMADA BASE — Análise hemodinâmica (insumo das demais)
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

    // Lateralidade e assimetria
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

    // Padrão focal × difuso
    const anteriorVms = nz([mcaR, mcaL, acaR, acaL]);
    const allAnteriorElev = anteriorVms.length >= 2 && anteriorVms.every(v => v >= 100);
    const mcaElev = (present(mcaR) && mcaR >= 120) || (present(mcaL) && mcaL >= 120);
    const mcaSevere = (present(mcaR) && mcaR >= 200) || (present(mcaL) && mcaL >= 200);
    const focalPattern = !!dominantSide && asymmetryPct > 40;
    const diffusePattern = allAnteriorElev && !focalPattern;

    // Contexto sistêmico para hiperemia
    const hyperemiaContext = [];
    if (s.PaCO2_mmHg && s.PaCO2_mmHg > 45) hyperemiaContext.push('hipercapnia (PaCO₂ ' + s.PaCO2_mmHg + ' mmHg)');
    if (s.hematocrit_pct && s.hematocrit_pct < 30) hyperemiaContext.push('anemia (Ht ' + s.hematocrit_pct + '%)');
    if (s.HR && s.HR > 100) hyperemiaContext.push('estado hiperdinâmico clínico (FC ' + s.HR + ' bpm)');
    if (s.context === 'pos_op_neurovascular') hyperemiaContext.push('contexto pós-revascularização');

    // Moduladores do IP elevado
    const ipContext = [];
    if (present(ipMean) && ipMean > 1.2) {
      if (s.PaCO2_mmHg && s.PaCO2_mmHg < 32) ipContext.push('hipocapnia (PaCO₂ ' + s.PaCO2_mmHg + ' mmHg)');
      if (s.sedation && /propofol|midaz|fent|barbi|tiopent|dexme/i.test(s.sedation)) ipContext.push('sedação profunda ativa');
      if (s.MAP_mmHg && s.MAP_mmHg < 65) ipContext.push('PAM baixa (' + s.MAP_mmHg + ' mmHg)');
    }

    // Concordância Lindegaard
    let lrConcordance = null;
    if (mcaSevere && present(lrD) && present(lrE)) {
      const lrMax = Math.max(lrD, lrE);
      if (lrMax > 6) lrConcordance = 'concordante (LR > 6)';
      else if (lrMax >= 3) lrConcordance = 'parcialmente concordante (LR 3-6 com Vm grave)';
      else lrConcordance = 'discordante (LR < 3 com Vm grave)';
    } else if (mcaElev && (present(lrD) || present(lrE))) {
      const lrMax = Math.max(lrD || 0, lrE || 0);
      lrConcordance = lrMax >= 3 ? 'concordante (LR ≥ 3)' : 'discordante (LR < 3)';
    }

    // Posterior Sloan
    let posteriorSloanFlag = null;
    if ((present(bas) && bas >= 95) || (present(vertR) && vertR >= 80) || (present(vertL) && vertL >= 80)) {
      posteriorSloanFlag = 'limiares de Sloan cruzados em circulação posterior (alta especificidade, baixa sensibilidade)';
    }

    // Basilar isolada
    const basilarIsolated = present(bas) && bas >= 80 && !mcaElev;

    return {
      mcaR, mcaL, acaR, acaL, pcaR, pcaL, bas, vertR, vertL,
      ipR, ipL, ipMean, lrD, lrE, dR, dL,
      asymmetryPct, dominantSide,
      mcaElev, mcaSevere, focalPattern, diffusePattern,
      allAnteriorElev, basilarIsolated,
      hyperemiaContext, ipContext, lrConcordance,
      posteriorSloanFlag,
    };
  }

  // ============================================================
  // CAMADA 1 — Assinatura hemodinâmica global (P5: coerência cruzada Vm/IP/LR)
  // ============================================================
  function buildHemodynamicSignature(s, h) {
    const tokens = [];
    const vs = (s.states && s.states.vasospasm_axis) || {};
    const pr = (s.states && s.states.pressure_axis) || {};

    // Estabilidade geral — coerência horizontal preservada
    if (vs.state === 'VS-0' && pr.state === 'PR-0') {
      tokens.push('padrão hemodinâmico estável');
    }

    // Distribuição
    if (h.focalPattern) {
      tokens.push('aceleração focal hemisférica ' + h.dominantSide);
    } else if (h.diffusePattern) {
      tokens.push('perfil hiperêmico difuso bilateral em circulação anterior');
    }

    // Gravidade vasoespástica
    if (vs.state === 'VS-3') tokens.push('padrão compatível com vasoespasmo proximal grave em circulação anterior');
    else if (vs.state === 'VS-2_with_discordance') tokens.push('desacoplamento Vm/LR (velocidades em território de gravidade sem corroboração proporcional do Lindegaard)');
    else if (vs.state === 'VS-2') tokens.push('padrão compatível com vasoespasmo proximal leve a moderado');
    else if (vs.state === 'VS-1' && h.hyperemiaContext.length) tokens.push('perfil sugestivo de hiperemia em contexto sistêmico favorável');

    // Pressão / resistência distal
    if (pr.state === 'PR-1' && h.ipContext.length === 0) tokens.push('padrão compatível com aumento de resistência distal');
    else if (pr.state === 'PR-1' && h.ipContext.length) tokens.push('elevação de pulsatilidade em contexto fisiológico potencialmente contributivo');
    else if (pr.state === 'PR-2') tokens.push('padrão de perfusão apenas sistólica');
    else if (pr.state === 'PR-3') tokens.push('padrão de fluxo diastólico reverso');
    else if (['PR-4','PR-5','PR-6'].includes(pr.state)) tokens.push('padrão compatível com falência hemodinâmica avançada');

    // Posterior
    if (h.posteriorSloanFlag) tokens.push('predomínio em circulação posterior por critérios de Sloan');
    else if (h.basilarIsolated) tokens.push('elevação basilar isolada (sem correlato em circulação anterior)');

    // Coerência IP/Vm — P5: leitura cruzada pulsatilidade × velocidade
    // (apenas observação descritiva, sem novo threshold ou inferência)
    if (h.mcaElev && pr.state === 'PR-0' && present(h.ipMean) && h.ipMean <= 1.2) {
      tokens.push('coerência IP/Vm preservada — velocidades elevadas com pulsatilidade não-elevada favorecem estreitamento luminal sobre aumento de resistência distal');
    } else if (h.mcaSevere && pr.state === 'PR-1') {
      tokens.push('coexistência de velocidades em território de gravidade e pulsatilidade elevada — situação que tende a indicar componente distal somado ao proximal');
    }

    // Tendência
    if (present(h.dR) && Math.abs(h.dR) >= 50) {
      tokens.push('progressão hemodinâmica dinâmica nas últimas 24 horas');
    } else if (present(h.dR) && present(h.dL) && Math.abs(h.dR - h.dL) > 30) {
      tokens.push('progressão assimétrica entre hemisférios');
    } else if (s.exam_sequence_number === 1 || !s.exam_sequence_number) {
      tokens.push('estabilidade macrovascular nesta avaliação isolada');
    }

    // Pulsatilidade preservada apesar de alteração velocimétrica (já implícito acima, mantido para outros estados VS)
    if (pr.state === 'PR-0' && vs.state !== 'VS-0' && vs.state !== 'VS-INDETERMINATE' && !(h.mcaElev && present(h.ipMean) && h.ipMean <= 1.2)) {
      tokens.push('pulsatilidade preservada apesar das alterações de velocidade');
    }

    // INDETERMINATE
    if (vs.state === 'VS-INDETERMINATE' && pr.state === 'PR-INDETERMINATE') {
      tokens.push('caracterização hemodinâmica não factível por limitação técnica');
    }

    if (tokens.length === 0) tokens.push('caracterização hemodinâmica sem padrão dominante destacável');

    return joinList(tokens, ', ', '; com ') + '.';
  }

  // ============================================================
  // CAMADA 2 — Contextualização por doença
  // ============================================================
  function contextualizeByDisease(s, h) {
    const ctx = s.context;
    const vs = (s.states && s.states.vasospasm_axis) || {};
    const pr = (s.states && s.states.pressure_axis) || {};
    const out = [];

    if (ctx === 'HSA') {
      const day = s.post_bleed_day;
      const fisher = s.fisher;
      const hh = s.hunt_hess;
      const highRisk = (fisher == 3 || fisher == 4 || fisher === '3' || fisher === '4' ||
                        ['III','IV','V'].includes(hh));

      // Janela temporal
      let windowPhrase = '';
      if (present(day) && day >= 7 && day <= 10) windowPhrase = 'pico da janela vasoespástica (dia ' + day + ')';
      else if (present(day) && day >= 3 && day <= 14) windowPhrase = 'dentro da janela vasoespástica (dia ' + day + ' de 3-14)';
      else if (present(day) && day < 3) windowPhrase = 'fase pré-janela vasoespástica (dia ' + day + ')';
      else if (present(day) && day > 14) windowPhrase = 'além da janela clássica de vasoespasmo (dia ' + day + ')';

      // Estratificação
      let stratPhrase = '';
      if (highRisk) stratPhrase = 'estratificação de alto risco (' + (fisher ? 'Fisher ' + fisher : '') + (hh ? ' / Hunt-Hess ' + hh : '') + ')';
      else if (fisher || hh) stratPhrase = 'estratificação de baixo risco (' + (fisher ? 'Fisher ' + fisher : '') + (hh ? ' / Hunt-Hess ' + hh : '') + ')';

      // Construção
      let intro = 'Em contexto de hemorragia subaracnóidea';
      if (windowPhrase) intro += ' em ' + windowPhrase;
      if (stratPhrase) intro += ', com ' + stratPhrase;
      intro += ', ';

      // Padrão atual + leitura
      if (vs.state === 'VS-3') {
        intro += 'os achados elevam substancialmente a probabilidade pré-teste de evolução vasoespástica clinicamente significativa, embora não autorizem diagnóstico isolado de isquemia cerebral tardia (BC-02).';
      } else if (vs.state === 'VS-2_with_discordance') {
        intro += 'o desacoplamento entre velocidades elevadas e Lindegaard não-concordante exige diferencial cuidadoso entre vasoespasmo proximal com vasoconstrição coexistente do sifão (mascarando o denominador, BC-05), hiperemia regional ou contribuição sistêmica.';
      } else if (vs.state === 'VS-2') {
        intro += 'os achados sustentam padrão compatível com evolução vasoespástica leve a moderada, com necessidade de correlação clínica seriada (BC-02).';
      } else if (vs.state === 'VS-1' && h.hyperemiaContext.length) {
        intro += 'a elevação velocimétrica anterior com Lindegaard preservado em contexto de ' + h.hyperemiaContext.join(' + ') + ' favorece interpretação hiperêmica antes de inferência vasoespástica primária.';
      } else if (vs.state === 'VS-0') {
        if (s.clinical_status === 'piorando' || s.clinical_status === 'deficit_novo' || s.clinical_status === 'deteriorando_sem_definicao') {
          intro += 'a ausência de padrão vasoespástico proximal coexiste com clínica em piora — situação que merece investigação direcionada, dado que o método não exclui vasoespasmo distal nem fenômeno microcirculatório (BC-05); a clínica prevalece (BC-07).';
        } else {
          intro += 'os achados servem como referência basal para vigilância serial; ausência de vasoespasmo proximal não exclui fenômenos distais ou microcirculatórios (BC-05).';
        }
      } else if (vs.state === 'VS-INDETERMINATE') {
        intro += 'a limitação técnica de classificação eleva o valor de complementação por neuroimagem vascular, particularmente quando ' + (highRisk ? 'a estratificação caracteriza alto risco' : 'há suspeita clínica de evolução vasoespástica') + '.';
      }

      out.push(intro);

      // Posterior
      if (h.posteriorSloanFlag) {
        out.push('Em circulação posterior, achados cruzam limiares de Sloan, configurando padrão posterior provável — com alta especificidade mas sensibilidade limitada, recomendando-se considerar imagem complementar (BC-05).');
      }
    }

    if (ctx === 'TCE' || ctx === 'HIC_suspeita') {
      if (pr.state === 'PR-0') {
        out.push('Em contexto neurocrítico (TCE/HIC suspeita), eixo de pressão preservado nesta janela documenta complacência hemodinâmica macrovascular, sem assinatura de aumento de resistência distal significativo — observação que compõe avaliação multimodal e não substitui monitorização invasiva de PIC (BC-01).');
      } else if (pr.state === 'PR-1') {
        const modContext = h.ipContext.length ? ' Importante notar que ' + h.ipContext.join(' e ') + ' podem contribuir parcialmente ao padrão, recomendando cautela antes de inferir aumento de resistência distal puro.' : '';
        // P5: leitura coerente entre PaCO2 e IP — vasoconstrição farmacológica × HIC
        let paco2Note = '';
        if (present(s.PaCO2_mmHg) && s.PaCO2_mmHg < 32) {
          paco2Note = ' Na presença de hipocapnia ativa (PaCO₂ ' + s.PaCO2_mmHg + ' mmHg), a vasoconstrição farmacológica passa a ser componente possível e o IP isolado perde poder discriminatório para HIC; a tendência seriada — sob PaCO₂ estável — preserva maior valor interpretativo.';
        }
        out.push('Em contexto neurocrítico, pulsatilidade sustentadamente elevada favorece aumento de resistência distal — diferenciais incluem edema, hipertensão intracraniana, vasoconstrição farmacológica e vasoespasmo a jusante.' + modContext + paco2Note + ' TCD reproduz tendência hemodinâmica; estimativa absoluta de PIC permanece domínio da monitorização invasiva (BC-01).');
      } else if (pr.state === 'PR-2') {
        out.push('Padrão de perfusão apenas sistólica em contexto neurocrítico configura assinatura de elevação crítica de resistência distal — situação que demanda reavaliação imediata de PaCO₂, PPC e estratégias de neuroproteção pela equipe assistencial, sem que o método substitua a monitorização invasiva de PIC (BC-01).');
      } else if (pr.state === 'PR-3') {
        out.push('Inversão diastólica em contexto neurocrítico caracteriza progressão hemodinâmica em direção a falência de perfusão. Avaliação de protocolo institucional de HIC refratária pela equipe assistencial é apropriada (BC-03).');
      }
    }

    if (ctx === 'AVC_suspeita_oclusao') {
      out.push('Em contexto de suspeita de oclusão arterial aguda, achados hemodinâmicos compõem avaliação funcional do segmento mas não substituem caracterização anatômica por angio-TC/angio-RM ou angiografia (BC-04). Decisão de trombólise/trombectomia é multidisciplinar, integrando NIHSS, janela terapêutica, neuroimagem e contexto.');
    }

    if (ctx === 'pos_op_neurovascular') {
      out.push('Em pós-operatório neurovascular, achados servem prioritariamente como (i) documentação de baseline imediato, (ii) detecção precoce de hiperperfusão pós-revascularização e (iii) vigilância de vasoespasmo em HSA tratada. Comparabilidade serial com técnica idêntica é essencial (BC-08).');
    }

    if (ctx === 'protocolo_ME_ativo') {
      const cfm = s.CFM_2173_2017_activated;
      const pp = s.previously_demonstrated_patent;
      const ce = s.clinical_exam_concordant;
      const at = s.apnea_test_done;
      const bi = s.bilateral_assessment_done;
      const oc = s.operator_certified_for_ME;

      if (['PR-4','PR-5','PR-6'].includes(pr.state)) {
        if (!cfm) {
          out.push('Padrão espectral compatível com parada circulatória cerebral em evolução; entretanto, o protocolo CFM 2.173/2017 não está formalmente ativado, e portanto o achado é registrado como observação ancilar — não autoriza inferência diagnóstica de morte encefálica (BC-03). Conduzir como referência hemodinâmica integrada à avaliação da equipe assistencial responsável.');
        } else if (pr.state === 'PR-6' && !pp) {
          out.push('Ausência de sinal observada sem documentação prévia de janela patente neste paciente; o achado não é interpretável como ausência hemodinâmica de fluxo (BC-06) — pode representar limitação técnica anatômica primária.');
        } else if (cfm) {
          const missing = [];
          if (!ce) missing.push('exame clínico concordante');
          if (!at) missing.push('teste de apneia');
          if (!bi) missing.push('avaliação bilateral');
          if (!oc) missing.push('certificação do operador');
          if (missing.length) {
            out.push('Padrão espectral compatível com parada circulatória cerebral, dentro do protocolo CFM 2.173/2017 ativado. Pré-requisitos pendentes: ' + missing.join(', ') + '. Achado compõe avaliação ancilar somente quando o conjunto dos pré-requisitos legais e técnicos estiver íntegro (BC-03).');
          } else {
            out.push('Padrão espectral compatível com parada circulatória cerebral, em paciente com protocolo CFM 2.173/2017 ativado e pré-requisitos técnicos preenchidos. Achado compõe avaliação ancilar; o diagnóstico de morte encefálica permanece clínico-legal e exige integração completa do protocolo (BC-03).');
          }
        }
      } else {
        out.push('Contexto de protocolo de morte encefálica ativo, porém eixo de pressão atual (' + (pr.state || '—') + ') não configura padrão de parada circulatória cerebral. Achados registrados como observação evolutiva.');
      }
    }

    return out;
  }

  // ============================================================
  // CAMADA 3 — Trajetória temporal verdadeira
  // ============================================================
  function analyzeTemporalTrajectory(s, h) {
    const hasPrevious = !!(s.previous_exam_datetime_iso || present(s.prev_vm_mca_right) || present(s.prev_vm_mca_left));
    const techniqueIdentical = s.technique_identical !== false;

    const out = {
      hasPrevious,
      techniqueIdentical,
      trajectory: null,
      narrative: null,
      flags: [],
    };

    if (!hasPrevious) {
      out.trajectory = 'baseline';
      out.narrative = 'Sem comparação serial nesta sessão; o exame configura potencial referência basal individual para vigilância subsequente.';
      return out;
    }

    if (!techniqueIdentical) {
      out.flags.push('comparação serial frágil — técnica não declarada como idêntica (BC-08)');
    }

    const deltas = [];
    if (present(h.dR)) deltas.push({ side: 'direita', delta: h.dR });
    if (present(h.dL)) deltas.push({ side: 'esquerda', delta: h.dL });

    if (deltas.length === 0) {
      out.trajectory = 'baseline_incompleto';
      out.narrative = 'Exame anterior registrado, porém sem dados quantitativos comparáveis informados.';
      return out;
    }

    const absMax = Math.max(...deltas.map(d => Math.abs(d.delta)));
    const maxDelta = maxBy(deltas, 'delta');

    // Classificação de trajetória
    if (absMax < 15) {
      out.trajectory = 'estabilizacao';
    } else if (deltas.some(d => d.delta >= 50)) {
      out.trajectory = 'aceleracao_significativa';
    } else if (deltas.some(d => d.delta >= 30)) {
      out.trajectory = 'tendencia_ascendente';
    } else if (deltas.some(d => d.delta <= -30)) {
      out.trajectory = 'desaceleracao';
    } else {
      out.trajectory = 'variabilidade';
    }

    // Narrativa
    const parts = [];
    deltas.forEach(d => {
      const sign = d.delta > 0 ? '+' : '';
      if (Math.abs(d.delta) < 15) {
        parts.push('ACM ' + d.side + ' estável (Δ ' + sign + d.delta.toFixed(1) + ' cm/s)');
      } else if (d.delta >= 50) {
        parts.push('ACM ' + d.side + ' com aceleração sustentada que supera a variabilidade técnica esperada (Δ ' + sign + d.delta.toFixed(1) + ' cm/s/24h)');
        out.flags.push('aceleração sustentada ACM ' + d.side + ' — peso prognóstico independente (BC-08)');
      } else if (d.delta >= 30) {
        parts.push('ACM ' + d.side + ' com tendência ascendente (Δ +' + d.delta.toFixed(1) + ' cm/s/24h)');
      } else if (d.delta <= -30) {
        parts.push('ACM ' + d.side + ' com desaceleração relevante (Δ ' + d.delta.toFixed(1) + ' cm/s/24h)');
      } else {
        parts.push('ACM ' + d.side + ' com variabilidade discreta (Δ ' + sign + d.delta.toFixed(1) + ' cm/s/24h)');
      }
    });

    out.narrative = 'Em comparação à avaliação anterior, ' + joinList(parts, '; ', '; ') + '.';

    // Pseudonormalização (queda relevante após exame com vasoespasmo prévio)
    if (deltas.some(d => d.delta <= -50)) {
      out.narrative += ' A magnitude da desaceleração pode refletir resposta terapêutica documentável, normalização de variável sistêmica modulatória ou — em contexto de vasoespasmo grave prévio — possibilidade de pseudonormalização por queda de pressão de perfusão, exigindo correlação clínica cuidadosa.';
      out.flags.push('possível pseudonormalização — interpretar com cautela');
    }

    // Dissociação hemisférica
    if (deltas.length === 2 && Math.abs(deltas[0].delta - deltas[1].delta) > 30) {
      out.narrative += ' A progressão assimétrica entre hemisférios favorece fenômeno hemodinâmico focal em detrimento de modulação sistêmica difusa.';
      out.flags.push('dissociação hemisférica');
    }

    return out;
  }

  // ============================================================
  // CAMADA 4 — Discordâncias fisiológicas
  // ============================================================
  function detectPhysiologicDiscordances(s, h, traj) {
    const out = [];
    const vs = (s.states && s.states.vasospasm_axis) || {};
    const pr = (s.states && s.states.pressure_axis) || {};

    // Vm extrema sem corroboração do Lindegaard
    if (h.mcaSevere && h.lrConcordance && /discord/i.test(h.lrConcordance)) {
      out.push({
        type: 'vm-lr',
        text: 'A dissociação entre aceleração de fluxo em território de gravidade e razão de Lindegaard discordante favorece três interpretações que merecem diferencial: (i) hiperemia regional intensa em contexto sistêmico, (ii) vasoespasmo proximal com vasoconstrição coexistente do sifão carotídeo mascarando o denominador (BC-05), ou (iii) variável técnica não capturada. Investigação direcionada e correlação seriada são apropriadas.',
      });
    }

    // Vm elevada bilateralmente sem LR alto + contexto hiperêmico
    if (h.diffusePattern && (!h.focalPattern) && h.hyperemiaContext.length) {
      out.push({
        type: 'difuse-hiperemia',
        text: 'A elevação velocimétrica simultânea bilateral em circulação anterior, em contexto de ' + h.hyperemiaContext.join(' + ') + ', favorece estado hiperêmico sistêmico em detrimento de vasoespasmo focal clássico, sustentando interpretação probabilística e não diagnóstica.',
      });
    }

    // IP alto com modulador
    if (h.ipContext.length) {
      out.push({
        type: 'ip-modulado',
        text: 'A pulsatilidade elevada coexiste com ' + h.ipContext.join(' + ') + ' — fatores que podem contribuir ao padrão independentemente de elevação de resistência distal patológica, recomendando cautela antes de inferir hipertensão intracraniana isolada (BC-01).',
      });
    }

    // IP alto com Vm normal/baixa
    if (present(h.ipMean) && h.ipMean > 1.2 && !h.mcaElev) {
      out.push({
        type: 'ip-isolado',
        text: 'Pulsatilidade elevada com velocidades médias preservadas constitui assinatura compatível com aumento de resistência distal pura, sem componente proximal estenótico ou vasoespástico associado — diferencial entre edema, hipertensão intracraniana e vasoconstrição farmacológica é apropriado.',
      });
    }

    // Discordância clínica × TCD
    const clin = s.clinical_status;
    if ((clin === 'piorando' || clin === 'deficit_novo' || clin === 'deteriorando_sem_definicao') &&
        ['VS-0','VS-1'].includes(vs.state) && ['PR-0','PR-1'].includes(pr.state)) {
      out.push({
        type: 'tcd-clinica',
        text: 'A clínica em piora não encontra correlato hemodinâmico macrovascular nesta avaliação. A clínica prevalece (BC-07); deve-se considerar vasoespasmo distal não acessível ao método (BC-05), fenômeno microcirculatório ou outra etiologia, com investigação por neuroimagem vascular.',
      });
    }

    // Topografia déficit ↔ vaso
    const def = s.deficit_side;
    if (def && def !== 'nenhum' && def !== 'indefinido' && h.dominantSide) {
      const expectedSide = def === 'esquerdo' ? 'direita' : (def === 'direito' ? 'esquerda' : null);
      if (expectedSide === h.dominantSide) {
        out.push({
          type: 'topografia-concordante',
          text: 'Há concordância topográfica entre o déficit clínico (' + def + ') e o predomínio hemodinâmico (ACM ' + h.dominantSide + '), padrão que aumenta a relevância pré-teste de comprometimento vascular hemisférico contralateral ao déficit.',
        });
      } else if (expectedSide && expectedSide !== h.dominantSide) {
        out.push({
          type: 'topografia-discordante',
          text: 'O predomínio hemodinâmico (ACM ' + h.dominantSide + ') não corresponde topograficamente ao déficit relatado (' + def + ') — considerar causa não vasoespástica, vasoespasmo distal contralateral (BC-05) ou outra etiologia.',
        });
      }
    }

    // Basilar isolada
    if (h.basilarIsolated) {
      out.push({
        type: 'basilar-isolada',
        text: 'Elevação basilar isolada, sem alteração em circulação anterior, merece consideração de vasoespasmo de circulação posterior, hiperfluxo posterior por estimulação ou contribuição técnica (ângulo de insonação) — limiares de Sloan oferecem alta especificidade mas sensibilidade limitada (BC-05).',
      });
    }

    // Pseudonormalização sinalizada pela trajetória
    if (traj.flags && traj.flags.includes('possível pseudonormalização — interpretar com cautela')) {
      out.push({
        type: 'pseudonormalizacao',
        text: 'A magnitude da desaceleração relativa ao exame anterior, em contexto compatível, pode representar não apenas resposta terapêutica favorável mas também pseudonormalização hemodinâmica por queda de pressão de perfusão — diferenciar pela correlação clínica e variáveis sistêmicas (BC-08).',
      });
    }

    // P5 — Coerência sistêmica: PaCO2 baixa + IP elevado em contexto neurocrítico
    // (atenua inferência de HIC pura; sugere vasoconstrição farmacológica primária)
    if (present(h.ipMean) && h.ipMean > 1.2 && present(s.PaCO2_mmHg) && s.PaCO2_mmHg < 32 &&
        (s.context === 'TCE' || s.context === 'HIC_suspeita')) {
      out.push({
        type: 'ip-paco2',
        text: 'A coexistência de pulsatilidade elevada e hipocapnia (PaCO₂ ' + s.PaCO2_mmHg + ' mmHg) atenua a inferência de hipertensão intracraniana pura — a vasoconstrição farmacológica responde por componente plausível do padrão; a tendência seriada sob PaCO₂ estabilizada oferece maior poder discriminatório (BC-01).',
      });
    }

    // P5 — Coerência fluxo global: Vm baixa bilateral em paciente com clínica grave
    // sem assinatura de pressão crítica (PR-0/PR-1) — pode representar baixo débito
    // sistêmico ou estado hipodinâmico, não fenômeno cerebral primário.
    if (present(h.mcaR) && present(h.mcaL) && h.mcaR < 35 && h.mcaL < 35 &&
        ['VS-0','VS-INDETERMINATE'].includes(vs.state) && ['PR-0','PR-1'].includes(pr.state) &&
        (s.clinical_status === 'piorando' || s.clinical_status === 'deteriorando_sem_definicao')) {
      out.push({
        type: 'fluxo-global-baixo',
        text: 'Velocidades médias bilateralmente baixas em paciente com clínica em piora — leitura que precede investigação de baixo débito sistêmico, hipotensão ou variável sistêmica modulatória antes de inferência primariamente cerebral. A correlação com PAM, FC e estado hemodinâmico sistêmico é apropriada.',
      });
    }

    // P5 — Coerência Vm/IP: velocidade crítica com IP preservado favorece estreitamento
    // luminal sobre disfunção microcirculatória difusa.
    if (h.mcaSevere && present(h.ipMean) && h.ipMean <= 1.0) {
      out.push({
        type: 'vm-ip-coerencia',
        text: 'A coexistência de velocidades em território de gravidade com pulsatilidade preservada favorece padrão de estreitamento luminal proximal sobre disfunção microcirculatória distal difusa — leitura compatível com vasoespasmo proximal isolado, especialmente quando a Lindegaard é concordante.',
      });
    }

    return out;
  }

  // ============================================================
  // CAMADA 5 — Camada prognóstica qualificada
  // ============================================================
  function buildPrognosticLayer(s, h, traj) {
    const items = [];
    const ctx = s.context;
    const vs = (s.states && s.states.vasospasm_axis) || {};
    const pr = (s.states && s.states.pressure_axis) || {};

    // HSA — peso prognóstico
    if (ctx === 'HSA') {
      const day = s.post_bleed_day;
      const fisher = s.fisher;
      const hh = s.hunt_hess;
      const highRisk = (fisher == 3 || fisher == 4 || fisher === '3' || fisher === '4' ||
                        ['III','IV','V'].includes(hh));

      if (present(day) && day >= 7 && day <= 10) {
        items.push('Paciente em janela de maior vulnerabilidade vasoespástica (dias 7–10), o que amplifica a relevância clínica de qualquer aceleração hemodinâmica detectada.');
      } else if (present(day) && day >= 3 && day <= 14) {
        items.push('Dentro da janela de vigilância vasoespástica (dias 3–14), qualquer aceleração temporal merece valoração específica.');
      }

      if (highRisk) {
        items.push('Estratificação de alto risco (Fisher ' + (fisher || '?') + ' / Hunt-Hess ' + (hh || '?') + ') caracteriza vulnerabilidade aumentada a isquemia cerebral tardia, sustentando cadência intensificada e baixo limiar de complementação por imagem.');
      }

      if (vs.state === 'VS-3') {
        items.push('Padrão atual de vasoespasmo proximal grave eleva substancialmente a probabilidade pré-teste de isquemia cerebral tardia clinicamente significativa; correlação clínica e neuroimagem são imprescindíveis antes de qualquer decisão terapêutica (BC-02).');
      }
    }

    // Δ24h significativo
    if (present(h.dR) && h.dR >= 50) {
      items.push('Aceleração de ' + h.dR.toFixed(0) + ' cm/s/24h na ACM direita possui peso prognóstico independente do valor absoluto (BC-08), sustentando relevância clínica do achado.');
    }
    if (present(h.dL) && h.dL >= 50) {
      items.push('Aceleração de ' + h.dL.toFixed(0) + ' cm/s/24h na ACM esquerda possui peso prognóstico independente do valor absoluto (BC-08), sustentando relevância clínica do achado.');
    }

    // Progressão assimétrica
    if (traj.trajectory === 'aceleracao_significativa' && traj.flags.includes('dissociação hemisférica')) {
      items.push('A progressão assimétrica entre hemisférios eleva a relevância prognóstica do achado focal em detrimento de modulação sistêmica difusa, sustentando vigilância intensificada lateralizada.');
    }

    // Topografia concordante
    const def = s.deficit_side;
    if (def && def !== 'nenhum' && h.dominantSide) {
      const expectedSide = def === 'esquerdo' ? 'direita' : (def === 'direito' ? 'esquerda' : null);
      if (expectedSide === h.dominantSide) {
        items.push('A concordância topográfica déficit ↔ território hemodinâmico amplifica a relevância clínica e prognóstica do achado.');
      }
    }

    // Eixo de pressão crítico
    if (['PR-3','PR-4','PR-5','PR-6'].includes(pr.state)) {
      items.push('Eixo de pressão em padrão de falência hemodinâmica avançada — momento de elevada vulnerabilidade clínica, com correlação multimodal e protocolo institucional como referências (BC-01/BC-03).');
    }

    // Janela inadequada em paciente vulnerável
    if (vs.state === 'VS-INDETERMINATE' && ctx === 'HSA') {
      const fisher = s.fisher;
      const hh = s.hunt_hess;
      const highRisk = (fisher == 3 || fisher == 4 || fisher === '3' || fisher === '4' ||
                        ['III','IV','V'].includes(hh));
      if (highRisk) {
        items.push('Limitação técnica em paciente HSA de alto risco eleva a relevância de complementação por neuroimagem vascular — repetição do método não resolve limitação anatômica primária.');
      }
    }

    // P5 — Estabilização dentro da janela vasoespástica em paciente alto risco
    // sustenta manutenção de vigilância sem urgência adicional de imagem.
    if (ctx === 'HSA' && traj.trajectory === 'estabilizacao' &&
        present(s.post_bleed_day) && s.post_bleed_day >= 3 && s.post_bleed_day <= 14 &&
        ['VS-0','VS-1','VS-2'].includes(vs.state)) {
      const fisher = s.fisher;
      const hh = s.hunt_hess;
      const highRisk = (fisher == 3 || fisher == 4 || fisher === '3' || fisher === '4' ||
                        ['III','IV','V'].includes(hh));
      if (highRisk) {
        items.push('Trajetória estabilizada dentro da janela vasoespástica em paciente de alto risco favorece manutenção da vigilância serial regular, sem que o achado isolado adicione, neste momento, urgência incremental de complementação por imagem.');
      }
    }

    // P5 — Coerência: aceleração focal concordante com déficit clínico amplifica
    // a relevância prognóstica, mesmo abaixo do limiar absoluto de Δ24h.
    if (traj.trajectory === 'tendencia_ascendente' && h.dominantSide && s.deficit_side) {
      const expectedSide = s.deficit_side === 'esquerdo' ? 'direita' : (s.deficit_side === 'direito' ? 'esquerda' : null);
      if (expectedSide === h.dominantSide && ctx === 'HSA') {
        items.push('Aceleração focal concordante com o território do déficit clínico amplifica a relevância prognóstica em contexto de HSA, mesmo quando os valores absolutos de Δ24h não atingem o patamar de aceleração sustentada — a coerência topográfica adiciona valor pré-teste à vigilância seriada.');
      }
    }

    if (items.length === 0) {
      items.push('Sem fatores prognósticos hemodinâmicos destacáveis nesta avaliação. Manutenção de vigilância conforme contexto clínico.');
    }

    return items;
  }

  // ============================================================
  // CAMADA 6 — Confiança multidimensional
  // ============================================================
  function buildMultidimensionalConfidence(s, h, traj) {
    function level(score) {
      if (score >= 2) return 'ALTA';
      if (score >= 0) return 'MODERADA';
      return 'BAIXA';
    }

    // Confiança técnica (janela, ângulo, qualidade)
    let tech = 0;
    const techFactors = [];
    if (s.transtemporal_right === 'patente' && s.transtemporal_left === 'patente') {
      tech += 2; techFactors.push('janela transtemporal bilateral patente');
    } else if (s.transtemporal_right !== 'ausente' && s.transtemporal_left !== 'ausente') {
      tech += 1; techFactors.push('janela transtemporal acessível ao menos unilateralmente');
    } else {
      tech -= 2; techFactors.push('janela transtemporal ausente bilateralmente (limitação primária)');
    }
    if (s.transtemporal_right === 'limitada' || s.transtemporal_left === 'limitada') {
      tech -= 1; techFactors.push('janela limitada em ao menos um lado');
    }

    // Confiança anatômica (cobertura territorial, LR computável)
    let anat = 0;
    const anatFactors = [];
    if (present(h.lrD) && present(h.lrE)) {
      anat += 2; anatFactors.push('Lindegaard bilateralmente computável');
    } else if (present(h.lrD) || present(h.lrE)) {
      anat += 0; anatFactors.push('Lindegaard unilateral');
    } else if (h.mcaElev) {
      anat -= 2; anatFactors.push('Lindegaard não computável com velocidades elevadas — diferencial hiperemia/vasoespasmo prejudicado (BC-05)');
    } else {
      anat += 0; anatFactors.push('Lindegaard não calculado, sem repercussão na interpretação atual');
    }
    if (s.suboccipital === 'acessível') { anat += 1; anatFactors.push('circulação posterior avaliável'); }
    else if (s.suboccipital === 'ausente') { anat -= 1; anatFactors.push('circulação posterior não avaliável'); }

    // Confiança temporal (comparação serial, técnica reprodutível)
    let temp = 0;
    const tempFactors = [];
    if (traj.hasPrevious && traj.techniqueIdentical) {
      temp += 2; tempFactors.push('comparação serial com técnica reprodutível');
    } else if (traj.hasPrevious && !traj.techniqueIdentical) {
      temp -= 1; tempFactors.push('comparação serial com técnica não-reprodutível (BC-08)');
    } else {
      temp -= 1; tempFactors.push('sem comparação serial — interpretação restrita à fotografia única');
    }

    // Confiança fisiológica (variáveis sistêmicas, contexto clínico)
    let phys = 0;
    const physFactors = [];
    const sysVarsCount = [s.PaCO2_mmHg, s.hematocrit_pct, s.sedation, s.head_elevation_degrees].filter(x => present(x) || (x && x.length)).length;
    if (sysVarsCount >= 3) { phys += 1; physFactors.push('variáveis sistêmicas suficientes para contextualização'); }
    else if (sysVarsCount >= 1) { phys += 0; physFactors.push('variáveis sistêmicas parciais'); }
    else { phys -= 1; physFactors.push('variáveis sistêmicas ausentes — sem ancoragem fisiológica'); }
    if (s.clinical_status && s.clinical_status !== 'estavel') {
      phys += 1; physFactors.push('contexto clínico ativo informado (' + s.clinical_status + ')');
    }
    if (s.deficit_side && s.deficit_side !== 'nenhum' && s.deficit_side !== 'indefinido') {
      phys += 1; physFactors.push('lateralidade do déficit informada — permite análise topográfica');
    }

    // P5 — Coerência cruzada Vm/IP/LR: ajustes finos (sem alterar limites de level())
    // Bônus: assinatura interna coerente (Vm severa + LR concordante + IP coerente)
    if (h.mcaSevere && h.lrConcordance && /^concord/i.test(h.lrConcordance) &&
        present(h.ipMean) && h.ipMean <= 1.2) {
      phys += 1;
      physFactors.push('coerência interna Vm/LR/IP — assinatura proximal consistente');
    }
    // Penalização: dissociações conhecidas (Vm severa mas LR discordante)
    if (h.mcaSevere && h.lrConcordance && /discord/i.test(h.lrConcordance)) {
      phys -= 1;
      physFactors.push('dissociação Vm/LR observada — interpretação probabilística com diferencial pendente');
    }
    // Penalização: hiperemia sistêmica simultânea a IP modulado em paciente sem contexto crítico
    if (h.hyperemiaContext.length >= 2 && h.ipContext.length >= 1 &&
        (!s.clinical_status || s.clinical_status === 'estavel')) {
      phys -= 1;
      physFactors.push('múltiplos moduladores sistêmicos concomitantes — diferencial vasoespasmo × hiperemia limitado por ruído sistêmico');
    }

    return {
      tecnica: { level: level(tech), score: tech, factors: techFactors },
      anatomica: { level: level(anat), score: anat, factors: anatFactors },
      temporal: { level: level(temp), score: temp, factors: tempFactors },
      fisiologica: { level: level(phys), score: phys, factors: physFactors },
    };
  }

  // ============================================================
  // CAMADA 7 — Conclusão em 4 camadas
  // ============================================================
  function buildFourLayerConclusion(s, h, signature, ctxLines, traj, physDisc, prognostic) {
    const vs = (s.states && s.states.vasospasm_axis) || {};
    const pr = (s.states && s.states.pressure_axis) || {};

    // CAMADA 1 — DESCRITIVA
    const descritiva = describeFindings(s, h);

    // CAMADA 2 — FISIOPATOLÓGICA
    const fisiopatologica = describePhysiopathology(s, h, signature, traj);

    // CAMADA 3 — PROGNÓSTICA
    const prognostica = prognostic.length
      ? prognostic.join(' ')
      : 'Nesta sessão, não emergem fatores prognósticos hemodinâmicos destacáveis.';

    // CAMADA 4 — OPERACIONAL
    const operacional = describeOperational(s, h, traj, vs, pr);

    return { descritiva, fisiopatologica, prognostica, operacional };
  }

  function describeFindings(s, h) {
    const parts = [];
    if (present(h.mcaR) || present(h.mcaL)) {
      const ant = [];
      if (present(h.mcaR)) ant.push('ACM direita Vm ' + fmt(h.mcaR, 0) + ' cm/s' + (present(h.ipR) ? ' (IP ' + fmt(h.ipR, 2) + ')' : ''));
      if (present(h.mcaL)) ant.push('ACM esquerda Vm ' + fmt(h.mcaL, 0) + ' cm/s' + (present(h.ipL) ? ' (IP ' + fmt(h.ipL, 2) + ')' : ''));
      let antLine = 'Na circulação anterior, ' + ant.join('; ') + '.';
      if (present(h.lrD) || present(h.lrE)) {
        const lr = [];
        if (present(h.lrD)) lr.push('LR D ' + fmt(h.lrD, 2));
        if (present(h.lrE)) lr.push('LR E ' + fmt(h.lrE, 2));
        antLine += ' Razão de Lindegaard ' + lr.join(', ') + '.';
      } else if (h.mcaElev) {
        antLine += ' A razão de Lindegaard não pôde ser computada nesta sessão.';
      }
      parts.push(antLine);
    }

    const post = [];
    if (present(h.bas)) post.push('basilar Vm ' + fmt(h.bas, 0));
    if (present(h.vertR)) post.push('vertebral direita ' + fmt(h.vertR, 0));
    if (present(h.vertL)) post.push('vertebral esquerda ' + fmt(h.vertL, 0));
    if (post.length) parts.push('Em circulação posterior, ' + post.join('; ') + ' cm/s.');

    if (h.asymmetryPct !== null && h.asymmetryPct > 30) {
      parts.push('A assimetria hemisférica anterior alcança ' + h.asymmetryPct.toFixed(0) + '%' + (h.dominantSide ? ', com predomínio à ' + h.dominantSide : '') + '.');
    }

    if (parts.length === 0) parts.push('A avaliação não dispõe de dados quantitativos suficientes para descrição detalhada nesta sessão.');

    return parts.join(' ');
  }

  function describePhysiopathology(s, h, signature, traj) {
    const vs = (s.states && s.states.vasospasm_axis) || {};
    const pr = (s.states && s.states.pressure_axis) || {};
    const parts = [];

    // Eixo vasoespasmo / hiperemia — voz médica humanizada, mesmos gates
    if (vs.state === 'VS-0' && pr.state === 'PR-0') {
      parts.push('O perfil hemodinâmico se mostra estável, sem assinatura de vasoespasmo proximal, de hiperemia patológica ou de aumento da resistência distal; a coerência horizontal e territorial permanece preservada.');
    } else if (vs.state === 'VS-3') {
      parts.push('O conjunto velocimétrico recai sobre vasoespasmo proximal grave em circulação anterior' + (h.dominantSide ? ', com predomínio à ' + h.dominantSide : '') + ', acompanhado de razão de Lindegaard ' + (h.lrConcordance || 'não computável') + '.');
    } else if (vs.state === 'VS-2_with_discordance') {
      parts.push('Há dissociação relevante entre as velocidades em território de gravidade e a razão de Lindegaard, situação em que o diferencial entre vasoespasmo proximal mascarado por vasoconstrição do sifão (BC-05), hiperemia regional intensa e variável técnica é apropriado.');
    } else if (vs.state === 'VS-2') {
      parts.push('Os achados se alinham a vasoespasmo proximal leve a moderado, com razão de Lindegaard concordante em faixa intermediária.');
    } else if (vs.state === 'VS-1' && h.hyperemiaContext.length) {
      parts.push('O perfil aponta para hiperemia em contexto sistêmico favorável (' + h.hyperemiaContext.join(' + ') + '), leitura que precede a inferência vasoespástica primária e demanda normalização das variáveis sistêmicas antes de reavaliação.');
    }

    // Eixo de pressão / resistência distal — alternar conectores e cadência
    if (pr.state === 'PR-1' && h.ipContext.length === 0 && vs.state !== 'VS-3') {
      parts.push('A pulsatilidade sustentadamente elevada favorece aumento da resistência distal; o diferencial inclui edema, hipertensão intracraniana, vasoconstrição farmacológica e vasoespasmo a jusante.');
    } else if (pr.state === 'PR-1' && h.ipContext.length) {
      parts.push('A elevação da pulsatilidade está contextualizada por ' + h.ipContext.join(' e ') + ', o que recomenda cautela antes de inferir aumento isolado da resistência distal.');
    } else if (pr.state === 'PR-2') {
      parts.push('A perfusão apenas sistólica caracteriza uma assinatura que recai sobre elevação crítica da resistência distal.');
    } else if (pr.state === 'PR-3') {
      parts.push('A inversão diastólica desenha progressão hemodinâmica em direção à falência de perfusão.');
    } else if (['PR-4','PR-5','PR-6'].includes(pr.state)) {
      parts.push('O padrão espectral é sugestivo de parada circulatória cerebral em evolução.');
    }

    if (h.posteriorSloanFlag) {
      parts.push('Em circulação posterior, ' + h.posteriorSloanFlag + '.');
    }

    // Trajetória temporal — variar verbos e abrir/fechar a sentença
    if (traj.trajectory === 'aceleracao_significativa') {
      parts.push('No plano temporal, a aceleração ultrapassa a variabilidade esperada e favorece progressão hemodinâmica relevante em detrimento de modulação sistêmica transiente.');
    } else if (traj.trajectory === 'tendencia_ascendente') {
      parts.push('A trajetória, em ascensão, merece valoração específica em série subsequente.');
    } else if (traj.trajectory === 'desaceleracao') {
      parts.push('A trajetória em desaceleração é coerente com resposta terapêutica favorável ou com normalização de variável sistêmica modulatória.');
    }

    if (parts.length === 0) parts.push('Nesta avaliação, não emerge assinatura hemodinâmica destacável.');

    return parts.join(' ');
  }

  function describeOperational(s, h, traj, vs, pr) {
    const items = [];

    if (vs.state === 'VS-3' || pr.state === 'PR-2' || pr.state === 'PR-3') {
      items.push('Cabe comunicação imediata à equipe assistencial — intensivista e neurocirurgia — com correlação clínica integrada antes de qualquer decisão terapêutica.');
      items.push('A reavaliação em janela curta, com técnica reprodutível, segue a pactuação institucional (BC-09).');
      items.push('Recomenda-se considerar neuroimagem vascular complementar, uma vez que a caracterização anatômica fina escapa ao escopo do método (BC-04).');
    } else if (vs.state === 'VS-2' || vs.state === 'VS-2_with_discordance' || pr.state === 'PR-1') {
      items.push('Sugere-se comunicação ao intensivista no plantão e intensificação da cadência seriada, mantida a técnica idêntica (BC-08).');
      items.push('A correlação seriada com variáveis sistêmicas — PaCO₂, sedação, hematócrito, PAM — permanece essencial.');
    } else if (vs.state === 'VS-INDETERMINATE' && s.context === 'HSA') {
      const fisher = s.fisher;
      const hh = s.hunt_hess;
      const highRisk = (fisher == 3 || fisher == 4 || fisher === '3' || fisher === '4' ||
                        ['III','IV','V'].includes(hh));
      if (highRisk) {
        items.push('Em paciente de alto risco, a limitação anatômica de janela não se resolve por repetição e indica complementação por neuroimagem vascular (angio-TC/RM).');
      } else {
        items.push('Vale considerar a repetição com troubleshooting documentado; a neuroimagem vascular complementar fica reservada para o cenário em que a vulnerabilidade clínica aumentar.');
      }
    } else if (s.context === 'HSA') {
      items.push('A vigilância serial deve seguir o protocolo institucional, preservada a técnica reprodutível para comparabilidade.');
    }

    if (['PR-4','PR-5','PR-6'].includes(pr.state)) {
      items.push('A integração ao protocolo CFM 2.173/2017 só se aplica se já ativado pela equipe assistencial responsável; o achado isolado não autoriza inferência diagnóstica de morte encefálica.');
    }

    if (traj.flags.includes('aceleração sustentada ACM direita — peso prognóstico independente (BC-08)') ||
        traj.flags.includes('aceleração sustentada ACM esquerda — peso prognóstico independente (BC-08)')) {
      items.push('Reforça-se a comunicação à equipe assistencial: aceleração superior a 50 cm/s em 24h adiciona peso prognóstico independente.');
    }

    if (h.posteriorSloanFlag) {
      items.push('Vale também considerar avaliação dirigida da circulação posterior por imagem complementar.');
    }

    if (items.length === 0) items.push('A cadência deve seguir o contexto clínico e o protocolo institucional vigente.');

    return items.join(' ');
  }

  // ============================================================
  // Compor narrativa final (formato de laudo médico)
  // ============================================================
  function buildNarrative(s, h, sig, ctxLines, traj, physDisc, prognostic, multiConf, conclusion) {
    const sep = '═══════════════════════════════════════════════════════════════════';
    const dash = '───────────────────────────────────────────────────────────────────';
    const lines = [];

    lines.push(sep);
    lines.push('INTERPRETAÇÃO HEMODINÂMICA NEUROVASCULAR — DOPPLER TRANSCRANIANO');
    if (s.context === 'HSA') lines.push('Hemorragia subaracnóidea — vigilância vasoespástica');
    else if (s.context === 'TCE' || s.context === 'HIC_suspeita') lines.push('Neurocrítico — eixo de pressão');
    else if (s.context === 'protocolo_ME_ativo') lines.push('Avaliação ancilar — protocolo CFM 2.173/2017');
    else if (s.context === 'AVC_suspeita_oclusao') lines.push('Suspeita de oclusão arterial aguda');
    else if (s.context === 'pos_op_neurovascular') lines.push('Pós-operatório neurovascular');
    lines.push(sep);
    lines.push('');

    // -- Assinatura hemodinâmica --
    lines.push('▌ ASSINATURA HEMODINÂMICA');
    lines.push(sig);
    lines.push('');

    // -- Contextualização por doença --
    if (ctxLines && ctxLines.length) {
      lines.push('▌ CONTEXTUALIZAÇÃO CLÍNICA');
      ctxLines.forEach(l => lines.push(l));
      lines.push('');
    }

    // -- Trajetória temporal --
    lines.push('▌ TRAJETÓRIA TEMPORAL');
    lines.push(traj.narrative || '—');
    if (traj.flags && traj.flags.length) {
      lines.push('Sinalizações temporais: ' + traj.flags.join(' · '));
    }
    lines.push('');

    // -- Discordâncias fisiológicas --
    if (physDisc && physDisc.length) {
      lines.push('▌ DISCORDÂNCIAS FISIOLÓGICAS');
      physDisc.forEach(d => lines.push('· ' + d.text));
      lines.push('');
    }

    // -- Conclusão em 4 camadas --
    lines.push('▌ SÍNTESE EM CAMADAS');
    lines.push('');
    lines.push('  [1] DESCRITIVA');
    lines.push('  ' + indent(conclusion.descritiva, '  '));
    lines.push('');
    lines.push('  [2] FISIOPATOLÓGICA');
    lines.push('  ' + indent(conclusion.fisiopatologica, '  '));
    lines.push('');
    lines.push('  [3] PROGNÓSTICA');
    lines.push('  ' + indent(conclusion.prognostica, '  '));
    lines.push('');
    lines.push('  [4] OPERACIONAL');
    lines.push('  ' + indent(conclusion.operacional, '  '));
    lines.push('');

    // -- Confiança interpretativa multidimensional --
    lines.push('▌ CONFIANÇA INTERPRETATIVA');
    lines.push('  Técnica: ' + multiConf.tecnica.level + '  ·  Anatômica: ' + multiConf.anatomica.level + '  ·  Temporal: ' + multiConf.temporal.level + '  ·  Fisiológica: ' + multiConf.fisiologica.level);
    lines.push('');
    lines.push('  · Técnica — ' + multiConf.tecnica.factors.join('; ') + '.');
    lines.push('  · Anatômica — ' + multiConf.anatomica.factors.join('; ') + '.');
    lines.push('  · Temporal — ' + multiConf.temporal.factors.join('; ') + '.');
    lines.push('  · Fisiológica — ' + multiConf.fisiologica.factors.join('; ') + '.');
    lines.push('');

    // -- Boundaries finais obrigatórios --
    lines.push(dash);
    lines.push('O Doppler transcraniano integra a avaliação hemodinâmica multimodal e não substitui o exame neurológico, a neuroimagem vascular ou estrutural, a monitorização invasiva nem a decisão clínica especializada.');
    lines.push('');
    lines.push('O método informa a dinâmica hemodinâmica; a interpretação clínica integrada e a conduta cabem à equipe assistencial.');
    lines.push(dash);
    lines.push('Laudo de apoio interpretativo — homologação médica é requerida antes de uso clínico.');

    return lines.join('\n');
  }

  function indent(text, prefix) {
    if (!text) return '';
    return text.split('\n').join('\n' + prefix);
  }

  // ============================================================
  // Compatibilidade: resumo executivo curto (mantido para a UI)
  // ============================================================
  function buildExecutiveSummary(s, h, signature) {
    return signature;
  }

  // ============================================================
  // ENTRY POINT
  // ============================================================
  function generate(snapshot) {
    if (!snapshot || typeof snapshot !== 'object') {
      return { error: 'dados insuficientes para interpretação' };
    }

    const h = analyzeHemodynamics(snapshot);
    const signature = buildHemodynamicSignature(snapshot, h);
    const ctxLines = contextualizeByDisease(snapshot, h);
    const traj = analyzeTemporalTrajectory(snapshot, h);
    const physDisc = detectPhysiologicDiscordances(snapshot, h, traj);
    const prognostic = buildPrognosticLayer(snapshot, h, traj);
    const multiConf = buildMultidimensionalConfidence(snapshot, h, traj);
    const conclusion = buildFourLayerConclusion(snapshot, h, signature, ctxLines, traj, physDisc, prognostic);
    const fullNarrative = buildNarrative(snapshot, h, signature, ctxLines, traj, physDisc, prognostic, multiConf, conclusion);

    // Confidence summary (compatibilidade com UI atual)
    const overallConfidence = overallConfidenceLabel(multiConf);

    return {
      // Novas camadas
      hemodynamicSignature: signature,
      diseaseContext: ctxLines,
      temporalTrajectory: traj,
      physiologicDiscordances: physDisc.map(d => d.text),
      prognosticLayer: prognostic,
      multidimensionalConfidence: multiConf,
      fourLayerConclusion: conclusion,

      // Saída principal
      fullNarrative: fullNarrative,

      // Compatibilidade com a UI atual (manter shape antigo onde apropriado)
      executiveSummary: signature,
      hemodynamicSynthesis: conclusion.descritiva,
      temporalComparison: traj.narrative,
      clinicalSignificance: ctxLines.concat(prognostic),
      hemodynamicAlerts: physDisc.map(d => d.text),
      confidenceLevel: overallConfidence,
      confidenceScore: multiConf.tecnica.score + multiConf.anatomica.score + multiConf.temporal.score + multiConf.fisiologica.score,
      confidenceFactors: [
        'Técnica ' + multiConf.tecnica.level + ': ' + multiConf.tecnica.factors.join('; '),
        'Anatômica ' + multiConf.anatomica.level + ': ' + multiConf.anatomica.factors.join('; '),
        'Temporal ' + multiConf.temporal.level + ': ' + multiConf.temporal.factors.join('; '),
        'Fisiológica ' + multiConf.fisiologica.level + ': ' + multiConf.fisiologica.factors.join('; '),
      ],

      _internal: { h, traj, multiConf },
    };
  }

  function overallConfidenceLabel(multiConf) {
    const order = { 'BAIXA': 0, 'MODERADA': 1, 'ALTA': 2 };
    const all = [multiConf.tecnica.level, multiConf.anatomica.level, multiConf.temporal.level, multiConf.fisiologica.level];
    const min = all.reduce((m, l) => order[l] < order[m] ? l : m, 'ALTA');
    // overall = mínima entre as 4 dimensões
    return min === 'ALTA' ? 'ALTO' : (min === 'MODERADA' ? 'MODERADO' : 'BAIXO');
  }

  // Expose
  global.DTCInterpretive = {
    generate: generate,
    version: '0.5-physiologic-coherence',
  };

})(typeof window !== 'undefined' ? window : globalThis);
