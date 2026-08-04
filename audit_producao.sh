#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# NEUROAUTH — AUDITORIA DE PRODUÇÃO (21/abril/2026)
# Executar no Mac: bash ~/neuroauth/backend/audit_producao.sh
# ═══════════════════════════════════════════════════════════════

BASE="https://neuroauth-auth.onrender.com"

echo "╔═══════════════════════════════════════════════════╗"
echo "║  NEUROAUTH — AUDITORIA DE PRODUÇÃO               ║"
echo "║  $(date '+%Y-%m-%d %H:%M:%S')                    ║"
echo "╚═══════════════════════════════════════════════════╝"
echo ""

# ══ FASE 1: HEALTHCHECKS ══════════════════════════════════════
echo "━━━ FASE 1: HEALTHCHECKS ━━━"
echo ""

echo "► /health (v1)"
H1=$(curl -s -w "\n%{http_code}|%{time_total}" "$BASE/health?diag=true")
H1_CODE=$(echo "$H1" | tail -1 | cut -d'|' -f1)
H1_TIME=$(echo "$H1" | tail -1 | cut -d'|' -f2)
H1_BODY=$(echo "$H1" | sed '$d')
echo "  HTTP: $H1_CODE (${H1_TIME}s)"
echo "$H1_BODY" | python3 -m json.tool 2>/dev/null || echo "  $H1_BODY"
echo ""

echo "► /v2/health (v2.3.1)"
H2=$(curl -s -w "\n%{http_code}|%{time_total}" "$BASE/v2/health")
H2_CODE=$(echo "$H2" | tail -1 | cut -d'|' -f1)
H2_TIME=$(echo "$H2" | tail -1 | cut -d'|' -f2)
H2_BODY=$(echo "$H2" | sed '$d')
echo "  HTTP: $H2_CODE (${H2_TIME}s)"
echo "$H2_BODY" | python3 -m json.tool 2>/dev/null || echo "  $H2_BODY"

# Extrair rules_loaded
RULES=$(echo "$H2_BODY" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('rules_loaded',0))" 2>/dev/null)
if [ "$RULES" -ge 29 ] 2>/dev/null; then
  echo "  ✅ rules_loaded=$RULES (esperado >=29)"
else
  echo "  ⚠️  rules_loaded=$RULES (esperado >=29)"
fi
echo ""

# ══ FASE 2: AUTH GATE ═════════════════════════════════════════
echo "━━━ FASE 2: AUTH GATE (sem JWT) ━━━"
echo ""

echo "► POST /decide SEM JWT"
D1=$(curl -s -w "\n%{http_code}" -X POST "$BASE/decide" \
  -H "Content-Type: application/json" \
  -d '{"convenio":"Unimed","procedimento":"teste","cid_principal":"M51"}')
D1_CODE=$(echo "$D1" | tail -1)
D1_BODY=$(echo "$D1" | sed '$d')
echo "  HTTP: $D1_CODE"
if [ "$D1_CODE" = "401" ] || [ "$D1_CODE" = "403" ] || [ "$D1_CODE" = "422" ]; then
  echo "  ✅ Gate A bloqueando corretamente"
else
  echo "  ⚠️  Esperava 401/403/422, recebeu $D1_CODE"
fi
echo ""

echo "► POST /v2/decide SEM JWT"
D2=$(curl -s -w "\n%{http_code}" -X POST "$BASE/v2/decide" \
  -H "Content-Type: application/json" \
  -d '{"convenio":"Unimed","procedimento":"teste","cid_principal":"M51"}')
D2_CODE=$(echo "$D2" | tail -1)
echo "  HTTP: $D2_CODE"
if [ "$D2_CODE" = "401" ] || [ "$D2_CODE" = "403" ] || [ "$D2_CODE" = "422" ]; then
  echo "  ✅ Gate A v2 bloqueando corretamente"
else
  echo "  ⚠️  Esperava 401/403/422, recebeu $D2_CODE"
fi
echo ""

echo "► GET /relay/profile SEM JWT"
R1=$(curl -s -w "\n%{http_code}" "$BASE/relay/profile?email=test@test.com")
R1_CODE=$(echo "$R1" | tail -1)
echo "  HTTP: $R1_CODE"
if [ "$R1_CODE" = "401" ] || [ "$R1_CODE" = "403" ]; then
  echo "  ✅ Relay Gate A bloqueando"
else
  echo "  ⚠️  Esperava 401/403, recebeu $R1_CODE"
fi
echo ""

# ══ FASE 3: CORS ═════════════════════════════════════════════
echo "━━━ FASE 3: CORS PREFLIGHT ━━━"
echo ""

CORS=$(curl -s -i -X OPTIONS "$BASE/decide" \
  -H "Origin: https://neuroauth.com.br" \
  -H "Access-Control-Request-Method: POST" \
  -H "Access-Control-Request-Headers: authorization,content-type" 2>&1)
CORS_ORIGIN=$(echo "$CORS" | grep -i "access-control-allow-origin" | head -1)
CORS_HEADERS=$(echo "$CORS" | grep -i "access-control-allow-headers" | head -1)
echo "  $CORS_ORIGIN"
echo "  $CORS_HEADERS"
if echo "$CORS_ORIGIN" | grep -qi "neuroauth.com.br"; then
  echo "  ✅ CORS OK para neuroauth.com.br"
else
  echo "  ⚠️  CORS pode não estar configurado corretamente"
fi
echo ""

# ══ FASE 4: GIT STATUS ══════════════════════════════════════
echo "━━━ FASE 4: GIT STATUS ━━━"
echo ""
cd ~/neuroauth/backend 2>/dev/null
echo "► Último commit:"
git log --oneline -1
echo ""
echo "► Branch:"
git branch --show-current
echo ""
echo "► Dirty files:"
git status --short
echo ""

# ══ FASE 5: GITHUB TOKEN ════════════════════════════════════
echo "━━━ FASE 5: GITHUB TOKEN ━━━"
echo ""
# Verificar se push ainda funciona (dry-run)
git push --dry-run origin main 2>&1 | head -3
echo ""

# ══ RESUMO ═══════════════════════════════════════════════════
echo "╔═══════════════════════════════════════════════════╗"
echo "║  RESUMO DA AUDITORIA                             ║"
echo "╚═══════════════════════════════════════════════════╝"
echo ""
echo "  /health (v1):       HTTP $H1_CODE (${H1_TIME}s)"
echo "  /v2/health:         HTTP $H2_CODE (${H2_TIME}s) — $RULES regras"
echo "  /decide sem JWT:    HTTP $D1_CODE"
echo "  /v2/decide sem JWT: HTTP $D2_CODE"
echo "  /relay/profile:     HTTP $R1_CODE"
echo "  CORS:               $(echo "$CORS_ORIGIN" | grep -qi neuroauth && echo OK || echo CHECK)"
echo ""
echo "  Último commit: $(git log --oneline -1 2>/dev/null)"
echo ""
echo "═══════════════════════════════════════════════════"
echo "PRÓXIMO: Teste E2E no browser (neuroauth.com.br)"
echo "═══════════════════════════════════════════════════"
