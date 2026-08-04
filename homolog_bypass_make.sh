#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# HOMOLOGAÇÃO: Bypass Make.com — relay direto no Sheets
# Commit: a92a94a | Data: 2026-04-21
# ═══════════════════════════════════════════════════════════════

BASE="https://neuroauth-auth.onrender.com"
EMAIL="josejuniorsaraiva@gmail.com"

echo "╔═══════════════════════════════════════════╗"
echo "║  NEUROAUTH — HOMOLOGAÇÃO BYPASS MAKE      ║"
echo "╚═══════════════════════════════════════════╝"
echo ""

# ── TESTE 1: /health (sem auth) ──
echo "━━━ TESTE 1: /health?diag=true ━━━"
HEALTH=$(curl -s -w "\n%{http_code}" "$BASE/health?diag=true")
HEALTH_CODE=$(echo "$HEALTH" | tail -1)
HEALTH_BODY=$(echo "$HEALTH" | sed '$d')
echo "HTTP: $HEALTH_CODE"
echo "$HEALTH_BODY" | python3 -m json.tool 2>/dev/null || echo "$HEALTH_BODY"
echo ""

# ── TESTE 2: /v2/health (sem auth) ──
echo "━━━ TESTE 2: /v2/health ━━━"
V2H=$(curl -s -w "\n%{http_code}" "$BASE/v2/health")
V2H_CODE=$(echo "$V2H" | tail -1)
V2H_BODY=$(echo "$V2H" | sed '$d')
echo "HTTP: $V2H_CODE"
echo "$V2H_BODY" | python3 -m json.tool 2>/dev/null || echo "$V2H_BODY"
echo ""

# ── TESTE 3: /relay/profile SEM JWT (deve retornar 401/403) ──
echo "━━━ TESTE 3: /relay/profile SEM JWT ━━━"
RP=$(curl -s -w "\n%{http_code}" "$BASE/relay/profile?email=$EMAIL")
RP_CODE=$(echo "$RP" | tail -1)
RP_BODY=$(echo "$RP" | sed '$d')
echo "HTTP: $RP_CODE"
echo "$RP_BODY" | python3 -m json.tool 2>/dev/null || echo "$RP_BODY"
if [ "$RP_CODE" = "401" ] || [ "$RP_CODE" = "403" ]; then
  echo "✅ Gate A funcionando — auth exigida"
else
  echo "⚠️  Esperava 401/403, recebeu $RP_CODE"
fi
echo ""

# ── TESTE 4: /relay/notify SEM JWT (deve retornar 401/403) ──
echo "━━━ TESTE 4: /relay/notify SEM JWT ━━━"
RN=$(curl -s -w "\n%{http_code}" -X POST "$BASE/relay/notify" \
  -H "Content-Type: application/json" \
  -d '{"procedimento":"TESTE","convenio":"Unimed"}')
RN_CODE=$(echo "$RN" | tail -1)
RN_BODY=$(echo "$RN" | sed '$d')
echo "HTTP: $RN_CODE"
echo "$RN_BODY" | python3 -m json.tool 2>/dev/null || echo "$RN_BODY"
if [ "$RN_CODE" = "401" ] || [ "$RN_CODE" = "403" ]; then
  echo "✅ Gate A funcionando — auth exigida"
else
  echo "⚠️  Esperava 401/403, recebeu $RN_CODE"
fi
echo ""

# ── TESTE 5: CORS preflight /relay/notify ──
echo "━━━ TESTE 5: CORS preflight /relay/notify ━━━"
CORS=$(curl -s -i -X OPTIONS "$BASE/relay/notify" \
  -H "Origin: https://neuroauth.com.br" \
  -H "Access-Control-Request-Method: POST" \
  -H "Access-Control-Request-Headers: authorization,content-type" 2>&1)
echo "$CORS" | grep -i "access-control\|HTTP/"
echo ""

# ── RESUMO ──
echo "═══════════════════════════════════════"
echo "RESUMO DA HOMOLOGAÇÃO"
echo "═══════════════════════════════════════"
echo "  /health:          HTTP $HEALTH_CODE"
echo "  /v2/health:       HTTP $V2H_CODE"
echo "  /relay/profile:   HTTP $RP_CODE (sem JWT)"
echo "  /relay/notify:    HTTP $RN_CODE (sem JWT)"
echo ""
echo "PRÓXIMO: submeter caso real via neuroauth.com.br"
echo "Console deve mostrar: 'relay documental concluído'"
echo "E a aba 24_RELAY_SUBMISSIONS no Sheets deve ter nova linha"
echo "═══════════════════════════════════════"
