"""
app/routers/cockpit.py
GET /cockpit — painel operacional HTML (MVP).
Serve HTML inline que consome /metrics e /audit/query via JS.
Requer JWT válido no query param ?token=...
"""

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter()

COCKPIT_HTML = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>NEUROAUTH Cockpit</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Segoe UI',system-ui,sans-serif;background:#0f172a;color:#e2e8f0;min-height:100vh}
.header{background:linear-gradient(135deg,#1e3a5f,#2563eb);padding:16px 24px;display:flex;align-items:center;justify-content:space-between}
.header h1{font-size:20px;font-weight:700;color:#fff}
.header .ts{font-size:12px;color:#93c5fd}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;padding:16px 24px}
.card{background:#1e293b;border-radius:10px;padding:16px;text-align:center;border:1px solid #334155}
.card .num{font-size:32px;font-weight:800;color:#60a5fa}
.card .lbl{font-size:11px;color:#94a3b8;text-transform:uppercase;margin-top:4px}
.card.go .num{color:#4ade80}
.card.ressalva .num{color:#fbbf24}
.card.nogo .num{color:#f87171}
.card.risco .num{color:#fb923c}
.section{padding:8px 24px}
.section h2{font-size:15px;font-weight:600;color:#94a3b8;margin-bottom:8px;text-transform:uppercase;letter-spacing:1px}
table{width:100%;border-collapse:collapse;font-size:13px}
th{text-align:left;padding:8px 10px;color:#64748b;border-bottom:1px solid #334155;font-weight:600}
td{padding:7px 10px;border-bottom:1px solid #1e293b}
tr:hover{background:#1e293b}
.badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:700}
.badge-go{background:#166534;color:#4ade80}
.badge-res{background:#713f12;color:#fbbf24}
.badge-no{background:#7f1d1d;color:#f87171}
.audit-box{background:#1e293b;border-radius:10px;padding:16px;margin:0 24px 16px;border:1px solid #334155}
.audit-box input{background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:8px 12px;border-radius:6px;width:260px;margin-right:8px}
.audit-box button{background:#2563eb;color:#fff;border:none;padding:8px 16px;border-radius:6px;cursor:pointer;font-weight:600}
.audit-box button:hover{background:#1d4ed8}
#audit-result{margin-top:12px;font-size:13px;white-space:pre-wrap;max-height:300px;overflow:auto}
.loading{text-align:center;padding:40px;color:#64748b}
#error-bar{display:none;background:#7f1d1d;color:#fca5a5;padding:10px 24px;font-size:13px}
</style>
</head>
<body>
<div class="header">
  <h1>NEUROAUTH Cockpit</h1>
  <span class="ts" id="gen-at">carregando...</span>
</div>
<div id="error-bar"></div>
<div class="grid" id="summary-grid">
  <div class="card"><div class="num" id="s-total">-</div><div class="lbl">Total casos</div></div>
  <div class="card go"><div class="num" id="s-go">-</div><div class="lbl">GO</div></div>
  <div class="card ressalva"><div class="num" id="s-res">-</div><div class="lbl">Ressalvas</div></div>
  <div class="card nogo"><div class="num" id="s-nogo">-</div><div class="lbl">NO-GO</div></div>
  <div class="card risco"><div class="num" id="s-risco">-</div><div class="lbl">Risco Alto+</div></div>
</div>

<div class="section">
  <h2>Audit — Reconstruir Caso</h2>
</div>
<div class="audit-box">
  <input id="audit-input" placeholder="DR-XXXXXXXX ou EP-XXXXXXXX-YYYYYY">
  <button onclick="runAudit()">Buscar</button>
  <div id="audit-result"></div>
</div>

<div class="section">
  <h2>Casos Recentes</h2>
</div>
<div style="padding:0 24px 24px;overflow-x:auto">
  <table>
    <thead><tr>
      <th>Run ID</th><th>Episodio</th><th>Classificacao</th><th>Score</th><th>Risco</th><th>Status</th><th>Data</th>
    </tr></thead>
    <tbody id="cases-tbody"><tr><td colspan="7" class="loading">Carregando...</td></tr></tbody>
  </table>
</div>

<script>
const BASE = window.location.origin;
let JWT = null;

function getJwt() {
  if (JWT) return JWT;
  const p = new URLSearchParams(window.location.search);
  JWT = p.get('token');
  if (!JWT) {
    try { const s = sessionStorage.getItem('na_jwt_tmp'); if(s){JWT=JSON.parse(s).jwt;} } catch(e){}
  }
  return JWT;
}

function authHeaders() {
  const t = getJwt();
  return t ? {'Authorization':'Bearer '+t,'Content-Type':'application/json'} : {'Content-Type':'application/json'};
}

function badge(cls) {
  if (cls==='GO') return '<span class="badge badge-go">GO</span>';
  if (cls==='GO_COM_RESSALVAS') return '<span class="badge badge-res">RESSALVA</span>';
  return '<span class="badge badge-no">NO-GO</span>';
}

async function loadMetrics() {
  try {
    const r = await fetch(BASE+'/metrics', {headers:authHeaders()});
    if (!r.ok) throw new Error('HTTP '+r.status);
    const d = await r.json();
    document.getElementById('gen-at').textContent = 'Atualizado: '+new Date(d.generated_at).toLocaleString('pt-BR');
    document.getElementById('s-total').textContent = d.summary.total;
    document.getElementById('s-go').textContent = d.summary.go;
    document.getElementById('s-res').textContent = d.summary.go_com_ressalvas;
    document.getElementById('s-nogo').textContent = d.summary.no_go;
    document.getElementById('s-risco').textContent = d.summary.risco_alto + d.summary.risco_critico;
    const tb = document.getElementById('cases-tbody');
    if (!d.recent_cases.length) { tb.innerHTML='<tr><td colspan="7">Nenhum caso</td></tr>'; return; }
    tb.innerHTML = d.recent_cases.map(c => `<tr>
      <td>${c.decision_run_id}</td>
      <td>${c.episode_id}</td>
      <td>${badge(c.classification)}</td>
      <td>${c.score!=null?c.score:'-'}</td>
      <td>${c.risk_level||'-'}</td>
      <td>${c.decision_status}</td>
      <td>${c.updated_at?new Date(c.updated_at).toLocaleString('pt-BR'):'-'}</td>
    </tr>`).join('');
  } catch(e) {
    const eb = document.getElementById('error-bar');
    eb.style.display='block';
    eb.textContent='Erro ao carregar metricas: '+e.message;
  }
}

async function runAudit() {
  const v = document.getElementById('audit-input').value.trim();
  const out = document.getElementById('audit-result');
  if (!v) { out.textContent='Informe um ID.'; return; }
  out.textContent='Buscando...';
  const param = v.startsWith('EP-') ? 'episodio_id='+v : 'decision_run_id='+v;
  try {
    const r = await fetch(BASE+'/audit/query?'+param, {headers:authHeaders()});
    const d = await r.json();
    out.textContent = JSON.stringify(d, null, 2);
  } catch(e) {
    out.textContent = 'Erro: '+e.message;
  }
}

loadMetrics();
</script>
</body>
</html>"""


@router.get("", response_class=HTMLResponse)
async def cockpit():
    """Cockpit operacional — HTML com JS que consome /metrics e /audit."""
    return HTMLResponse(content=COCKPIT_HTML)
