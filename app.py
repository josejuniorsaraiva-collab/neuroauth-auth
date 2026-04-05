"""
NEUROAUTH — Aplicação Flask

Versão: 2.1.0

Ponto de entrada. Registra blueprints, configura logging.

Não contém lógica de negócio.
"""

from __future__ import annotations

import logging
import sys
import traceback

from flask import Flask, jsonify, send_from_directory
from routes import motor_bp, decision_bp, episodios_bp


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["JSON_SORT_KEYS"] = False

    # Logging estruturado
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    # Registrar blueprints
    app.register_blueprint(motor_bp)
    app.register_blueprint(decision_bp)
    app.register_blueprint(episodios_bp)

    @app.get("/health")
    def health():
        from flask import jsonify
        from motor.decision_classifier import ENGINE_VERSION
        return jsonify({"status": "ok", "engine_version": ENGINE_VERSION}), 200

    @app.get("/form")
    def serve_form():
        """
        GET /form — Formulário oficial de entrada v2.
        Serve neuroauth_form_v2.html diretamente do diretório frontend/.
        URL pública: https://neuroauth-auth.onrender.com/form
        """
        import os
        frontend_dir = os.path.join(os.path.dirname(__file__), "frontend")
        return send_from_directory(frontend_dir, "neuroauth_form_v2.html")

    # ── GET /clinical/protocols ────────────────────────────────────────────────────────────────────────────
    @app.get("/clinical/protocols")
    def get_clinical_protocols():
        """
        GET /clinical/protocols
        Lista todos os protocolos clínicos ativos do seed local.
        Usado pelo frontend para popular o dropdown de procedimentos.
        Retorna: {"procedures": [...]}
        CORS habilitado.
        """
        from repositories.clinical_protocols import list_protocols
        resp = jsonify({"procedures": list_protocols()})
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Methods"] = "GET,OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
        return resp, 200

    @app.route("/clinical/protocols", methods=["OPTIONS"])
    def clinical_protocols_options():
        resp = jsonify({})
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Methods"] = "GET,OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
        return resp, 204

    # ── Global JSON error handler ────────────────────────────────────────────────────────────────────────────
    @app.errorhandler(Exception)
    def handle_unhandled_exception(e: Exception):
        logging.getLogger("neuroauth.app").error(
            "handle_unhandled_exception: %s\n%s", e, traceback.format_exc()
        )
        return jsonify({
            "decision_status": "ERRO_INTERNO",
            "message": "Erro interno inesperado no servidor.",
            "error_code": "SYS_GLOBAL_ERROR",
        }), 500

    return app


# Exportar instância para gunicorn (Render / WSGI)
# Dois aliases: 'application' (Procfile) e 'app' (Render Start Command legado)
application = create_app()
app = application # backward compat: gunicorm app:app

if __name__ == "__main__":
    application.run(debug=True, port=5099)
