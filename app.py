"""
NEUROAUTH — Aplicação Flask
Versão: 2.0.0

Ponto de entrada. Registra blueprints, configura logging.
Não contém lógica de negócio.
"""
from __future__ import annotations

import logging
import sys
import traceback
from flask import Flask, jsonify

from routes import motor_bp, decision_bp


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

    @app.get("/health")
    def health():
        from flask import jsonify
        from motor.decision_classifier import ENGINE_VERSION
        return jsonify({"status": "ok", "engine_version": ENGINE_VERSION}), 200


    # ── Global JSON error handler ──────────────────────────────────────
    @app.errorhandler(Exception)
    def handle_unhandled_exception(e: Exception):
        logging.getLogger("neuroauth.app").error(
            "handle_unhandled_exception: %s\n%s", e, traceback.format_exc()
        )
        return jsonify({
            "decision_status": "ERRO_INTERNO",
            "message":         "Erro interno inesperado no servidor.",
            "error_code":      "SYS_GLOBAL_ERROR",
        }), 500

    return app


# Exportar instância para gunicorn (Render / WSGI)
# Dois aliases: 'application' (Procfile) e 'app' (Render Start Command legado)
application = create_app()
app = application  # backward compat: gunicorn app:app

if __name__ == "__main__":
    application.run(debug=True, port=5099)
