"""
NEUROAUTH — Aplicação Flask
Versão: 2.0.0

Ponto de entrada. Registra blueprints, configura logging.
Não contém lógica de negócio.
"""
from __future__ import annotations

import logging
import sys
from flask import Flask

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

    return app


if __name__ == "__main__":
    application = create_app()
    application.run(debug=True, port=5099)
