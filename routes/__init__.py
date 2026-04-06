"""routes - blueprints Flask do NEUROAUTH v2.1.0."""

from .motor_routes import motor_bp
from .decision_routes import decision_bp
from .episodios_routes import episodios_bp
from .gateway_routes import gateway_bp

__all__ = ["motor_bp", "decision_bp", "episodios_bp", "gateway_bp"]
