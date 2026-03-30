"""routes — blueprints Flask do NEUROAUTH v2.0.0."""
from .motor_routes import motor_bp
from .decision_routes import decision_bp

__all__ = ["motor_bp", "decision_bp"]
