from flask import Blueprint

api = Blueprint("api", __name__, url_prefix="/api")

# Import routes to register them with the blueprint
from app.api import routes  # noqa: F401
