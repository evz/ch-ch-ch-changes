from flask import Blueprint

views = Blueprint("views", __name__)

# Import routes to register them with the blueprint
from app.views import routes  # noqa: F401, E402
