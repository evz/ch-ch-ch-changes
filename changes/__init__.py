import os
from os.path import join, dirname, abspath
import json
from flask import Flask
from changes.api import api
from changes.views import views


def create_app():
    app = Flask(__name__)
    
    app.url_map.strict_slashes = False
    
    app.register_blueprint(api)
    app.register_blueprint(views)
    
    return app
