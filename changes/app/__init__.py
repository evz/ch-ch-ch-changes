import os
from os.path import join, dirname, abspath
import json

import click
from flask import Flask

from app_config import Config
from app.extensions import db


def create_app():
    app = Flask(__name__, instance_relative_config=True)
    
    app.config.from_object(Config)
    app.url_map.strict_slashes = False

    db.init_app(app)
    
    from app.api import api
    from app.views import views

    app.register_blueprint(api)
    app.register_blueprint(views)
    
    from app.etl import ETL
    
    @app.cli.command('run-etl')
    @click.option('--file-date', type=click.DateTime())
    @click.option('--storage-dir', type=click.Path())
    def run_etl(file_date, storage_dir):
        if not storage_dir:
            storage_dir = ''
        
        if file_date:
            etl = ETL(storage_dir, file_date=file_date)
        else:
            etl = ETL(storage_dir)
        
        etl.run()

    return app
