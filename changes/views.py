import os
import json
from flask import request, current_app, Blueprint, render_template
from changes.database import session as db_session

views = Blueprint('views', __name__)

@api.route('/')
def index():
    return render_template('index.html')

