import os
import json
from flask import request, current_app, Blueprint, render_template
from changes.database import engine

views = Blueprint('views', __name__)

@views.route('/')
def index():
    return render_template('index.html')

