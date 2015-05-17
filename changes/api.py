import os
import json
from flask import request, current_app, Blueprint, make_response
from changes.database import session as db_session

api = Blueprint('api', __name__, url_prefix='/api')

@api.route('/')
def listing():
    response = make_response(json.dumps([{},{}]))
    response.headers['Content-Type'] = 'application/json'
    return response

