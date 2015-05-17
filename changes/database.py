import os
from sqlalchemy import create_engine
from changes.app_config import DB_CONN

engine = create_engine(DB_CONN, convert_unicode=True)

