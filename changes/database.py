import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from changes.app_config import DB_CONN

engine = create_engine(DB_CONN, convert_unicode=True)

db_session = scoped_session(sessionmaker(bind=engine,
                                      autocommit=False,
                                      autoflush=False))
