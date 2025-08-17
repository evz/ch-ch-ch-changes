import os


class Config:
    """Application configuration."""

    # Database configuration
    SQLALCHEMY_DATABASE_URI = os.environ.get("SQLALCHEMY_DATABASE_URI")

    if not SQLALCHEMY_DATABASE_URI:
        # Build PostgreSQL URI from components
        DB_USER = os.environ.get("DB_USER", "changes")
        DB_PASS = os.environ.get("DB_PASS", "changes-password")
        DB_HOST = os.environ.get("DB_HOST", "localhost")
        DB_PORT = os.environ.get("DB_PORT", "5432")
        DB_NAME = os.environ.get("DB_NAME", "changes")
        SQLALCHEMY_DATABASE_URI = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # Flask configuration
    SECRET_KEY = os.environ.get("SECRET_KEY", "dev-key-change-in-production")

    # Application settings
    DEBUG = os.environ.get("FLASK_DEBUG", "False").lower() == "true"
