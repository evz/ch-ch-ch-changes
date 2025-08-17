import sys

import pytest

# Add the code directory to Python path for Docker container
sys.path.insert(0, "/code")

from app import create_app
from app.extensions import db


@pytest.fixture
def app():
    """Create application for testing."""
    app = create_app()
    app.config.update({"TESTING": True, "WTF_CSRF_ENABLED": False})

    with app.app_context():
        yield app
        # Clean up any tables created during tests
        from sqlalchemy import text

        try:
            db.session.execute(text("DROP TABLE IF EXISTS etl_tracker CASCADE"))
            db.session.execute(text("DROP TABLE IF EXISTS dat_chicago_crime CASCADE"))
            db.session.execute(text("DROP TABLE IF EXISTS src_chicago_crime CASCADE"))
            db.session.execute(text("DROP TABLE IF EXISTS dup_chicago_crime CASCADE"))
            db.session.execute(text("DROP TABLE IF EXISTS chg_chicago_crime CASCADE"))
            db.session.execute(
                text("DROP MATERIALIZED VIEW IF EXISTS changed_records CASCADE")
            )
            db.session.commit()
        except Exception:
            db.session.rollback()


@pytest.fixture
def client(app):
    """Create test client."""
    return app.test_client()


@pytest.fixture
def runner(app):
    """Create test CLI runner."""
    return app.test_cli_runner()


@pytest.fixture
def dat_chicago_crime_table(app):
    """Create and populate dat_chicago_crime table with test data."""
    with app.app_context():
        from sqlalchemy import text

        from app.etl import ETL

        # Create the table using ETL logic
        etl = ETL("")
        etl.make_data_table()

        # Insert some test data with changes
        db.session.execute(
            text(
                """
            INSERT INTO dat_chicago_crime 
            (id, case_number, orig_date, block, primary_type, description, location_description,
             arrest, fbi_code, start_date, current_flag, updated_on) VALUES 
            (1, 'TEST001', '2024-01-01', '100 BLOCK OF MAIN ST', 'THEFT', 'OVER $500', 'STREET', 
             false, '06', '2024-01-01', false, '2024-01-01'),
            (1, 'TEST001', '2024-01-01', '100 BLOCK OF MAIN ST', 'THEFT', 'OVER $500', 'STREET', 
             true, '06', '2024-01-02', true, '2024-01-02'),
            (3, 'TEST003', '2024-01-01', '200 BLOCK OF ELM ST', 'BURGLARY', 'UNLAWFUL ENTRY', 'RESIDENCE', 
             false, '05', '2024-01-01', false, '2024-01-01'),
            (3, 'TEST003', '2024-01-01', '200 BLOCK OF ELM ST', 'BURGLARY', 'UNLAWFUL ENTRY', 'RESIDENCE', 
             false, '05', '2024-01-02', true, '2024-01-02')
        """
            )
        )
        db.session.commit()

        yield


@pytest.fixture
def changed_records_view(app, dat_chicago_crime_table):
    """Create the changed_records materialized view for tests."""
    with app.app_context():
        from sqlalchemy import text

        # Create the materialized view
        db.session.execute(
            text(
                """
            CREATE MATERIALIZED VIEW IF NOT EXISTS changed_records AS (
                SELECT d.* FROM dat_chicago_crime AS d
                JOIN (
                    SELECT id FROM dat_chicago_crime
                    GROUP BY id
                    HAVING(COUNT(*) > 1)
                ) AS s
                    ON d.id = s.id
            )
        """
            )
        )
        db.session.commit()

        yield


@pytest.fixture(autouse=True)
def cleanup_db():
    """Clean up database after each test."""
    yield
    # Clean up any failed transactions
    try:
        db.session.rollback()
        db.session.close()
    except Exception:
        pass
