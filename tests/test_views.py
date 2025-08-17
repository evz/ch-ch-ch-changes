from sqlalchemy import text

from app.extensions import db


class TestViews:
    """Test web interface functionality."""

    def test_index_no_data(self, client, app):
        """Test index page when no ETL data exists."""
        with app.app_context():
            response = client.get("/")
            assert response.status_code == 200
            # Should show default message when no ETL data exists
            assert (
                b"Database not initialized" in response.data
                or b"No data" in response.data
            )

    def test_change_list_no_data(self, client, app):
        """Test change list when no ETL data exists."""
        with app.app_context():
            response = client.get("/change-list/")
            assert response.status_code == 200
            # Should show error message since view doesn't exist
            assert b"Database not ready - run ETL first" in response.data

    def test_index_with_data(self, client, app):
        """Test index page with ETL tracking data."""
        with app.app_context():
            # Insert test ETL tracking data
            db.session.execute(
                text(
                    """
                CREATE TABLE IF NOT EXISTS etl_tracker(
                    filename VARCHAR,
                    date_added TIMESTAMP DEFAULT NOW(),
                    etl_status VARCHAR,
                    file_date DATE
                )
            """
                )
            )

            db.session.execute(
                text(
                    """
                INSERT INTO etl_tracker (filename, etl_status, file_date) VALUES 
                ('test-2024-01-01.csv', 'success', '2024-01-01'),
                ('test-2024-01-02.csv', 'success', '2024-01-02')
            """
                )
            )
            db.session.commit()

            response = client.get("/")
            assert response.status_code == 200
            assert b"January" in response.data  # Should show formatted dates

    def test_change_list_with_data(self, client, app, changed_records_view):
        """Test change list when change data exists."""
        with app.app_context():
            response = client.get("/change-list/")
            assert response.status_code == 200
            # Should show actual changed records from our test data
            assert b"TEST001" in response.data or b"TEST003" in response.data

    def test_change_list_input_validation(self, client, changed_records_view):
        """Test change list handles invalid input parameters."""
        # Test invalid limit
        response = client.get("/change-list/?limit=invalid")
        assert response.status_code == 200

        # Test invalid page
        response = client.get("/change-list/?page=invalid")
        assert response.status_code == 200

        # Test excessive limit (should be capped)
        response = client.get("/change-list/?limit=99999")
        assert response.status_code == 200
