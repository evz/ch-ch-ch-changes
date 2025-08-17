from app.etl import ETL


class TestETLChangeDetection:
    """Test the core change detection logic."""

    def test_deduplication_logic(self, app):
        """Test that deduplication keeps the latest record per ID."""
        with app.app_context():
            etl = ETL("")

            # Mock source data with duplicates
            etl.make_source_table()
            etl.make_new_dup_tables()

            # Insert test data with same ID, different line numbers
            from sqlalchemy import text

            from app.extensions import db

            db.session.execute(
                text(
                    """
                INSERT INTO src_chicago_crime (id, case_number, line_num) VALUES 
                (123, 'CASE001', 1),
                (123, 'CASE001', 2),
                (123, 'CASE001', 3)
            """
                )
            )
            db.session.commit()

            etl.find_dup_rows()

            # Should rank line_num=3 as dup_ver=1 (latest)
            result = db.session.execute(
                text(
                    """
                SELECT dup_ver FROM dup_chicago_crime 
                WHERE id = 123 AND line_num = 3
            """
                )
            ).first()

            assert result.dup_ver == 1

    def test_change_detection_arrest_status(self, app):
        """Test detection of arrest status changes."""
        with app.app_context():
            etl = ETL("")

            # Setup tables
            etl.make_source_table()
            etl.make_data_table()

            from sqlalchemy import text

            from app.extensions import db

            # Insert existing record with arrest=false
            db.session.execute(
                text(
                    """
                INSERT INTO dat_chicago_crime 
                (id, arrest, current_flag, start_date) VALUES 
                (456, false, true, NOW())
            """
                )
            )

            # Insert new data with arrest=true
            db.session.execute(
                text(
                    """
                INSERT INTO src_chicago_crime 
                (id, arrest) VALUES (456, true)
            """
                )
            )
            db.session.commit()

            etl.find_changed_rows()

            # Should detect the change
            result = db.session.execute(
                text(
                    """
                SELECT COUNT(*) as count FROM chg_chicago_crime WHERE id = 456
            """
                )
            ).first()

            assert result.count == 1

    def test_change_detection_fbi_code(self, app):
        """Test detection of FBI code changes (index/non-index)."""
        with app.app_context():
            etl = ETL("")

            # Setup tables
            etl.make_source_table()
            etl.make_data_table()

            from sqlalchemy import text

            from app.extensions import db

            # Insert existing record with FBI code '01A' (index crime)
            db.session.execute(
                text(
                    """
                INSERT INTO dat_chicago_crime 
                (id, fbi_code, current_flag, start_date) VALUES 
                (789, '01A', true, NOW())
            """
                )
            )

            # Insert new data with FBI code '26' (non-index)
            db.session.execute(
                text(
                    """
                INSERT INTO src_chicago_crime 
                (id, fbi_code) VALUES (789, '26')
            """
                )
            )
            db.session.commit()

            etl.find_changed_rows()

            # Should detect the change
            result = db.session.execute(
                text(
                    """
                SELECT COUNT(*) as count FROM chg_chicago_crime WHERE id = 789
            """
                )
            ).first()

            assert result.count == 1

    def test_no_change_detection(self, app):
        """Test that identical records don't trigger changes."""
        with app.app_context():
            from sqlalchemy import text

            from app.extensions import db

            # Create tables
            etl = ETL("")
            etl.make_source_table()
            etl.make_data_table()

            # Insert identical records with same data
            db.session.execute(
                text(
                    """
                INSERT INTO dat_chicago_crime 
                (id, case_number, orig_date, primary_type, arrest, fbi_code, current_flag, start_date) VALUES 
                (999, 'CASE999', '2024-01-01', 'THEFT', false, '06', true, NOW())
            """
                )
            )

            db.session.execute(
                text(
                    """
                INSERT INTO src_chicago_crime 
                (id, case_number, orig_date, primary_type, arrest, fbi_code) VALUES 
                (999, 'CASE999', '2024-01-01', 'THEFT', false, '06')
            """
                )
            )
            db.session.commit()

            # Create change detection table and run detection
            etl.make_new_dup_tables()
            etl.find_changed_rows()

            # Should NOT detect any changes since records are identical
            result = db.session.execute(
                text(
                    """
                SELECT COUNT(*) as count FROM chg_chicago_crime WHERE id = 999
            """
                )
            ).first()

            assert result.count == 0
