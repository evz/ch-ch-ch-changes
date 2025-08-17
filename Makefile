.PHONY: help build up down logs shell db-shell etl test clean lint format check

# Default target
help:
	@echo "Available commands:"
	@echo "  make build     - Build the Docker images"
	@echo "  make up        - Start the application and database"
	@echo "  make down      - Stop the application and database" 
	@echo "  make logs      - Show application logs"
	@echo "  make shell     - Open shell in the application container"
	@echo "  make db-shell  - Open PostgreSQL shell"
	@echo "  make etl       - Run the ETL process to load data"
	@echo "  make test      - Run tests locally"
	@echo "  make lint      - Run code linting (flake8)"
	@echo "  make format    - Format code (black, isort)"
	@echo "  make check     - Check code formatting without changes"
	@echo "  make clean     - Stop containers and remove volumes"
	@echo ""
	@echo "Quick start:"
	@echo "  1. make build"
	@echo "  2. make up" 
	@echo "  3. make etl"
	@echo "  4. Open http://localhost:5000"

# Build Docker images
build:
	docker compose build

# Start services (depends on build)
up: build
	docker compose up -d
	@echo "Application starting..."
	@echo "Database will be available at localhost:5432"
	@echo "Web app will be available at http://localhost:5000"
	@echo "Run 'make logs' to see startup progress"

# Stop services  
down:
	docker compose down

# Show logs (depends on build)
logs: build
	docker compose logs -f

# Open shell in application container (depends on build)
shell: build
	docker compose exec changes /bin/bash

# Open PostgreSQL shell (depends on build)
db-shell: build
	docker compose exec db psql -U changes -d changes

# Run ETL process (depends on build)
etl: build
	@echo "Starting ETL process..."
	@echo "This will download ~1.8GB of data and may take 15-20 minutes"
	@read -p "Continue? [y/N] " -n 1 -r; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		echo ""; \
		docker compose exec changes flask run-etl; \
	else \
		echo ""; \
		echo "ETL cancelled"; \
	fi

# Run tests (requires database)
test: build
	@echo "Running tests against PostgreSQL database..."
	@echo "Starting services..."
	docker compose up -d
	@echo "Waiting for database to be ready..."
	@for i in $$(seq 1 15); do \
		if docker compose exec -T db pg_isready -h localhost -U changes >/dev/null 2>&1; then \
			echo "Database ready!"; \
			break; \
		fi; \
		echo "Waiting for DB... ($$i/15)"; \
		sleep 2; \
	done
	docker compose exec changes pytest /app/tests/ -v

# Clean up - remove containers and volumes
clean:
	docker compose down -v
	docker system prune -f
	@echo "Cleaned up containers and volumes"

# Code linting and formatting (uses Docker)
lint: build
	@echo "Running flake8..."
	docker compose exec changes pip install -q flake8==7.0.0
	docker compose exec changes flake8 . /app/tests/

format: build
	@echo "Formatting code with black and isort..."
	docker compose exec changes pip install -q black==24.3.0 isort==5.13.2
	docker compose exec changes black . /app/tests/
	docker compose exec changes isort . /app/tests/

check: build
	@echo "Checking code formatting..."
	docker compose exec changes pip install -q black==24.3.0 isort==5.13.2 flake8==7.0.0
	docker compose exec changes black --check --diff . /app/tests/
	docker compose exec changes isort --check-only --diff . /app/tests/
	docker compose exec changes flake8 . /app/tests/