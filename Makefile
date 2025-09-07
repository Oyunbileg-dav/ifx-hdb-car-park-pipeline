.PHONY: init run run-current run-historical run-complete historical-6pm-full up down logs clean clean-db help

# Use virtual environment if it exists, otherwise use system Python
PYTHON := $(shell if [ -d "venv" ]; then echo "venv/bin/python"; else echo "python3"; fi)

# Database initialization
init:
	PYTHONPATH=. $(PYTHON) scripts/init_db.py

# Pipeline execution commands
run: run-complete
	@echo "✅ Complete pipeline execution finished"

run-complete:
	@echo "🚀 Running complete analysis (both business questions)..."
	$(PYTHON) -m flows.pipeline complete

run-current:
	@echo "📊 Running current occupancy analysis only..."
	$(PYTHON) -m flows.pipeline current

run-historical:
	@echo "🌆 Running historical 6pm utilization analysis (delta loading)..."
	$(PYTHON) -m flows.pipeline historical

run-historical-full:
	@echo "🔄 Running historical 6pm analysis with FULL REFRESH..."
	$(PYTHON) -m flows.historical_6pm_pipeline full

# Docker and database management
up:
	@echo "🐘 Starting PostgreSQL database..."
	docker compose up -d

down:
	@echo "🛑 Stopping PostgreSQL database..."
	docker compose down

logs:
	@echo "📋 Showing database logs..."
	docker compose logs -f db

# Data management
clean:
	@echo "🧹 Cleaning reports directory..."
	rm -rf reports/*

clean-db:
	@echo "🗑️  Clearing all database tables..."
	docker compose exec db psql -U ifx -d ifx -c "TRUNCATE TABLE raw_carpark_current_availability, ref_carpark_info, raw_carpark_availability_6pm_last_30days;"

clean-current:
	@echo "🗑️  Clearing current availability data..."
	docker compose exec db psql -U ifx -d ifx -c "TRUNCATE TABLE raw_carpark_current_availability;"

clean-historical:
	@echo "🗑️  Clearing historical 6pm data..."
	docker compose exec db psql -U ifx -d ifx -c "TRUNCATE TABLE raw_carpark_availability_6pm_last_30days;"

# Development and testing
test-db:
	@echo "🔧 Testing database connection..."
	$(PYTHON) -c "from etl.db import get_conn; print('✅ Database connection OK' if get_conn() else '❌ Database connection failed')"

test-api:
	@echo "🔧 Testing API connectivity..."
	$(PYTHON) -c "from etl.extract import fetch_carpark_availability; print('✅ API connection OK' if fetch_carpark_availability() else '❌ API connection failed')"

# Quick setup and run
quick-start: up init run
	@echo "🎉 Quick start completed! Pipeline has been executed."

# Help command
help:
	@echo "🏗️  IFX HDB Carpark Pipeline - Available Commands"
	@echo "=" 
	@echo ""
	@echo "📋 Main Pipeline Commands:"
	@echo "  make run                  - Run complete analysis (both business questions)"
	@echo "  make run-current          - Run current occupancy analysis only"
	@echo "  make run-historical       - Run historical 6pm analysis (delta loading)"
	@echo "  make run-historical-full  - Run historical 6pm analysis (full refresh)"
	@echo ""
	@echo "🐘 Database Management:"
	@echo "  make up                   - Start PostgreSQL database"
	@echo "  make down                 - Stop PostgreSQL database" 
	@echo "  make init                 - Initialize database schema"
	@echo "  make logs                 - Show database logs"
	@echo ""
	@echo "🗑️  Data Management:"
	@echo "  make clean                - Clean reports directory"
	@echo "  make clean-db             - Clear all database tables"
	@echo "  make clean-current        - Clear current availability data"
	@echo "  make clean-historical     - Clear historical 6pm data"
	@echo ""
	@echo "🔧 Testing & Setup:"
	@echo "  make test-db              - Test database connection"
	@echo "  make test-api             - Test API connectivity"
	@echo "  make quick-start          - Full setup and run (up + init + run)"
	@echo ""
	@echo "⚡ Delta Loading Features:"
	@echo "  - Historical pipeline automatically checks existing data"
	@echo "  - Only fetches missing dates (much faster!)"
	@echo "  - Use '-full' commands for complete refresh when needed"
	@echo ""
	@echo "📚 Business Questions Answered:"
	@echo "  1. How many HDB carpark lots are currently occupied?"
	@echo "     → make run-current"
	@echo ""
	@echo "  2. How many HDB carparks with electronic parking are utilised"
	@echo "     at ≥80% capacity on average at approximately 6pm this month?"
	@echo "     → make run-historical (delta) or make run-historical-full (complete)"