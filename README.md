# IFX HDB Carpark Pipeline

A data pipeline for analyzing Singapore HDB carpark availability data using Python, SQL, Prefect, and PostgreSQL.

## Features

- **Real-time Data**: Fetches current carpark availability from Singapore's data.gov.sg API
- **Historical Analysis**: Collects and analyzes 6pm availability data for trend analysis
- **Electronic Parking Focus**: Filters data to include only carparks with electronic parking systems
- **Singapore Time**: All timestamps stored and processed in Singapore Time (SGT)
- **Automated Pipeline**: Uses Prefect for workflow orchestration
- **Data Quality**: Filters stale records and handles missing data gracefully

## Prerequisites

- **Python 3.8+**
- **Docker & Docker Compose** (for database)
- **Git** (for version control)

## Quick Start

### 1. Clone and Setup

```bash
# Clone the repository
git clone <repository-url>
cd ifx-hdb-pipeline

# Create and activate virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Environment Configuration

```bash
# Copy environment template
cp .env.example .env

# Edit .env file with your settings (if needed)
# Default settings work for local development
```

**Required Environment Variables:**
```bash
# Database connection
DATABASE_URL=postgresql://ifx:ifx@localhost:5432/ifx

# API endpoints (Singapore government APIs)
HDB_CARPARK_API_URL=https://api.data.gov.sg/v1/transport/carpark-availability
HDB_CARPARK_INFO_URL=https://data.gov.sg/api/action/datastore_search?resource_id=139a3035-e624-4f56-b63f-89ae28d4ae4c
```

### 3. Database Setup

```bash
# Start PostgreSQL database with Docker
make up

# Initialize database schema
make init
```

### 4. Run the Pipeline

```bash
# Run the complete data pipeline
make run
```

### 5. View Results

```bash
# Access database via Adminer (web interface)
# URL: http://localhost:8080
# System: PostgreSQL
# Server: db
# Username: ifx
# Password: ifx
# Database: ifx

# Or connect directly with psql
psql postgresql://ifx:ifx@localhost:5432/ifx
```

## Manual Setup (Without Make)

If you prefer to run commands manually:

### Start Database
```bash
docker-compose up -d
```

### Initialize Schema
```bash
PYTHONPATH=. python scripts/init_db.py
```

### Run Pipeline
```bash
PYTHONPATH=. python flows/pipeline.py
```

## Project Structure

```
ifx-hdb-pipeline/
├── etl/                    # ETL modules
│   ├── extract.py         # Data extraction from APIs
│   ├── transform.py       # Data transformation logic
│   ├── load.py           # Database loading functions
│   ├── reports.py        # Analysis and reporting
│   └── db.py             # Database connection utilities
├── flows/                 # Prefect workflow definitions
│   └── pipeline.py       # Main pipeline orchestration
├── sql/                  # Database schema
│   └── 001_schema.sql    # Table definitions
├── scripts/              # Utility scripts
│   └── init_db.py        # Database initialization
├── docker-compose.yml    # PostgreSQL database setup
├── Makefile             # Common commands
├── requirements.txt     # Python dependencies
└── README.md           # This file
```

## Pipeline Overview

The pipeline consists of three main stages:

### 1. Extract
- **Current Availability**: Fetches real-time carpark data
- **Carpark Info**: Downloads reference data about carpark locations and systems
- **Historical 6pm Data**: Collects availability data from the past 30 days at 6pm SGT

### 2. Transform
- **Data Cleaning**: Removes invalid or incomplete records
- **Timezone Handling**: Converts all timestamps to Singapore Time
- **Age Filtering**: Filters out stale data (>10 hours for current, >30 days for historical)
- **Data Validation**: Ensures data quality and consistency

### 3. Load
- **Current Data**: Loads into `raw_carpark_current_availability` table
- **Reference Data**: Updates `ref_carpark_info` table
- **Historical Data**: Loads into `raw_carpark_availability_6pm_last_30days` table
- **Conflict Resolution**: Handles duplicate records gracefully

## Database Schema

### Tables

1. **`raw_carpark_current_availability`**
   - Real-time carpark availability data
   - Includes ingest timestamp, carpark details, and availability counts

2. **`ref_carpark_info`**
   - Reference data about carpark locations and systems
   - Used for filtering electronic parking systems

3. **`raw_carpark_availability_6pm_last_30days`**
   - Historical availability data for trend analysis
   - Focuses on 6pm data for consistent comparison

## Configuration

### Data Retention
- **Current Data**: Filters records older than 10 hours
- **Historical Data**: Keeps data from the last 30 days
- **Electronic Parking Only**: Filters to include only electronic parking systems

### API Settings
The pipeline uses Singapore's official data.gov.sg APIs:
- **Carpark Availability**: Real-time and historical availability data
- **Carpark Information**: Reference data about carpark facilities

## Troubleshooting

### Common Issues

1. **psycopg2 Import Error (macOS)**
   ```bash
   # If you see "symbol not found in flat namespace '_PQbackendPID'"
   source venv/bin/activate
   pip uninstall psycopg2-binary -y
   pip install psycopg2-binary --no-cache-dir
   ```

2. **Database Connection Error**
   ```bash
   # Ensure Docker is running and database is up
   docker-compose ps
   make up
   ```

3. **API Rate Limiting**
   ```bash
   # The pipeline includes error handling for API failures
   # Check logs for specific error messages
   ```

4. **Permission Errors**
   ```bash
   # Ensure proper file permissions
   chmod +x scripts/init_db.py
   chmod +x setup.sh
   ```

5. **Python Path Issues**
   ```bash
   # Always run with PYTHONPATH set
   PYTHONPATH=. python flows/pipeline.py
   ```

### Logs and Monitoring

- Pipeline execution logs are printed to console
- Database operations include success/failure counts
- API calls include retry logic and error handling

## Development

### Adding New Features

1. **New Data Sources**: Add extraction functions in `etl/extract.py`
2. **Data Transformations**: Extend `etl/transform.py`
3. **Database Changes**: Update `sql/001_schema.sql` and run `make init`
4. **Analysis**: Add new functions to `etl/reports.py`

### Testing

```bash
# Test database connection
python -c "from etl.db import get_conn; print('DB OK' if get_conn() else 'DB Error')"

# Test API connectivity
python -c "from etl.extract import fetch_carpark_availability; print('API OK' if fetch_carpark_availability() else 'API Error')"
```

## License

This project is for educational and research purposes.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

---

For questions or issues, please check the troubleshooting section or create an issue in the repository.