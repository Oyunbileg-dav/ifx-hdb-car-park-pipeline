# IFX HDB Carpark Pipeline

A comprehensive data pipeline for analyzing Singapore HDB carpark availability data using Python, SQL, Prefect, and PostgreSQL. This pipeline answers two key business questions about HDB parking utilization.

## Features

- **Real-time Analysis**: Fetches current carpark availability with interactive map visualization
- **Historical 6pm Analysis**: Analyzes 30-day historical data for peak hour utilization patterns
- **Interactive HTML Reports**: Generates beautiful, responsive reports with charts and maps
- **Delta Loading**: Optimized data fetching that only retrieves new data
- **Singapore Time**: All timestamps stored and processed in Singapore Time (SGT)
- **Automated Pipeline**: Uses Prefect for workflow orchestration with separate flows
- **Data Quality**: Filters stale records and handles missing data gracefully
- **Search Functionality**: Interactive search by address or carpark number
- **Capacity Analysis**: Bucketed analysis by carpark size (Small, Medium, Large, Very Large)

## Prerequisites

- **Python 3.8+ (Python 3.10+ recommended)**
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
# Run current occupancy analysis (with interactive map)
make run-current

# Run historical 6pm analysis (with scatterplot and capacity buckets)
make run-historical

# Run historical analysis with full data refresh
make run-historical-full

# Run both analyses
make run-all
```

### 5. View Results

```bash
# View HTML reports (generated automatically)
open reports/current_occupancy_report.html    # Current occupancy with interactive map
open reports/historical_6pm_report.html       # Historical analysis with scatterplot

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
# Current occupancy analysis
PYTHONPATH=. python flows/current_occupancy_pipeline.py

# Historical 6pm analysis
PYTHONPATH=. python flows/historical_6pm_pipeline.py

# Historical analysis with full refresh
PYTHONPATH=. python flows/historical_6pm_pipeline.py full
```

## Project Structure

```
ifx-hdb-pipeline/
├── etl/                    # ETL modules
│   ├── extract.py         # Data extraction from APIs
│   ├── transform.py       # Data transformation logic
│   ├── load.py           # Database loading functions
│   ├── reports.py        # Analysis and reporting functions
│   └── db.py             # Database connection utilities
├── flows/                 # Prefect workflow definitions
│   ├── current_occupancy_pipeline.py    # Real-time analysis with map
│   ├── historical_6pm_pipeline.py       # Historical 6pm analysis
│   └── pipeline.py       # Legacy pipeline (deprecated)
├── reports/               # Generated HTML reports
│   ├── current_occupancy_report.html    # Current occupancy report
│   └── historical_6pm_report.html      # Historical analysis report
├── sql/                  # Database schema
│   └── 001_schema.sql    # Table definitions
├── scripts/              # Utility scripts
│   └── init_db.py        # Database initialization
├── docker-compose.yml    # PostgreSQL database setup
├── Makefile             # Common commands
├── requirements.txt     # Python dependencies
├── setup.sh            # Automated setup script
└── README.md           # This file
```

## Business Questions Answered

The pipeline addresses two key business questions:

### 1. Current Occupancy Analysis
**Question**: "How many HDB carpark lots are currently occupied?"

**Features**:
- Real-time data collection from Singapore's data.gov.sg API
- Interactive map visualization with Leaflet.js
- Search functionality by address or carpark number
- Bubble sizes represent total capacity, colors show occupancy levels
- Covers all HDB carparks (not just electronic)

### 2. Historical 6pm Analysis
**Question**: "How many HDB carparks with electronic parking are utilised at ≥80% capacity on average at approximately 6pm this month?"

**Features**:
- 30-day historical data analysis
- Interactive scatterplot (capacity vs utilization)
- Capacity-bucketed analysis (Small, Medium, Large, Very Large)
- Focuses on electronic parking systems only
- Delta loading for efficient data updates

## Pipeline Overview

The pipeline consists of three main stages:

### 1. Extract
- **Current Availability**: Fetches real-time carpark data
- **Carpark Info**: Downloads reference data about carpark locations and systems
- **Historical 6pm Data**: Collects availability data from the past 30 days at 6pm SGT
- **Delta Loading**: Only fetches new data to optimize performance

### 2. Transform
- **Data Cleaning**: Removes invalid or incomplete records
- **Timezone Handling**: Converts all timestamps to Singapore Time
- **Age Filtering**: Filters out stale data (>10 hours for current, >30 days for historical)
- **Time Window Filtering**: Historical data filtered to 6pm ±1 hour (5pm-7pm SGT)
- **Data Validation**: Ensures data quality and consistency

### 3. Load
- **Current Data**: Loads into `raw_carpark_current_availability` table
- **Reference Data**: Updates `ref_carpark_info` table
- **Historical Data**: Loads into `raw_carpark_availability_6pm_last_30days` table
- **Conflict Resolution**: Handles duplicate records gracefully with upsert operations

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

## HTML Reports

The pipeline automatically generates interactive HTML reports:

### Current Occupancy Report (`reports/current_occupancy_report.html`)
- **Real-time metrics**: Occupied lots, total lots, availability
- **Interactive map**: Leaflet.js visualization with zoom and pan
- **Search functionality**: Find carparks by address or number
- **Visual indicators**: Bubble sizes (capacity) and colors (occupancy)
- **Responsive design**: Works on desktop and mobile

### Historical 6pm Report (`reports/historical_6pm_report.html`)
- **Summary statistics**: High utilization counts and averages
- **Interactive scatterplot**: Chart.js visualization (capacity vs utilization)
- **Capacity buckets**: Top performers by size category
- **Detailed tables**: Comprehensive data breakdowns
- **Business question answer**: Complete analysis explanation

## Configuration

### Data Retention
- **Current Data**: Filters records older than 10 hours
- **Historical Data**: Keeps data from the last 30 days
- **Electronic Parking**: Historical analysis focuses on electronic parking systems only
- **All Carparks**: Current occupancy analysis includes all HDB carparks

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
5. **New Pipelines**: Create new flow files in `flows/` directory
6. **Report Generation**: Add HTML report functions to pipeline flows

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