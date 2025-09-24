# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Docker-based SQL playground featuring PostgreSQL 17 and PGAdmin with real NYC Yellow Taxi data (3.4+ million records). The architecture centers around a two-service Docker setup with volume-based data persistence and Python-based data loading utilities.

## Common Commands

### Environment Management
```bash
# Start the full environment with automatic data loading
docker-compose up -d --build

# Monitor data loading progress (first startup only)
docker logs -f sql-playground-postgres

# Stop all services
docker-compose down

# Full rebuild (clears all data and reloads from source files)
docker-compose down -v && docker-compose up -d --build
```

### Manual Data Loading (Optional)
```bash
# Data loads automatically during Docker startup, but manual loading is available:

# Load reference data only
uv run python python-scripts/load_reference_data.py

# Load trip data with custom parameters
uv run python python-scripts/load_taxi_data.py --max-rows 1000000

# Load full dataset manually
uv run python python-scripts/load_taxi_data.py
```

### Python Development
```bash
# Install dependencies
uv sync

# Add new dependencies
uv add package-name

# Run Python scripts
uv run python python-scripts/script_name.py
```

## Architecture Overview

### Data Flow Architecture
1. **Raw Data Sources**:
   - NYC taxi trip parquet files stored in `data/yellow/` (59MB, 3.4M+ records)
   - NYC taxi zone reference data stored in `data/zones/` (CSV + shapefiles, 265 zones)
2. **Single-Script Initialization**: Custom PostgreSQL Docker image with Python environment
   - Complete initialization via `docker/init-data.py` (runs SQL scripts + data loading)
   - Executes SQL scripts from `sql-scripts/init-scripts/` in proper order
   - PostGIS-enabled geospatial processing with CRS conversion
3. **Data Processing**: Built-in ETL during initialization:
   - Complete parquet file processing (chunked loading for memory efficiency)
   - Shapefile-to-PostGIS conversion with geometry validation
   - Reference data normalization and foreign key establishment
4. **Query Interface**: PGAdmin provides web-based SQL execution with pre-loaded analytical queries

### Database Schema
- **Primary Schema**: `nyc_taxi` schema with PostGIS-enabled geospatial support
- **Main Table**: `yellow_taxi_trips` (20 columns) matching NYC TLC official format exactly
- **Reference Tables**: Complete NYC taxi zone data with geometry:
  - `taxi_zone_lookup`: 265 official taxi zones with borough and service zone info
  - `taxi_zone_shapes`: PostGIS geometry table with polygon boundaries (EPSG:2263)
  - `vendor_lookup`, `payment_type_lookup`, `rate_code_lookup`: Supporting reference data
- **Indexing Strategy**: Spatial GIST indexes plus optimized indexes for time-series, location queries, and payment analytics

### Volume Mapping Strategy
- **Database Persistence**: `postgres_data` volume for PostgreSQL data
- **PGAdmin Config**: `pgadmin_data` volume for user settings
- **Init Scripts**: `./sql-scripts/init-scripts/` → `/docker-entrypoint-initdb.d` (auto-executed)
- **Report Scripts**: `./sql-scripts/reports-scripts/` → PGAdmin storage (user accessible)
- **All SQL Scripts**: `./sql-scripts/` → `/sql-scripts` (PostgreSQL access)

## Key Integration Points

### Environment Configuration
All services use variables from `.env` file with sensible defaults:
- PostgreSQL: Database name, user, password, port
- PGAdmin: Email, password, port
- Data loading: Chunk size, test row limits

### Python-Database Integration
**Trip Data Loading** (`load_taxi_data.py`):
- Parquet file validation and chunk processing
- SQLAlchemy-based connection management
- Data type conversion for PostgreSQL compatibility
- Progress tracking for large dataset loads

**Reference Data Loading** (`load_reference_data.py`):
- CSV loading for taxi zone lookup table
- Shapefile processing with geopandas and PostGIS integration
- CRS conversion to NYC State Plane (EPSG:2263)
- Geometry validation and MULTIPOLYGON conversion

### SQL Script Organization
- **Init Scripts**: Database schema, indexes, reference data (executed once)
- **Report Scripts**: Analytical queries accessible via PGAdmin interface
- Schema changes require container restart to take effect

## Data Model Specifics

### NYC Taxi Data Schema
- 20 columns matching official NYC TLC format
- Key fields: VendorID, pickup/dropoff timestamps, location IDs, financial data
- New 2025 field: `cbd_congestion_fee`
- Optimized for analytical queries on 3.4M+ records

### Performance Considerations
- Composite indexes on datetime + vendor, location + datetime combinations
- Chunked loading (10K rows default) for memory management
- Date-based partitioning ready (implementation pending)

## Access Points

### PGAdmin Web Interface
- URL: http://localhost:8080
- Credentials: admin@admin.com / admin123
- PostgreSQL connection: host=postgres, port=5432, db=playground, user=admin

### Direct Database Access
- Host: localhost:5432
- Database: playground
- Schema: nyc_taxi
- Main table: yellow_taxi_trips