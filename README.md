# SQL Playgrounds with NYC Taxi Data

A production-ready Docker-based SQL playground featuring PostgreSQL 17 + PostGIS 3.5 and PGAdmin with authentic NYC Yellow Taxi trip data. Features automated data backfill system that can load multiple months (3.47+ million records per month) with single-command deployment and unified data management.

## Features

- **PostgreSQL 17 + PostGIS 3.5** with geospatial support and custom Python environment
- **PGAdmin 4** (latest) for web-based database management and query execution
- **Automated Backfill System** - Download and load multiple months of authentic NYC taxi data
- **Flexible Data Loading** - Load specific months, last 6/12 months, or all available data (2020-2025)
- **Complete Geospatial Data** - 263 official NYC taxi zones with polygon boundaries (auto-downloaded)
- **Unified Data Management** - Single location for all data with automatic downloads from official sources
- **Dictionary Table Cleaning** - Fresh reference data loaded with each backfill for consistency
- **Production-Scale Performance** - Optimized schema with spatial indexes for big data analytics
- **Memory-Efficient Loading** - Chunked processing handles large datasets safely
- **Persistent Logging** - Organized logs by configuration with full traceability

## Quick Start

1. **Start the complete SQL playground:**
   ```bash
   docker-compose up -d --build
   ```

   ⏱️ **Default configuration loads last 12 months of data** (configure via `.env` file). First startup takes time based on data volume selected.

2. **Configure data backfill (optional):**
   Edit `.env` file to customize data loading:
   ```bash
   # Load last 12 months (default)
   BACKFILL_MONTHS=last_12_months

   # Load specific months
   BACKFILL_MONTHS=2024-01,2024-02,2024-03

   # Load all available data (2020-2025, WARNING: Very large!)
   BACKFILL_MONTHS=all
   ```

3. **Monitor the initialization progress:**
   ```bash
   docker logs -f sql-playground-postgres
   ```

   You'll see real-time progress updates as the system:
   - Creates PostGIS-enabled database schema
   - Downloads and loads 263 NYC taxi zones with geospatial boundaries
   - Downloads and processes trip data in chunks (10K rows each)
   - Performs data integrity verification

4. **Access PGAdmin Web Interface:**
   - **URL**: http://localhost:8080
   - **Login**: admin@admin.com / admin123

5. **Connect to PostgreSQL in PGAdmin:**
   - **Host**: postgres
   - **Port**: 5432
   - **Database**: playground
   - **Username**: admin
   - **Password**: admin123
   - **Schema**: nyc_taxi

## Architecture & Data Loading

### Automated Backfill System
The system features a **flexible backfill system** that automatically downloads and loads data from official NYC TLC sources:

**Data Sources:**
- **Trip Data**: NYC Yellow Taxi records (2020-2025, ~3.47M records per month)
- **Zone Data**: 263 official NYC TLC taxi zones with lookup table and PostGIS geometries
- **Reference Data**: Vendors, payment types, rate codes with proper relationships

### Complete Initialization Process
The `docker/init-data.py` script orchestrates the entire process:

1. **PostgreSQL Readiness**: Waits for database to be fully operational
2. **Schema Creation**: Executes SQL scripts in proper order with PostGIS extensions
3. **Dictionary Table Cleaning**: Cleans all reference tables for fresh data loading
4. **Zone Data Processing**:
   - Downloads CSV lookup table and shapefile ZIP from official sources
   - Extracts shapefiles and loads with NULL value cleanup (263 valid zones)
   - Processes geometries with CRS conversion to NYC State Plane (EPSG:2263)
   - Reloads all lookup tables (rate codes, payment types, vendors)
5. **Trip Data Backfill**:
   - Downloads parquet files for configured months automatically
   - Converts column names to lowercase for schema compatibility
   - Handles numeric precision issues and NULL values
   - Loads in 10K row chunks for memory efficiency with duplicate prevention
6. **Data Verification**: Performs integrity checks with sample analytical queries

### Architecture Benefits
- **Zero Manual Data Management**: All data downloaded automatically from official sources
- **Flexible Backfill**: Load specific months, last N months, or all available data
- **Clean State Management**: Dictionary tables refreshed with each backfill for consistency
- **Production-Ready**: Handles real-world data challenges (case sensitivity, precision, memory)
- **Error Recovery**: `docker-compose down -v` provides clean slate for troubleshooting
- **Persistent Storage**: Data persists between container restarts via Docker volumes
- **Organized Logging**: Logs organized by configuration with full traceability

## Database Schema & Analytics

### NYC Taxi Data Structure
**Main Table**: `nyc_taxi.yellow_taxi_trips` (variable records based on backfill configuration, 21 columns)
- **Trip Identifiers**: vendorid, pickup/dropoff timestamps
- **Location Data**: pulocationid, dolocationid (references taxi zones)
- **Financial Data**: fare_amount, tip_amount, total_amount, taxes, fees
- **Trip Metrics**: passenger_count, trip_distance, payment_type
- **Duplicate Prevention**: row_hash (SHA-256 hash of all row values for ultimate duplicate prevention)
- **Recent Additions**: cbd_congestion_fee (Central Business District fee), airport_fee

**Geospatial Tables**:
- `taxi_zone_lookup`: 263 official NYC taxi zones with borough and service zone info
- `taxi_zone_shapes`: PostGIS MULTIPOLYGON geometries in NYC State Plane coordinates

**Reference Tables**: vendor_lookup, payment_type_lookup, rate_code_lookup

### Performance Optimizations
- **Spatial Index**: GIST index on geometry column for fast spatial queries
- **Time-Series Indexes**: On pickup_datetime and dropoff_datetime
- **Location Indexes**: On pickup and dropoff location IDs
- **Composite Indexes**: Combined indexes for common analytical patterns

### Sample Analytics Queries

```sql
-- Trip volume by hour with zone names (uses lowercase column names)
SELECT EXTRACT(HOUR FROM yt.tpep_pickup_datetime) as hour,
       COUNT(*) as trips,
       STRING_AGG(DISTINCT pickup_zone.borough, ', ') as pickup_boroughs
FROM nyc_taxi.yellow_taxi_trips yt
JOIN nyc_taxi.taxi_zone_lookup pickup_zone ON yt.pulocationid = pickup_zone.locationid
GROUP BY hour ORDER BY hour;

-- Geospatial: Largest taxi zones by area (PostGIS spatial functions)
SELECT zone, borough,
       ROUND((ST_Area(geometry) / 43560)::numeric, 2) as area_acres
FROM nyc_taxi.taxi_zone_shapes tzs
JOIN nyc_taxi.taxi_zone_lookup tzl ON tzs.locationid = tzl.locationid
ORDER BY ST_Area(geometry) DESC LIMIT 10;

-- Cross-borough trip analysis with corrected column names
SELECT pickup_zone.borough as pickup_borough,
       dropoff_zone.borough as dropoff_borough,
       COUNT(*) as trip_count,
       AVG(yt.fare_amount) as avg_fare
FROM nyc_taxi.yellow_taxi_trips yt
JOIN nyc_taxi.taxi_zone_lookup pickup_zone ON yt.pulocationid = pickup_zone.locationid
JOIN nyc_taxi.taxi_zone_lookup dropoff_zone ON yt.dolocationid = dropoff_zone.locationid
GROUP BY pickup_zone.borough, dropoff_zone.borough
ORDER BY trip_count DESC;

-- Financial analysis: Payment patterns by borough
SELECT pickup_zone.borough,
       ptl.payment_type_desc,
       COUNT(*) as trips,
       AVG(yt.total_amount) as avg_total
FROM nyc_taxi.yellow_taxi_trips yt
JOIN nyc_taxi.taxi_zone_lookup pickup_zone ON yt.pulocationid = pickup_zone.locationid
JOIN nyc_taxi.payment_type_lookup ptl ON yt.payment_type = ptl.payment_type
GROUP BY pickup_zone.borough, ptl.payment_type_desc
ORDER BY pickup_zone.borough, trips DESC;
```

## Project Structure

```
sql-playgrounds/
├── docker-compose.yml              # Multi-service configuration (PostgreSQL + PGAdmin)
├── .env                            # Environment variables (credentials, ports, backfill config)
├── CLAUDE.md                       # Detailed architecture guide for Claude Code
├── sql-scripts/                    # SQL scripts and unified data location
│   ├── data/                       # Unified data location (auto-populated during initialization)
│   │   ├── zones/                  # NYC taxi zone reference data (auto-downloaded)
│   │   │   ├── taxi_zone_lookup.csv # 263 official taxi zones
│   │   │   └── taxi_zones.* (shp/dbf/shx/prj/sbn/sbx) # Extracted shapefiles
│   │   └── yellow/                 # NYC Yellow taxi trip data (auto-downloaded)
│   │       └── yellow_tripdata_*.parquet # Trip data files based on backfill config
│   ├── init-scripts/               # Database schema creation (executed automatically)
│   │   ├── 00-postgis-setup.sql    # PostGIS extensions and spatial references
│   │   └── 01-nyc-taxi-schema.sql  # Complete NYC taxi schema (lowercase columns)
│   └── reports-scripts/            # Pre-built analytical queries (available in PGAdmin)
│       ├── nyc-taxi-analytics.sql  # Trip volume, financial, and temporal analysis
│       └── geospatial-taxi-analytics.sql # PostGIS spatial queries and zone analysis
├── logs/                           # Persistent logging (organized by backfill configuration)
│   ├── last_12_months/             # Logs for 12-month backfill
│   ├── all/                        # Logs for complete data backfill
│   └── [config]/                   # Logs organized by BACKFILL_MONTHS setting
├── docker/                         # Custom PostgreSQL container
│   ├── Dockerfile.postgres         # Custom image: PostgreSQL + PostGIS + Python environment
│   └── init-data.py                # Comprehensive initialization script with backfill system
├── pyproject.toml                  # Python dependencies (for local development only)
└── uv.lock                         # Dependency lock file (uv package manager)
```

## Container Architecture

### Custom PostgreSQL Container
- **Base**: `postgis/postgis:17-3.5` (PostgreSQL 17 + PostGIS 3.5)
- **Python Environment**: Virtual environment with data processing packages
- **Custom Entrypoint**: Starts PostgreSQL, waits for readiness, runs initialization
- **Embedded Dependencies**: pandas, geopandas, pyarrow, psycopg2, sqlalchemy

### Volume Strategy
- **Database Persistence**: `postgres_data` volume (survives container restarts)
- **PGAdmin Configuration**: `pgadmin_data` volume (settings, connections)
- **Unified Data Location**: `./sql-scripts:/sql-scripts` (contains auto-downloaded data files)
- **Persistent Logging**: `./logs:/sql-scripts/logs` (organized by backfill configuration)
- **Script Access**: SQL scripts available both for initialization and PGAdmin queries

### Memory & Performance
- **Chunked Loading**: 10K rows per chunk prevents memory overflow
- **Progress Tracking**: Real-time logging with execution time tracking
- **Optimized Indexes**: Spatial GIST, temporal, location, and composite indexes
- **Production Scale**: Handles millions of records efficiently based on backfill configuration
- **Duplicate Prevention**: Hash-based system prevents any duplicate rows across backfills

## Configuration & Development

### Environment Variables (`.env`)
```env
# PostgreSQL Configuration
POSTGRES_DB=playground
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin123
POSTGRES_PORT=5432

# PGAdmin Configuration
PGADMIN_EMAIL=admin@admin.com
PGADMIN_PASSWORD=admin123
PGADMIN_PORT=8080

# Data Loading Control
DATA_CHUNK_SIZE=10000
INIT_LOAD_ALL_DATA=true

# Backfill Configuration (controls which months to load)
BACKFILL_MONTHS=last_12_months
```

### System Requirements
- **Docker & Docker Compose** (only requirement for deployment)
- **Python 3.12+ with uv** (optional, for local development only)
- **Available Memory**: 4GB+ recommended for data loading process
- **Available Storage**: ~2GB for complete dataset and indexes

### Development Commands
```bash
# Full deployment (production-ready)
docker-compose up -d --build

# Development with clean rebuild (removes all data)
docker-compose down -v && docker-compose up -d --build

# Check data loading status
docker exec sql-playground-postgres psql -U admin -d playground -c "SELECT COUNT(*) FROM nyc_taxi.yellow_taxi_trips;"

# Check backfill progress
docker exec sql-playground-postgres psql -U admin -d playground -c "SELECT * FROM nyc_taxi.data_processing_log ORDER BY data_year, data_month;"

# Direct database access
docker exec -it sql-playground-postgres psql -U admin -d playground

# Monitor initialization progress
docker logs -f sql-playground-postgres

# Monitor persistent logs (organized by configuration)
tail -f logs/last_12_months/log_*.log
```

### Local Python Development (Optional)
```bash
# Install dependencies for local scripts
uv sync

# Add new dependencies
uv add package-name
```

**Note**: Python environment is embedded in Docker container. Local Python setup only needed for custom script development.

## Data Sources & Authenticity

### NYC Yellow Taxi Trip Records
**Source**: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- **Format**: Official parquet files (updated monthly by NYC TLC)
- **Available Data**: 2020-2025 (monthly files, ~59MB/3.47M+ records per month)
- **Auto-Download**: System automatically downloads configured months from official sources
- **Coverage**: Complete months of taxi trip data from NYC Taxi & Limousine Commission

### NYC Taxi Zone Reference Data
**Source**: [NYC TLC Taxi Zone Maps and Lookup Tables](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- **Lookup Table**: `taxi_zone_lookup.csv` - 263 official taxi zones (automatically downloaded)
- **Geospatial Data**: Complete shapefile ZIP with polygon boundaries (automatically downloaded and extracted)
- **Coordinate System**: Converted to NYC State Plane (EPSG:2263) for optimal spatial analysis
- **Components**: `.shp`, `.dbf`, `.shx`, `.prj`, `.sbn`, `.sbx` files extracted automatically

### Data Authenticity & Scale
This is **production-scale real-world data** from New York City's official transportation authority:
- ✅ **Authentic NYC taxi trips** - every record represents a real taxi ride
- ✅ **Flexible temporal coverage** - load specific months, last N months, or all available data
- ✅ **Official geospatial boundaries** - precise NYC taxi zone polygons
- ✅ **Rich analytical dimensions** - financial, temporal, spatial, and operational data
- ✅ **Always current** - downloads latest data directly from official NYC TLC sources

Perfect for learning advanced SQL, big data analytics, and geospatial analysis with realistic datasets that mirror production database challenges. The flexible backfill system allows you to work with datasets ranging from a single month to 5+ years of historical data.