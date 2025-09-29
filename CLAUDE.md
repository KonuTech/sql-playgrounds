# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Docker-based SQL playground featuring PostgreSQL 17 + PostGIS 3.5, PGAdmin, and Apache Superset with real NYC Yellow Taxi data (3.47+ million records per month). The architecture uses a custom PostgreSQL Docker image with embedded Python data loading that runs a single comprehensive initialization script during container startup. Includes comprehensive SQL technical interview questions and star schema modeling examples.

## Common Commands

### Environment Management
```bash
# Start the full environment (PostgreSQL + PGAdmin + Superset + Redis)
docker-compose up -d --build

# Monitor data loading progress (first startup takes ~30-45 minutes)
docker logs -f sql-playground-postgres

# Monitor Superset initialization
docker logs -f sql-playground-superset

# Stop all services
docker-compose down

# Full rebuild (clears all data and reloads from source files)
docker-compose down -v && docker-compose up -d --build

# Check container status
docker ps
```

### Data Management
```bash
# Basic single-month loading (default)
docker-compose up -d --build

# Backfill specific months - edit .env file before starting:
# BACKFILL_MONTHS=2024-01,2024-02,2024-03
docker-compose down -v && docker-compose up -d --build

# Backfill last 6 months - edit .env file:
# BACKFILL_MONTHS=last_6_months
docker-compose down -v && docker-compose up -d --build

# Backfill all available data (2020-present) - WARNING: Very large!
# BACKFILL_MONTHS=all
docker-compose down -v && docker-compose up -d --build

# Check current data status
docker exec sql-playground-postgres psql -U admin -d playground -c "SELECT COUNT(*) FROM nyc_taxi.yellow_taxi_trips;"

# Monitor download/loading progress (console logs)
docker logs -f sql-playground-postgres

# Monitor file logs (persistent, organized by configuration)
tail -f logs/$BACKFILL_MONTHS/log_*.log

# Example: Monitor logs for last_12_months configuration
tail -f logs/last_12_months/log_*.log
```

### Python Development (Local)
```bash
# Install dependencies (for local development only)
uv sync

# Add new dependencies
uv add package-name

# Python environment is embedded in Docker - no local Python needed for normal operation
```

## Architecture Overview

### Logging System
- **Dual Logging**: Both console (Docker logs) and persistent file logging
- **Organized Structure**: Logs stored in `./logs/$BACKFILL_MONTHS/` directories
- **Timestamped Files**: Each execution creates `log_YYYYMMDD_HHMMSS.log`
- **Full Traceability**: Complete record of downloads, data loading, and errors
- **Host Access**: Logs accessible from host filesystem for analysis and monitoring

### Data Flow Architecture
1. **Raw Data Sources**:
   - **Unified data location**: `/sql-scripts/data/` (single consolidated location)
   - **Remote data**: NYC TLC Trip Record Data (automatically downloaded via backfill)
   - **URL Pattern**: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`
   - **Available data**: 2020-2025 (monthly files, ~59MB/3.47M+ records per month)
   - **Reference data**: `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`
   - **Shapefile data**: `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip`
2. **Custom Docker Container**: PostgreSQL 17 + PostGIS 3.5 + Python 3.11 environment
   - **Single initialization script**: `docker/init-data.py` handles complete setup
   - **Custom entrypoint**: Starts PostgreSQL, then runs initialization after DB is ready
   - **All dependencies embedded**: numpy, pandas, geopandas, pyarrow, psycopg2, sqlalchemy, requests, zipfile
   - **Backfill capability**: Automatic download and loading of multiple months
3. **Automated Data Processing** (during container startup):
   - **Dictionary table cleaning**: All reference tables cleaned before each backfill
   - SQL schema creation: `sql-scripts/init-scripts/` executed in order
   - **Unified data download**: CSV and shapefile ZIP downloaded to single location
   - CSV processing: taxi zone lookup table (263 zones)
   - Shapefile processing: ZIP extraction + PostGIS geometry conversion (EPSG:2263)
   - **Backfill processing**: Downloads missing parquet files from NYC TLC
   - Parquet processing: chunked loading (10K rows/chunk) with data type conversion
   - **Lookup table reloading**: Rate codes, payment types, and vendor tables refreshed
   - Progress tracking: real-time logging of download/loading status with dual logging
4. **Query Interface**: PGAdmin 4 web interface with pre-loaded analytical queries

### Database Schema
- **Primary Schema**: `nyc_taxi` with PostGIS spatial extensions enabled
- **Main Table**: `yellow_taxi_trips` (21 columns, lowercase names) - 3.47M+ records
  - All numeric columns use appropriate DECIMAL precision (e.g., `ratecodeid DECIMAL(4,1)`)
  - Column names converted to lowercase during data loading for consistency
  - **Hash-Based ID**: `row_hash VARCHAR(64) UNIQUE` - SHA-256 hash of all row values for ultimate duplicate prevention
- **Reference Tables** (automatically populated):
  - `taxi_zone_lookup`: 263 NYC taxi zones (locationid, borough, zone, service_zone)
  - `taxi_zone_shapes`: PostGIS MULTIPOLYGON geometries (EPSG:2263 coordinate system)
  - `vendor_lookup`, `payment_type_lookup`, `rate_code_lookup`: Lookup tables for codes
  - **`data_processing_log`**: Tracks processed months to prevent duplicate loading
- **Performance Optimizations**:
  - Spatial GIST index on geometry column
  - Time-series indexes on pickup/dropoff datetime
  - Composite indexes for common query patterns (location+datetime, vendor+datetime)
- **Data Integrity**:
  - Automatic duplicate detection and skipping
  - Processing status tracking (in_progress, completed, failed)
  - Safe re-runs without data duplication

### Container Architecture
- **PostgreSQL Container**: `postgis/postgis:17-3.5` (PostgreSQL 17 + PostGIS 3.5)
  - **Python Environment**: Virtual environment at `/opt/venv` with data processing packages
  - **Custom Entrypoint**: `/usr/local/bin/custom-entrypoint.sh`
    - Starts PostgreSQL in background
    - Waits for DB readiness, then runs `/usr/local/bin/init-taxi-data.py`
    - Keeps PostgreSQL running in foreground
- **PGAdmin Container**: `dpage/pgadmin4:latest` for web-based database management
- **Superset Container**: Custom Apache Superset build with PostgreSQL connectivity
  - **Configuration**: Custom `superset_config.py` with optimized settings
  - **Metadata Database**: SQLite for persistent dashboard/chart storage (`/app/superset_home/superset.db`)
  - **Initialization**: Automated database connection setup via `init-superset.sh`
  - **Dependencies**: Redis for caching and session management
- **Redis Container**: `redis:7-alpine` for Superset caching
- **Volume Mappings**:
  - `postgres_data`: PostgreSQL data persistence
  - `pgadmin_data`: PGAdmin settings persistence
  - `superset_data`: Superset configuration and dashboards
  - `redis_data`: Redis data persistence
  - `./sql-scripts:/sql-scripts`: SQL scripts and unified data location
  - `./logs:/sql-scripts/logs`: Persistent logging with organized folder structure

## Key Integration Points

### Initialization Process (`docker/init-data.py`)
**Critical Functions**:
- `wait_for_postgres()`: Ensures DB is ready before data loading
- `execute_sql_scripts()`: Runs all SQL files in `sql-scripts/init-scripts/` in order
- `download_taxi_zone_data()`: Downloads CSV and shapefile ZIP to unified location
- `load_taxi_zones()`: Dictionary table cleaning + CSV/shapefile processing + lookup table reloading
- `load_trip_data()`: Backfill processing with automatic downloads and chunked loading
- `verify_data_load()`: Final data integrity checks with sample queries

**Dictionary Table Management**:
- **Clean-and-reload pattern**: All reference tables (except `yellow_taxi_trips`) are cleaned before each backfill
- **Tables cleaned**: `taxi_zone_lookup`, `taxi_zone_shapes`, `rate_code_lookup`, `payment_type_lookup`, `vendor_lookup`
- **Automatic reloading**: Lookup tables repopulated after cleaning
- **Trip data preservation**: Only `yellow_taxi_trips` maintains data across runs

**Error Handling**:
- Column name case sensitivity: converts all to lowercase
- Numeric precision: schema uses DECIMAL(4,1) for ratecodeid, DECIMAL(4,1) for passenger_count
- NULL value handling: drops invalid taxi zone records, fills numeric NULLs with 0
- Chunked loading: 10K rows per chunk to manage memory usage
- Hash-based duplicate prevention: SHA-256 hashes prevent any duplicate rows

### Environment Configuration
`.env` file controls all service parameters:
```
POSTGRES_DB=playground
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin123
POSTGRES_PORT=5432
PGADMIN_EMAIL=admin@admin.com
PGADMIN_PASSWORD=admin123
PGADMIN_PORT=8080
DATA_CHUNK_SIZE=10000
INIT_LOAD_ALL_DATA=true

# Backfill Configuration Options:
BACKFILL_MONTHS=last_12_months      # Default: Load last 12 months
# BACKFILL_MONTHS=                  # Empty: Load only existing local files
# BACKFILL_MONTHS=2024-01           # Single month
# BACKFILL_MONTHS=2024-01,2024-02   # Specific months (comma-separated)
# BACKFILL_MONTHS=last_6_months     # Last 6 months
# BACKFILL_MONTHS=all               # All data (2020-2025, WARNING: Very large!)

# Superset Configuration
SUPERSET_PORT=8088
SUPERSET_ADMIN_USER=admin
SUPERSET_ADMIN_EMAIL=admin@admin.com
SUPERSET_ADMIN_FIRSTNAME=Admin
SUPERSET_ADMIN_LASTNAME=User
SUPERSET_ADMIN_PASSWORD=admin123
SUPERSET_LOAD_EXAMPLES=false
```

### SQL Script Execution Order
1. `00-postgis-setup.sql`: PostGIS extensions and spatial reference systems
2. `01-nyc-taxi-schema.sql`: Complete schema with lowercase column names
3. Python data loading via `docker/init-data.py`
4. Analytical queries available in `sql-scripts/reports-scripts/`

## Critical Implementation Details

### Data Loading Challenges & Solutions
**Common Issues During Initialization**:
- **Column case mismatch**: Parquet has `VendorID`, schema expects `vendorid` → Fixed by `df.columns = df.columns.str.lower()`
- **Numeric overflow**: Schema precision too small → Fixed by using `DECIMAL(4,1)` for ratecodeid/passenger_count
- **PostgreSQL timing**: Initialization runs before DB ready → Fixed with custom entrypoint + wait logic
- **NULL values**: Invalid taxi zone records → Fixed by dropping NULLs in required fields
- **Duplicate data**: Re-running system would create duplicates → Fixed with composite UNIQUE constraint and processing tracking

**Data Validation**:
- **Trip data**: 3,475,226 records (verified via `len(df)` check)
- **Zone data**: 263 zones after NULL cleanup (originally 265)
- **Geospatial**: CRS conversion from shapefile CRS to EPSG:2263 (NYC State Plane)
- **Duplicate Prevention**: Hash-based unique constraint prevents ANY duplicate row, processing log prevents month re-processing

### Duplicate Prevention System
**Four-Layer Protection**:
1. **Processing Tracking**: `data_processing_log` table tracks completed months
2. **Month-Level Skipping**: Already processed months automatically skipped
3. **Hash-Based Prevention**: SHA-256 hash of all row values prevents ANY duplicate row
4. **Graceful Error Handling**: Duplicate chunks logged as warnings, not errors

**Hash-Based System**:
- **SHA-256 Hash**: Calculated from all 20 data columns in deterministic manner
- **Collision Detection**: Within-batch hash collision detection and removal
- **Ultimate Protection**: Even minor data variations create different hashes
- **Performance**: Hash index allows fast duplicate detection at database level

**Safe Re-runs**: System can be restarted multiple times without data duplication or corruption

### Clean Initialization Pipeline
**Dictionary Table Pattern**: All tables except `yellow_taxi_trips` are treated as dictionary/reference tables:
1. **🧹 Clean**: TRUNCATE all reference tables at start of each backfill
2. **📥 Download**: Fresh download of CSV and shapefile ZIP from official sources
3. **📊 Load**: Process and load reference data into cleaned tables
4. **📚 Reload**: Repopulate lookup tables (rate codes, payment types, vendors)
5. **🚕 Process**: Load trip data (preserved across runs due to processing log tracking)

**Benefits**:
- **No duplicate key errors**: Reference tables always clean before loading
- **Fresh reference data**: Always uses latest official NYC TLC data
- **Preserved trip data**: Only new months are processed, existing data remains
- **Consistent state**: Every initialization produces identical reference tables

### Unified Data System
**Data Storage Location**: All data files organized in `/sql-scripts/data/` with logical subdirectories:

**Zone Data**: `/sql-scripts/data/zones/`
- **Zone CSV**: `taxi_zone_lookup.csv` (downloaded during each initialization)
- **Shapefile components**: `taxi_zones.{shp,dbf,shx,prj,sbn,sbx}` (extracted from ZIP during initialization)

**Trip Data**: `/sql-scripts/data/yellow/`
- **Trip data**: `yellow_tripdata_YYYY-MM.parquet` files (automatically downloaded via backfill)

**Schema files**: `sql-scripts/init-scripts/01-nyc-taxi-schema.sql` (must use lowercase column names)

**No Manual Data Management Required**: System automatically downloads all required data files from official NYC TLC sources into organized subdirectories

### Production Deployment Notes
- **First startup**: Takes 30-45 minutes to load 3.47M records
- **Subsequent startups**: Instant (data persisted in `postgres_data` volume)
- **Memory usage**: 10K row chunks prevent memory overflow during loading
- **Error recovery**: `docker-compose down -v` forces complete reload

## Access Points

### PGAdmin Web Interface
- **URL**: http://localhost:8080
- **Login**: admin@admin.com / admin123
- **Database Connection**: host=postgres, port=5432, db=playground, user=admin/admin123
- **Schema**: nyc_taxi
- **Pre-loaded queries**: Available in mounted `/var/lib/pgadmin/storage/sql-scripts/`

### Apache Superset Web Interface
- **URL**: http://localhost:8088
- **Login**: admin / admin123 (configurable via environment variables)
- **Features**:
  - Pre-configured PostgreSQL database connection
  - Interactive dashboards and visualizations
  - SQL Lab for advanced querying
  - Chart creation and sharing capabilities
  - **Persistent SQLite metadata database** for dashboard/chart storage
  - **Enhanced caching** with Redis for optimal performance
  - **Native filters and cross-filtering** for interactive dashboards
  - **Async query execution** for large datasets
  - **Advanced data types support** and tagging system
- **Database**: Automatically connected to playground database, nyc_taxi schema
- **Persistence**: All dashboards, charts, and user settings are saved in SQLite database (`/app/superset_home/superset.db`)
- **Performance**:
  - Connection pooling (20 core + 30 overflow connections)
  - Multi-tier Redis caching (5 separate databases for different cache types)
  - Query result caching up to 24 hours
  - Row limits: 5,000 default, 100,000 maximum

### Direct Database Access
```bash
# Command line access
docker exec -it sql-playground-postgres psql -U admin -d playground

# Quick data check
docker exec sql-playground-postgres psql -U admin -d playground -c "SELECT COUNT(*) FROM nyc_taxi.yellow_taxi_trips;"
```

### Sample Analytical Queries
- **Trip volume by hour**: Joins trips with taxi zone lookup for borough analysis
- **Geospatial analysis**: ST_Area calculations on taxi zone polygons
- **Cross-borough trips**: Origin-destination analysis using zone lookups
- **Payment analysis**: Credit card vs cash patterns by time and location

## Advanced Features

### Star Schema Implementation
The project includes a complete dimensional modeling implementation in `sql-scripts/model-scripts/`:

**Available Scripts**:
- `01-phase1-star-schema.sql`: Complete dimensional model with 6 dimension tables and fact table
- `02-phase2-partitioning.sql`: Monthly range partitioning with automated maintenance
- `03-phase3-performance-indexing.sql`: Advanced indexing strategy with partition-local and covering indexes
- `04-data-migration.sql`: Data transformation from normalized to star schema

**Implementation Order**:
```bash
# Execute in PostgreSQL container
docker exec -it sql-playground-postgres psql -U admin -d playground

# Phase 1: Create star schema
\i /sql-scripts/model-scripts/01-phase1-star-schema.sql

# Phase 2: Add partitioning
\i /sql-scripts/model-scripts/02-phase2-partitioning.sql

# Phase 3: Optimize with advanced indexes
\i /sql-scripts/model-scripts/03-phase3-performance-indexing.sql

# Phase 4: Migrate data to new schema
\i /sql-scripts/model-scripts/04-data-migration.sql
SELECT migrate_taxi_data_to_star_schema(50000);
```

**Benefits**:
- **67-100x performance improvement** for analytical queries
- **Monthly partitioning** for efficient data management
- **Business-friendly** dimension tables with hierarchies
- **Automated maintenance** functions for production use

### SQL Technical Interview Resources
The project includes comprehensive interview preparation materials:

**Interview Questions**: `docs/interviews/sql-interview-questions.md`
- **15 detailed questions** covering data modeling, ETL, performance optimization
- **Multi-level difficulty** suitable for mid to senior SQL developers
- **Production scenarios** based on real NYC taxi data challenges
- **Complete answers** with explanations and alternative approaches

**Categories Covered**:
- Data Modeling & Schema Design
- Data Ingestion & ETL Pipeline Design
- Performance & Query Optimization
- Complex Analytics & Window Functions
- Geospatial Analysis with PostGIS
- Data Quality & Integrity
- System Architecture & Scalability

**Usage for Interview Prep**:
```bash
# Access interview questions
cat docs/interviews/sql-interview-questions.md

# Practice with real data in PGAdmin or Superset
# All questions use the actual NYC taxi dataset
```

## Common Development Workflows

### Initial Setup and Data Loading
```bash
# 1. Start environment (first time - includes data loading)
docker-compose up -d --build

# 2. Monitor initialization progress
docker logs -f sql-playground-postgres

# 3. Check data loading completion
docker exec sql-playground-postgres psql -U admin -d playground -c "SELECT COUNT(*) FROM nyc_taxi.yellow_taxi_trips; SELECT * FROM nyc_taxi.data_processing_log ORDER BY data_year, data_month;"

# 4. Access web interfaces
# PGAdmin: http://localhost:8080
# Superset: http://localhost:8088
```

### Working with Star Schema Models
```bash
# 1. Implement dimensional model
docker exec -it sql-playground-postgres psql -U admin -d playground
\i /sql-scripts/model-scripts/01-phase1-star-schema.sql

# 2. Add partitioning for performance
\i /sql-scripts/model-scripts/02-phase2-partitioning.sql

# 3. Optimize with advanced indexes
\i /sql-scripts/model-scripts/03-phase3-performance-indexing.sql

# 4. Migrate data to new schema
\i /sql-scripts/model-scripts/04-data-migration.sql
SELECT migrate_taxi_data_to_star_schema(50000);

# 5. Test performance improvements
\i /sql-scripts/model-scripts/examples/sample-queries.sql
```

### Troubleshooting Common Issues

**Data Loading Issues**:
```bash
# Check if data loading is still in progress
docker logs sql-playground-postgres | grep "Processing"

# Check for errors in data loading
docker logs sql-playground-postgres | grep -i error

# Reset and reload all data
docker-compose down -v && docker-compose up -d --build
```

**Superset Connection Issues**:
```bash
# Check Superset logs
docker logs sql-playground-superset

# Restart Superset if needed
docker-compose restart superset

# Check if Redis is accessible (used for caching)
docker exec sql-playground-superset redis-cli -h redis ping

# Verify SQLite metadata database exists
docker exec sql-playground-superset ls -la /app/superset_home/

# Check Redis cache usage across all databases
docker exec sql-playground-superset redis-cli -h redis info keyspace

# Monitor cache performance
docker exec sql-playground-superset redis-cli -h redis --scan --pattern "superset_*" | head -10
```

**Performance Debugging**:
```bash
# Check active connections and queries
docker exec sql-playground-postgres psql -U admin -d playground -c "SELECT pid, usename, application_name, state, query FROM pg_stat_activity WHERE state = 'active';"

# Analyze query performance
docker exec sql-playground-postgres psql -U admin -d playground -c "EXPLAIN ANALYZE SELECT * FROM nyc_taxi.yellow_taxi_trips LIMIT 100;"

# Check index usage
docker exec sql-playground-postgres psql -U admin -d playground -c "SELECT schemaname, tablename, indexname, idx_scan, idx_tup_read, idx_tup_fetch FROM pg_stat_user_indexes ORDER BY idx_scan DESC;"
```

### Development Best Practices

**Schema Changes**:
- Always test schema changes on a copy of production data first
- Use transactions for complex multi-statement operations
- Check for existing data before dropping/recreating tables

**Performance Testing**:
- Compare query performance before/after schema changes
- Use `EXPLAIN ANALYZE` to understand query execution plans
- Monitor resource usage during large data operations

**Data Management**:
- Use the `data_processing_log` table to track which months are loaded
- Implement proper error handling for new data processing scripts
- Consider the hash-based duplicate prevention system when adding new data