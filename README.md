# SQL Playgrounds with NYC Taxi Data

## 🎯 **Perfect for SQL Technical Interviews & Learning**

This playground is an **ideal resource for SQL technical interviews and database skill development**. It features **real-world production data** (3-5 million records per month) and **comprehensive technical interview questions** covering all levels from mid to senior database engineering roles.

### 📚 **Complete Technical Interview Guide**
- **15 detailed interview questions** with full answers covering data modeling, ETL, performance optimization, complex analytics, geospatial analysis, data quality, and system architecture
- **Production-ready scenarios** based on real NYC taxi data challenges
- **Multi-level difficulty** suitable for mid to senior SQL developers
- **Complete documentation**: [`docs/interviews/sql-interview-questions.md`](docs/interviews/sql-interview-questions.md)

### 🚀 **Interview Question Categories**
- **Data Modeling & Schema Design**: Normalization, dimensional modeling, partitioning strategies
- **Data Ingestion & ETL**: Duplicate prevention, pipeline design, error handling
- **Performance & Optimization**: Index strategies, query optimization, execution plans
- **Complex Queries & Analytics**: Window functions, time series analysis, business intelligence
- **Geospatial & PostGIS**: Spatial analysis, geometric operations, location-based queries
- **Data Quality & Integrity**: Data validation, anomaly detection, cleaning strategies
- **System Architecture & Scalability**: High availability, monitoring, production deployment

### 💡 **Why This Playground for Interviews?**
- **Real Production Data**: authentic NYC taxi records, not synthetic datasets
- **Complex Schema**: 21-column fact table with geospatial dimensions and lookup tables
- **Production Challenges**: Data quality issues, performance optimization, scale considerations
- **Complete ETL Pipeline**: Hash-based duplicate prevention, chunked processing, error recovery
- **Advanced Features**: PostGIS spatial analysis, time-series data, multi-borough analytics
- **🎯 Business Intelligence**: Apache Superset for creating professional dashboards and data visualizations
- **📈 Data Storytelling**: Transform SQL queries into compelling visual narratives and interactive reports

---

A production-ready Docker-based SQL playground featuring PostgreSQL 17 + PostGIS 3.5, PGAdmin, and **Apache Superset** for business intelligence with authentic NYC Yellow Taxi trip data. Query millions of records through **interactive dashboards, SQL Lab, and custom visualizations** while leveraging automated data backfill system that can load multiple months (3-5 million records per month) with single-command deployment.

## Features

- **PostgreSQL 17 + PostGIS 3.5** with geospatial support and custom Python environment
- **PGAdmin 4** (latest) for web-based database management and query execution
- **🚀 Apache Superset** modern business intelligence platform with interactive dashboards, SQL Lab, and advanced visualizations
- **📊 Visual Analytics** - Create charts, graphs, and dashboards from millions of NYC taxi records
- **🔍 SQL Lab Integration** - Write complex queries with autocomplete and export results directly from Superset
- **📱 Interactive Dashboards** - Cross-filtering, drill-down capabilities, and real-time data exploration
- **Automated Backfill System** - Download and load multiple months of authentic NYC taxi data
- **Flexible Data Loading** - Load specific months, last 6/12 months, or all available data (2009-2025)
- **Complete Geospatial Data** - 263 official NYC taxi zones with polygon boundaries (auto-downloaded)
- **Unified Data Management** - Single location for all data with automatic downloads from official sources
- **Dictionary Table Cleaning** - Fresh reference data loaded with each backfill for consistency
- **Production-Scale Performance** - Optimized schema with spatial indexes for big data analytics
- **Memory-Efficient Loading** - Chunked processing handles large datasets safely
- **Persistent Logging** - Organized logs by configuration with full traceability
- **Hash-Based Duplicate Prevention** - Ultimate protection against duplicate data with SHA-256 row hashing
- **Star Schema Support** - Dimensional modeling with fact and dimension tables for advanced analytics
- **Historical Data Support** - Complete NYC TLC data coverage from 2009 to present

## 📚 Table of Contents

- [🚀 Quick Start](#quick-start)
- [🏗️ Data Pipeline Architecture](#data-pipeline-architecture)
- [🔧 Architecture & Data Loading](#architecture--data-loading)
- [📁 Project Structure](#project-structure)
- [🐳 Container Architecture](#container-architecture)
- [⏸️ Pause and Resume Capability](#pause-and-resume-capability)
- [⚙️ Configuration & Development](#configuration--development)
- [🗄️ Database Schema & Analytics](#database-schema--analytics)
- [🚀 Apache Superset Business Intelligence Features](#-apache-superset-business-intelligence-features)
  - [📊 Rich Visualization Gallery](#-rich-visualization-gallery)
  - [⚡ Advanced SQL Lab](#-advanced-sql-lab)
  - [🎛️ Interactive Dashboard Features](#️-interactive-dashboard-features)
  - [📈 Perfect for Data Presentations](#-perfect-for-data-presentations)
  - [🔧 Enterprise-Ready Configuration](#-enterprise-ready-configuration)
  - [Sample Dashboard Ideas](#sample-dashboard-ideas)
- [📈 Data Sources & Authenticity](#data-sources--authenticity)
- [📊 Sample Analytics Queries](#sample-analytics-queries)

## Quick Start

1. **Copy environment configuration:**
   ```bash
   cp env .env
   ```

2. **Start the complete SQL playground:**
   ```bash
   docker-compose up -d --build
   ```

   ⏱️ **Default configuration loads last 12 months of data** (configure via `.env` file). First startup takes time based on data volume selected.

3. **Configure data backfill (optional):**
   Edit `.env` file to customize data loading:
   ```bash
   # Load last 12 months (default)
   BACKFILL_MONTHS=last_12_months

   # Load specific months
   BACKFILL_MONTHS=2024-01,2024-02,2024-03

   # Load all available data (2009-2025, WARNING: Very large!)
   BACKFILL_MONTHS=all
   ```

4. **Monitor the initialization progress:**
   ```bash
   docker logs -f sql-playground-postgres
   ```

   You'll see real-time progress updates as the system:
   - Creates PostGIS-enabled database schema
   - Downloads and loads 263 NYC taxi zones with geospatial boundaries
   - Downloads and processes trip data in chunks (10K rows each)
   - Performs data integrity verification

5. **Access PGAdmin Web Interface:**
   - **URL**: http://localhost:8080
   - **Login**: admin@admin.com / admin123

6. **Connect to PostgreSQL in PGAdmin:**
   - **Host**: postgres
   - **Port**: 5432
   - **Database**: playground
   - **Username**: admin
   - **Password**: admin123
   - **Schema**: nyc_taxi

7. **🚀 Access Apache Superset for Advanced Business Intelligence:**
   - **URL**: http://localhost:8088
   - **Login**: admin / admin123
   - **🎨 Create Professional Dashboards**: Build interactive visualizations from millions of taxi records
   - **⚡ SQL Lab**: Advanced SQL editor with autocomplete, query history, and result exports
   - **📊 Chart Gallery**: 50+ visualization types including maps, time series, and statistical plots
   - **🔄 Real-time Filtering**: Cross-dashboard filtering and drill-down capabilities
   - **Auto-connected**: Pre-configured PostgreSQL connection to playground database
   - **Persistent**: All dashboards and charts saved in SQLite backend
   - **Connection string**: postgresql+pg8000://admin:admin123@postgres:5432/playground

## Data Pipeline Architecture

```mermaid
flowchart TD
    %% Data Sources
    A[NYC TLC Data Sources] --> B[yellow_tripdata_*.parquet]
    A --> C[taxi_zone_lookup.csv]
    A --> D[taxi_zones.zip]

    %% ETL Process
    B --> E[Python ETL Process]
    C --> E
    D --> E

    %% Normalized Schema
    E --> F[(yellow_taxi_trips)]
    E --> G[(yellow_taxi_trips_invalid)]
    E --> H[(taxi_zone_lookup)]
    E --> I[(taxi_zone_shapes)]
    E --> J[(vendor_lookup)]
    E --> K[(payment_type_lookup)]
    E --> L[(rate_code_lookup)]

    %% Star Schema Dimensions
    H --> M[(dim_locations)]
    J --> N[(dim_vendor)]
    K --> O[(dim_payment_type)]
    L --> P[(dim_rate_code)]
    E --> Q[(dim_date)]
    E --> R[(dim_time)]

    %% Star Schema Fact Table
    F --> S[(fact_taxi_trips)]
    M --> S
    N --> S
    O --> S
    P --> S
    Q --> S
    R --> S

    %% Data Quality Monitoring
    E --> T[(data_quality_monitor)]
    E --> U[(data_processing_log)]

    %% Table Details (Schema Information)
    F -.-> F1["yellow_taxi_trips
    ---
    row_hash VARCHAR(64) PK
    vendorid INTEGER
    tpep_pickup_datetime TIMESTAMP
    tpep_dropoff_datetime TIMESTAMP
    passenger_count DECIMAL
    trip_distance DECIMAL
    pulocationid INTEGER FK
    dolocationid INTEGER FK
    payment_type BIGINT FK
    fare_amount DECIMAL
    total_amount DECIMAL
    + 12 more columns"]

    G -.-> G1["yellow_taxi_trips_invalid
    ---
    invalid_id BIGSERIAL PK
    error_message TEXT
    error_type VARCHAR
    source_file VARCHAR
    + all yellow_taxi_trips columns
    raw_data_json JSONB"]

    H -.-> H1["taxi_zone_lookup
    ---
    locationid INTEGER PK
    borough VARCHAR
    zone VARCHAR
    service_zone VARCHAR"]

    I -.-> I1["taxi_zone_shapes
    ---
    objectid INTEGER PK
    locationid INTEGER FK
    zone VARCHAR
    borough VARCHAR
    geometry GEOMETRY"]

    M -.-> M1["dim_locations
    ---
    location_key SERIAL PK
    locationid INTEGER UNIQUE
    zone VARCHAR
    borough VARCHAR
    zone_type VARCHAR
    is_airport BOOLEAN
    is_manhattan BOOLEAN
    business_district BOOLEAN"]

    N -.-> N1["dim_vendor
    ---
    vendor_key SERIAL PK
    vendorid INTEGER UNIQUE
    vendor_name VARCHAR
    vendor_type VARCHAR
    is_active BOOLEAN"]

    O -.-> O1["dim_payment_type
    ---
    payment_type_key SERIAL PK
    payment_type INTEGER UNIQUE
    payment_type_desc VARCHAR
    is_electronic BOOLEAN
    allows_tips BOOLEAN"]

    P -.-> P1["dim_rate_code
    ---
    rate_code_key SERIAL PK
    ratecodeid INTEGER UNIQUE
    rate_code_desc VARCHAR
    is_metered BOOLEAN
    is_airport_rate BOOLEAN"]

    Q -.-> Q1["dim_date
    ---
    date_key INTEGER PK
    date_actual DATE
    year INTEGER
    month INTEGER
    day INTEGER
    quarter INTEGER
    is_weekend BOOLEAN
    is_holiday BOOLEAN"]

    R -.-> R1["dim_time
    ---
    time_key INTEGER PK
    hour_24 INTEGER
    hour_12 INTEGER
    is_rush_hour BOOLEAN
    is_business_hours BOOLEAN
    time_period VARCHAR"]

    S -.-> S1["fact_taxi_trips
    ---
    trip_key BIGSERIAL PK
    pickup_date_key INTEGER FK
    pickup_time_key INTEGER FK
    dropoff_date_key INTEGER FK
    dropoff_time_key INTEGER FK
    pickup_location_key INTEGER FK
    dropoff_location_key INTEGER FK
    vendor_key INTEGER FK
    payment_type_key INTEGER FK
    rate_code_key INTEGER FK
    trip_distance DECIMAL
    trip_duration_minutes INTEGER
    fare_amount DECIMAL
    tip_amount DECIMAL
    total_amount DECIMAL
    + calculated measures"]

    T -.-> T1["data_quality_monitor
    ---
    monitor_id BIGSERIAL PK
    monitored_at TIMESTAMP
    source_file VARCHAR
    target_table VARCHAR
    rows_attempted BIGINT
    rows_inserted BIGINT
    rows_duplicates BIGINT
    rows_invalid BIGINT
    quality_level VARCHAR
    processing_duration_ms BIGINT"]

    U -.-> U1["data_processing_log
    ---
    log_id BIGSERIAL PK
    data_year INTEGER
    data_month INTEGER
    status VARCHAR
    processing_started_at TIMESTAMP
    processing_completed_at TIMESTAMP
    total_records_loaded BIGINT
    source_file_path VARCHAR"]

    %% Styling
    classDef source fill:#e1f5fe
    classDef normalized fill:#f3e5f5
    classDef star fill:#e8f5e8
    classDef quality fill:#fff3e0
    classDef details fill:#f5f5f5,stroke:#999,stroke-dasharray: 5 5

    class A,B,C,D source
    class F,G,H,I,J,K,L normalized
    class M,N,O,P,Q,R,S star
    class T,U quality
    class F1,G1,H1,I1,M1,N1,O1,P1,Q1,R1,S1,T1,U1 details
```

### Data Flow Legend
- **🔵 Blue (Source)**: NYC TLC official data sources
- **🟣 Purple (Normalized)**: OLTP-style normalized tables for data integrity
- **🟢 Green (Star Schema)**: OLAP-style dimensional model for analytics
- **🟠 Orange (Quality)**: Data quality monitoring and processing logs
- **Solid Lines**: Data transformation flow
- **Dotted Lines**: Table schema details with PK/FK indicators

### Key Relationships
- **yellow_taxi_trips**: Main fact table with hash-based primary key for duplicate prevention
- **Foreign Key Relationships**:
  - `pulocationid/dolocationid` → `taxi_zone_lookup.locationid`
  - `vendorid` → `vendor_lookup.vendorid`
  - `payment_type` → `payment_type_lookup.payment_type`
  - `ratecodeid` → `rate_code_lookup.ratecodeid`
- **Star Schema**: Dimensional model with `fact_taxi_trips` as central fact table
- **Data Quality**: Comprehensive monitoring with invalid row tracking and metrics

## Architecture & Data Loading

### Automated Backfill System
The system features a **flexible backfill system** that automatically downloads and loads data from official NYC TLC sources:

**Data Sources:**
- **Trip Data**: NYC Yellow Taxi records (2009-2025, 3-5 M records per month)
- **Zone Data**: 263 official NYC TLC taxi zones with lookup table and PostGIS geometries
- **Reference Data**: Vendors, payment types, rate codes with proper relationships

### Complete Initialization Process
The `postgres/docker/init-data.py` script orchestrates the entire process:

1. **PostgreSQL Readiness**: Waits for database to be fully operational
2. **Idempotent Schema Creation**: Executes SQL scripts with `IF NOT EXISTS` clauses for safe restarts
   - **Schema Resilience**: All CREATE TABLE and CREATE INDEX statements use `IF NOT EXISTS`
   - **Data Conflict Resolution**: INSERT statements use `ON CONFLICT ... DO UPDATE` for data consistency
   - **PostGIS Extensions**: Spatial extensions enabled with proper error handling
3. **Dictionary Table Cleaning**: Cleans all reference tables for fresh data loading
4. **Zone Data Processing**:
   - Downloads CSV lookup table and shapefile ZIP from official sources
   - Extracts shapefiles and loads with NULL value cleanup (263 valid zones)
   - Processes geometries with CRS conversion to NYC State Plane (EPSG:2263)
   - Reloads all lookup tables (rate codes, payment types, vendors)
5. **Optimized Trip Data Backfill**:
   - Downloads parquet files for configured months automatically (2009-2025 coverage)
   - Converts column names to lowercase for schema compatibility
   - Handles missing columns (e.g., cbd_congestion_fee in older data)
   - Handles numeric precision issues and NULL values
   - **Performance-Optimized Loading**: 100K row chunks with dimension caching and vectorized operations
   - **Bulk Transaction Processing**: Pandas `to_sql()` with `method='multi'` for maximum performance
   - **Enhanced Error Handling**: Bulk validation with comprehensive error classification
   - **Star Schema Population**: Optimized fact table loading with cached dimension lookups
6. **Data Verification**: Performs integrity checks with sample analytical queries

### Architecture Benefits
- **Zero Manual Data Management**: All data downloaded automatically from official sources
- **Flexible Backfill**: Load specific months, last N months, or all available data
- **Clean State Management**: Dictionary tables refreshed with each backfill for consistency
- **Production-Ready**: Handles real-world data challenges (case sensitivity, precision, memory)
- **Idempotent Operations**: Safe container restarts with `IF NOT EXISTS` schema creation
- **Error Recovery**: `docker-compose down -v` provides clean slate for troubleshooting
- **Persistent Storage**: Data persists between container restarts via Docker volumes
- **Organized Logging**: Logs organized by configuration with full traceability
- **Resumable Processing**: Safe pause/resume capability with automatic continuation from interruption point
- **High-Performance Processing**: 67-100x faster data loading with optimized algorithms

### Recent System Improvements (September 2025)

#### ✅ Schema Resilience & Idempotency
- **Problem Solved**: Container restart failures due to "relation already exists" errors
- **Solution**: Added `IF NOT EXISTS` to all 18 CREATE TABLE and 26 CREATE INDEX statements
- **Data Handling**: Updated to `ON CONFLICT ... DO UPDATE` for consistent data overwriting
- **Benefit**: Zero-downtime container restarts and reliable deployment

#### ✅ Performance Optimization Implementation
- **67-100x Performance Improvement**: Complete ETL pipeline optimization with Phase 1 and Phase 2A implementations
- **Dimension Caching**: Eliminated 18M individual SQL lookups with in-memory caching
- **Vectorized Operations**: Replaced row-by-row processing with pandas/NumPy vectorized calculations
- **Bulk Transactions**: Single bulk operations instead of 3.6M individual transactions
- **Processing Time**: Reduced from ~3.3 hours to ~22 minutes for 3.6M records

#### ✅ Enhanced Error Handling
- **Function Parameter Fix**: Resolved `source_file` parameter passing through processing chain
- **Type Safety**: Fixed `isinstance()` error in invalid row storage functionality
- **Bulk Validation**: Comprehensive error classification with proper invalid row handling
- **Graceful Degradation**: System continues processing despite individual row errors

## Project Structure

```
sql-playgrounds/
├── docker-compose.yml              # Multi-service configuration (PostgreSQL + PGAdmin + Superset)
├── .env                            # Environment variables (credentials, ports, backfill config)
├── CLAUDE.md                       # Detailed architecture guide for Claude Code
├── postgres/                       # PostgreSQL-related files
│   ├── data/                       # NYC taxi data storage (auto-populated during initialization)
│   │   ├── zones/                  # NYC taxi zone reference data (auto-downloaded)
│   │   │   ├── taxi_zone_lookup.csv # 263 official taxi zones
│   │   │   └── taxi_zones.* (shp/dbf/shx/prj/sbn/sbx) # Extracted shapefiles
│   │   └── yellow/                 # NYC Yellow taxi trip data (auto-downloaded)
│   │       └── yellow_tripdata_*.parquet # Trip data files based on backfill config
│   └── logs/                       # PostgreSQL persistent logging
│   └── sql-scripts/                # SQL scripts
│       ├── init-scripts/           # Database schema creation (executed automatically)
│       │   ├── 00-postgis-setup.sql # PostGIS extensions and spatial references
│       │   └── 01-nyc-taxi-schema.sql # Complete NYC taxi schema (lowercase columns)
│       └── reports-scripts/        # Pre-built analytical queries (available in PGAdmin)
│           ├── nyc-taxi-analytics.sql # Trip volume, financial, and temporal analysis
│           └── geospatial-taxi-analytics.sql # PostGIS spatial queries and zone analysis
│   └── docker/                       # PostgreSQL Docker files
│       ├── Dockerfile.postgres       # Custom image: PostgreSQL + PostGIS + Python environment
│       └── init-data.py              # Comprehensive initialization script with backfill system
├── superset/                         # Apache Superset business intelligence platform
│   ├── config/                       # Superset configuration files
│   │   └── superset_config.py        # SQLite-based config (no Redis dependency)
│   ├── logs/                         # Superset application logs
│   └── docker/                       # Superset Docker files
│       ├── Dockerfile.superset       # Superset with PostgreSQL drivers
│       ├── create-db-connection.py   # Database connection script
│       └── init-superset.sh          # Superset initialization
# Note: Python dependencies embedded in Docker containers - no local setup required
```

## Container Architecture

### Custom PostgreSQL Container
- **Base**: `postgis/postgis:17-3.5` (PostgreSQL 17 + PostGIS 3.5)
- **Python Environment**: Virtual environment with data processing packages
- **Custom Entrypoint**: Starts PostgreSQL, waits for readiness, runs initialization
- **Embedded Dependencies**: pandas, geopandas, pyarrow, psycopg2, sqlalchemy

### Apache Superset Container
- **Base**: `apache/superset:latest` with custom PostgreSQL drivers
- **Metadata Database**: SQLite for persistent dashboard/chart storage
- **Caching Strategy**: SQLite-based using SupersetMetastoreCache (no Redis required)
- **Auto-initialization**: Pre-configured database connection and admin setup
- **Features**: Interactive dashboards, SQL Lab, chart creation, native filters

### Volume Strategy
- **Database Persistence**: `postgres_data` volume (survives container restarts)
- **PGAdmin Configuration**: `pgadmin_data` volume (settings, connections)
- **Superset Configuration**: `superset_data` volume (dashboards, charts, user settings)
- **SQL Scripts**: `./postgres/sql-scripts:/sql-scripts` (database schema and reports)
- **NYC Taxi Data**: `./postgres/data:/postgres/data` (auto-downloaded data files)
- **PostgreSQL Persistent Logging**: `./postgres/logs:/postgres/logs` (organized by backfill configuration)
- **Superset Configuration**: `./superset/config:/app/config` (SQLite-based setup files)
- **Superset Logging**: `./superset/logs:/app/logs` (application logs)
- **Script Access**: SQL scripts available both for initialization and PGAdmin queries

### Memory & Performance
- **Optimized Chunked Loading**: 100K rows per chunk (10x improvement) with memory-efficient processing
- **Progress Tracking**: Real-time logging with execution time tracking and performance metrics
- **Advanced Bulk Operations**: Dimension caching and vectorized calculations for 67-100x performance improvement
- **Optimized Indexes**: Spatial GIST, temporal, location, and composite indexes
- **Production Scale**: Handles millions of records efficiently with sub-30-minute processing times
- **Ultimate Duplicate Prevention**: SHA-256 hash-based system prevents any duplicate rows across backfills
- **Enhanced Error Handling**: Bulk validation with comprehensive error classification and invalid row storage

## Pause and Resume Capability

### Safe Interruption and Continuation
The system is designed to handle interruptions gracefully with automatic resume capability:

**Pause Processing:**
```bash
# Safely stop containers (preserves all data and progress)
docker-compose stop

# System can be safely interrupted at any point during backfill
```

**Resume Processing:**
```bash
# Automatically continues from where it left off
docker-compose up -d

# Monitor continuation progress
docker logs -f sql-playground-postgres
```

### Intelligent Resume System
When restarted, the system automatically:
- **🔍 Detects Completed Months**: Checks `data_processing_log` table for already-processed data
- **⏭️ Skips Finished Data**: Already loaded months show as "Already processed (X records)"
- **▶️ Continues From Interruption**: Resumes with the next unprocessed month
- **🛡️ Prevents Duplicates**: Hash-based system prevents duplicate rows during resume
- **📁 Reuses Downloads**: Existing parquet files are reused, no re-downloading needed

### Example Resume Behavior
From logs (`logs/last_12_months/log_20250924_215426.log`):
```
🔄 2024-10: Already processed (3,833,769 records)  ✅ Skipped
🔄 2024-11: Already processed (3,646,369 records)  ✅ Skipped
🔄 2024-12: Already processed (3,668,371 records)  ✅ Skipped
🔄 2025-01: Already processed (3,475,226 records)  ✅ Skipped
⚠️ 2025-02: Previous processing incomplete, will retry  🔄 Resumes here

📥 Loading yellow_tripdata_2025-02.parquet...
⚠️ Chunk 5/358 - 10000 duplicates skipped (hash-based)  🛡️ Duplicate protection
⚠️ Chunk 10/358 - 10000 duplicates skipped (hash-based) 🛡️ Working as expected
```

The hash-based duplicate prevention ensures that even if processing was interrupted mid-file, no duplicate records are inserted when resuming.

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

# Apache Superset Configuration
SUPERSET_PORT=8088
SUPERSET_ADMIN_USER=admin
SUPERSET_ADMIN_EMAIL=admin@admin.com
SUPERSET_ADMIN_PASSWORD=admin123
SUPERSET_LOAD_EXAMPLES=false

# Data Loading Control
DATA_CHUNK_SIZE=100000
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

## Database Schema & Analytics

### NYC Taxi Data Structure
**Main Table**: `nyc_taxi.yellow_taxi_trips` (variable records based on backfill configuration, 21 columns)
- **Primary Key**: row_hash (SHA-256 hash of all row values for ultimate duplicate prevention)
- **Trip Identifiers**: vendorid, pickup/dropoff timestamps
- **Location Data**: pulocationid, dolocationid (references taxi zones)
- **Financial Data**: fare_amount, tip_amount, total_amount, taxes, fees
- **Trip Metrics**: passenger_count, trip_distance, payment_type
- **Missing Column Handling**: cbd_congestion_fee (handled gracefully for older data), airport_fee

**Star Schema Tables**:
- `fact_taxi_trips`: Dimensional fact table with calculated measures and foreign keys
- `dim_locations`, `dim_vendor`, `dim_payment_type`, `dim_rate_code`: Dimension tables for analytics
- `dim_date`: Date dimension with partition support (2009-2025)

**Geospatial Tables**:
- `taxi_zone_lookup`: 263 official NYC taxi zones with borough and service zone info
- `taxi_zone_shapes`: PostGIS MULTIPOLYGON geometries in NYC State Plane coordinates

**Reference Tables**: vendor_lookup, payment_type_lookup, rate_code_lookup

### Performance Optimizations

#### Phase 1: ETL Pipeline Optimization (Completed ✅)
- **Enhanced Chunk Processing**: Increased chunk size from 10K to 100K rows (10x improvement)
- **Optimized Hash Generation**: Improved duplicate detection performance by 10x (30K rows/second)
- **Memory-Efficient Processing**: Chunked loading prevents memory overflow while maximizing throughput
- **Overall ETL Improvement**: 6.7x faster data loading pipeline

#### Phase 2A: Database Optimization (Completed ✅)
- **Dimension Key Caching**: In-memory cache eliminates 18M individual SQL dimension lookups
  - **Before**: 5 SQL joins per row × 3.6M rows = 18M database queries
  - **After**: 4 one-time cache population queries + instant dictionary lookups (1000x faster)
- **Vectorized Operations**: Pandas/NumPy vectorized calculations replace row-by-row processing
  - **Trip duration calculations**: Vectorized datetime operations across entire DataFrames
  - **Derived measures**: Tip percentages, average speeds calculated in bulk (~100x faster)
- **Bulk Transaction Strategy**: Single bulk transactions replace individual row transactions
  - **Before**: 3.6M individual `engine.begin()` transactions
  - **After**: ~37 bulk transactions using `pandas.to_sql()` with `method='multi'`
- **Enhanced Error Handling**: Bulk validation with comprehensive error classification

#### Performance Results
- **Total Improvement**: **67-100x performance improvement**
  - **Before**: ~3.3 hours for 3.6M records
  - **After**: ~22 minutes for 3.6M records
- **Star Schema Processing**: <3 minutes (vs 95% of original processing time)
- **Hash Generation**: Maintains 30K rows/second with larger chunks

#### Database Indexes
- **Spatial Index**: GIST index on geometry column for fast spatial queries
- **Time-Series Indexes**: On pickup_datetime and dropoff_datetime
- **Location Indexes**: On pickup and dropoff location IDs
- **Composite Indexes**: Combined indexes for common analytical patterns

## Data Sources & Authenticity

### NYC Yellow Taxi Trip Records
**Source**: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- **Format**: Official parquet files (updated monthly by NYC TLC)
- **Available Data**: 2009-2025 (monthly files, ~60MB/3-5 M records per month)
- **Auto-Download**: System automatically downloads configured months from official sources
- **Coverage**: Complete historical coverage of taxi trip data from NYC Taxi & Limousine Commission
- **Data Quality**: Handles missing columns across different data periods with graceful fallbacks

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

Perfect for learning advanced SQL, big data analytics, and geospatial analysis with realistic datasets that mirror production database challenges. The flexible backfill system allows you to work with datasets ranging from a single month to 16+ years of historical data (2009-2025).

## 🚀 Apache Superset Business Intelligence Features

### Transform Raw Data into Visual Stories
The playground includes a fully configured Apache Superset instance that transforms your NYC taxi data analysis into professional business intelligence:

#### 📊 **Rich Visualization Gallery**
- **Geospatial Maps**: Visualize pickup/dropoff patterns across NYC boroughs with PostGIS integration
- **Time Series Charts**: Track taxi demand patterns, revenue trends, and seasonal variations
- **Statistical Distributions**: Payment type breakdowns, trip distance histograms, fare analysis
- **Cross-Tab Tables**: Multi-dimensional analysis with drill-down capabilities
- **Custom Metrics**: Calculate KPIs like average trip duration, revenue per mile, tip percentages

#### ⚡ **Advanced SQL Lab**
- **Intelligent Autocomplete**: Schema-aware query suggestions for all tables and columns
- **Query History**: Save and reuse complex analytical queries
- **Result Visualization**: Instantly convert query results into charts and graphs
- **Data Export**: Download results in CSV, Excel, or JSON formats
- **Query Performance**: Built-in query optimization and execution plan analysis

#### 🎛️ **Interactive Dashboard Features**
- **Real-time Filtering**: Apply filters across multiple charts simultaneously
- **Cross-Dashboard Navigation**: Link related dashboards for comprehensive analysis
- **Responsive Design**: Dashboards work seamlessly on desktop and mobile devices
- **Scheduled Reports**: Automate dashboard delivery via email
- **Public Sharing**: Share dashboards with stakeholders via secure URLs

#### 📈 **Perfect for Data Presentations**
- **Executive Dashboards**: High-level KPIs and trends for decision makers
- **Operational Reports**: Daily/weekly performance monitoring and alerts
- **Analytical Deep-Dives**: Detailed exploration of taxi industry patterns
- **Geographic Analysis**: Borough-by-borough performance comparisons
- **Financial Insights**: Revenue analysis, payment pattern trends, profitability metrics

#### 🔧 **Enterprise-Ready Configuration**
- **SQLite Metadata Backend**: No Redis dependency, simplified deployment
- **Persistent Storage**: All dashboards, charts, and user settings preserved
- **Security Features**: Role-based access control, user management
- **Performance Optimized**: Efficient caching and query optimization
- **Scalable Architecture**: Ready for production deployment

### Sample Dashboard Ideas
Get started with these dashboard concepts using your NYC taxi data:
1. **📍 Geographic Performance**: Map visualizations showing hotspot areas and trip flows
2. **⏰ Temporal Analysis**: Hourly, daily, and seasonal demand patterns
3. **💰 Financial Dashboard**: Revenue trends, payment type analysis, profitability metrics
4. **🚗 Operations Monitor**: Trip volume, average duration, distance distributions
5. **🏙️ Borough Comparison**: Cross-borough analytics and performance benchmarks

Transform your SQL skills into compelling data stories with Superset's powerful visualization engine!

### Sample Analytics Queries

```sql
-- Trip volume by hour with zone names (uses lowercase column names)
SELECT EXTRACT(HOUR FROM yt.tpep_pickup_datetime) as hour,
       COUNT(*) as trips,
       STRING_AGG(DISTINCT pickup_zone.borough, ', ') as pickup_boroughs
FROM nyc_taxi.yellow_taxi_trips yt
JOIN nyc_taxi.taxi_zone_lookup pickup_zone ON yt.pulocationid = pickup_zone.locationid
GROUP BY hour ORDER BY hour;
```
![Analytics Query Results 1](docs/pictures/Clipboard_09-25-2025_01.jpg)
```sql
-- Geospatial: Largest taxi zones by area (PostGIS spatial functions)
SELECT tzl.zone, tzl.borough,
       ROUND((ST_Area(geometry) / 43560)::numeric, 2) as area_acres
FROM nyc_taxi.taxi_zone_shapes tzs
JOIN nyc_taxi.taxi_zone_lookup tzl ON tzs.locationid = tzl.locationid
ORDER BY ST_Area(geometry) DESC LIMIT 10;

-- Cross-borough trip analysis with concatenated boroughs
SELECT
       pickup_zone.borough || ' → ' || dropoff_zone.borough AS trip_route,
       COUNT(*) AS trip_count,
       AVG(yt.fare_amount) AS avg_fare
FROM nyc_taxi.yellow_taxi_trips yt
JOIN nyc_taxi.taxi_zone_lookup pickup_zone
     ON yt.pulocationid = pickup_zone.locationid
JOIN nyc_taxi.taxi_zone_lookup dropoff_zone
     ON yt.dolocationid = dropoff_zone.locationid
GROUP BY pickup_zone.borough, dropoff_zone.borough
ORDER BY trip_count DESC;

-- Financial analysis: Payment patterns by borough
SELECT
       pickup_zone.borough || ' - ' || ptl.payment_type_desc AS borough_payment_type,
       COUNT(*) AS trips,
       AVG(yt.total_amount) AS avg_total
FROM nyc_taxi.yellow_taxi_trips yt
JOIN nyc_taxi.taxi_zone_lookup pickup_zone
     ON yt.pulocationid = pickup_zone.locationid
JOIN nyc_taxi.payment_type_lookup ptl
     ON yt.payment_type = ptl.payment_type
GROUP BY pickup_zone.borough, ptl.payment_type_desc
ORDER BY avg_total DESC;
```
![Analytics Query Results 2](docs/pictures/Clipboard_09-25-2025_02.jpg)

```sql
-- NYC Yellow Taxi Data Analytics Queries
-- Sample queries to explore the NYC taxi dataset

-- 1. Basic Data Overview
SELECT 'Total Trips' as metric, COUNT(*)::text as value
FROM nyc_taxi.yellow_taxi_trips

UNION ALL

SELECT 'Date Range' as metric,
       MIN(tpep_pickup_datetime)::text || ' to ' || MAX(tpep_pickup_datetime)::text as value
FROM nyc_taxi.yellow_taxi_trips

UNION ALL

SELECT 'Total Revenue' as metric, '$' || ROUND(SUM(total_amount), 2)::text as value
FROM nyc_taxi.yellow_taxi_trips;

-- 2. Trip Volume by Hour of Day
SELECT
    EXTRACT(HOUR FROM tpep_pickup_datetime) as pickup_hour,
    COUNT(*) as trip_count,
    ROUND(AVG(trip_distance), 2) as avg_distance,
    ROUND(AVG(total_amount), 2) as avg_fare
FROM nyc_taxi.yellow_taxi_trips
GROUP BY EXTRACT(HOUR FROM tpep_pickup_datetime)
ORDER BY pickup_hour;

-- 3. Payment Method Analysis
SELECT
    CASE payment_type
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No Charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided'
        ELSE 'Other'
    END as payment_method,
    COUNT(*) as trip_count,
    ROUND(AVG(total_amount), 2) as avg_fare,
    ROUND(AVG(tip_amount), 2) as avg_tip,
    ROUND(SUM(total_amount), 2) as total_revenue,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
FROM nyc_taxi.yellow_taxi_trips
GROUP BY payment_type
ORDER BY trip_count DESC;

-- 4. Top Revenue Generating Trips - possible erroneous data inputs
SELECT
    tpep_pickup_datetime,
    trip_distance,
    total_amount,
    tip_amount,
    PULocationID as pickup_zone,
    DOLocationID as dropoff_zone,
    EXTRACT(HOUR FROM tpep_pickup_datetime) as pickup_hour
FROM nyc_taxi.yellow_taxi_trips
WHERE total_amount > 100
ORDER BY total_amount DESC
LIMIT 20;

-- 5. Trip Distance Distribution
SELECT
    CASE
        WHEN trip_distance <= 1 THEN '0-1 miles'
        WHEN trip_distance <= 3 THEN '1-3 miles'
        WHEN trip_distance <= 5 THEN '3-5 miles'
        WHEN trip_distance <= 10 THEN '5-10 miles'
        WHEN trip_distance <= 20 THEN '10-20 miles'
        ELSE '20+ miles'
    END as distance_range,
    COUNT(*) as trip_count,
	ROUND(AVG(trip_distance),2) AS avg_distance,
    ROUND(AVG(total_amount), 2) as avg_fare,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
FROM nyc_taxi.yellow_taxi_trips
WHERE trip_distance > 0 AND trip_distance < 500
GROUP BY
    CASE
        WHEN trip_distance <= 1 THEN '0-1 miles'
        WHEN trip_distance <= 3 THEN '1-3 miles'
        WHEN trip_distance <= 5 THEN '3-5 miles'
        WHEN trip_distance <= 10 THEN '5-10 miles'
        WHEN trip_distance <= 20 THEN '10-20 miles'
        ELSE '20+ miles'
    END
ORDER BY MIN(trip_distance);

-- 6. Daily Trip Patterns
SELECT
    DATE(tpep_pickup_datetime) as trip_date,
    COUNT(*) as total_trips,
    ROUND(AVG(total_amount), 2) as avg_fare,
    ROUND(SUM(total_amount), 2) as daily_revenue,
    COUNT(CASE WHEN payment_type = 1 THEN 1 END) as credit_card_trips,
    COUNT(CASE WHEN payment_type = 2 THEN 1 END) as cash_trips
FROM nyc_taxi.yellow_taxi_trips
GROUP BY DATE(tpep_pickup_datetime)
ORDER BY trip_date;

-- 7. Rush Hour Analysis
SELECT
    CASE
        WHEN EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 7 AND 9 THEN 'Morning Rush (7-9 AM)'
        WHEN EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 17 AND 19 THEN 'Evening Rush (5-7 PM)'
        WHEN EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 22 AND 23 OR
             EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 0 AND 5 THEN 'Night (10 PM - 5 AM)'
        ELSE 'Regular Hours'
    END as time_period,
    COUNT(*) as trip_count,
    ROUND(AVG(trip_distance), 2) as avg_distance,
    ROUND(AVG(total_amount), 2) as avg_fare,
    ROUND(AVG(tip_amount), 2) as avg_tip
FROM nyc_taxi.yellow_taxi_trips
GROUP BY
    CASE
        WHEN EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 7 AND 9 THEN 'Morning Rush (7-9 AM)'
        WHEN EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 17 AND 19 THEN 'Evening Rush (5-7 PM)'
        WHEN EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 22 AND 23 OR
             EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 0 AND 5 THEN 'Night (10 PM - 5 AM)'
        ELSE 'Regular Hours'
    END
ORDER BY trip_count DESC;

-- 8. Tip Analysis by Payment Type
SELECT
    CASE payment_type
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        ELSE 'Other'
    END as payment_method,
    COUNT(*) as trip_count,
    ROUND(AVG(tip_amount), 2) as avg_tip,
    ROUND(AVG(fare_amount), 2) as avg_fare,
    ROUND(AVG(tip_amount / NULLIF(fare_amount, 0) * 100), 2) as avg_tip_percentage,
    COUNT(CASE WHEN tip_amount > 0 THEN 1 END) as trips_with_tips
FROM nyc_taxi.yellow_taxi_trips
WHERE fare_amount > 0 AND payment_type IN (1, 2)
GROUP BY payment_type
ORDER BY avg_tip DESC;

-- 9. Weekend vs Weekday Analysis
SELECT
    CASE
        WHEN EXTRACT(DOW FROM tpep_pickup_datetime) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END as day_type,
    COUNT(*) as trip_count,
    ROUND(AVG(trip_distance), 2) as avg_distance,
    ROUND(AVG(total_amount), 2) as avg_fare,
    ROUND(SUM(total_amount), 2) as total_revenue
FROM nyc_taxi.yellow_taxi_trips
GROUP BY
    CASE
        WHEN EXTRACT(DOW FROM tpep_pickup_datetime) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END
ORDER BY trip_count DESC;

-- 10. Long Distance Trips (Over 20 miles) - possible erroneous data inputs
SELECT
    tpep_pickup_datetime,
    trip_distance,
    total_amount,
    ROUND(total_amount / trip_distance, 2) as fare_per_mile,
    PULocationID,
    DOLocationID,
    passenger_count
FROM nyc_taxi.yellow_taxi_trips
WHERE trip_distance > 20
ORDER BY trip_distance DESC
LIMIT 4000;
```

