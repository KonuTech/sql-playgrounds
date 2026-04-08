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
  - [Automated Backfill System](#automated-backfill-system)
  - [Complete Initialization Process](#complete-initialization-process)
  - [Architecture Benefits](#architecture-benefits)
- [📁 Project Structure](#project-structure)
- [🐳 Container Architecture](#container-architecture)
  - [Custom PostgreSQL Container](#custom-postgresql-container)
  - [Apache Superset Container](#apache-superset-container)
  - [Volume Strategy](#volume-strategy)
  - [Memory & Performance](#memory--performance)
- [⏸️ Pause and Resume Capability](#pause-and-resume-capability)
  - [Safe Interruption and Continuation](#safe-interruption-and-continuation)
  - [Intelligent Resume System](#intelligent-resume-system)
- [⚙️ Configuration & Development](#configuration--development)
  - [Environment Variables](#environment-variables-env)
  - [System Requirements](#system-requirements)
  - [Development Commands](#development-commands)
- [🗄️ Database Schema & Analytics](#database-schema--analytics)
  - [NYC Taxi Data Structure](#nyc-taxi-data-structure)
  - [Performance Optimizations](#performance-optimizations)
    - [Database Indexes](#database-indexes)
    - [Materialized Views](#materialized-views)
    - [PostgreSQL Runtime Tuning](#postgresql-runtime-tuning)
    - [PostgreSQL vs BigQuery: Architectural Differences](#postgresql-vs-bigquery-architectural-differences)
- [📈 Data Sources & Authenticity](#data-sources--authenticity)
- [🚀 Apache Superset Business Intelligence Features](#-apache-superset-business-intelligence-features)
  - [📊 Rich Visualization Gallery](#-rich-visualization-gallery)
  - [⚡ Advanced SQL Lab](#-advanced-sql-lab)
  - [🎛️ Interactive Dashboard Features](#️-interactive-dashboard-features)
  - [📈 Perfect for Data Presentations](#-perfect-for-data-presentations)
  - [🔧 Enterprise-Ready Configuration](#-enterprise-ready-configuration)
  - [Sample Dashboard Ideas](#sample-dashboard-ideas)
- [📊 Sample Analytics Queries](#sample-analytics-queries)
  - [1. Trip Volume by Hour](#1-trip-volume-by-hour)
  - [2. Largest Taxi Zones by Area (PostGIS)](#2-largest-taxi-zones-by-area-postgis)
  - [3. Cross-Borough Trip Analysis](#3-cross-borough-trip-analysis)
  - [4. Payment Patterns by Borough](#4-payment-patterns-by-borough)
  - [Why Pre-Aggregation Helps: Hash Join Explained](#why-pre-aggregation-helps-hash-join-explained)
  - [5. Basic Data Overview](#5-basic-data-overview)
  - [6. Payment Method Analysis](#6-payment-method-analysis)
  - [7. Top Revenue Generating Trips](#7-top-revenue-generating-trips)
  - [8. Trip Distance Distribution](#8-trip-distance-distribution)
  - [9. Daily Trip Patterns](#9-daily-trip-patterns)
  - [10. Rush Hour Analysis](#10-rush-hour-analysis)
  - [11. Tip Analysis by Payment Type](#11-tip-analysis-by-payment-type)
  - [12. Weekend vs Weekday Analysis](#12-weekend-vs-weekday-analysis)
  - [13. Long Distance Trips (Over 20 Miles)](#13-long-distance-trips-over-20-miles)
- [📊 Materialized View Queries](#sample-analytics-queries--materialized-view-versions)

## Quick Start

1. **Copy environment configuration:**
   ```bash
   cp env .env
   ```

2. **Start the complete SQL playground:**
   ```bash
   ./start.sh
   ```

   The `start.sh` script automatically detects port conflicts and falls back to alternative ports:

   | Service    | Default | Fallbacks   |
   |------------|---------|-------------|
   | PostgreSQL | 5432    | 5433, 5434  |
   | PGAdmin    | 8080    | 8081, 8082  |
   | Superset   | 8088    | 8089, 8090  |

   The actual ports are printed at startup so you always know where to connect. You can still override defaults via `.env` (e.g. `POSTGRES_PORT=5555`).

   > **Alternative**: You can still use `docker-compose up -d --build` directly if you prefer fixed ports.

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

Schema defined in [`01-nyc-taxi-schema.sql`](postgres/sql-scripts/init-scripts/01-nyc-taxi-schema.sql).

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

The `yellow_taxi_trips` table carries 13 indexes totaling more storage than the data itself:

| Component | Size |
|-----------|------|
| Table data | 6.8 GB |
| Indexes (13 total) | 11 GB |
| **Combined** | **17 GB** |

**Index breakdown by size:**

| Index | Size | Purpose |
|-------|------|---------|
| `yellow_taxi_trips_pkey` (row_hash) | 3.9 GB | SHA-256 primary key for duplicate prevention |
| `idx_yellow_taxi_location_datetime` | 1.5 GB | Composite: location + datetime queries |
| `idx_yellow_taxi_datetime_vendor` | 990 MB | Composite: datetime + vendor queries |
| `yellow_taxi_trips_id_key` | 702 MB | Unique constraint |
| `idx_yellow_taxi_total_amount` | 696 MB | Top revenue queries (12ms vs ~2s without) |
| `idx_yellow_taxi_pickup_datetime` | 689 MB | Time-series lookups, hourly analysis |
| `idx_yellow_taxi_dropoff_datetime` | 686 MB | Dropoff time lookups |
| `idx_yellow_taxi_trip_distance` | 669 MB | Distance filtering (12ms vs ~2s without) |
| 5 smaller indexes | ~1 GB | Payment type, locations, vendor, date+payment |

**The tradeoff**: indexes make data loading ~2x slower (every INSERT must update all 13 indexes) but turn specific lookups from seconds to milliseconds. For a read-heavy playground that loads data once and queries many times, this is the right balance.

#### Materialized Views

Three pre-aggregated materialized views compress the full trips table for BigQuery-like response times on analytical queries (see [`02-materialized-views.sql`](postgres/sql-scripts/init-scripts/02-materialized-views.sql)):

| View | Source rows | Materialized rows | Size | Compression |
|------|-------------|-------------------|------|-------------|
| `trip_hourly_summary` | ~32M | ~25K | 4 MB | 1,280x |
| `trip_location_summary` | ~32M | ~121K | 12 MB | 264x |
| `trip_distance_summary` | ~32M | 6 | 40 KB | 5,300,000x |

**Sample speedups (raw table vs materialized view):**

| Query | Raw table | Materialized view | Speedup |
|-------|-----------|-------------------|---------|
| Cross-Borough Analysis | 1.78s | 32ms | 56x |
| Distance Distribution | 2.22s | 0.07ms | 31,700x |
| Weekend vs Weekday | 1.5s | 5ms | 300x |
| Payment Method Analysis | 1.5s | 4ms | 380x |

Views are refreshed automatically after each data load (Step 4 in init pipeline). For ad-hoc refresh:
```sql
REFRESH MATERIALIZED VIEW CONCURRENTLY nyc_taxi.trip_hourly_summary;
REFRESH MATERIALIZED VIEW CONCURRENTLY nyc_taxi.trip_location_summary;
REFRESH MATERIALIZED VIEW CONCURRENTLY nyc_taxi.trip_distance_summary;
```

#### PostgreSQL Runtime Tuning

PostgreSQL ships with conservative defaults designed to run on a 512MB shared hosting server from 2005. On a modern machine with 32 CPUs and 31 GB RAM, these defaults leave most of the hardware idle. The container is tuned at startup via command-line flags to fully utilize available resources.

**Memory settings — telling PostgreSQL how much RAM it can use:**

| Setting | PostgreSQL default | Our configuration | What changes |
|---------|-------------------|-------------------|--------------|
| `shared_buffers` | 128 MB | **8 GB** | PostgreSQL's own data cache — hot table pages stay in memory instead of being re-read from disk on every query |
| `effective_cache_size` | 4 GB | **24 GB** | Tells the query planner how much total cache (PostgreSQL + OS) is available — higher values make it prefer index scans over sequential scans |
| `work_mem` | 4 MB | **256 MB** | Memory for sorts and hash tables per operation — eliminates disk spill (our queries were spilling 267MB-1.4GB to disk at default) |
| `maintenance_work_mem` | 64 MB | **512 MB** | Memory for `CREATE INDEX`, `VACUUM`, bulk loading — speeds up data initialization |

**Parallelism settings — using all available CPU cores:**

| Setting | PostgreSQL default | Our configuration | What changes |
|---------|-------------------|-------------------|--------------|
| `max_worker_processes` | 8 | **24** | Total background workers PostgreSQL can spawn |
| `max_parallel_workers` | 8 | **20** | Workers available for parallel query execution |
| `max_parallel_workers_per_gather` | 2 | **16** | Workers per individual query — a full-table scan of 32M rows splits across 16 cores instead of 2 |

**The impact is cumulative.** A query that previously ran with 2 workers sorting 267MB to disk because `work_mem` was 4MB now runs with 16 workers keeping everything in RAM. The theoretical improvement for full-table scans is ~8x from parallelism alone, plus elimination of disk I/O from memory tuning.

**Before tuning (PostgreSQL defaults):**
```
Query: Trip volume by hour
Plan:  2 workers -> seq scan -> 267MB disk sort -> GroupAggregate
Time:  6.9 seconds
```

**After tuning (current configuration):**
```
Query: Trip volume by hour
Plan:  16 workers -> parallel seq scan -> in-memory HashAggregate
Time:  <1 second
```

#### PostgreSQL vs BigQuery: Architectural Differences

This playground uses PostgreSQL — a traditional row-oriented database designed for transactional workloads. Cloud analytical engines like BigQuery were built from the ground up for a different purpose: scanning terabytes of data across thousands of machines. Understanding these differences explains why certain optimizations are needed here but not there — and what we can borrow from BigQuery's playbook.

| Aspect | PostgreSQL (this project) | BigQuery |
|--------|--------------------------|----------|
| **Storage format** | Row-based — reads entire rows even if query needs 2 columns | Columnar — reads only the columns used in the query |
| **Parallelism** | Up to 16 workers on a single machine | Thousands of workers across a cluster |
| **Indexes** | Must be manually created; cost storage + write speed (our indexes are 1.6x the data size) | No traditional indexes — relies on partitioning + clustering |
| **Materialized views** | Must be manually created and refreshed | Has materialized views, plus automatic caching layers |
| **Memory model** | Shared buffers (8 GB) + OS page cache | Distributed memory across cluster, effectively unlimited |
| **Cost model** | Fixed infrastructure cost (your Docker container) | Pay-per-query based on bytes scanned |
| **Best for** | OLTP + moderate analytics, full SQL control | Large-scale analytics, ad-hoc exploration |

**What we replicate from BigQuery's approach:**
- **Materialized views** — pre-aggregated summaries compress 32M rows to 25K, giving sub-millisecond response times (up to 31,700x speedup)
- **Aggressive parallelism** — 16 workers instead of the default 2, similar in spirit to BigQuery splitting work across many machines
- **Memory-first processing** — 8GB shared buffers + 256MB work_mem keeps hot data and intermediate results in RAM, avoiding disk I/O like BigQuery's in-memory processing

**What we can't replicate:**
- **Columnar storage** — PostgreSQL reads full rows; a query needing just `trip_distance` still reads all 21 columns from disk (BigQuery would read only that one column, ~5% of the data)
- **Elastic parallelism** — we're capped at one machine's cores; BigQuery can throw 10,000 workers at a query
- **Separation of storage and compute** — BigQuery scales compute independently per query; our storage and compute share the same Docker container

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

#### 1. Trip Volume by Hour

Counts trips per hour with average distance, average fare, and a list of all boroughs. Optimized with CROSS JOIN LATERAL — aggregates trips first, then attaches the borough list separately instead of joining 11M rows to the lookup table before grouping (~3x faster).

```sql
SELECT h.hour, h.trips, h.avg_distance, h.avg_fare, b.pickup_boroughs
FROM (
    SELECT EXTRACT(HOUR FROM tpep_pickup_datetime) as hour,
           COUNT(*) as trips,
           ROUND(AVG(trip_distance), 2) as avg_distance,
           ROUND(AVG(total_amount), 2) as avg_fare
    FROM nyc_taxi.yellow_taxi_trips
    GROUP BY 1
) h
CROSS JOIN LATERAL (
    SELECT STRING_AGG(DISTINCT tz.borough, ', ') as pickup_boroughs
    FROM nyc_taxi.taxi_zone_lookup tz
) b
ORDER BY h.hour;

-- Original version (slower — joins 11M rows to lookup before grouping, causing 267MB disk sort):
-- SELECT EXTRACT(HOUR FROM yt.tpep_pickup_datetime) as hour,
--        COUNT(*) as trips,
--        STRING_AGG(DISTINCT pickup_zone.borough, ', ') as pickup_boroughs
-- FROM nyc_taxi.yellow_taxi_trips yt
-- JOIN nyc_taxi.taxi_zone_lookup pickup_zone ON yt.pulocationid = pickup_zone.locationid
-- GROUP BY hour ORDER BY hour;
```

<!-- ![Trip Volume by Hour - Query Results](docs/pictures/query-01-trip-volume-by-hour.png) -->

#### 2. Largest Taxi Zones by Area (PostGIS)

Uses `ST_Area()` on PostGIS geometries to find the 10 largest taxi zones in acres. Fast by nature — only 263 rows in the shapes table.

```sql
SELECT tzl.zone, tzl.borough,
       ROUND((ST_Area(geometry) / 43560)::numeric, 2) as area_acres
FROM nyc_taxi.taxi_zone_shapes tzs
JOIN nyc_taxi.taxi_zone_lookup tzl ON tzs.locationid = tzl.locationid
ORDER BY ST_Area(geometry) DESC LIMIT 10;
```

<!-- ![Largest Taxi Zones - Query Results](docs/pictures/query-02-largest-taxi-zones.png) -->

#### 3. Cross-Borough Trip Analysis

Shows trip counts and average fares between borough pairs. Optimized by pre-aggregating by location ID pair (~43K groups) before joining to the lookup table, so hash joins operate on 43K rows instead of 11M (~37% faster).

```sql
SELECT pz.borough || ' -> ' || dz.borough AS trip_route,
       SUM(agg.trip_count) AS trip_count,
       SUM(agg.avg_fare * agg.trip_count) / SUM(agg.trip_count) AS avg_fare
FROM (
    SELECT pulocationid, dolocationid,
           COUNT(*) AS trip_count,
           AVG(fare_amount) AS avg_fare
    FROM nyc_taxi.yellow_taxi_trips
    GROUP BY pulocationid, dolocationid
) agg
JOIN nyc_taxi.taxi_zone_lookup pz ON agg.pulocationid = pz.locationid
JOIN nyc_taxi.taxi_zone_lookup dz ON agg.dolocationid = dz.locationid
GROUP BY pz.borough, dz.borough
ORDER BY trip_count DESC;

-- Original version (slower — joins 11M rows to lookup twice before grouping by borough):
-- SELECT
--        pickup_zone.borough || ' -> ' || dropoff_zone.borough AS trip_route,
--        COUNT(*) AS trip_count,
--        AVG(yt.fare_amount) AS avg_fare
-- FROM nyc_taxi.yellow_taxi_trips yt
-- JOIN nyc_taxi.taxi_zone_lookup pickup_zone
--      ON yt.pulocationid = pickup_zone.locationid
-- JOIN nyc_taxi.taxi_zone_lookup dropoff_zone
--      ON yt.dolocationid = dropoff_zone.locationid
-- GROUP BY pickup_zone.borough, dropoff_zone.borough
-- ORDER BY trip_count DESC;
```

<!-- ![Cross-Borough Trip Analysis - Query Results](docs/pictures/query-03-cross-borough.png) -->

#### 4. Payment Patterns by Borough

Financial breakdown of payment types per borough. Same pre-aggregation optimization as query 3 — groups by raw IDs first (~1.2K groups), then joins to lookup tables for display names (~38% faster).

```sql
SELECT pz.borough || ' - ' || ptl.payment_type_desc AS borough_payment_type,
       SUM(agg.trips) AS trips,
       SUM(agg.avg_total * agg.trips) / SUM(agg.trips) AS avg_total
FROM (
    SELECT pulocationid, payment_type,
           COUNT(*) AS trips,
           AVG(total_amount) AS avg_total
    FROM nyc_taxi.yellow_taxi_trips
    GROUP BY pulocationid, payment_type
) agg
JOIN nyc_taxi.taxi_zone_lookup pz ON agg.pulocationid = pz.locationid
JOIN nyc_taxi.payment_type_lookup ptl ON agg.payment_type = ptl.payment_type
GROUP BY pz.borough, ptl.payment_type_desc
ORDER BY avg_total DESC;

-- Original version (slower — joins 11M rows to two lookups before grouping):
-- SELECT
--        pickup_zone.borough || ' - ' || ptl.payment_type_desc AS borough_payment_type,
--        COUNT(*) AS trips,
--        AVG(yt.total_amount) AS avg_total
-- FROM nyc_taxi.yellow_taxi_trips yt
-- JOIN nyc_taxi.taxi_zone_lookup pickup_zone
--      ON yt.pulocationid = pickup_zone.locationid
-- JOIN nyc_taxi.payment_type_lookup ptl
--      ON yt.payment_type = ptl.payment_type
-- GROUP BY pickup_zone.borough, ptl.payment_type_desc
-- ORDER BY avg_total DESC;
```

<!-- ![Payment Patterns by Borough - Query Results](docs/pictures/query-04-payment-borough.png) -->

#### Why Pre-Aggregation Helps: Hash Join Explained

Queries 3 and 4 use the same optimization pattern — **pre-aggregate by raw IDs, then join to lookup tables**. The key to understanding why this is faster lies in how PostgreSQL's hash join works.

**Hash join** is PostgreSQL's strategy when joining a large table to a small one. It works in two phases:

1. **Build phase** — scan the small table (e.g., `taxi_zone_lookup`, 263 rows) and build an in-memory hash table keyed on the join column (`locationid`). This takes ~20KB of memory.

2. **Probe phase** — scan the large table and for each row, hash the join column and look it up in the hash table. Each lookup is O(1).

The hash join itself is fast — the cost per probe is negligible. The problem is **what flows through it**. In the original queries, the full 11M-row trips table is probed against the lookup, producing 11M joined rows that then need to be aggregated by borough. In the optimized version, the trips are first grouped by location ID (~1-43K groups depending on the query), and only those groups are probed — reducing the downstream aggregation work by orders of magnitude.

```
Original:  11M trips --hash join--> 11M joined rows --GROUP BY borough--> 24-35 results
Optimized: 11M trips --GROUP BY id--> 1-43K groups --hash join--> 1-43K rows --GROUP BY borough--> 24-35 results
```

The sequential scan on the 11M-row table is unavoidable either way — but the join and final aggregation operate on dramatically fewer rows.

#### 5. Basic Data Overview

Quick summary of total trips, date range, and total revenue. Optimized to compute all aggregates in a single table scan using `unnest(ARRAY[...])` instead of 3 separate scans with `UNION ALL` (~2.8x faster).

```sql
SELECT unnest(ARRAY['Total Trips', 'Date Range', 'Total Revenue']) as metric,
       unnest(ARRAY[
           COUNT(*)::text,
           MIN(tpep_pickup_datetime)::text || ' to ' || MAX(tpep_pickup_datetime)::text,
           '$' || ROUND(SUM(total_amount), 2)::text
       ]) as value
FROM nyc_taxi.yellow_taxi_trips;

-- Original version (slower — 3 separate full table scans via UNION ALL):
-- SELECT 'Total Trips' as metric, COUNT(*)::text as value
-- FROM nyc_taxi.yellow_taxi_trips
-- UNION ALL
-- SELECT 'Date Range' as metric,
--        MIN(tpep_pickup_datetime)::text || ' to ' || MAX(tpep_pickup_datetime)::text as value
-- FROM nyc_taxi.yellow_taxi_trips
-- UNION ALL
-- SELECT 'Total Revenue' as metric, '$' || ROUND(SUM(total_amount), 2)::text as value
-- FROM nyc_taxi.yellow_taxi_trips;
```

<!-- ![Basic Data Overview - Query Results](docs/pictures/query-05-basic-overview.png) -->

#### 6. Payment Method Analysis

Breakdown by payment type with trip counts, averages, totals, and percentage share using a window function.

```sql
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
```

<!-- ![Payment Method Analysis - Query Results](docs/pictures/query-06-payment-method.png) -->

#### 7. Top Revenue Generating Trips

Highest-value trips — useful for spotting possible erroneous data inputs (e.g., $900+ fares).

```sql
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
```

<!-- ![Top Revenue Generating Trips - Query Results](docs/pictures/query-07-top-revenue.png) -->

#### 8. Trip Distance Distribution

Bucketed distance ranges with trip counts, averages, and percentage distribution. Filters out zero-distance and extreme outliers.

```sql
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
```

<!-- ![Trip Distance Distribution - Query Results](docs/pictures/query-08-distance-distribution.png) -->

#### 9. Daily Trip Patterns

Day-by-day totals with revenue and payment method split — good for time-series visualization in Superset.

```sql
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
```

<!-- ![Daily Trip Patterns - Query Results](docs/pictures/query-09-daily-patterns.png) -->

#### 10. Rush Hour Analysis

Groups trips into four time-of-day buckets (morning rush, evening rush, night, regular) with distance and fare averages. Optimized with two-stage aggregation: groups by hour first (24 groups), then maps to time periods — avoids 1.2GB disk sort caused by grouping on the CASE text expression directly (~18% faster).

```sql
SELECT
    CASE
        WHEN h BETWEEN 7 AND 9 THEN 'Morning Rush (7-9 AM)'
        WHEN h BETWEEN 17 AND 19 THEN 'Evening Rush (5-7 PM)'
        WHEN h >= 22 OR h <= 5 THEN 'Night (10 PM - 5 AM)'
        ELSE 'Regular Hours'
    END as time_period,
    SUM(cnt) as trip_count,
    ROUND((SUM(dist) / SUM(cnt))::numeric, 2) as avg_distance,
    ROUND((SUM(amt) / SUM(cnt))::numeric, 2) as avg_fare,
    ROUND((SUM(tip) / SUM(cnt))::numeric, 2) as avg_tip
FROM (
    SELECT EXTRACT(HOUR FROM tpep_pickup_datetime)::int as h,
           COUNT(*) as cnt, SUM(trip_distance) as dist,
           SUM(total_amount) as amt, SUM(tip_amount) as tip
    FROM nyc_taxi.yellow_taxi_trips
    GROUP BY 1
) sub
GROUP BY 1
ORDER BY trip_count DESC;

-- Original version (slower — groups on CASE text expression, causing 1.2GB disk sort):
-- SELECT
--     CASE
--         WHEN EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 7 AND 9 THEN 'Morning Rush (7-9 AM)'
--         WHEN EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 17 AND 19 THEN 'Evening Rush (5-7 PM)'
--         WHEN EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 22 AND 23 OR
--              EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 0 AND 5 THEN 'Night (10 PM - 5 AM)'
--         ELSE 'Regular Hours'
--     END as time_period,
--     COUNT(*) as trip_count,
--     ROUND(AVG(trip_distance), 2) as avg_distance,
--     ROUND(AVG(total_amount), 2) as avg_fare,
--     ROUND(AVG(tip_amount), 2) as avg_tip
-- FROM nyc_taxi.yellow_taxi_trips
-- GROUP BY 1
-- ORDER BY trip_count DESC;
```

<!-- ![Rush Hour Analysis - Query Results](docs/pictures/query-10-rush-hour.png) -->

#### 11. Tip Analysis by Payment Type

Compares tipping behavior between credit card and cash payments — tip percentage, average tip, and how many trips include a tip.

```sql
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
```

<!-- ![Tip Analysis - Query Results](docs/pictures/query-11-tip-analysis.png) -->

#### 12. Weekend vs Weekday Analysis

Compares trip volume, distance, fare, and revenue between weekdays and weekends. Optimized with two-stage aggregation: groups by day-of-week first (7 groups), then maps to weekend/weekday — avoids planner overestimating CASE expression cardinality.

```sql
SELECT
    CASE WHEN dow IN (0, 6) THEN 'Weekend' ELSE 'Weekday' END as day_type,
    SUM(cnt) as trip_count,
    ROUND((SUM(dist) / SUM(cnt))::numeric, 2) as avg_distance,
    ROUND((SUM(amt) / SUM(cnt))::numeric, 2) as avg_fare,
    ROUND(SUM(amt)::numeric, 2) as total_revenue
FROM (
    SELECT EXTRACT(DOW FROM tpep_pickup_datetime)::int as dow,
           COUNT(*) as cnt, SUM(trip_distance) as dist,
           SUM(total_amount) as amt
    FROM nyc_taxi.yellow_taxi_trips
    GROUP BY 1
) sub
GROUP BY 1
ORDER BY trip_count DESC;

-- Original version (slower — groups on CASE text expression, planner loses parallelism):
-- SELECT
--     CASE
--         WHEN EXTRACT(DOW FROM tpep_pickup_datetime) IN (0, 6) THEN 'Weekend'
--         ELSE 'Weekday'
--     END as day_type,
--     COUNT(*) as trip_count,
--     ROUND(AVG(trip_distance), 2) as avg_distance,
--     ROUND(AVG(total_amount), 2) as avg_fare,
--     ROUND(SUM(total_amount), 2) as total_revenue
-- FROM nyc_taxi.yellow_taxi_trips
-- GROUP BY
--     CASE
--         WHEN EXTRACT(DOW FROM tpep_pickup_datetime) IN (0, 6) THEN 'Weekend'
--         ELSE 'Weekday'
--     END
-- ORDER BY trip_count DESC;
```

<!-- ![Weekend vs Weekday - Query Results](docs/pictures/query-12-weekend-weekday.png) -->

#### 13. Long Distance Trips (Over 20 Miles)

Identifies unusually long trips with fare-per-mile calculation — useful for spotting possible erroneous data inputs or airport runs.

```sql
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

<!-- ![Long Distance Trips - Query Results](docs/pictures/query-13-long-distance.png) -->

### Sample Analytics Queries — Materialized View Versions

The queries below produce the same results as the Sample Analytics Queries above but run against pre-aggregated materialized views instead of scanning the full 19M-row trips table. Response times drop from seconds to single-digit milliseconds.

Three materialized views are created automatically during initialization (see [`02-materialized-views.sql`](postgres/sql-scripts/init-scripts/02-materialized-views.sql)):

| View | Rows | Covers queries |
|------|------|----------------|
| `nyc_taxi.trip_hourly_summary` | ~25K | 1, 5, 6, 9, 10, 11, 12 |
| `nyc_taxi.trip_location_summary` | ~121K | 3, 4 |
| `nyc_taxi.trip_distance_summary` | 6 | 8 |

Queries 2 (PostGIS zones), 7 (top revenue trips), and 13 (long distance trips) are already fast and don't benefit from materialized views — they either operate on small tables or use index scans.

Refresh all views after loading new data:
```sql
REFRESH MATERIALIZED VIEW CONCURRENTLY nyc_taxi.trip_hourly_summary;
REFRESH MATERIALIZED VIEW CONCURRENTLY nyc_taxi.trip_location_summary;
REFRESH MATERIALIZED VIEW CONCURRENTLY nyc_taxi.trip_distance_summary;
```

#### MV-1. Trip Volume by Hour

Uses `trip_hourly_summary`. Boroughs are still attached via CROSS JOIN LATERAL from the lookup table. **~6ms** (vs ~2.3s on raw table).

```sql
SELECT h.pickup_hour as hour,
       SUM(h.trip_count) as trips,
       ROUND((SUM(h.total_distance) / SUM(h.trip_count))::numeric, 2) as avg_distance,
       ROUND((SUM(h.total_amount) / SUM(h.trip_count))::numeric, 2) as avg_fare,
       b.pickup_boroughs
FROM nyc_taxi.trip_hourly_summary h
CROSS JOIN LATERAL (
    SELECT STRING_AGG(DISTINCT tz.borough, ', ') as pickup_boroughs
    FROM nyc_taxi.taxi_zone_lookup tz
) b
GROUP BY h.pickup_hour, b.pickup_boroughs
ORDER BY h.pickup_hour;
```

![MV Trip Volume by Hour - Query Results](docs/pictures/mv-01-trip-volume.png)

#### MV-3. Cross-Borough Trip Analysis

Uses `trip_location_summary`. Joins 121K pre-aggregated rows to lookup tables instead of 19M. **~22ms** (vs ~1.1s on raw table).

```sql
SELECT pz.borough || ' -> ' || dz.borough AS trip_route,
       SUM(ls.trip_count) AS trip_count,
       ROUND((SUM(ls.total_fare) / SUM(ls.trip_count))::numeric, 2) AS avg_fare
FROM nyc_taxi.trip_location_summary ls
JOIN nyc_taxi.taxi_zone_lookup pz ON ls.pulocationid = pz.locationid
JOIN nyc_taxi.taxi_zone_lookup dz ON ls.dolocationid = dz.locationid
GROUP BY pz.borough, dz.borough
ORDER BY trip_count DESC;
```

![MV Cross-Borough - Query Results](docs/pictures/mv-03-cross-borough.png)

#### MV-4. Payment Patterns by Borough

Uses `trip_location_summary`. Joins to both zone lookup and payment type lookup. **~20ms** (vs ~1.2s on raw table).

```sql
SELECT pz.borough || ' - ' || ptl.payment_type_desc AS borough_payment_type,
       SUM(ls.trip_count) AS trips,
       ROUND((SUM(ls.total_amount) / SUM(ls.trip_count))::numeric, 2) AS avg_total
FROM nyc_taxi.trip_location_summary ls
JOIN nyc_taxi.taxi_zone_lookup pz ON ls.pulocationid = pz.locationid
JOIN nyc_taxi.payment_type_lookup ptl ON ls.payment_type = ptl.payment_type
GROUP BY pz.borough, ptl.payment_type_desc
ORDER BY avg_total DESC;
```

![MV Payment Patterns - Query Results](docs/pictures/mv-04-payment-borough.png)

#### MV-5. Basic Data Overview

Uses `trip_hourly_summary`. Single scan of 25K rows instead of 19M. **~3ms** (vs ~0.9s on raw table).

```sql
SELECT unnest(ARRAY['Total Trips', 'Date Range', 'Total Revenue']) as metric,
       unnest(ARRAY[
           SUM(trip_count)::text,
           MIN(min_pickup)::text || ' to ' || MAX(max_pickup)::text,
           '$' || ROUND(SUM(total_amount)::numeric, 2)::text
       ]) as value
FROM nyc_taxi.trip_hourly_summary;
```

![MV Basic Overview - Query Results](docs/pictures/mv-05-basic-overview.png)

#### MV-6. Payment Method Analysis

Uses `trip_hourly_summary`. Aggregates 25K rows by payment type instead of 19M. **~4ms** (vs ~1.6s on raw table).

```sql
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
    SUM(trip_count) as trip_count,
    ROUND((SUM(total_amount) / SUM(trip_count))::numeric, 2) as avg_fare,
    ROUND((SUM(total_tip) / SUM(trip_count))::numeric, 2) as avg_tip,
    ROUND(SUM(total_amount)::numeric, 2) as total_revenue,
    ROUND(100.0 * SUM(trip_count) / SUM(SUM(trip_count)) OVER(), 2) as percentage
FROM nyc_taxi.trip_hourly_summary
GROUP BY payment_type
ORDER BY trip_count DESC;
```

![MV Payment Method - Query Results](docs/pictures/mv-06-payment-method.png)

#### MV-8. Trip Distance Distribution

Uses `trip_distance_summary`. Reads 6 pre-computed rows — effectively instant. **~0.06ms** (vs ~2.2s on raw table).

```sql
SELECT distance_range,
       trip_count,
       ROUND((total_distance / trip_count)::numeric, 2) as avg_distance,
       ROUND((total_amount / trip_count)::numeric, 2) as avg_fare,
       ROUND(100.0 * trip_count / SUM(trip_count) OVER(), 2) as percentage
FROM nyc_taxi.trip_distance_summary
ORDER BY distance_bucket;
```

![MV Distance Distribution - Query Results](docs/pictures/mv-08-distance-distribution.png)

#### MV-9. Daily Trip Patterns

Uses `trip_hourly_summary`. Groups by date across 25K rows instead of 19M. **~5ms** (vs ~1s on raw table).

```sql
SELECT trip_date,
       SUM(trip_count) as total_trips,
       ROUND((SUM(total_amount) / SUM(trip_count))::numeric, 2) as avg_fare,
       ROUND(SUM(total_amount)::numeric, 2) as daily_revenue,
       SUM(CASE WHEN payment_type = 1 THEN trip_count ELSE 0 END) as credit_card_trips,
       SUM(CASE WHEN payment_type = 2 THEN trip_count ELSE 0 END) as cash_trips
FROM nyc_taxi.trip_hourly_summary
GROUP BY trip_date
ORDER BY trip_date;
```

![MV Daily Patterns - Query Results](docs/pictures/mv-09-daily-patterns.png)

#### MV-10. Rush Hour Analysis

Uses `trip_hourly_summary`. Groups by hour bucket across 25K rows instead of 19M. **~6ms** (vs ~7.1s on raw table).

```sql
SELECT
    CASE
        WHEN pickup_hour BETWEEN 7 AND 9 THEN 'Morning Rush (7-9 AM)'
        WHEN pickup_hour BETWEEN 17 AND 19 THEN 'Evening Rush (5-7 PM)'
        WHEN pickup_hour >= 22 OR pickup_hour <= 5 THEN 'Night (10 PM - 5 AM)'
        ELSE 'Regular Hours'
    END as time_period,
    SUM(trip_count) as trip_count,
    ROUND((SUM(total_distance) / SUM(trip_count))::numeric, 2) as avg_distance,
    ROUND((SUM(total_amount) / SUM(trip_count))::numeric, 2) as avg_fare,
    ROUND((SUM(total_tip) / SUM(trip_count))::numeric, 2) as avg_tip
FROM nyc_taxi.trip_hourly_summary
GROUP BY
    CASE
        WHEN pickup_hour BETWEEN 7 AND 9 THEN 'Morning Rush (7-9 AM)'
        WHEN pickup_hour BETWEEN 17 AND 19 THEN 'Evening Rush (5-7 PM)'
        WHEN pickup_hour >= 22 OR pickup_hour <= 5 THEN 'Night (10 PM - 5 AM)'
        ELSE 'Regular Hours'
    END
ORDER BY trip_count DESC;
```

![MV Rush Hour - Query Results](docs/pictures/mv-10-rush-hour.png)

#### MV-11. Tip Analysis by Payment Type

Uses `trip_hourly_summary`. **~3ms** (vs ~1.8s on raw table). Note: `avg_tip_percentage` is approximated as the ratio of total tip to total fare (not the average of per-row percentages), which is more statistically meaningful for large datasets.

```sql
SELECT
    CASE payment_type
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        ELSE 'Other'
    END as payment_method,
    SUM(trip_count) as trip_count,
    ROUND((SUM(total_tip) / SUM(trip_count))::numeric, 2) as avg_tip,
    ROUND((SUM(total_fare) / SUM(trip_count))::numeric, 2) as avg_fare,
    ROUND((SUM(total_tip) / NULLIF(SUM(total_fare), 0) * 100)::numeric, 2) as avg_tip_percentage,
    SUM(trips_with_tips) as trips_with_tips
FROM nyc_taxi.trip_hourly_summary
WHERE total_fare > 0 AND payment_type IN (1, 2)
GROUP BY payment_type
ORDER BY avg_tip DESC;
```

![MV Tip Analysis - Query Results](docs/pictures/mv-11-tip-analysis.png)

#### MV-12. Weekend vs Weekday Analysis

Uses `trip_hourly_summary`. Groups by pre-computed `day_of_week` column across 25K rows. **~5ms** (vs ~7.1s on raw table).

```sql
SELECT
    CASE WHEN day_of_week IN (0, 6) THEN 'Weekend' ELSE 'Weekday' END as day_type,
    SUM(trip_count) as trip_count,
    ROUND((SUM(total_distance) / SUM(trip_count))::numeric, 2) as avg_distance,
    ROUND((SUM(total_amount) / SUM(trip_count))::numeric, 2) as avg_fare,
    ROUND(SUM(total_amount)::numeric, 2) as total_revenue
FROM nyc_taxi.trip_hourly_summary
GROUP BY CASE WHEN day_of_week IN (0, 6) THEN 'Weekend' ELSE 'Weekday' END
ORDER BY trip_count DESC;
```

![MV Weekend vs Weekday - Query Results](docs/pictures/mv-12-weekend-weekday.png)

