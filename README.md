# SQL Playgrounds with NYC Taxi Data

A production-ready Docker-based SQL playground featuring PostgreSQL 17 + PostGIS 3.5 and PGAdmin with authentic NYC Yellow Taxi trip data (3.47+ million records per month). Features single-command deployment with automatic data loading.

## Features

- **PostgreSQL 17 + PostGIS 3.5** with geospatial support and custom Python environment
- **PGAdmin 4** (latest) for web-based database management and query execution
- **Authentic NYC Taxi Data** - 3.47+ million real taxi trip records from January 2025
- **Complete Geospatial Data** - 263 official NYC taxi zones with polygon boundaries
- **Automatic Initialization** - Single-script deployment loads all data automatically
- **Production-Scale Performance** - Optimized schema with spatial indexes for big data analytics
- **Memory-Efficient Loading** - Chunked processing handles large datasets safely
- **PostGIS Integration** - Full spatial analysis capabilities with CRS conversion

## Quick Start

1. **Start the complete SQL playground:**
   ```bash
   docker-compose up -d --build
   ```

   ⏱️ **First startup takes 30-45 minutes** to build the custom PostgreSQL image and load all 3.47+ million taxi records plus complete geospatial data.

2. **Monitor the initialization progress:**
   ```bash
   docker logs -f sql-playground-postgres
   ```

   You'll see real-time progress updates as the system:
   - Creates PostGIS-enabled database schema
   - Loads 263 NYC taxi zones with geospatial boundaries
   - Processes 3.47M+ trip records in chunks (10K rows each)
   - Performs data integrity verification

3. **Access PGAdmin Web Interface:**
   - **URL**: http://localhost:8080
   - **Login**: admin@admin.com / admin123

4. **Connect to PostgreSQL in PGAdmin:**
   - **Host**: postgres
   - **Port**: 5432
   - **Database**: playground
   - **Username**: admin
   - **Password**: admin123
   - **Schema**: nyc_taxi

## Architecture & Data Loading

### Fully Automated Initialization
The system uses a **single comprehensive initialization script** that runs automatically during container startup:

**Data Sources Loaded:**
- **3,475,226 Trip Records** - Complete January 2025 Yellow Taxi dataset (59MB parquet file)
- **263 Taxi Zones** - Official NYC TLC taxi zone lookup + PostGIS polygon geometries
- **Reference Tables** - Vendors, payment types, rate codes with proper relationships

### Complete Initialization Process
The `docker/init-data.py` script orchestrates everything:

1. **PostgreSQL Readiness**: Waits for database to be fully operational
2. **Schema Creation**: Executes SQL scripts in proper order with PostGIS extensions
3. **Zone Data Processing**:
   - Loads CSV lookup table with NULL value cleanup (263 valid zones)
   - Processes shapefiles with CRS conversion to NYC State Plane (EPSG:2263)
   - Creates PostGIS MULTIPOLYGON geometries for spatial analysis
4. **Trip Data Loading**:
   - Reads complete 59MB parquet file (3.47M+ records)
   - Converts column names to lowercase for schema compatibility
   - Handles numeric precision issues and NULL values
   - Loads in 10K row chunks for memory efficiency
5. **Data Verification**: Performs integrity checks with sample analytical queries

### Architecture Benefits
- **Zero Manual Setup**: Single `docker-compose up` command deploys everything
- **Production-Ready**: Handles real-world data challenges (case sensitivity, precision, memory)
- **Error Recovery**: `docker-compose down -v` provides clean slate for troubleshooting
- **Persistent Storage**: Data persists between container restarts via Docker volumes

## Database Schema & Analytics

### NYC Taxi Data Structure
**Main Table**: `nyc_taxi.yellow_taxi_trips` (3,475,226 records, 20 columns)
- **Trip Identifiers**: vendorid, pickup/dropoff timestamps
- **Location Data**: pulocationid, dolocationid (references taxi zones)
- **Financial Data**: fare_amount, tip_amount, total_amount, taxes, fees
- **Trip Metrics**: passenger_count, trip_distance, payment_type
- **2025 Addition**: cbd_congestion_fee (Central Business District fee)

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
├── .env                            # Environment variables (credentials, ports)
├── CLAUDE.md                       # Detailed architecture guide for Claude Code
├── data/                           # Raw data files (mounted read-only into container)
│   ├── zones/                      # NYC taxi zone reference data
│   │   ├── taxi_zone_lookup.csv    # 263 official taxi zones
│   │   └── taxi_zones.* (shp/dbf/shx/prj) # Complete shapefile set
│   └── yellow/                     # NYC Yellow taxi trip data
│       └── yellow_tripdata_2025-01.parquet # 3.47M+ records (59MB)
├── sql-scripts/                    # SQL scripts for schema and analysis
│   ├── init-scripts/               # Database schema creation (executed automatically)
│   │   ├── 00-postgis-setup.sql    # PostGIS extensions and spatial references
│   │   └── 01-nyc-taxi-schema.sql  # Complete NYC taxi schema (lowercase columns)
│   └── reports-scripts/            # Pre-built analytical queries (available in PGAdmin)
│       ├── nyc-taxi-analytics.sql  # Trip volume, financial, and temporal analysis
│       └── geospatial-taxi-analytics.sql # PostGIS spatial queries and zone analysis
├── docker/                         # Custom PostgreSQL container
│   ├── Dockerfile.postgres         # Custom image: PostgreSQL + PostGIS + Python environment
│   └── init-data.py                # Single comprehensive initialization script (355 lines)
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
- **Data Access**: Host directories mounted read-only into container
- **Script Access**: SQL scripts available both for initialization and PGAdmin queries

### Memory & Performance
- **Chunked Loading**: 10K rows per chunk prevents memory overflow
- **Progress Tracking**: Real-time logging during 30-45 minute initialization
- **Optimized Indexes**: Spatial GIST, temporal, location, and composite indexes
- **Production Scale**: Handles 3.47M+ records efficiently

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
INIT_LOAD_ALL_DATA=true
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

# Development with rebuild
docker-compose down -v && docker-compose up -d --build

# Data status check
docker exec sql-playground-postgres psql -U admin -d playground -c "SELECT COUNT(*) FROM nyc_taxi.yellow_taxi_trips;"

# Direct database access
docker exec -it sql-playground-postgres psql -U admin -d playground

# Container logs monitoring
docker logs -f sql-playground-postgres
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
- **Current Dataset**: January 2025 - `yellow_tripdata_2025-01.parquet`
- **Size**: 59MB compressed, 3,475,226 records
- **Coverage**: Complete month of taxi trip data from NYC Taxi & Limousine Commission

### NYC Taxi Zone Reference Data
**Source**: [NYC TLC Taxi Zone Maps and Lookup Tables](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- **Lookup Table**: `taxi_zone_lookup.csv` - 263 official taxi zones with borough classifications
- **Geospatial Data**: Complete shapefile set (`.shp`, `.dbf`, `.shx`, `.prj`) with polygon boundaries
- **Coordinate System**: Converted to NYC State Plane (EPSG:2263) for optimal spatial analysis

### Data Authenticity & Scale
This is **production-scale real-world data** from New York City's official transportation authority:
- ✅ **Authentic NYC taxi trips** - every record represents a real taxi ride
- ✅ **Complete temporal coverage** - 31 days of continuous data (January 1-31, 2025)
- ✅ **Official geospatial boundaries** - precise NYC taxi zone polygons
- ✅ **Rich analytical dimensions** - financial, temporal, spatial, and operational data

Perfect for learning advanced SQL, big data analytics, and geospatial analysis with realistic datasets that mirror production database challenges.