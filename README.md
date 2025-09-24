# SQL Playgrounds with NYC Taxi Data

A Docker-based SQL playground featuring PostgreSQL 17 and PGAdmin with real-world NYC Yellow Taxi trip data (3.4+ million records per month).

## Features

- **PostgreSQL 17 + PostGIS 3.5** (latest stable versions with geospatial support)
- **PGAdmin 4** (latest version) for web-based database management
- **Real NYC Taxi Data** - 3.4+ million taxi trip records from NYC TLC
- **Complete Taxi Zone Data** - Official NYC taxi zone lookup table and shapefiles
- **Geospatial Analytics** - PostGIS-enabled spatial queries and analysis
- **Volume Storage** for persistent data and SQL scripts
- **Python Data Loaders** for importing parquet files and geospatial data
- **Optimized Schema** with spatial indexes for big data analytics

## Quick Start

1. **Start the services (includes automatic data loading):**
   ```bash
   docker-compose up -d --build
   ```

   *Note: First startup will take several minutes as it builds the custom PostgreSQL image and loads all 3.4+ million taxi records plus complete taxi zone data.*

2. **Monitor the data loading progress:**
   ```bash
   docker logs -f sql-playground-postgres
   ```

3. **Access PGAdmin:**
   - URL: http://localhost:8080
   - Email: admin@admin.com
   - Password: admin123

4. **Connect to PostgreSQL in PGAdmin:**
   - Host: postgres
   - Port: 5432
   - Database: playground
   - Username: admin
   - Password: admin123

## Automatic Data Loading

The project automatically loads comprehensive NYC taxi data during Docker initialization:
- **3.4+ Million Trip Records** - Complete January 2025 Yellow Taxi dataset
- **265 Official Taxi Zones** - NYC TLC taxi zone lookup table and shapefiles with PostGIS geometry
- **Reference Tables** - Vendors, payment types, rate codes

### Complete Initialization Process:
The `docker/init-data.py` script handles everything in sequence:
1. **Schema Creation**: Executes SQL scripts to create PostGIS-enabled schema, tables, and indexes
2. **Zone Data**: Loads 265 official NYC taxi zones from CSV and shapefile with coordinate conversion
3. **Trip Data**: Loads complete parquet file with 3.4M+ records in memory-efficient chunks
4. **Verification**: Performs data integrity checks and displays sample analytics

### Manual Loading (Optional):
If you prefer to load data manually or want to update with different datasets:

```bash
# Load reference data only
uv run python python-scripts/load_reference_data.py

# Load trip data with custom parameters
uv run python python-scripts/load_taxi_data.py --max-rows 1000000
```

## NYC Taxi Data Schema

The database includes a comprehensive schema with 20 columns matching the official NYC TLC format:

- **Trip Data**: VendorID, pickup/dropoff times, locations
- **Financial**: fare_amount, tip_amount, total_amount, taxes, fees
- **Trip Details**: passenger_count, trip_distance, payment_type
- **New Fields**: cbd_congestion_fee (2025 addition)

### Sample Queries

```sql
-- Trip volume by hour with zone names
SELECT EXTRACT(HOUR FROM yt.tpep_pickup_datetime) as hour,
       COUNT(*) as trips,
       STRING_AGG(DISTINCT pickup_zone.borough, ', ') as pickup_boroughs
FROM nyc_taxi.yellow_taxi_trips yt
JOIN nyc_taxi.taxi_zone_lookup pickup_zone ON yt.PULocationID = pickup_zone.locationid
GROUP BY hour ORDER BY hour;

-- Geospatial: Largest taxi zones by area
SELECT zone, borough,
       ROUND((ST_Area(geometry) / 43560)::numeric, 2) as area_acres
FROM nyc_taxi.taxi_zone_shapes tzs
JOIN nyc_taxi.taxi_zone_lookup tzl ON tzs.locationid = tzl.locationid
ORDER BY ST_Area(geometry) DESC LIMIT 10;

-- Cross-borough trip analysis
SELECT pickup_zone.borough as pickup_borough,
       dropoff_zone.borough as dropoff_borough,
       COUNT(*) as trip_count
FROM nyc_taxi.yellow_taxi_trips yt
JOIN nyc_taxi.taxi_zone_lookup pickup_zone ON yt.PULocationID = pickup_zone.locationid
JOIN nyc_taxi.taxi_zone_lookup dropoff_zone ON yt.DOLocationID = dropoff_zone.locationid
GROUP BY pickup_zone.borough, dropoff_zone.borough
ORDER BY trip_count DESC;
```

## Project Structure

```
sql-playgrounds/
├── docker-compose.yml              # Main service configuration
├── .env                            # Environment variables
├── data/                           # Data storage (auto-loaded during initialization)
│   ├── zones/                      # NYC taxi zone reference data
│   │   ├── taxi_zone_lookup.csv    # Official taxi zone lookup table (265 zones)
│   │   └── taxi_zones.* (shp/dbf/etc) # Official taxi zone shapefiles
│   └── yellow/                     # NYC Yellow taxi data
│       └── yellow_tripdata_2025-01.parquet (3.4M+ records)
├── sql-scripts/                    # SQL scripts
│   ├── init-scripts/               # Database initialization scripts
│   │   └── 01-nyc-taxi-schema.sql  # NYC taxi schema with indexes
│   └── reports-scripts/            # Analytical SQL queries (visible in PGAdmin)
│       ├── nyc-taxi-analytics.sql  # Sample analytical queries
│       └── geospatial-taxi-analytics.sql  # PostGIS spatial queries
├── docker/                         # Docker configuration
│   ├── Dockerfile.postgres         # Custom PostgreSQL image with Python
│   └── init-data.py                # Automatic data loading script
├── python-scripts/                 # Manual data loading utilities (optional)
│   ├── load_taxi_data.py          # Trip data loader (parquet)
│   ├── load_reference_data.py     # Reference data loader (CSV + shapefiles)
│   └── main.py                    # Main script entry point
├── docs/                          # Documentation
├── pyproject.toml                 # Python dependencies
└── README.md
```

## Volume Storage

- **Report Scripts**: `./sql-scripts/reports-scripts` → Available in PGAdmin at `/var/lib/pgadmin/storage/sql-scripts`
- **Init Scripts**: `./sql-scripts/init-scripts` → Auto-executed during PostgreSQL startup
- **Database Data**: `postgres_data` → Persistent PostgreSQL data
- **PGAdmin Settings**: `pgadmin_data` → Persistent PGAdmin configuration
- **Taxi Data**: `./data/yellow/` → NYC Yellow taxi parquet files
- **Zone Data**: `./data/zones/` → NYC taxi zone lookup and shapefiles

## Performance Features

The schema includes optimized indexes for big data analytics:
- Datetime indexes for time-series analysis
- Location indexes for geographical queries
- Composite indexes for complex analytical queries
- Payment type indexes for financial analysis

## Environment Configuration

Customize settings in `.env`:

```env
# PostgreSQL
POSTGRES_DB=playground
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin123
POSTGRES_PORT=5432

# PGAdmin
PGADMIN_EMAIL=admin@admin.com
PGADMIN_PASSWORD=admin123
PGADMIN_PORT=8080
```

## Development

### Requirements
- Docker & Docker Compose
- Python 3.12+ (managed by uv)
- uv (Python package manager)

### Dependencies
- pandas & pyarrow (parquet file handling)
- geopandas & geoalchemy2 (geospatial data handling)
- psycopg2-binary & sqlalchemy (PostgreSQL connectivity)

## Data Source

NYC Yellow Taxi data sourced from [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- Format: Parquet files updated monthly
- Size: ~60MB per month (~3.4M records)
- Data: January 2025 (latest available)
- Location: `./data/yellow/yellow_tripdata_2025-01.parquet`
- **Taxi Zone Data**: Official NYC TLC taxi zone lookup and shapefiles
- Location: `./data/zones/taxi_zone_lookup.csv` and `./data/zones/taxi_zones.shp`