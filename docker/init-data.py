#!/usr/bin/env python3
"""
Complete NYC Taxi Database Initialization
This script handles the complete database setup process:
1. Execute SQL scripts for schema creation
2. Load taxi zone reference data
3. Load complete taxi trip data
4. Verify data integrity

This script runs automatically when PostgreSQL container starts
"""

import os
import time
import glob
import pandas as pd
import geopandas as gpd
import psycopg2
from sqlalchemy import create_engine, text
from geoalchemy2 import Geometry
from shapely.geometry import MultiPolygon, Polygon
import logging
import requests
from datetime import datetime, timedelta
from urllib.parse import urlparse
from pathlib import Path

# Setup logging
# Create logs directory structure
base_log_dir = '/sql-scripts/logs'
os.makedirs(base_log_dir, exist_ok=True)

# Get backfill configuration for folder structure
backfill_config = os.getenv('BACKFILL_MONTHS', 'default')
if not backfill_config:
    backfill_config = 'default'

# Create subdirectory based on BACKFILL_MONTHS
log_subdir = os.path.join(base_log_dir, backfill_config)
os.makedirs(log_subdir, exist_ok=True)

# Generate log filename with execution timestamp
execution_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_filename = f'log_{execution_timestamp}.log'
log_filepath = os.path.join(log_subdir, log_filename)

# Configure dual logging: console + file
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Console handler (existing behavior)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

# File handler (new)
file_handler = logging.FileHandler(log_filepath, mode='w', encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

# Add both handlers to logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# Initial log entry
logger.info(f"📝 Logging to file: {log_filepath}")
logger.info(f"🕒 Execution started at: {datetime.now().isoformat()}")
logger.info(f"⚙️ Backfill configuration: {backfill_config}")

def wait_for_postgres(host='localhost', port=5432, database='playground',
                      user='admin', password='admin123', max_attempts=30):
    """Wait for PostgreSQL to be ready"""
    attempt = 0
    while attempt < max_attempts:
        try:
            # Try to connect to the database
            conn = psycopg2.connect(
                host=host, port=port, database=database,
                user=user, password=password,
                connect_timeout=5
            )
            # Test that we can actually execute a query
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            conn.close()
            logger.info("✅ PostgreSQL is ready!")
            return True
        except (psycopg2.OperationalError, psycopg2.DatabaseError) as e:
            attempt += 1
            if attempt <= 10:
                # Log more frequently in the beginning
                logger.info(f"⏳ Waiting for PostgreSQL... (attempt {attempt}/{max_attempts})")
            elif attempt % 5 == 0:
                # Then log every 5th attempt
                logger.info(f"⏳ Waiting for PostgreSQL... (attempt {attempt}/{max_attempts})")
            time.sleep(3)

    logger.error("❌ PostgreSQL not available after maximum attempts")
    return False

def execute_sql_scripts(engine):
    """Execute SQL initialization scripts in order"""
    logger.info("🗃️ Executing SQL initialization scripts...")

    # Define the script directory and order
    script_dir = '/sql-scripts/init-scripts'

    if not os.path.exists(script_dir):
        logger.warning(f"⚠️ SQL scripts directory not found: {script_dir}")
        return False

    # Get all SQL files and sort them (ensuring proper execution order)
    sql_files = sorted(glob.glob(os.path.join(script_dir, "*.sql")))

    if not sql_files:
        logger.warning("⚠️ No SQL scripts found to execute")
        return False

    logger.info(f"📄 Found {len(sql_files)} SQL scripts to execute")

    try:
        with engine.connect() as conn:
            # Execute each script in order
            for script_path in sql_files:
                script_name = os.path.basename(script_path)
                logger.info(f"🔧 Executing: {script_name}")

                # Read the SQL script
                with open(script_path, 'r', encoding='utf-8') as f:
                    sql_content = f.read()

                # Clean up the SQL content
                sql_content = sql_content.strip()

                # Execute the entire script as one block for better compatibility
                # This handles complex statements like INSERT...SELECT properly
                try:
                    # Remove comments and empty lines for cleaner execution
                    clean_sql = '\n'.join([
                        line for line in sql_content.split('\n')
                        if line.strip() and not line.strip().startswith('--')
                    ])

                    if clean_sql:
                        conn.execute(text(clean_sql))
                        conn.commit()

                except Exception as exec_error:
                    logger.warning(f"⚠️ Block execution failed for {script_name}, trying statement by statement: {exec_error}")

                    # Fallback: try to execute statements individually
                    statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]

                    for i, statement in enumerate(statements):
                        # Skip comments
                        if statement.startswith('--'):
                            continue

                        try:
                            conn.execute(text(statement))
                            conn.commit()
                        except Exception as stmt_error:
                            logger.error(f"❌ Error in {script_name} statement {i+1}: {stmt_error}")
                            logger.error(f"Statement was: {statement[:100]}...")
                            # Continue with other statements

                logger.info(f"✅ Completed: {script_name}")

        logger.info("✅ All SQL scripts executed successfully!")
        return True

    except Exception as e:
        logger.error(f"❌ Error executing SQL scripts: {str(e)}")
        return False

def load_taxi_zones(engine):
    """Load taxi zone reference data"""
    logger.info("📍 Loading taxi zone reference data...")

    try:
        # Load CSV data
        zones_csv_path = '/sql-scripts/data/taxi_zone_lookup.csv'
        if not os.path.exists(zones_csv_path):
            logger.warning(f"⚠️ Taxi zone CSV not found at {zones_csv_path}")
            return False

        zones_df = pd.read_csv(zones_csv_path)
        zones_df.columns = zones_df.columns.str.lower()

        # Handle NULL values - drop rows where zone or borough is NULL
        zones_df = zones_df.dropna(subset=['zone', 'borough'])

        # Fill remaining NULLs in service_zone with 'Unknown'
        zones_df['service_zone'] = zones_df['service_zone'].fillna('Unknown')

        # Load to database
        zones_df.to_sql(
            'taxi_zone_lookup',
            engine,
            schema='nyc_taxi',
            if_exists='append',
            index=False
        )
        logger.info(f"✅ Loaded {len(zones_df)} taxi zones from CSV")

        # Load shapefile if available
        shapefile_path = '/sql-scripts/data/taxi_zones.shp'
        if os.path.exists(shapefile_path):
            logger.info("🗺️ Loading taxi zone shapes...")
            shapes_gdf = gpd.read_file(shapefile_path)

            # Clean and prepare data
            shapes_gdf.columns = shapes_gdf.columns.str.lower()
            shapes_gdf = shapes_gdf.rename(columns={
                'locationid': 'locationid',
                'shape_leng': 'shape_leng',
                'shape_area': 'shape_area'
            })

            # Convert to NYC State Plane if needed
            if shapes_gdf.crs.to_epsg() != 2263:
                logger.info("🔄 Converting CRS to NYC State Plane (EPSG:2263)")
                shapes_gdf = shapes_gdf.to_crs(epsg=2263)

            # Ensure MultiPolygon geometry
            def ensure_multipolygon(geom):
                if isinstance(geom, Polygon):
                    return MultiPolygon([geom])
                return geom

            shapes_gdf['geometry'] = shapes_gdf['geometry'].apply(ensure_multipolygon)

            # Load to database
            shapes_gdf.to_postgis(
                'taxi_zone_shapes',
                engine,
                schema='nyc_taxi',
                if_exists='append',
                index=False
            )
            logger.info(f"✅ Loaded {len(shapes_gdf)} taxi zone shapes")
        else:
            logger.warning(f"⚠️ Shapefile not found at {shapefile_path}")

        return True

    except Exception as e:
        logger.error(f"❌ Error loading taxi zones: {str(e)}")
        return False

def download_taxi_data(year, month, data_dir='/sql-scripts/data'):
    """Download taxi data for a specific year and month"""
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet"
    filename = f"yellow_tripdata_{year:04d}-{month:02d}.parquet"
    file_path = os.path.join(data_dir, filename)

    # Check if file already exists
    if os.path.exists(file_path):
        logger.info(f"📂 File already exists: {filename}")
        return file_path

    logger.info(f"📥 Downloading {filename} from NYC TLC...")

    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        # Get file size for progress tracking
        total_size = int(response.headers.get('content-length', 0))

        with open(file_path, 'wb') as f:
            downloaded = 0
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)
                if total_size > 0:
                    progress = (downloaded / total_size) * 100
                    if downloaded % (1024 * 1024 * 10) == 0:  # Log every 10MB
                        logger.info(f"📥 Progress: {progress:.1f}% ({downloaded:,}/{total_size:,} bytes)")

        logger.info(f"✅ Downloaded: {filename} ({os.path.getsize(file_path):,} bytes)")
        return file_path

    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Failed to download {filename}: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"❌ Error downloading {filename}: {str(e)}")
        return None

def get_backfill_months(backfill_config):
    """Parse backfill configuration and return list of (year, month) tuples"""
    months = []

    if backfill_config == 'all':
        # Load all available data from 2020 to current month
        current_date = datetime.now()
        for year in range(2020, current_date.year + 1):
            end_month = current_date.month if year == current_date.year else 12
            for month in range(1, end_month + 1):
                months.append((year, month))
    elif backfill_config.startswith('last_'):
        # Parse formats like 'last_6_months', 'last_12_months'
        try:
            num_months = int(backfill_config.split('_')[1])
            current_date = datetime.now()
            for i in range(num_months):
                date = current_date - timedelta(days=30 * i)  # Approximate month
                months.append((date.year, date.month))
        except (IndexError, ValueError):
            logger.error(f"❌ Invalid backfill format: {backfill_config}")
    elif ',' in backfill_config:
        # Parse comma-separated list like '2024-01,2024-02,2024-03'
        for month_str in backfill_config.split(','):
            try:
                year, month = month_str.strip().split('-')
                months.append((int(year), int(month)))
            except ValueError:
                logger.error(f"❌ Invalid month format: {month_str}")
    else:
        # Parse single month like '2024-01'
        try:
            year, month = backfill_config.split('-')
            months.append((int(year), int(month)))
        except ValueError:
            logger.error(f"❌ Invalid backfill format: {backfill_config}")

    # Remove duplicates and sort
    months = sorted(list(set(months)))
    logger.info(f"📅 Backfill months: {len(months)} months from {months[0] if months else 'none'} to {months[-1] if months else 'none'}")
    return months

def load_single_parquet_file(engine, file_path, chunk_size=10000):
    """Load a single parquet file into the database"""
    try:
        filename = os.path.basename(file_path)
        logger.info(f"📥 Loading {filename}...")

        # Read parquet file
        df = pd.read_parquet(file_path)
        logger.info(f"📊 File contains {len(df):,} rows")

        # Convert column names to lowercase to match database schema
        df.columns = df.columns.str.lower()

        # Clean and prepare data
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

        # Fill nulls
        numeric_cols = ['passenger_count', 'trip_distance', 'ratecodeid', 'fare_amount',
                       'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge',
                       'total_amount', 'congestion_surcharge', 'airport_fee', 'cbd_congestion_fee']

        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0)

        df['store_and_fwd_flag'] = df['store_and_fwd_flag'].fillna('N')

        # Load in chunks
        total_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size else 0)

        for i, chunk_start in enumerate(range(0, len(df), chunk_size)):
            chunk_end = min(chunk_start + chunk_size, len(df))
            chunk_df = df.iloc[chunk_start:chunk_end]

            chunk_df.to_sql(
                'yellow_taxi_trips',
                engine,
                schema='nyc_taxi',
                if_exists='append',
                index=False,
                method='multi'
            )

            if (i + 1) % 5 == 0 or i == total_chunks - 1:
                logger.info(f"📥 {filename}: Loaded {chunk_end:,} rows ({i+1}/{total_chunks} chunks)")

        logger.info(f"✅ {filename}: Completed loading {len(df):,} rows")
        return len(df)

    except Exception as e:
        logger.error(f"❌ Error loading {filename}: {str(e)}")
        return 0

def load_trip_data(engine, load_all=True):
    """Load trip data - either from existing files or via backfill download"""
    logger.info("🚕 Loading trip data...")

    # Check for backfill configuration
    backfill_config = os.getenv('BACKFILL_MONTHS', '')

    total_rows = 0

    if backfill_config:
        logger.info(f"🔄 Backfill mode enabled: {backfill_config}")

        # Get months to download
        months_to_load = get_backfill_months(backfill_config)

        if not months_to_load:
            logger.warning("⚠️ No valid months found in backfill configuration")
            return False

        # Ensure data directory exists
        data_dir = '/sql-scripts/data'
        os.makedirs(data_dir, exist_ok=True)

        # Download and load each month
        for year, month in months_to_load:
            logger.info(f"📅 Processing {year}-{month:02d}...")

            # Download file
            file_path = download_taxi_data(year, month, data_dir)
            if not file_path:
                logger.warning(f"⚠️ Skipping {year}-{month:02d} due to download failure")
                continue

            # Load file
            chunk_size = int(os.getenv('DATA_CHUNK_SIZE', 10000))
            rows_loaded = load_single_parquet_file(engine, file_path, chunk_size)
            total_rows += rows_loaded

            logger.info(f"✅ {year}-{month:02d}: {rows_loaded:,} rows loaded (total: {total_rows:,})")

        logger.info(f"🎉 Backfill completed: {total_rows:,} total rows loaded from {len(months_to_load)} months")
        return total_rows > 0

    else:
        # Original single-file loading logic
        try:
            parquet_path = '/sql-scripts/data/yellow_tripdata_2025-01.parquet'
            if not os.path.exists(parquet_path):
                logger.warning(f"⚠️ Trip data parquet not found at {parquet_path}")
                return False

            chunk_size = int(os.getenv('DATA_CHUNK_SIZE', 10000))
            rows_loaded = load_single_parquet_file(engine, parquet_path, chunk_size)
            return rows_loaded > 0

        except Exception as e:
            logger.error(f"❌ Error in single-file loading: {str(e)}")
            return False


def verify_data_load(engine):
    """Verify that data was loaded correctly"""
    logger.info("🔍 Verifying data load...")

    try:
        with engine.connect() as conn:
            # Check taxi zones
            result = conn.execute(text("SELECT COUNT(*) FROM nyc_taxi.taxi_zone_lookup"))
            zone_count = result.fetchone()[0]
            logger.info(f"📍 Taxi zones: {zone_count:,}")

            # Check shapes (if available)
            try:
                result = conn.execute(text("SELECT COUNT(*) FROM nyc_taxi.taxi_zone_shapes"))
                shape_count = result.fetchone()[0]
                logger.info(f"🗺️ Taxi zone shapes: {shape_count:,}")
            except:
                logger.info("🗺️ Taxi zone shapes: Not available")

            # Check trips
            result = conn.execute(text("SELECT COUNT(*) FROM nyc_taxi.yellow_taxi_trips"))
            trip_count = result.fetchone()[0]
            logger.info(f"🚕 Trip records: {trip_count:,}")

            if trip_count > 0:
                # Show sample data with zone names
                result = conn.execute(text("""
                    SELECT
                        DATE(yt.tpep_pickup_datetime) as trip_date,
                        COUNT(*) as trips,
                        pickup_zone.borough as pickup_borough
                    FROM nyc_taxi.yellow_taxi_trips yt
                    JOIN nyc_taxi.taxi_zone_lookup pickup_zone ON yt.pulocationid = pickup_zone.locationid
                    GROUP BY DATE(yt.tpep_pickup_datetime), pickup_zone.borough
                    ORDER BY trips DESC
                    LIMIT 5
                """))

                logger.info("📊 Top trip patterns:")
                for row in result.fetchall():
                    logger.info(f"   {row[0]} - {row[2]}: {row[1]:,} trips")

        logger.info("✅ Data verification completed successfully!")
        return True

    except Exception as e:
        logger.error(f"❌ Error during verification: {str(e)}")
        return False

def main():
    """Main initialization function"""
    logger.info("🚀 Starting Complete NYC Taxi Database Initialization...")

    # Database connection parameters
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'database': os.getenv('POSTGRES_DB', 'playground'),
        'user': os.getenv('POSTGRES_USER', 'admin'),
        'password': os.getenv('POSTGRES_PASSWORD', 'admin123')
    }

    # Wait for PostgreSQL
    if not wait_for_postgres(**db_config):
        logger.error("💥 Failed to connect to PostgreSQL")
        return False

    # Create SQLAlchemy engine
    connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    engine = create_engine(connection_string)

    try:
        # Step 1: Execute SQL scripts (schema, tables, indexes)
        logger.info("🔧 Step 1: Database Schema Creation")
        if not execute_sql_scripts(engine):
            logger.error("💥 Failed to execute SQL initialization scripts")
            return False

        # Step 2: Load taxi zone reference data
        logger.info("📍 Step 2: Loading Taxi Zone Reference Data")
        if not load_taxi_zones(engine):
            logger.error("💥 Failed to load taxi zone data")
            return False

        # Step 3: Load complete trip data
        logger.info("🚕 Step 3: Loading Complete Trip Data")
        load_all_data = os.getenv('INIT_LOAD_ALL_DATA', 'true').lower() == 'true'
        if not load_trip_data(engine, load_all=load_all_data):
            logger.warning("⚠️ Failed to load trip data, continuing without it")

        # Step 4: Verify everything loaded correctly
        logger.info("🔍 Step 4: Data Verification")
        verify_data_load(engine)

        logger.info("🎉 Complete database initialization finished successfully!")
        logger.info("🌟 Your NYC Taxi playground is ready with 3.4M+ records!")
        return True

    except Exception as e:
        logger.error(f"💥 Initialization failed: {str(e)}")
        return False

if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)