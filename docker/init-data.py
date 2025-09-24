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
import hashlib
import json

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
        # Clean all dictionary tables (excluding yellow_taxi_trips which contains trip data)
        logger.info("🧹 Cleaning dictionary tables for fresh data load...")
        with engine.begin() as conn:
            # Clean taxi zone tables
            conn.execute(text("TRUNCATE TABLE nyc_taxi.taxi_zone_shapes CASCADE"))
            conn.execute(text("TRUNCATE TABLE nyc_taxi.taxi_zone_lookup CASCADE"))
            # Clean lookup tables
            conn.execute(text("TRUNCATE TABLE nyc_taxi.rate_code_lookup CASCADE"))
            conn.execute(text("TRUNCATE TABLE nyc_taxi.payment_type_lookup CASCADE"))
            conn.execute(text("TRUNCATE TABLE nyc_taxi.vendor_lookup CASCADE"))
        logger.info("✅ Dictionary tables cleaned")

        # Ensure data directory exists and download all taxi zone data files
        data_dir = '/sql-scripts/data'
        zones_dir = os.path.join(data_dir, 'zones')

        # Always download fresh taxi zone data during initialization
        logger.info("📥 Downloading taxi zone reference data...")
        if not download_taxi_zone_data(data_dir):
            logger.error("💥 Failed to download taxi zone data")
            return False

        zones_csv_path = os.path.join(zones_dir, 'taxi_zone_lookup.csv')

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
        shapefile_path = os.path.join(zones_dir, 'taxi_zones.shp')
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

        # Reload lookup tables since we cleaned them
        logger.info("📚 Reloading lookup tables...")
        with engine.begin() as conn:
            # Rate code lookup
            conn.execute(text("""
                INSERT INTO nyc_taxi.rate_code_lookup (ratecodeid, rate_code_desc) VALUES
                (1, 'Standard rate'),
                (2, 'JFK'),
                (3, 'Newark'),
                (4, 'Nassau or Westchester'),
                (5, 'Negotiated fare'),
                (6, 'Group ride')
            """))

            # Payment type lookup
            conn.execute(text("""
                INSERT INTO nyc_taxi.payment_type_lookup (payment_type, payment_type_desc) VALUES
                (1, 'Credit card'),
                (2, 'Cash'),
                (3, 'No charge'),
                (4, 'Dispute'),
                (5, 'Unknown'),
                (6, 'Voided trip')
            """))

            # Vendor lookup
            conn.execute(text("""
                INSERT INTO nyc_taxi.vendor_lookup (vendorid, vendor_name) VALUES
                (1, 'Creative Mobile Technologies'),
                (2, 'VeriFone Inc.')
            """))
        logger.info("✅ Lookup tables reloaded")

        return True

    except Exception as e:
        logger.error(f"❌ Error loading taxi zones: {str(e)}")
        return False

def download_taxi_zone_data(data_dir='/sql-scripts/data'):
    """Download taxi zone reference data (CSV and shapefiles)"""
    logger.info("📥 Downloading taxi zone reference data...")

    # Ensure zones subdirectory exists
    zones_dir = os.path.join(data_dir, 'zones')
    os.makedirs(zones_dir, exist_ok=True)

    # Download taxi zone lookup CSV
    csv_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    csv_filename = "taxi_zone_lookup.csv"
    csv_path = os.path.join(zones_dir, csv_filename)

    # Always download fresh CSV during initialization
    logger.info(f"📥 Downloading {csv_filename}...")
    try:
        response = requests.get(csv_url)
        response.raise_for_status()
        with open(csv_path, 'wb') as f:
            f.write(response.content)
        logger.info(f"✅ Downloaded: {csv_filename}")
    except Exception as e:
        logger.error(f"❌ Failed to download {csv_filename}: {str(e)}")
        return False

    # Download taxi zone shapefile (ZIP format)
    zip_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
    zip_filename = "taxi_zones.zip"
    zip_path = os.path.join(zones_dir, zip_filename)

    # Always download fresh shapefile ZIP during initialization
    logger.info(f"📥 Downloading {zip_filename}...")
    try:
        response = requests.get(zip_url)
        response.raise_for_status()
        with open(zip_path, 'wb') as f:
            f.write(response.content)
        logger.info(f"✅ Downloaded: {zip_filename}")

        # Extract the ZIP file
        import zipfile
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(zones_dir)
        logger.info("✅ Extracted shapefile components")

        # Clean up the ZIP file
        os.remove(zip_path)
        logger.info("🗑️ Cleaned up ZIP file")

    except Exception as e:
        logger.error(f"❌ Failed to download or extract {zip_filename}: {str(e)}")
        return False

    # Verify shapefile components exist
    required_files = ['taxi_zones.shp', 'taxi_zones.dbf', 'taxi_zones.shx', 'taxi_zones.prj']
    for filename in required_files:
        file_path = os.path.join(zones_dir, filename)
        if not os.path.exists(file_path):
            logger.warning(f"⚠️ Missing shapefile component: {filename}")

    logger.info("✅ All taxi zone reference data files are available")
    return True

def download_taxi_data(year, month, data_dir='/sql-scripts/data'):
    """Download taxi data for a specific year and month"""
    # Ensure yellow subdirectory exists
    yellow_dir = os.path.join(data_dir, 'yellow')
    os.makedirs(yellow_dir, exist_ok=True)

    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet"
    filename = f"yellow_tripdata_{year:04d}-{month:02d}.parquet"
    file_path = os.path.join(yellow_dir, filename)

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

def check_month_already_processed(engine, year, month):
    """Check if a specific month has already been processed"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT status, records_loaded, processing_completed_at FROM nyc_taxi.data_processing_log WHERE data_year = :year AND data_month = :month"
            ), {"year": year, "month": month})
            row = result.fetchone()

            if row:
                status, records_loaded, completed_at = row
                if status == 'completed':
                    logger.info(f"🔄 {year}-{month:02d}: Already processed ({records_loaded:,} records at {completed_at})")
                    return True
                elif status == 'in_progress':
                    logger.warning(f"⚠️ {year}-{month:02d}: Previous processing incomplete, will retry")
                    # Delete incomplete record to allow retry
                    conn.execute(text(
                        "DELETE FROM nyc_taxi.data_processing_log WHERE data_year = :year AND data_month = :month"
                    ), {"year": year, "month": month})
                    conn.commit()
                elif status == 'failed':
                    logger.info(f"🔄 {year}-{month:02d}: Previous attempt failed, will retry")
                    # Delete failed record to allow retry
                    conn.execute(text(
                        "DELETE FROM nyc_taxi.data_processing_log WHERE data_year = :year AND data_month = :month"
                    ), {"year": year, "month": month})
                    conn.commit()

            return False
    except Exception as e:
        logger.error(f"❌ Error checking processing status for {year}-{month:02d}: {str(e)}")
        return False

def start_processing_log(engine, year, month, filename, backfill_config):
    """Start processing log entry for a month"""
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO nyc_taxi.data_processing_log
                (data_year, data_month, file_name, backfill_config, status)
                VALUES (:year, :month, :filename, :config, 'in_progress')
            """), {
                "year": year,
                "month": month,
                "filename": filename,
                "config": backfill_config
            })
            conn.commit()
            logger.info(f"📝 Started processing log for {year}-{month:02d}")
    except Exception as e:
        logger.error(f"❌ Error starting processing log for {year}-{month:02d}: {str(e)}")

def complete_processing_log(engine, year, month, records_loaded):
    """Complete processing log entry for a month"""
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                UPDATE nyc_taxi.data_processing_log
                SET status = 'completed',
                    records_loaded = :records,
                    processing_completed_at = CURRENT_TIMESTAMP
                WHERE data_year = :year AND data_month = :month
            """), {
                "year": year,
                "month": month,
                "records": records_loaded
            })
            conn.commit()
            logger.info(f"✅ Completed processing log for {year}-{month:02d}: {records_loaded:,} records")
    except Exception as e:
        logger.error(f"❌ Error completing processing log for {year}-{month:02d}: {str(e)}")

def fail_processing_log(engine, year, month):
    """Mark processing log entry as failed for a month"""
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                UPDATE nyc_taxi.data_processing_log
                SET status = 'failed'
                WHERE data_year = :year AND data_month = :month
            """), {"year": year, "month": month})
            conn.commit()
            logger.info(f"❌ Marked processing as failed for {year}-{month:02d}")
    except Exception as e:
        logger.error(f"❌ Error marking processing as failed for {year}-{month:02d}: {str(e)}")

def calculate_row_hash(row):
    """Calculate SHA-256 hash of all row values for duplicate detection"""
    try:
        # Convert all values to string, handling NaN/None values consistently
        row_dict = {}
        for column, value in row.items():
            if pd.isna(value) or value is None:
                row_dict[column] = ""
            elif isinstance(value, (int, float)):
                # Format numbers consistently to avoid precision issues
                row_dict[column] = f"{value:.10f}" if isinstance(value, float) else str(value)
            elif isinstance(value, pd.Timestamp):
                # Format timestamps consistently
                row_dict[column] = value.isoformat() if not pd.isna(value) else ""
            else:
                row_dict[column] = str(value)

        # Create deterministic JSON string (sorted keys for consistency)
        row_json = json.dumps(row_dict, sort_keys=True, separators=(',', ':'))

        # Calculate SHA-256 hash
        hash_obj = hashlib.sha256(row_json.encode('utf-8'))
        return hash_obj.hexdigest()

    except Exception as e:
        # Return a hash based on string representation as fallback
        logger.warning(f"⚠️ Hash calculation fallback for row: {str(e)}")
        fallback_string = '|'.join([str(v) if not pd.isna(v) else '' for v in row.values])
        return hashlib.sha256(fallback_string.encode('utf-8')).hexdigest()

def add_row_hash_column(df):
    """Add row_hash column to dataframe"""
    logger.info("🔐 Calculating row hashes for duplicate prevention...")

    # Calculate hash for each row
    df['row_hash'] = df.apply(calculate_row_hash, axis=1)

    # Check for hash collisions within the dataframe (should be extremely rare)
    hash_counts = df['row_hash'].value_counts()
    collisions = hash_counts[hash_counts > 1]

    if len(collisions) > 0:
        logger.warning(f"⚠️ Found {len(collisions)} hash collisions within batch - removing duplicates")
        # Keep first occurrence of each hash
        df = df.drop_duplicates(subset=['row_hash'], keep='first')

    logger.info(f"✅ Generated {len(df):,} unique row hashes")
    return df

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
    """Load a single parquet file into the database with duplicate handling"""
    filename = os.path.basename(file_path)

    try:
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

        # Add row hash for duplicate prevention (before chunking)
        df = add_row_hash_column(df)

        # Load in chunks with upsert logic
        total_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size else 0)
        loaded_rows = 0
        duplicate_rows = 0

        for i, chunk_start in enumerate(range(0, len(df), chunk_size)):
            chunk_end = min(chunk_start + chunk_size, len(df))
            chunk_df = df.iloc[chunk_start:chunk_end]

            try:
                # Try to insert chunk, handle duplicates gracefully
                chunk_df.to_sql(
                    'yellow_taxi_trips',
                    engine,
                    schema='nyc_taxi',
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                loaded_rows += len(chunk_df)

                if (i + 1) % 5 == 0 or i == total_chunks - 1:
                    logger.info(f"📥 {filename}: Loaded {chunk_end:,} rows ({i+1}/{total_chunks} chunks)")

            except Exception as chunk_error:
                # Handle duplicate constraint violations
                error_str = str(chunk_error).lower()
                is_duplicate_error = any(keyword in error_str for keyword in [
                    'unique constraint', 'duplicate key', 'row_hash'
                ])

                if is_duplicate_error:
                    duplicate_rows += len(chunk_df)
                    if (i + 1) % 5 == 0 or i == total_chunks - 1:
                        logger.warning(f"⚠️ {filename}: Chunk {i+1}/{total_chunks} - {len(chunk_df)} duplicates skipped (hash-based)")
                else:
                    logger.error(f"❌ {filename}: Error in chunk {i+1}: {str(chunk_error)}")
                    raise chunk_error

        if duplicate_rows > 0:
            logger.info(f"✅ {filename}: Loaded {loaded_rows:,} new rows, skipped {duplicate_rows:,} duplicates")
        else:
            logger.info(f"✅ {filename}: Completed loading {loaded_rows:,} rows")

        return loaded_rows

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

            # Check if month already processed
            if check_month_already_processed(engine, year, month):
                continue

            # Download file
            file_path = download_taxi_data(year, month, data_dir)
            if not file_path:
                logger.warning(f"⚠️ Skipping {year}-{month:02d} due to download failure")
                continue

            # Start processing log
            filename = os.path.basename(file_path)
            start_processing_log(engine, year, month, filename, backfill_config)

            try:
                # Load file
                chunk_size = int(os.getenv('DATA_CHUNK_SIZE', 10000))
                rows_loaded = load_single_parquet_file(engine, file_path, chunk_size)

                if rows_loaded > 0:
                    # Mark as completed
                    complete_processing_log(engine, year, month, rows_loaded)
                    total_rows += rows_loaded
                    logger.info(f"✅ {year}-{month:02d}: {rows_loaded:,} rows loaded (total: {total_rows:,})")
                else:
                    # Mark as failed
                    fail_processing_log(engine, year, month)
                    logger.warning(f"⚠️ {year}-{month:02d}: No rows loaded")

            except Exception as e:
                # Mark as failed
                fail_processing_log(engine, year, month)
                logger.error(f"❌ {year}-{month:02d}: Processing failed - {str(e)}")
                continue

        logger.info(f"🎉 Backfill completed: {total_rows:,} total rows loaded from processed months")
        return total_rows > 0

    else:
        # Original single-file loading logic
        try:
            data_dir = '/sql-scripts/data'
            yellow_dir = os.path.join(data_dir, 'yellow')
            parquet_path = os.path.join(yellow_dir, 'yellow_tripdata_2025-01.parquet')
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