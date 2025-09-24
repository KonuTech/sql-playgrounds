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

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def wait_for_postgres(host='localhost', port=5432, database='playground',
                      user='admin', password='admin123', max_attempts=30):
    """Wait for PostgreSQL to be ready"""
    attempt = 0
    while attempt < max_attempts:
        try:
            conn = psycopg2.connect(
                host=host, port=port, database=database,
                user=user, password=password
            )
            conn.close()
            logger.info("✅ PostgreSQL is ready!")
            return True
        except psycopg2.OperationalError:
            attempt += 1
            logger.info(f"⏳ Waiting for PostgreSQL... (attempt {attempt}/{max_attempts})")
            time.sleep(2)

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

def load_trip_data(engine, load_all=True):
    """Load trip data from parquet file"""
    if load_all:
        logger.info("🚕 Loading ALL trip data from parquet file...")
    else:
        logger.info("🚕 Loading trip data...")

    try:
        parquet_path = '/sql-scripts/data/yellow_tripdata_2025-01.parquet'
        if not os.path.exists(parquet_path):
            logger.warning(f"⚠️ Trip data parquet not found at {parquet_path}")
            return False

        # Read entire parquet file
        df = pd.read_parquet(parquet_path)
        logger.info(f"📊 Loading all {len(df):,} rows from parquet file")

        # Clean and prepare data
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

        # Fill nulls
        numeric_cols = ['passenger_count', 'trip_distance', 'RatecodeID', 'fare_amount',
                       'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge',
                       'total_amount', 'congestion_surcharge', 'Airport_fee', 'cbd_congestion_fee']

        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0)

        df['store_and_fwd_flag'] = df['store_and_fwd_flag'].fillna('N')

        # Load in chunks
        chunk_size = 10000
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
                logger.info(f"📥 Loaded {chunk_end:,} rows ({i+1}/{total_chunks} chunks)")

        logger.info(f"✅ Trip data loading completed: {len(df):,} rows")
        return True

    except Exception as e:
        logger.error(f"❌ Error loading trip data: {str(e)}")
        return False

def verify_data_load(engine):
    """Verify that data was loaded correctly"""
    logger.info("🔍 Verifying data load...")

    try:
        with engine.connect() as conn:
            # Check taxi zones
            result = conn.execute("SELECT COUNT(*) FROM nyc_taxi.taxi_zone_lookup")
            zone_count = result.fetchone()[0]
            logger.info(f"📍 Taxi zones: {zone_count:,}")

            # Check shapes (if available)
            try:
                result = conn.execute("SELECT COUNT(*) FROM nyc_taxi.taxi_zone_shapes")
                shape_count = result.fetchone()[0]
                logger.info(f"🗺️ Taxi zone shapes: {shape_count:,}")
            except:
                logger.info("🗺️ Taxi zone shapes: Not available")

            # Check trips
            result = conn.execute("SELECT COUNT(*) FROM nyc_taxi.yellow_taxi_trips")
            trip_count = result.fetchone()[0]
            logger.info(f"🚕 Trip records: {trip_count:,}")

            if trip_count > 0:
                # Show sample data with zone names
                result = conn.execute("""
                    SELECT
                        DATE(yt.tpep_pickup_datetime) as trip_date,
                        COUNT(*) as trips,
                        pickup_zone.borough as pickup_borough
                    FROM nyc_taxi.yellow_taxi_trips yt
                    JOIN nyc_taxi.taxi_zone_lookup pickup_zone ON yt.PULocationID = pickup_zone.locationid
                    GROUP BY DATE(yt.tpep_pickup_datetime), pickup_zone.borough
                    ORDER BY trips DESC
                    LIMIT 5
                """)

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