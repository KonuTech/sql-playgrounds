#!/usr/bin/env python3
"""
NYC Yellow Taxi Data Loader
Loads parquet files from tmp/ directory into PostgreSQL database
"""

import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
from pathlib import Path
import argparse

def load_taxi_data(db_host='localhost', db_port=5432, db_name='playground',
                  db_user='admin', db_password='admin123',
                  parquet_file='./data/yellow/yellow_tripdata_2025-01.parquet',
                  chunk_size=10000, max_rows=None):
    """
    Load NYC Yellow Taxi data from parquet file into PostgreSQL

    Args:
        db_host: PostgreSQL host
        db_port: PostgreSQL port
        db_name: Database name
        db_user: Database user
        db_password: Database password
        parquet_file: Path to parquet file
        chunk_size: Number of rows to process at once
        max_rows: Maximum rows to load (None for all)
    """

    # Check if parquet file exists
    if not os.path.exists(parquet_file):
        print(f"Error: Parquet file {parquet_file} not found")
        return False

    # Create connection string
    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    try:
        # Create SQLAlchemy engine
        engine = create_engine(connection_string)

        # Test connection
        with engine.connect() as conn:
            result = conn.execute("SELECT version()")
            print(f"Connected to PostgreSQL: {result.fetchone()[0]}")

        # Read parquet file info
        df_info = pd.read_parquet(parquet_file, nrows=0)
        print(f"\nParquet file columns: {list(df_info.columns)}")

        # Read the parquet file
        print(f"Reading parquet file: {parquet_file}")
        if max_rows:
            # For testing with limited rows
            df = pd.read_parquet(parquet_file)
            df = df.head(max_rows)
            print(f"Loading first {max_rows} rows for testing")
        else:
            df = pd.read_parquet(parquet_file)

        print(f"Loaded {len(df)} rows from parquet file")
        print(f"Data shape: {df.shape}")

        # Clean and prepare data
        print("Cleaning data...")

        # Handle null values and data type conversions
        # Convert datetime columns
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

        # Fill null values with appropriate defaults
        numeric_cols = ['passenger_count', 'trip_distance', 'RatecodeID', 'fare_amount',
                       'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge',
                       'total_amount', 'congestion_surcharge', 'Airport_fee', 'cbd_congestion_fee']

        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0)

        # Fill string columns
        df['store_and_fwd_flag'] = df['store_and_fwd_flag'].fillna('N')

        # Load data in chunks
        print(f"Loading data to PostgreSQL in chunks of {chunk_size}...")

        total_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size else 0)

        for i, chunk_start in enumerate(range(0, len(df), chunk_size)):
            chunk_end = min(chunk_start + chunk_size, len(df))
            chunk_df = df.iloc[chunk_start:chunk_end]

            # Load chunk to database
            chunk_df.to_sql(
                'yellow_taxi_trips',
                engine,
                schema='nyc_taxi',
                if_exists='append',
                index=False,
                method='multi'
            )

            print(f"Loaded chunk {i+1}/{total_chunks}: rows {chunk_start+1}-{chunk_end}")

        print(f"\n✅ Successfully loaded {len(df)} rows into nyc_taxi.yellow_taxi_trips")

        # Verify the load
        with engine.connect() as conn:
            result = conn.execute("SELECT COUNT(*) FROM nyc_taxi.yellow_taxi_trips")
            count = result.fetchone()[0]
            print(f"Total rows in database: {count}")

        return True

    except Exception as e:
        print(f"❌ Error loading data: {str(e)}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Load NYC Yellow Taxi data into PostgreSQL')
    parser.add_argument('--host', default='localhost', help='PostgreSQL host')
    parser.add_argument('--port', type=int, default=5432, help='PostgreSQL port')
    parser.add_argument('--database', default='playground', help='Database name')
    parser.add_argument('--user', default='admin', help='Database user')
    parser.add_argument('--password', default='admin123', help='Database password')
    parser.add_argument('--file', default='./data/yellow/yellow_tripdata_2025-01.parquet', help='Parquet file path')
    parser.add_argument('--chunk-size', type=int, default=10000, help='Chunk size for loading')
    parser.add_argument('--max-rows', type=int, help='Maximum rows to load (for testing)')

    args = parser.parse_args()

    success = load_taxi_data(
        db_host=args.host,
        db_port=args.port,
        db_name=args.database,
        db_user=args.user,
        db_password=args.password,
        parquet_file=args.file,
        chunk_size=args.chunk_size,
        max_rows=args.max_rows
    )

    if success:
        print("\n🎉 Data loading completed successfully!")
        print("You can now query the data using PGAdmin or psql:")
        print("  SELECT COUNT(*) FROM nyc_taxi.yellow_taxi_trips;")
        print("  SELECT * FROM nyc_taxi.yellow_taxi_trips LIMIT 10;")
    else:
        print("\n💥 Data loading failed!")

if __name__ == '__main__':
    main()