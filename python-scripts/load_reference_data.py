#!/usr/bin/env python3
"""
NYC Taxi Reference Data Loader
Loads taxi zone lookup table (CSV) and shapefile data into PostgreSQL
"""

import pandas as pd
import geopandas as gpd
import psycopg2
from sqlalchemy import create_engine
from geoalchemy2 import Geometry
import os
import argparse

def load_reference_data(db_host='localhost', db_port=5432, db_name='playground',
                       db_user='admin', db_password='admin123',
                       csv_file='./data/zones/taxi_zone_lookup.csv',
                       shapefile='./data/zones/taxi_zones.shp'):
    """
    Load NYC Taxi reference data from CSV and shapefile into PostgreSQL

    Args:
        db_host: PostgreSQL host
        db_port: PostgreSQL port
        db_name: Database name
        db_user: Database user
        db_password: Database password
        csv_file: Path to taxi zone lookup CSV
        shapefile: Path to taxi zones shapefile
    """

    # Check if files exist
    if not os.path.exists(csv_file):
        print(f"Error: CSV file {csv_file} not found")
        return False

    if not os.path.exists(shapefile):
        print(f"Error: Shapefile {shapefile} not found")
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

        print("\n1. Loading Taxi Zone Lookup Table...")
        # Load CSV data
        zones_df = pd.read_csv(csv_file)
        print(f"Loaded {len(zones_df)} taxi zones from CSV")
        print(f"Columns: {list(zones_df.columns)}")

        # Clean column names to match our schema
        zones_df.columns = zones_df.columns.str.lower()
        zones_df = zones_df.rename(columns={'locationid': 'locationid'})

        # Load to database
        zones_df.to_sql(
            'taxi_zone_lookup',
            engine,
            schema='nyc_taxi',
            if_exists='replace',
            index=False
        )
        print(f"✅ Loaded {len(zones_df)} taxi zones to database")

        print("\n2. Loading Taxi Zone Shapes...")
        # Load shapefile data
        shapes_gdf = gpd.read_file(shapefile)
        print(f"Loaded {len(shapes_gdf)} taxi zone shapes from shapefile")
        print(f"Columns: {list(shapes_gdf.columns)}")
        print(f"Geometry type: {shapes_gdf.geometry.geom_type.iloc[0]}")
        print(f"Original CRS: {shapes_gdf.crs}")

        # Clean column names and prepare data
        shapes_gdf.columns = shapes_gdf.columns.str.lower()
        shapes_gdf = shapes_gdf.rename(columns={
            'locationid': 'locationid',
            'shape_leng': 'shape_leng',
            'shape_area': 'shape_area'
        })

        # Convert to NYC State Plane (EPSG:2263) if not already
        if shapes_gdf.crs.to_epsg() != 2263:
            print(f"Converting CRS from {shapes_gdf.crs} to EPSG:2263 (NYC State Plane)")
            shapes_gdf = shapes_gdf.to_crs(epsg=2263)

        # Convert POLYGON to MULTIPOLYGON for consistency
        from shapely.geometry import MultiPolygon, Polygon
        def ensure_multipolygon(geom):
            if isinstance(geom, Polygon):
                return MultiPolygon([geom])
            return geom

        shapes_gdf['geometry'] = shapes_gdf['geometry'].apply(ensure_multipolygon)

        # Load to database with geospatial support
        shapes_gdf.to_postgis(
            'taxi_zone_shapes',
            engine,
            schema='nyc_taxi',
            if_exists='replace',
            index=False
        )
        print(f"✅ Loaded {len(shapes_gdf)} taxi zone shapes to database")

        # Verify the load
        print("\n3. Verifying data load...")
        with engine.connect() as conn:
            # Check taxi zones
            result = conn.execute("SELECT COUNT(*) FROM nyc_taxi.taxi_zone_lookup")
            zone_count = result.fetchone()[0]
            print(f"Taxi zones in database: {zone_count}")

            # Check shapes
            result = conn.execute("SELECT COUNT(*) FROM nyc_taxi.taxi_zone_shapes")
            shape_count = result.fetchone()[0]
            print(f"Taxi zone shapes in database: {shape_count}")

            # Sample queries
            result = conn.execute("""
                SELECT zone, borough, service_zone
                FROM nyc_taxi.taxi_zone_lookup
                WHERE locationid IN (1, 4, 79, 161)
                ORDER BY locationid
            """)
            sample_zones = result.fetchall()
            print(f"\nSample taxi zones:")
            for zone in sample_zones:
                print(f"  - {zone[0]} ({zone[1]}) - {zone[2]}")

            # Check PostGIS functionality
            result = conn.execute("""
                SELECT locationid, zone,
                       ROUND(ST_Area(geometry)::numeric, 2) as area_sq_feet
                FROM nyc_taxi.taxi_zone_shapes
                WHERE locationid IN (1, 4, 79, 161)
                ORDER BY locationid
            """)
            sample_shapes = result.fetchall()
            print(f"\nSample shape areas (sq feet):")
            for shape in sample_shapes:
                print(f"  - Zone {shape[0]} ({shape[1]}): {shape[2]:,} sq ft")

        return True

    except Exception as e:
        print(f"❌ Error loading reference data: {str(e)}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Load NYC Taxi reference data into PostgreSQL')
    parser.add_argument('--host', default='localhost', help='PostgreSQL host')
    parser.add_argument('--port', type=int, default=5432, help='PostgreSQL port')
    parser.add_argument('--database', default='playground', help='Database name')
    parser.add_argument('--user', default='admin', help='Database user')
    parser.add_argument('--password', default='admin123', help='Database password')
    parser.add_argument('--csv', default='./data/zones/taxi_zone_lookup.csv', help='Taxi zone CSV file path')
    parser.add_argument('--shapefile', default='./data/zones/taxi_zones.shp', help='Taxi zones shapefile path')

    args = parser.parse_args()

    print("🚕 NYC Taxi Reference Data Loader")
    print("=" * 40)

    success = load_reference_data(
        db_host=args.host,
        db_port=args.port,
        db_name=args.database,
        db_user=args.user,
        db_password=args.password,
        csv_file=args.csv,
        shapefile=args.shapefile
    )

    if success:
        print("\n🎉 Reference data loading completed successfully!")
        print("\nYou can now run geospatial queries like:")
        print("  SELECT zone, borough FROM nyc_taxi.taxi_zone_lookup WHERE borough = 'Manhattan';")
        print("  SELECT COUNT(*) FROM nyc_taxi.taxi_zone_shapes WHERE ST_Area(geometry) > 50000000;")
    else:
        print("\n💥 Reference data loading failed!")

if __name__ == '__main__':
    main()