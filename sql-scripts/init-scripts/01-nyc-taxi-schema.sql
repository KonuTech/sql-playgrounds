-- NYC Yellow Taxi Trip Records Schema for PostgreSQL
-- Based on actual NYC TLC Trip Record Data (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
-- This is a real-world big data model with 3.4+ million taxi trips per month
-- Schema based on analysis of yellow_tripdata_2025-01.parquet

-- Create schema for NYC Taxi data
CREATE SCHEMA IF NOT EXISTS nyc_taxi;
SET search_path = nyc_taxi, public;

-- Yellow Taxi Trip Records table
-- This table structure exactly matches the NYC TLC Yellow Taxi data format (20 columns)
CREATE TABLE yellow_taxi_trips (
    -- Trip identifiers
    vendorid INTEGER,                    -- Provider that provided the record (1= Creative Mobile Technologies, 2= VeriFone Inc.)

    -- Trip timing
    tpep_pickup_datetime TIMESTAMP,      -- Date and time when the meter was engaged
    tpep_dropoff_datetime TIMESTAMP,     -- Date and time when the meter was disengaged

    -- Passenger information
    passenger_count DECIMAL(4,1),        -- Number of passengers in the vehicle (can be fractional)

    -- Trip distance
    trip_distance DECIMAL(8,2),          -- Trip distance in miles

    -- Location and rate information
    ratecodeid DECIMAL(4,1),             -- Rate code in effect at the end of the trip
    store_and_fwd_flag VARCHAR(1),       -- Y= store and forward trip, N= not a store and forward trip
    pulocationid INTEGER,                -- TLC Taxi Zone where the taximeter was engaged
    dolocationid INTEGER,                -- TLC Taxi Zone where the taximeter was disengaged

    -- Payment information
    payment_type BIGINT,                 -- Payment method (1= Credit card, 2= Cash, 3= No charge, 4= Dispute, 5= Unknown, 6= Voided trip)
    fare_amount DECIMAL(8,2),           -- Time-and-distance fare calculated by the meter
    extra DECIMAL(8,2),                 -- Miscellaneous extras and surcharges ($0.50 and $1 rush hour and overnight charges)
    mta_tax DECIMAL(8,2),               -- $0.50 MTA tax that is automatically triggered based on the metered rate in use
    tip_amount DECIMAL(8,2),            -- Tip amount (automatically populated for credit card tips, cash tips not included)
    tolls_amount DECIMAL(8,2),          -- Total amount of all tolls paid in trip
    improvement_surcharge DECIMAL(8,2),  -- $0.30 improvement surcharge assessed trips at the flag drop
    total_amount DECIMAL(8,2),          -- Total amount charged to passengers (does not include cash tips)
    congestion_surcharge DECIMAL(8,2),  -- Total amount collected in trip for NYS congestion surcharge
    airport_fee DECIMAL(8,2),           -- $1.25 for pick up only at LaGuardia and John F. Kennedy Airports
    cbd_congestion_fee DECIMAL(8,2),     -- CBD (Central Business District) congestion fee

    -- Hash-based duplicate prevention (ultimate protection)
    row_hash VARCHAR(64) UNIQUE          -- SHA-256 hash of all row values prevents any duplicate row
);

-- Taxi Zone Lookup table (complete reference data from NYC TLC)
CREATE TABLE taxi_zone_lookup (
    locationid INTEGER PRIMARY KEY,
    borough VARCHAR(50) NOT NULL,
    zone VARCHAR(100) NOT NULL,
    service_zone VARCHAR(50) NOT NULL
);

-- Taxi Zone Shapes table (geospatial data from NYC TLC)
-- Contains polygon geometries for each taxi zone
CREATE TABLE taxi_zone_shapes (
    objectid INTEGER PRIMARY KEY,
    locationid INTEGER NOT NULL,
    zone VARCHAR(100) NOT NULL,
    borough VARCHAR(50) NOT NULL,
    shape_leng DECIMAL(15,6),
    shape_area DECIMAL(15,6),
    geometry GEOMETRY(MULTIPOLYGON, 2263), -- NYC State Plane coordinate system
    FOREIGN KEY (locationid) REFERENCES taxi_zone_lookup(locationid)
);

-- Create spatial index for efficient geospatial queries
CREATE INDEX idx_taxi_zone_shapes_geometry ON taxi_zone_shapes USING GIST (geometry);

-- Rate Code Lookup table
CREATE TABLE rate_code_lookup (
    ratecodeid INTEGER PRIMARY KEY,
    rate_code_desc VARCHAR(50)
);

-- Payment Type Lookup table
CREATE TABLE payment_type_lookup (
    payment_type INTEGER PRIMARY KEY,
    payment_type_desc VARCHAR(50)
);

-- Vendor Lookup table
CREATE TABLE vendor_lookup (
    vendorid INTEGER PRIMARY KEY,
    vendor_name VARCHAR(100)
);

-- Data Processing Tracking table
-- Tracks which months have been successfully processed to prevent duplicates
CREATE TABLE data_processing_log (
    id SERIAL PRIMARY KEY,
    data_year INTEGER NOT NULL,
    data_month INTEGER NOT NULL,
    file_name VARCHAR(200) NOT NULL,
    records_loaded BIGINT NOT NULL DEFAULT 0,
    processing_started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processing_completed_at TIMESTAMP,
    backfill_config VARCHAR(100),
    status VARCHAR(20) DEFAULT 'in_progress',
    CONSTRAINT unique_month_processing UNIQUE (data_year, data_month),
    CONSTRAINT valid_month CHECK (data_month BETWEEN 1 AND 12),
    CONSTRAINT valid_status CHECK (status IN ('in_progress', 'completed', 'failed'))
);

-- Insert reference data
INSERT INTO rate_code_lookup (ratecodeid, rate_code_desc) VALUES
(1, 'Standard rate'),
(2, 'JFK'),
(3, 'Newark'),
(4, 'Nassau or Westchester'),
(5, 'Negotiated fare'),
(6, 'Group ride');

INSERT INTO payment_type_lookup (payment_type, payment_type_desc) VALUES
(1, 'Credit card'),
(2, 'Cash'),
(3, 'No charge'),
(4, 'Dispute'),
(5, 'Unknown'),
(6, 'Voided trip');

INSERT INTO vendor_lookup (vendorid, vendor_name) VALUES
(1, 'Creative Mobile Technologies'),
(2, 'VeriFone Inc.');

-- Taxi zone data will be loaded from CSV and shapefile via Python script
-- Use: uv run python python-scripts/load_reference_data.py

-- Indexes for performance optimization on real NYC taxi data
CREATE INDEX idx_yellow_taxi_pickup_datetime ON yellow_taxi_trips (tpep_pickup_datetime);
CREATE INDEX idx_yellow_taxi_dropoff_datetime ON yellow_taxi_trips (tpep_dropoff_datetime);
CREATE INDEX idx_yellow_taxi_pickup_location ON yellow_taxi_trips (pulocationid);
CREATE INDEX idx_yellow_taxi_dropoff_location ON yellow_taxi_trips (dolocationid);
CREATE INDEX idx_yellow_taxi_payment_type ON yellow_taxi_trips (payment_type);
CREATE INDEX idx_yellow_taxi_vendor ON yellow_taxi_trips (vendorid);
CREATE INDEX idx_yellow_taxi_trip_distance ON yellow_taxi_trips (trip_distance);
CREATE INDEX idx_yellow_taxi_total_amount ON yellow_taxi_trips (total_amount);

-- Composite indexes for common analytical queries
CREATE INDEX idx_yellow_taxi_datetime_vendor ON yellow_taxi_trips (tpep_pickup_datetime, vendorid);
CREATE INDEX idx_yellow_taxi_location_datetime ON yellow_taxi_trips (pulocationid, tpep_pickup_datetime);
CREATE INDEX idx_yellow_taxi_date_payment ON yellow_taxi_trips (DATE(tpep_pickup_datetime), payment_type);

-- Partitioning by month for better performance (if using PostgreSQL 10+)
-- This would be implemented when loading actual data by month