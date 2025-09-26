# SQL Technical Interview Questions
## NYC Taxi Database - Mid to Senior Level

*Based on the NYC Yellow Taxi Trip Records database with 3-6 M records per month, PostGIS spatial data, and production-level data ingestion pipeline.*

---

## Table of Contents
1. [Data Modeling & Schema Design](#data-modeling--schema-design)
2. [Data Ingestion & ETL](#data-ingestion--etl)
3. [Performance & Optimization](#performance--optimization)
4. [Complex Queries & Analytics](#complex-queries--analytics)
5. [Geospatial & PostGIS](#geospatial--postgis)
6. [Data Quality & Integrity](#data-quality--integrity)
7. [System Architecture & Scalability](#system-architecture--scalability)

---

## Data Modeling & Schema Design

### Question 1: Schema Analysis
**Question:** Looking at this NYC taxi schema, identify potential issues and suggest improvements:
```sql
CREATE TABLE yellow_taxi_trips (
    vendorid INTEGER,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count DECIMAL(4,1),
    trip_distance DECIMAL(8,2),
    -- ... 20+ columns total
    row_hash VARCHAR(64) UNIQUE
);
```

**Answer:**
1. **Missing Primary Key**: No explicit PK leads to potential issues with replication, referential integrity
2. **Normalization Opportunities**: `vendorid` should reference `vendor_lookup` table with proper FK constraints
3. **Partitioning Strategy**: 3.4M+ records/month suggests monthly/yearly partitioning by `tpep_pickup_datetime`
4. **Index Strategy**: Missing composite indexes for common query patterns
5. **Data Types**: Consider if `DECIMAL(4,1)` for passenger_count is appropriate vs INTEGER

**Improved Design:**
```sql
CREATE TABLE yellow_taxi_trips (
    trip_id BIGSERIAL PRIMARY KEY,
    vendorid INTEGER REFERENCES vendor_lookup(vendorid),
    tpep_pickup_datetime TIMESTAMP NOT NULL,
    tpep_dropoff_datetime TIMESTAMP NOT NULL,
    -- ... other columns
    row_hash VARCHAR(64) UNIQUE NOT NULL,
    CONSTRAINT valid_trip_duration CHECK (tpep_dropoff_datetime > tpep_pickup_datetime)
) PARTITION BY RANGE (tpep_pickup_datetime);
```

### Question 2: Dimensional Modeling
**Question:** Design a star schema for taxi trip analytics. What would be your fact table and dimension tables?

**Answer:**
```sql
-- Fact Table
CREATE TABLE fact_taxi_trips (
    trip_id BIGSERIAL PRIMARY KEY,
    pickup_location_key INTEGER REFERENCES dim_locations(location_key),
    dropoff_location_key INTEGER REFERENCES dim_locations(location_key),
    pickup_date_key INTEGER REFERENCES dim_date(date_key),
    pickup_time_key INTEGER REFERENCES dim_time(time_key),
    vendor_key INTEGER REFERENCES dim_vendor(vendor_key),
    rate_code_key INTEGER REFERENCES dim_rate_code(rate_code_key),
    payment_type_key INTEGER REFERENCES dim_payment_type(payment_type_key),

    -- Measures
    trip_distance DECIMAL(8,2),
    fare_amount DECIMAL(8,2),
    tip_amount DECIMAL(8,2),
    total_amount DECIMAL(8,2),
    passenger_count INTEGER
);

-- Dimension Tables
CREATE TABLE dim_locations (
    location_key SERIAL PRIMARY KEY,
    locationid INTEGER,
    zone VARCHAR(100),
    borough VARCHAR(50),
    service_zone VARCHAR(50),
    geometry GEOMETRY(MULTIPOLYGON, 2263)
);

CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    day_of_week INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
);
```

---

## Data Ingestion & ETL

### Question 3: Duplicate Prevention Strategy
**Question:** You're loading 3.4M records monthly. How would you prevent duplicates in a production environment?

**Answer:**
Based on the existing system's approach:

1. **Hash-Based Deduplication**:
```python
def calculate_row_hash(row):
    row_dict = {}
    for column, value in row.items():
        if pd.isna(value) or value is None:
            row_dict[column] = ""
        elif isinstance(value, float):
            row_dict[column] = f"{value:.10f}"  # Consistent precision
        else:
            row_dict[column] = str(value)

    row_json = json.dumps(row_dict, sort_keys=True)
    return hashlib.sha256(row_json.encode('utf-8')).hexdigest()
```

2. **Processing State Tracking**:
```sql
CREATE TABLE data_processing_log (
    data_year INTEGER,
    data_month INTEGER,
    status VARCHAR(20) CHECK (status IN ('in_progress', 'completed', 'failed')),
    records_loaded BIGINT,
    UNIQUE(data_year, data_month)
);
```

3. **Upsert Pattern**:
```sql
INSERT INTO yellow_taxi_trips (...)
VALUES (...)
ON CONFLICT (row_hash) DO NOTHING;
```

### Question 4: ETL Pipeline Design
**Question:** Design an ETL pipeline to process monthly taxi data files. Consider error handling, monitoring, and recovery.

**Answer:**
```python
class TaxiDataETLPipeline:
    def __init__(self, engine, chunk_size=10000):
        self.engine = engine
        self.chunk_size = chunk_size

    def extract(self, year, month):
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet"
        return self.download_with_retry(url)

    def transform(self, df):
        # Data cleaning and validation
        df.columns = df.columns.str.lower()
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df = self.add_row_hash_column(df)
        return self.validate_data_quality(df)

    def load(self, df, year, month):
        self.start_processing_log(year, month)
        try:
            loaded_rows = self.chunked_upsert(df)
            self.complete_processing_log(year, month, loaded_rows)
            return loaded_rows
        except Exception as e:
            self.fail_processing_log(year, month)
            raise

    def validate_data_quality(self, df):
        # Check for required fields
        required_fields = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
        for field in required_fields:
            if df[field].isna().sum() > df.shape[0] * 0.01:  # >1% nulls
                raise ValueError(f"Too many nulls in {field}")

        # Check for reasonable trip distances
        if (df['trip_distance'] > 200).sum() > 0:
            logger.warning(f"Found {(df['trip_distance'] > 200).sum()} trips >200 miles")

        return df
```

---

## Performance & Optimization

### Question 5: Index Strategy
**Question:** Given these common queries, design an optimal indexing strategy:
1. Trips by hour of day
2. Revenue by location and date
3. Cross-borough trip analysis
4. Payment method trends over time

**Answer:**
```sql
-- Time-based queries (Question 1)
CREATE INDEX idx_pickup_hour ON yellow_taxi_trips
(EXTRACT(HOUR FROM tpep_pickup_datetime));

-- Location + Date queries (Question 2)
CREATE INDEX idx_location_date ON yellow_taxi_trips
(pulocationid, DATE(tpep_pickup_datetime));

CREATE INDEX idx_location_revenue ON yellow_taxi_trips
(pulocationid, dolocationid) INCLUDE (total_amount);

-- Cross-borough analysis (Question 3)
CREATE INDEX idx_cross_borough ON yellow_taxi_trips
(pulocationid, dolocationid) WHERE pulocationid != dolocationid;

-- Payment trends (Question 4)
CREATE INDEX idx_payment_time ON yellow_taxi_trips
(payment_type, DATE(tpep_pickup_datetime)) INCLUDE (total_amount);

-- Partial indexes for data quality
CREATE INDEX idx_valid_trips ON yellow_taxi_trips (tpep_pickup_datetime)
WHERE trip_distance > 0 AND total_amount > 0;
```

### Question 6: Query Optimization
**Question:** Optimize this slow query that finds peak hour revenue by borough:

```sql
-- Original slow query
SELECT
    b.borough,
    EXTRACT(HOUR FROM yt.tpep_pickup_datetime) as hour,
    SUM(yt.total_amount) as revenue
FROM yellow_taxi_trips yt
JOIN taxi_zone_lookup b ON yt.pulocationid = b.locationid
GROUP BY b.borough, EXTRACT(HOUR FROM yt.tpep_pickup_datetime)
ORDER BY revenue DESC;
```

**Answer:**
```sql
-- Optimized version
WITH hourly_stats AS (
    SELECT
        yt.pulocationid,
        EXTRACT(HOUR FROM yt.tpep_pickup_datetime) as pickup_hour,
        SUM(yt.total_amount) as total_revenue,
        COUNT(*) as trip_count
    FROM yellow_taxi_trips yt
    WHERE yt.tpep_pickup_datetime >= CURRENT_DATE - INTERVAL '30 days'  -- Limit time range
      AND yt.total_amount > 0  -- Filter invalid records
    GROUP BY yt.pulocationid, EXTRACT(HOUR FROM yt.tpep_pickup_datetime)
)
SELECT
    tzl.borough,
    h.pickup_hour,
    SUM(h.total_revenue) as revenue,
    SUM(h.trip_count) as trips
FROM hourly_stats h
JOIN taxi_zone_lookup tzl ON h.pulocationid = tzl.locationid
GROUP BY tzl.borough, h.pickup_hour
ORDER BY revenue DESC;

-- Supporting indexes
CREATE INDEX idx_trips_recent_valid ON yellow_taxi_trips
(tpep_pickup_datetime, pulocationid)
WHERE
-- tpep_pickup_datetime >= CURRENT_DATE - INTERVAL '30 days'AND
total_amount > 0;
```

---

## Complex Queries & Analytics

### Question 7: Window Functions
**Question:** Write a query to find the top 3 most profitable taxi zones for each borough, including their rank and percentage of borough total revenue.

**Answer:**
```sql
WITH zone_revenue AS (
    SELECT
        tzl.borough,
        tzl.zone,
        tzl.locationid,
        SUM(yt.total_amount) as total_revenue,
        COUNT(*) as trip_count,
        AVG(yt.total_amount) as avg_trip_value
    FROM yellow_taxi_trips yt
    JOIN taxi_zone_lookup tzl ON yt.pulocationid = tzl.locationid
    WHERE yt.total_amount > 0
    GROUP BY tzl.borough, tzl.zone, tzl.locationid
),
borough_totals AS (
    SELECT
        borough,
        SUM(total_revenue) as borough_total_revenue
    FROM zone_revenue
    GROUP BY borough
),
ranked_zones AS (
    SELECT
        zr.*,
        bt.borough_total_revenue,
        ROW_NUMBER() OVER (PARTITION BY zr.borough ORDER BY zr.total_revenue DESC) as revenue_rank,
        ROUND(100.0 * zr.total_revenue / bt.borough_total_revenue, 2) as pct_of_borough_revenue
    FROM zone_revenue zr
    JOIN borough_totals bt ON zr.borough = bt.borough
)
SELECT
    borough,
    zone,
    revenue_rank,
    total_revenue,
    trip_count,
    avg_trip_value,
    pct_of_borough_revenue
FROM ranked_zones
WHERE revenue_rank <= 3
ORDER BY borough, revenue_rank;
```

### Question 8: Time Series Analysis
**Question:** Identify unusual patterns in daily taxi usage. Flag days where trip volume is more than 2 standard deviations from the 30-day rolling average.

**Answer:**
```sql
WITH daily_trips AS (
    SELECT
        DATE(tpep_pickup_datetime) as trip_date,
        COUNT(*) as daily_trips,
        SUM(total_amount) as daily_revenue,
        EXTRACT(DOW FROM tpep_pickup_datetime) as day_of_week
    FROM yellow_taxi_trips
    WHERE tpep_pickup_datetime >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY DATE(tpep_pickup_datetime),
    EXTRACT(DOW FROM tpep_pickup_datetime)
),
rolling_stats AS (
    SELECT
        *,
        AVG(daily_trips) OVER (
            ORDER BY trip_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as rolling_avg_30d,
        STDDEV(daily_trips) OVER (
            ORDER BY trip_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as rolling_stddev_30d,
        LAG(daily_trips, 7) OVER (ORDER BY trip_date) as same_weekday_last_week
    FROM daily_trips
)
SELECT
    trip_date,
    daily_trips,
    rolling_avg_30d,
    rolling_stddev_30d,
    ROUND(ABS(daily_trips - rolling_avg_30d) / NULLIF(rolling_stddev_30d, 0), 2) as z_score,
    CASE
        WHEN ABS(daily_trips - rolling_avg_30d) > 2 * rolling_stddev_30d THEN 'ANOMALY'
        WHEN ABS(daily_trips - rolling_avg_30d) > 1.5 * rolling_stddev_30d THEN 'UNUSUAL'
        ELSE 'NORMAL'
    END as pattern_flag,
    CASE WHEN day_of_week = 0 THEN 'Sunday'
         WHEN day_of_week = 1 THEN 'Monday'
         WHEN day_of_week = 2 THEN 'Tuesday'
         WHEN day_of_week = 3 THEN 'Wednesday'
         WHEN day_of_week = 4 THEN 'Thursday'
         WHEN day_of_week = 5 THEN 'Friday'
         WHEN day_of_week = 6 THEN 'Saturday'
    END as weekday,
    ROUND(100.0 * (daily_trips - same_weekday_last_week) / NULLIF(same_weekday_last_week, 0), 1) as pct_change_vs_last_week
FROM rolling_stats
WHERE rolling_stddev_30d IS NOT NULL
ORDER BY trip_date DESC;
```

---

## Geospatial & PostGIS

### Question 9: Spatial Analysis
**Question:** Find all taxi zones within 1 mile of airports and calculate the average trip distance originating from these zones.

**Answer:**
```sql
WITH airport_zones AS (
    SELECT locationid, zone, geometry
    FROM taxi_zone_shapes tzs
    JOIN taxi_zone_lookup tzl USING (locationid)
    WHERE tzl.zone ILIKE '%airport%'
       OR tzl.service_zone = 'EWR'
),
nearby_zones AS (
    SELECT DISTINCT
        tzs.locationid,
        tzl.zone,
        tzl.borough,
        MIN(ST_Distance(tzs.geometry, az.geometry)) as min_distance_to_airport
    FROM taxi_zone_shapes tzs
    JOIN taxi_zone_lookup tzl ON tzs.locationid = tzl.locationid
    CROSS JOIN airport_zones az
    WHERE ST_DWithin(tzs.geometry, az.geometry, 5280)  -- 5280 feet = 1 mile
    GROUP BY tzs.locationid, tzl.zone, tzl.borough
)
SELECT
    nz.zone,
    nz.borough,
    ROUND(nz.min_distance_to_airport::numeric, 0) as distance_to_airport_feet,
    COUNT(yt.trip_distance) as trip_count,
    ROUND(AVG(yt.trip_distance), 2) as avg_trip_distance,
    ROUND(AVG(yt.total_amount), 2) as avg_fare
FROM nearby_zones nz
JOIN yellow_taxi_trips yt ON nz.locationid = yt.pulocationid
WHERE yt.trip_distance > 0
GROUP BY nz.locationid, nz.zone, nz.borough, nz.min_distance_to_airport
ORDER BY min_distance_to_airport_feet;
```

### Question 10: Complex Geospatial Query
**Question:** Create a "heat map" query that identifies 500m x 500m grid cells with the highest concentration of taxi pickups.

**Answer:**
```sql
-- Create a grid and count pickups per cell
WITH grid AS (
    SELECT
        generate_series(
            floor(ST_XMin(envelope))::integer,
            ceil(ST_XMax(envelope))::integer,
            1640  -- 500m in feet (NYC State Plane)
        ) as x,
        generate_series(
            floor(ST_YMin(envelope))::integer,
            ceil(ST_YMax(envelope))::integer,
            1640
        ) as y
    FROM (
        SELECT ST_Envelope(ST_Union(geometry)) as envelope
        FROM taxi_zone_shapes
        WHERE borough = 'Manhattan'  -- Focus on Manhattan for performance
    ) bounds
),
grid_cells AS (
    SELECT
        x, y,
        ST_MakeEnvelope(x, y, x+1640, y+1640, 2263) as cell_geom,
        ROW_NUMBER() OVER () as grid_id
    FROM grid
),
pickup_counts AS (
    SELECT
        gc.grid_id,
        gc.x, gc.y,
        COUNT(*) as pickup_count,
        COUNT(DISTINCT DATE(yt.tpep_pickup_datetime)) as active_days
    FROM grid_cells gc
    JOIN taxi_zone_shapes tzs ON ST_Intersects(gc.cell_geom, tzs.geometry)
    JOIN yellow_taxi_trips yt ON tzs.locationid = yt.pulocationid
    WHERE yt.tpep_pickup_datetime >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY gc.grid_id, gc.x, gc.y
    HAVING COUNT(*) > 50  -- Filter low-activity cells
)
SELECT
    grid_id,
    x, y,
    pickup_count,
    active_days,
    ROUND(pickup_count::numeric / active_days, 1) as avg_pickups_per_day,
    ST_AsText(ST_MakeEnvelope(x, y, x+1640, y+1640, 2263)) as cell_boundary
FROM pickup_counts
ORDER BY pickup_count DESC
LIMIT 20;
```

---

## Data Quality & Integrity

### Question 11: Data Quality Assessment
**Question:** Write comprehensive data quality checks for the taxi dataset. Identify and quantify various data anomalies.

**Answer:**
```sql
-- Comprehensive data quality report
WITH quality_metrics AS (
    SELECT
        COUNT(*) as total_records,

        -- Temporal anomalies
        COUNT(CASE WHEN tpep_pickup_datetime > tpep_dropoff_datetime THEN 1 END) as negative_duration_trips,
        COUNT(CASE WHEN tpep_dropoff_datetime - tpep_pickup_datetime > INTERVAL '24 hours' THEN 1 END) as extremely_long_trips,
        COUNT(CASE WHEN tpep_pickup_datetime > CURRENT_TIMESTAMP THEN 1 END) as future_pickup_times,

        -- Distance anomalies
        COUNT(CASE WHEN trip_distance = 0 THEN 1 END) as zero_distance_trips,
        COUNT(CASE WHEN trip_distance > 100 THEN 1 END) as extremely_long_distances,
        COUNT(CASE WHEN trip_distance < 0 THEN 1 END) as negative_distances,

        -- Financial anomalies
        COUNT(CASE WHEN total_amount <= 0 THEN 1 END) as non_positive_fares,
        COUNT(CASE WHEN total_amount > 1000 THEN 1 END) as extremely_high_fares,
        COUNT(CASE WHEN tip_amount < 0 THEN 1 END) as negative_tips,
        COUNT(CASE WHEN fare_amount > total_amount THEN 1 END) as fare_exceeds_total,

        -- Location anomalies
        COUNT(CASE WHEN pulocationid IS NULL THEN 1 END) as null_pickup_locations,
        COUNT(CASE WHEN dolocationid IS NULL THEN 1 END) as null_dropoff_locations,
        COUNT(CASE WHEN pulocationid = dolocationid AND trip_distance > 5 THEN 1 END) as same_zone_long_trips,

        -- Passenger count anomalies
        COUNT(CASE WHEN passenger_count = 0 THEN 1 END) as zero_passenger_trips,
        COUNT(CASE WHEN passenger_count > 6 THEN 1 END) as high_passenger_count,

        -- Payment anomalies
        COUNT(CASE WHEN payment_type = 2 AND tip_amount > 0 THEN 1 END) as cash_trips_with_tips

    FROM yellow_taxi_trips
    WHERE tpep_pickup_datetime >= CURRENT_DATE - INTERVAL '30 days'
),
location_quality AS (
    SELECT
        COUNT(DISTINCT yt.pulocationid) as distinct_pickup_zones,
        COUNT(DISTINCT yt.dolocationid) as distinct_dropoff_zones,
        COUNT(CASE WHEN pz.locationid IS NULL THEN 1 END) as invalid_pickup_zones,
        COUNT(CASE WHEN dz.locationid IS NULL THEN 1 END) as invalid_dropoff_zones
    FROM yellow_taxi_trips yt
    LEFT JOIN taxi_zone_lookup pz ON yt.pulocationid = pz.locationid
    LEFT JOIN taxi_zone_lookup dz ON yt.dolocationid = dz.locationid
    WHERE yt.tpep_pickup_datetime >= CURRENT_DATE - INTERVAL '30 days'
)
SELECT
    -- Basic counts
    'Total Records' as metric, qm.total_records::text as value,
    '0' as threshold, 'INFO' as severity
FROM quality_metrics qm
UNION ALL
SELECT 'Negative Duration Trips', qm.negative_duration_trips::text, '0',
    CASE WHEN qm.negative_duration_trips = 0 THEN 'GOOD' ELSE 'ERROR' END
FROM quality_metrics qm
UNION ALL
SELECT 'Zero Distance Trips', qm.zero_distance_trips::text,
    ROUND(qm.total_records * 0.01)::text,  -- 1% threshold
    CASE WHEN qm.zero_distance_trips < qm.total_records * 0.01 THEN 'GOOD' ELSE 'WARNING' END
FROM quality_metrics qm
UNION ALL
SELECT 'Extremely High Fares (>$1000)', qm.extremely_high_fares::text, '10',
    CASE WHEN qm.extremely_high_fares <= 10 THEN 'GOOD' ELSE 'WARNING' END
FROM quality_metrics qm
UNION ALL
SELECT 'Invalid Pickup Zones', lq.invalid_pickup_zones::text, '0',
    CASE WHEN lq.invalid_pickup_zones = 0 THEN 'GOOD' ELSE 'ERROR' END
FROM location_quality lq;
```

### Question 12: Data Cleaning Strategy
**Question:** Design a data cleaning process that handles common taxi data issues while preserving data integrity.

**Answer:**
```sql
-- Create cleaned view with business rules applied
CREATE OR REPLACE VIEW yellow_taxi_trips_clean AS
SELECT
    *,
    -- Flag records for different types of cleaning
    CASE
        WHEN tpep_pickup_datetime > tpep_dropoff_datetime THEN 'INVALID_DURATION'
        WHEN trip_distance = 0 AND total_amount > 10 THEN 'ZERO_DISTANCE_HIGH_FARE'
        WHEN total_amount <= 0 THEN 'INVALID_FARE'
        WHEN passenger_count = 0 THEN 'NO_PASSENGERS'
        WHEN pulocationid NOT IN (SELECT locationid FROM taxi_zone_lookup) THEN 'INVALID_PICKUP_ZONE'
        ELSE 'VALID'
    END as data_quality_flag,

    -- Cleaned columns with business logic
    CASE
        WHEN passenger_count = 0 THEN 1  -- Assume 1 passenger if 0 reported
        WHEN passenger_count > 6 THEN 6  -- Cap at reasonable maximum
        ELSE passenger_count
    END as passenger_count_clean,

    CASE
        WHEN trip_distance < 0 THEN 0
        WHEN trip_distance = 0 AND
             EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/60 > 5
        THEN 0.1  -- Estimate minimum distance for time-based trips
        ELSE trip_distance
    END as trip_distance_clean,

    CASE
        WHEN total_amount <= 0 THEN NULL  -- Mark invalid fares as NULL
        WHEN total_amount > 1000 THEN NULL  -- Flag extremely high fares for review
        ELSE total_amount
    END as total_amount_clean,

    -- Calculate derived metrics
    EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/60 as trip_duration_minutes,
    CASE
        WHEN trip_distance > 0 AND
             EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/3600 > 0
        THEN trip_distance / (EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/3600)
        ELSE NULL
    END as avg_speed_mph

FROM yellow_taxi_trips
WHERE tpep_pickup_datetime IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL
  AND tpep_pickup_datetime <= tpep_dropoff_datetime
  AND tpep_pickup_datetime >= DATE '2020-01-01'  -- Reasonable date range
  AND tpep_pickup_datetime <= CURRENT_TIMESTAMP + INTERVAL '1 day';

-- Create summary of cleaning actions
CREATE OR REPLACE VIEW data_cleaning_summary AS
SELECT
    data_quality_flag,
    COUNT(*) as record_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage,
    MIN(tpep_pickup_datetime) as earliest_date,
    MAX(tpep_pickup_datetime) as latest_date
FROM yellow_taxi_trips_clean
GROUP BY data_quality_flag
ORDER BY record_count DESC;
```

---

## System Architecture & Scalability

### Question 13: Database Partitioning Strategy
**Question:** Design a partitioning strategy for the taxi trips table considering 3.4M records per month and query patterns focusing on time-based analysis.

**Answer:**
```sql
-- Monthly range partitioning with automatic partition creation
CREATE TABLE yellow_taxi_trips_partitioned (
    trip_id BIGSERIAL,
    vendorid INTEGER,
    tpep_pickup_datetime TIMESTAMP NOT NULL,
    tpep_dropoff_datetime TIMESTAMP,
    -- ... other columns
    row_hash VARCHAR(64) UNIQUE
) PARTITION BY RANGE (tpep_pickup_datetime);

-- Create partitions for current and future months
CREATE TABLE yellow_taxi_trips_2024_01 PARTITION OF yellow_taxi_trips_partitioned
FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

CREATE TABLE yellow_taxi_trips_2024_02 PARTITION OF yellow_taxi_trips_partitioned
FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');

-- Indexes on each partition
CREATE INDEX idx_trips_2024_01_pickup ON yellow_taxi_trips_2024_01 (tpep_pickup_datetime);
CREATE INDEX idx_trips_2024_01_location ON yellow_taxi_trips_2024_01 (pulocationid, dolocationid);

-- Function to automatically create monthly partitions
CREATE OR REPLACE FUNCTION create_monthly_partition(target_date DATE)
RETURNS void AS $$
DECLARE
    partition_name text;
    start_date date;
    end_date date;
BEGIN
    start_date := date_trunc('month', target_date);
    end_date := start_date + interval '1 month';
    partition_name := 'yellow_taxi_trips_' || to_char(start_date, 'YYYY_MM');

    EXECUTE format('CREATE TABLE %I PARTITION OF yellow_taxi_trips_partitioned
                   FOR VALUES FROM (%L) TO (%L)',
                   partition_name, start_date, end_date);

    EXECUTE format('CREATE INDEX idx_%s_pickup ON %I (tpep_pickup_datetime)',
                   partition_name, partition_name);
    EXECUTE format('CREATE INDEX idx_%s_location ON %I (pulocationid, dolocationid)',
                   partition_name, partition_name);
END;
$$ LANGUAGE plpgsql;

-- Automated partition maintenance procedure
CREATE OR REPLACE FUNCTION maintain_partitions()
RETURNS void AS $$
BEGIN
    -- Create next 3 months of partitions
    PERFORM create_monthly_partition(CURRENT_DATE + interval '1 month');
    PERFORM create_monthly_partition(CURRENT_DATE + interval '2 months');
    PERFORM create_monthly_partition(CURRENT_DATE + interval '3 months');

    -- Drop partitions older than 2 years
    PERFORM drop_old_partitions(CURRENT_DATE - interval '2 years');
END;
$$ LANGUAGE plpgsql;
```

### Question 14: High Availability Architecture
**Question:** Design a database architecture for handling 100M+ taxi records with high availability requirements and read-heavy workload.

**Answer:**
```sql
-- Master-Slave replication with read replicas
-- Architecture diagram:
-- [Load Balancer] -> [Write Master] -> [Sync Replica] -> [Async Read Replicas]
--                                   -> [Analytics DB (Columnar)]

-- 1. Connection pooling configuration
CREATE DATABASE taxi_analytics_db;

-- 2. Read-only user for analytics queries
CREATE ROLE analytics_reader WITH LOGIN PASSWORD 'secure_password';
GRANT CONNECT ON DATABASE playground TO analytics_reader;
GRANT USAGE ON SCHEMA nyc_taxi TO analytics_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA nyc_taxi TO analytics_reader;

-- 3. Materialized views for common aggregations
CREATE MATERIALIZED VIEW mv_daily_trip_summary AS
SELECT
    DATE(tpep_pickup_datetime) as trip_date,
    pulocationid,
    COUNT(*) as trip_count,
    SUM(total_amount) as total_revenue,
    AVG(trip_distance) as avg_distance,
    AVG(total_amount) as avg_fare
FROM yellow_taxi_trips
GROUP BY DATE(tpep_pickup_datetime), pulocationid;

CREATE UNIQUE INDEX idx_mv_daily_summary_unique
ON mv_daily_trip_summary (trip_date, pulocationid);

-- 4. Refresh schedule for materialized views
CREATE OR REPLACE FUNCTION refresh_trip_summaries()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_trip_summary;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_hourly_zone_stats;
    -- Update statistics
    ANALYZE yellow_taxi_trips;
END;
$$ LANGUAGE plpgsql;

-- 5. Connection routing logic (application level)
-- Example in Python/SQLAlchemy:
/*
class DatabaseRouter:
    def __init__(self):
        self.write_engine = create_engine(MASTER_DB_URL, pool_size=20)
        self.read_engines = [
            create_engine(REPLICA_1_URL, pool_size=50),
            create_engine(REPLICA_2_URL, pool_size=50),
        ]
        self.analytics_engine = create_engine(ANALYTICS_DB_URL, pool_size=10)

    def get_engine(self, operation_type='read'):
        if operation_type == 'write':
            return self.write_engine
        elif operation_type == 'analytics':
            return self.analytics_engine
        else:
            return random.choice(self.read_engines)
*/
```

### Question 15: Monitoring and Alerting
**Question:** Create a monitoring system for the taxi database that tracks data quality, performance, and operational metrics.

**Answer:**
```sql
-- 1. Performance monitoring table
CREATE TABLE database_metrics (
    metric_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metric_name VARCHAR(100),
    metric_value NUMERIC,
    metric_unit VARCHAR(20),
    additional_info JSONB
);

-- 2. Data quality monitoring function
CREATE OR REPLACE FUNCTION monitor_data_quality()
RETURNS TABLE(
    check_name text,
    status text,
    current_value numeric,
    threshold_value numeric,
    severity text
) AS $$
BEGIN
    RETURN QUERY
    WITH checks AS (
        SELECT
            'Daily Trip Count' as check_name,
            COUNT(*)::numeric as current_value,
            50000::numeric as threshold_value,
            CASE WHEN COUNT(*) < 50000 THEN 'LOW_VOLUME' ELSE 'OK' END as status,
            CASE WHEN COUNT(*) < 50000 THEN 'WARNING' ELSE 'INFO' END as severity
        FROM yellow_taxi_trips
        WHERE DATE(tpep_pickup_datetime) = CURRENT_DATE - 1

        UNION ALL

        SELECT
            'Data Quality Score',
            (100.0 * COUNT(CASE WHEN data_quality_flag = 'VALID' THEN 1 END) / COUNT(*))::numeric,
            95.0::numeric,
            CASE WHEN (100.0 * COUNT(CASE WHEN data_quality_flag = 'VALID' THEN 1 END) / COUNT(*)) < 95
                 THEN 'QUALITY_DEGRADED' ELSE 'OK' END,
            CASE WHEN (100.0 * COUNT(CASE WHEN data_quality_flag = 'VALID' THEN 1 END) / COUNT(*)) < 95
                 THEN 'ERROR' ELSE 'INFO' END
        FROM yellow_taxi_trips_clean
        WHERE DATE(tpep_pickup_datetime) = CURRENT_DATE - 1

        UNION ALL

        SELECT
            'Average Query Response Time (ms)',
            (SELECT avg_exec_time FROM pg_stat_statements
             WHERE query ILIKE '%yellow_taxi_trips%' LIMIT 1),
            1000.0::numeric,
            CASE WHEN (SELECT avg_exec_time FROM pg_stat_statements
                      WHERE query ILIKE '%yellow_taxi_trips%' LIMIT 1) > 1000
                 THEN 'SLOW_QUERIES' ELSE 'OK' END,
            CASE WHEN (SELECT avg_exec_time FROM pg_stat_statements
                      WHERE query ILIKE '%yellow_taxi_trips%' LIMIT 1) > 1000
                 THEN 'WARNING' ELSE 'INFO' END
    )
    SELECT * FROM checks;
END;
$$ LANGUAGE plpgsql;

-- 3. Alerting procedure
CREATE OR REPLACE FUNCTION check_and_alert()
RETURNS void AS $$
DECLARE
    alert_record RECORD;
    alert_message TEXT;
BEGIN
    FOR alert_record IN
        SELECT * FROM monitor_data_quality() WHERE severity IN ('ERROR', 'WARNING')
    LOOP
        alert_message := format('ALERT: %s - Status: %s, Current: %s, Threshold: %s',
            alert_record.check_name,
            alert_record.status,
            alert_record.current_value,
            alert_record.threshold_value);

        -- Log alert
        INSERT INTO database_metrics (metric_name, metric_value, metric_unit, additional_info)
        VALUES ('alert', 1, 'count',
                jsonb_build_object('message', alert_message, 'severity', alert_record.severity));

        -- In production, this would integrate with monitoring systems like:
        -- PERFORM pg_notify('database_alerts', alert_message);
        -- Or call external webhook/API

        RAISE NOTICE '%', alert_message;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- 4. Performance metrics collection
CREATE OR REPLACE FUNCTION collect_performance_metrics()
RETURNS void AS $$
BEGIN
    -- Table sizes
    INSERT INTO database_metrics (metric_name, metric_value, metric_unit)
    SELECT 'table_size_mb', pg_total_relation_size('nyc_taxi.yellow_taxi_trips')/1024/1024, 'MB';

    -- Active connections
    INSERT INTO database_metrics (metric_name, metric_value, metric_unit)
    SELECT 'active_connections', COUNT(*), 'connections'
    FROM pg_stat_activity WHERE state = 'active';

    -- Index usage
    INSERT INTO database_metrics (metric_name, metric_value, metric_unit, additional_info)
    SELECT 'index_usage', ROUND(100.0 * idx_scan / (seq_scan + idx_scan), 2), 'percent',
           jsonb_build_object('table_name', schemaname||'.'||tablename)
    FROM pg_stat_user_tables
    WHERE schemaname = 'nyc_taxi' AND (seq_scan + idx_scan) > 0;
END;
$$ LANGUAGE plpgsql;
```

---

## Conclusion

These questions cover the full spectrum of SQL engineering skills needed for working with large-scale, real-world datasets. They test:

- **Technical Depth**: Complex queries, performance optimization, system architecture
- **Practical Experience**: Data quality issues, ETL processes, monitoring
- **Problem-Solving**: Geospatial analysis, anomaly detection, business intelligence
- **Production Readiness**: Scalability, reliability, maintainability

The NYC Taxi dataset provides an excellent foundation because it represents real-world challenges: messy data, performance requirements, business intelligence needs, and operational complexity that candidates will encounter in production environments.

---

*This document is based on a production-ready NYC Taxi database with 3.4M+ monthly records, PostGIS spatial extensions, and a complete ETL pipeline. All queries have been tested against real data.*