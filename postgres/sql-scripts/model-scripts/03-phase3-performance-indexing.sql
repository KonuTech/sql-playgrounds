-- ================================================================================
-- Phase 3: Performance & Indexing Implementation
-- Advanced indexing strategy building on partition structure
-- Based on Interview Questions 5-6: Index Strategy & Query Optimization
-- ================================================================================

-- Set schema context
SET search_path = nyc_taxi, public;

-- ================================================================================
-- PARTITION-LOCAL INDEXES FOR FACT TABLE
-- ================================================================================

-- Function to create indexes on all existing partitions
CREATE OR REPLACE FUNCTION create_partition_indexes()
RETURNS text[] AS $$
DECLARE
    partition_record RECORD;
    results text[] := ARRAY[]::text[];
    sql_cmd text;
    index_name text;
BEGIN
    -- Loop through all fact table partitions
    FOR partition_record IN
        SELECT schemaname, tablename
        FROM pg_tables
        WHERE schemaname = 'nyc_taxi'
        AND tablename LIKE 'fact_taxi_trips_____%%'
    LOOP
        -- 1. Primary performance index: pickup_date + location
        index_name := partition_record.tablename || '_pickup_date_location_idx';
        sql_cmd := format('CREATE INDEX IF NOT EXISTS %I ON nyc_taxi.%I (pickup_date, pickup_location_key)',
                         index_name, partition_record.tablename);
        EXECUTE sql_cmd;
        results := results || ('Created: ' || index_name);

        -- 2. Time-based analysis index
        index_name := partition_record.tablename || '_pickup_time_idx';
        sql_cmd := format('CREATE INDEX IF NOT EXISTS %I ON nyc_taxi.%I (pickup_time_key)',
                         index_name, partition_record.tablename);
        EXECUTE sql_cmd;
        results := results || ('Created: ' || index_name);

        -- 3. Revenue analysis covering index
        index_name := partition_record.tablename || '_revenue_covering_idx';
        sql_cmd := format('CREATE INDEX IF NOT EXISTS %I ON nyc_taxi.%I (pickup_location_key, payment_type_key) INCLUDE (total_amount, tip_amount, trip_distance)',
                         index_name, partition_record.tablename);
        EXECUTE sql_cmd;
        results := results || ('Created: ' || index_name);

        -- 4. Cross-borough analysis partial index
        index_name := partition_record.tablename || '_cross_borough_idx';
        sql_cmd := format('CREATE INDEX IF NOT EXISTS %I ON nyc_taxi.%I (pickup_location_key, dropoff_location_key) WHERE is_cross_borough_trip = true',
                         index_name, partition_record.tablename);
        EXECUTE sql_cmd;
        results := results || ('Created: ' || index_name);

        -- 5. Airport trips partial index
        index_name := partition_record.tablename || '_airport_trips_idx';
        sql_cmd := format('CREATE INDEX IF NOT EXISTS %I ON nyc_taxi.%I (pickup_date, vendor_key) INCLUDE (total_amount, trip_distance) WHERE is_airport_trip = true',
                         index_name, partition_record.tablename);
        EXECUTE sql_cmd;
        results := results || ('Created: ' || index_name);

        -- 6. Payment trends index
        index_name := partition_record.tablename || '_payment_trends_idx';
        sql_cmd := format('CREATE INDEX IF NOT EXISTS %I ON nyc_taxi.%I (pickup_date, payment_type_key) INCLUDE (total_amount, tip_percentage)',
                         index_name, partition_record.tablename);
        EXECUTE sql_cmd;
        results := results || ('Created: ' || index_name);

        -- 7. High-value trips partial index
        index_name := partition_record.tablename || '_high_value_idx';
        sql_cmd := format('CREATE INDEX IF NOT EXISTS %I ON nyc_taxi.%I (pickup_date, total_amount) WHERE total_amount > 50',
                         index_name, partition_record.tablename);
        EXECUTE sql_cmd;
        results := results || ('Created: ' || index_name);

        -- 8. Long distance trips partial index
        index_name := partition_record.tablename || '_long_distance_idx';
        sql_cmd := format('CREATE INDEX IF NOT EXISTS %I ON nyc_taxi.%I (pickup_date, trip_distance) WHERE is_long_distance = true',
                         index_name, partition_record.tablename);
        EXECUTE sql_cmd;
        results := results || ('Created: ' || index_name);

    END LOOP;

    RETURN results;
END;
$$ LANGUAGE plpgsql;

-- ================================================================================
-- EXPRESSION INDEXES FOR TIME-BASED PATTERNS
-- ================================================================================

-- Function to create expression indexes on partitions
CREATE OR REPLACE FUNCTION create_expression_indexes()
RETURNS text[] AS $$
DECLARE
    partition_record RECORD;
    results text[] := ARRAY[]::text[];
    sql_cmd text;
    index_name text;
BEGIN
    FOR partition_record IN
        SELECT schemaname, tablename
        FROM pg_tables
        WHERE schemaname = 'nyc_taxi'
        AND tablename LIKE 'fact_taxi_trips_____%%'
    LOOP
        -- Hour of day analysis (via time dimension join)
        index_name := partition_record.tablename || '_hour_analysis_idx';
        sql_cmd := format('CREATE INDEX IF NOT EXISTS %I ON nyc_taxi.%I (pickup_time_key, pickup_location_key)',
                         index_name, partition_record.tablename);
        EXECUTE sql_cmd;
        results := results || ('Created: ' || index_name);

        -- Day of week analysis (via date dimension join)
        index_name := partition_record.tablename || '_dow_analysis_idx';
        sql_cmd := format('CREATE INDEX IF NOT EXISTS %I ON nyc_taxi.%I (pickup_date_key, vendor_key)',
                         index_name, partition_record.tablename);
        EXECUTE sql_cmd;
        results := results || ('Created: ' || index_name);

        -- Revenue per mile expression index
        index_name := partition_record.tablename || '_efficiency_idx';
        sql_cmd := format('CREATE INDEX IF NOT EXISTS %I ON nyc_taxi.%I (revenue_per_mile) WHERE revenue_per_mile IS NOT NULL AND trip_distance > 0',
                         index_name, partition_record.tablename);
        EXECUTE sql_cmd;
        results := results || ('Created: ' || index_name);

    END LOOP;

    RETURN results;
END;
$$ LANGUAGE plpgsql;

-- ================================================================================
-- ADVANCED COVERING INDEXES
-- ================================================================================

-- Function to create covering indexes for common analytical queries
CREATE OR REPLACE FUNCTION create_covering_indexes()
RETURNS text[] AS $$
DECLARE
    partition_record RECORD;
    results text[] := ARRAY[]::text[];
    sql_cmd text;
    index_name text;
BEGIN
    FOR partition_record IN
        SELECT schemaname, tablename
        FROM pg_tables
        WHERE schemaname = 'nyc_taxi'
        AND tablename LIKE 'fact_taxi_trips_____%%'
    LOOP
        -- Comprehensive location analytics covering index
        index_name := partition_record.tablename || '_location_analytics_covering_idx';
        sql_cmd := format('CREATE INDEX IF NOT EXISTS %I ON nyc_taxi.%I (pickup_location_key, dropoff_location_key)
                          INCLUDE (trip_distance, total_amount, tip_amount, passenger_count, trip_duration_minutes)',
                         index_name, partition_record.tablename);
        EXECUTE sql_cmd;
        results := results || ('Created: ' || index_name);

        -- Time series analytics covering index
        index_name := partition_record.tablename || '_timeseries_covering_idx';
        sql_cmd := format('CREATE INDEX IF NOT EXISTS %I ON nyc_taxi.%I (pickup_date, pickup_time_key)
                          INCLUDE (total_amount, trip_distance, passenger_count, vendor_key)',
                         index_name, partition_record.tablename);
        EXECUTE sql_cmd;
        results := results || ('Created: ' || index_name);

        -- Vendor performance covering index
        index_name := partition_record.tablename || '_vendor_performance_covering_idx';
        sql_cmd := format('CREATE INDEX IF NOT EXISTS %I ON nyc_taxi.%I (vendor_key, pickup_date)
                          INCLUDE (total_amount, trip_distance, tip_percentage, avg_speed_mph)',
                         index_name, partition_record.tablename);
        EXECUTE sql_cmd;
        results := results || ('Created: ' || index_name);

    END LOOP;

    RETURN results;
END;
$$ LANGUAGE plpgsql;

-- ================================================================================
-- INDEX MONITORING AND ANALYSIS
-- ================================================================================

-- View to monitor index usage across all partitions
CREATE OR REPLACE VIEW index_usage_stats AS
SELECT
    schemaname,
    tablename,
    indexname,
    idx_tup_read,
    idx_tup_fetch,
    idx_scan,
    CASE
        WHEN idx_scan = 0 THEN 'UNUSED'
        WHEN idx_scan < 100 THEN 'LOW_USAGE'
        WHEN idx_scan < 1000 THEN 'MODERATE_USAGE'
        ELSE 'HIGH_USAGE'
    END as usage_category,
    pg_size_pretty(pg_relation_size(indexrelname::regclass)) as index_size
FROM pg_stat_user_indexes
WHERE schemaname = 'nyc_taxi'
AND tablename LIKE 'fact_taxi_trips_____%%'
ORDER BY schemaname, tablename, idx_scan DESC;

-- Function to analyze query performance
CREATE OR REPLACE FUNCTION analyze_query_performance(
    query_text TEXT,
    analyze_flag BOOLEAN DEFAULT FALSE
)
RETURNS TABLE(line_number int, plan_line text) AS $$
DECLARE
    sql_cmd text;
    analyze_option text;
BEGIN
    analyze_option := CASE WHEN analyze_flag THEN 'ANALYZE, ' ELSE '' END;

    sql_cmd := format('EXPLAIN (%sBUFFERS, FORMAT TEXT) %s', analyze_option, query_text);

    RETURN QUERY
    SELECT row_number() OVER ()::int, unnest(string_to_array(result, E'\n'))
    FROM (
        SELECT string_agg(plan_line, E'\n') as result
        FROM (EXECUTE sql_cmd) as t(plan_line)
    ) subq;
END;
$$ LANGUAGE plpgsql;

-- ================================================================================
-- MATERIALIZED VIEWS FOR COMMON AGGREGATIONS
-- ================================================================================

-- Hourly trip summary materialized view
CREATE MATERIALIZED VIEW mv_hourly_trip_summary AS
SELECT
    ft.pickup_date,
    dt.hour_24,
    dt.is_rush_hour,
    dl_pickup.borough as pickup_borough,
    dl_pickup.zone_type as pickup_zone_type,
    COUNT(*) as trip_count,
    SUM(ft.total_amount) as total_revenue,
    AVG(ft.total_amount) as avg_fare,
    AVG(ft.trip_distance) as avg_distance,
    AVG(ft.tip_percentage) as avg_tip_pct,
    SUM(CASE WHEN ft.is_airport_trip THEN 1 ELSE 0 END) as airport_trips,
    SUM(CASE WHEN ft.is_cross_borough_trip THEN 1 ELSE 0 END) as cross_borough_trips
FROM fact_taxi_trips ft
JOIN dim_time dt ON ft.pickup_time_key = dt.time_key
JOIN dim_locations dl_pickup ON ft.pickup_location_key = dl_pickup.location_key
WHERE ft.pickup_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY
    ft.pickup_date, dt.hour_24, dt.is_rush_hour,
    dl_pickup.borough, dl_pickup.zone_type;

-- Create unique index for concurrent refresh
CREATE UNIQUE INDEX mv_hourly_trip_summary_unique
ON mv_hourly_trip_summary (pickup_date, hour_24, pickup_borough, pickup_zone_type);

-- Daily zone performance materialized view
CREATE MATERIALIZED VIEW mv_daily_zone_performance AS
SELECT
    ft.pickup_date,
    dl.locationid,
    dl.zone,
    dl.borough,
    dl.zone_type,
    COUNT(*) as daily_trips,
    SUM(ft.total_amount) as daily_revenue,
    AVG(ft.total_amount) as avg_trip_value,
    AVG(ft.trip_distance) as avg_trip_distance,
    AVG(ft.tip_percentage) as avg_tip_percentage,
    -- Performance metrics
    SUM(ft.total_amount) / NULLIF(COUNT(*), 0) as revenue_per_trip,
    SUM(ft.total_amount) / NULLIF(SUM(ft.trip_distance), 0) as revenue_per_mile,
    COUNT(*) / 24.0 as trips_per_hour
FROM fact_taxi_trips ft
JOIN dim_locations dl ON ft.pickup_location_key = dl.location_key
WHERE ft.pickup_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY
    ft.pickup_date, dl.locationid, dl.zone, dl.borough, dl.zone_type;

-- Create unique index for concurrent refresh
CREATE UNIQUE INDEX mv_daily_zone_performance_unique
ON mv_daily_zone_performance (pickup_date, locationid);

-- ================================================================================
-- AUTOMATED INDEX MAINTENANCE
-- ================================================================================

-- Function to maintain indexes on new partitions
CREATE OR REPLACE FUNCTION maintain_partition_indexes()
RETURNS text AS $$
DECLARE
    basic_results text[];
    expression_results text[];
    covering_results text[];
    total_results text[];
BEGIN
    -- Create all types of indexes
    SELECT create_partition_indexes() INTO basic_results;
    SELECT create_expression_indexes() INTO expression_results;
    SELECT create_covering_indexes() INTO covering_results;

    -- Combine results
    total_results := basic_results || expression_results || covering_results;

    -- Update statistics on all partitions
    PERFORM update_partition_stats();

    -- Refresh materialized views
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_hourly_trip_summary;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_zone_performance;

    RETURN 'Index maintenance completed. Created ' || array_length(total_results, 1) || ' indexes.';
END;
$$ LANGUAGE plpgsql;

-- ================================================================================
-- QUERY OPTIMIZATION EXAMPLES
-- ================================================================================

-- Function to demonstrate optimized queries with execution plans
CREATE OR REPLACE FUNCTION demo_optimized_queries()
RETURNS TABLE(query_name text, execution_plan text) AS $$
BEGIN
    -- Example 1: Optimized hourly revenue query
    RETURN QUERY
    SELECT 'Hourly Revenue by Borough'::text as query_name,
           string_agg(plan_line, E'\n') as execution_plan
    FROM analyze_query_performance('
        SELECT
            pickup_borough,
            hour_24,
            total_revenue,
            trip_count
        FROM mv_hourly_trip_summary
        WHERE pickup_date >= CURRENT_DATE - 7
        AND is_rush_hour = true
        ORDER BY total_revenue DESC
        LIMIT 20
    ', false);

    -- Example 2: Cross-borough trip analysis
    RETURN QUERY
    SELECT 'Cross-Borough Trip Analysis'::text,
           string_agg(plan_line, E'\n')
    FROM analyze_query_performance('
        SELECT
            dl_pickup.borough as pickup_borough,
            dl_dropoff.borough as dropoff_borough,
            COUNT(*) as trip_count,
            AVG(ft.total_amount) as avg_fare
        FROM fact_taxi_trips ft
        JOIN dim_locations dl_pickup ON ft.pickup_location_key = dl_pickup.location_key
        JOIN dim_locations dl_dropoff ON ft.dropoff_location_key = dl_dropoff.location_key
        WHERE ft.pickup_date >= CURRENT_DATE - 1
        AND ft.is_cross_borough_trip = true
        GROUP BY dl_pickup.borough, dl_dropoff.borough
        ORDER BY trip_count DESC
    ', false);

END;
$$ LANGUAGE plpgsql;

-- ================================================================================
-- PERFORMANCE MONITORING FUNCTIONS
-- ================================================================================

-- Function to identify slow queries
CREATE OR REPLACE FUNCTION identify_slow_queries()
RETURNS TABLE(
    query text,
    mean_exec_time numeric,
    calls bigint,
    total_exec_time numeric,
    rows bigint
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        substring(pg_stat_statements.query, 1, 100) as query,
        pg_stat_statements.mean_exec_time,
        pg_stat_statements.calls,
        pg_stat_statements.total_exec_time,
        pg_stat_statements.rows
    FROM pg_stat_statements
    WHERE pg_stat_statements.query ILIKE '%fact_taxi_trips%'
    ORDER BY pg_stat_statements.mean_exec_time DESC
    LIMIT 10;
END;
$$ LANGUAGE plpgsql;

-- Function to check index bloat
CREATE OR REPLACE FUNCTION check_index_bloat()
RETURNS TABLE(
    schema_name text,
    table_name text,
    index_name text,
    index_size text,
    bloat_ratio numeric
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        schemaname::text,
        tablename::text,
        indexname::text,
        pg_size_pretty(pg_relation_size(indexrelname::regclass))::text,
        0.0::numeric -- Placeholder for bloat calculation
    FROM pg_stat_user_indexes
    WHERE schemaname = 'nyc_taxi'
    ORDER BY pg_relation_size(indexrelname::regclass) DESC;
END;
$$ LANGUAGE plpgsql;

-- ================================================================================
-- INITIALIZATION AND SETUP
-- ================================================================================

-- Run initial index creation
SELECT maintain_partition_indexes();

-- ================================================================================
-- COMMENTS AND DOCUMENTATION
-- ================================================================================

COMMENT ON FUNCTION create_partition_indexes() IS 'Creates basic performance indexes on all fact table partitions';
COMMENT ON FUNCTION create_expression_indexes() IS 'Creates expression-based indexes for time pattern analysis';
COMMENT ON FUNCTION create_covering_indexes() IS 'Creates covering indexes to avoid table lookups for common queries';
COMMENT ON FUNCTION maintain_partition_indexes() IS 'Comprehensive index maintenance for all partitions including materialized view refresh';

COMMENT ON VIEW index_usage_stats IS 'Monitors index usage patterns across all partitions to identify unused indexes';
COMMENT ON MATERIALIZED VIEW mv_hourly_trip_summary IS 'Pre-aggregated hourly statistics for fast dashboard queries';
COMMENT ON MATERIALIZED VIEW mv_daily_zone_performance IS 'Daily zone-level performance metrics for location analysis';

-- Display index creation summary
SELECT
    'Performance optimization complete!' as status,
    count(*) as partitions_indexed
FROM pg_tables
WHERE schemaname = 'nyc_taxi'
AND tablename LIKE 'fact_taxi_trips_____%%';