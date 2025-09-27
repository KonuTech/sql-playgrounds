-- NYC Yellow Taxi Geospatial Analytics Queries
-- Advanced queries using PostGIS and actual NYC taxi zone data

-- 1. Taxi Zone Overview with Geographic Context
SELECT
    borough,
    COUNT(*) as zone_count,
    service_zone,
    COUNT(*) as zones_per_service_type
FROM nyc_taxi.taxi_zone_lookup
GROUP BY CUBE(borough, service_zone)
ORDER BY borough NULLS LAST, service_zone NULLS LAST;

-- 2. Largest Taxi Zones by Area
SELECT
    tzl.zone,
    tzl.borough,
    tzl.service_zone,
    ROUND((ST_Area(tzs.geometry) / 43560)::numeric, 2) as area_acres,
    ROUND((ST_Area(tzs.geometry) / 5280^2)::numeric, 4) as area_sq_miles
FROM nyc_taxi.taxi_zone_shapes tzs
JOIN nyc_taxi.taxi_zone_lookup tzl ON tzs.locationid = tzl.locationid
ORDER BY ST_Area(tzs.geometry) DESC
LIMIT 15;

-- 3. Zone Density Analysis (only when trip data is loaded)
-- Uncomment when yellow_taxi_trips has data
/*
SELECT
    tzl.zone,
    tzl.borough,
    COUNT(*) as total_trips,
    ROUND((ST_Area(tzs.geometry) / 43560)::numeric, 2) as area_acres,
    ROUND((COUNT(*) / (ST_Area(tzs.geometry) / 43560))::numeric, 2) as trips_per_acre
FROM nyc_taxi.yellow_taxi_trips yt
JOIN nyc_taxi.taxi_zone_lookup tzl ON yt.PULocationID = tzl.locationid
JOIN nyc_taxi.taxi_zone_shapes tzs ON tzl.locationid = tzs.locationid
GROUP BY tzl.zone, tzl.borough, tzs.geometry
HAVING COUNT(*) > 1000
ORDER BY trips_per_acre DESC
LIMIT 20;
*/

-- 4. Borough Distribution
SELECT
    borough,
    COUNT(*) as zone_count,
    ROUND(AVG(ST_Area(geometry) / 43560)::numeric, 2) as avg_zone_size_acres,
    ROUND(SUM(ST_Area(geometry) / 5280^2)::numeric, 2) as total_borough_area_sq_miles
FROM nyc_taxi.taxi_zone_shapes tzs
JOIN nyc_taxi.taxi_zone_lookup tzl ON tzs.locationid = tzl.locationid
GROUP BY borough
ORDER BY total_borough_area_sq_miles DESC;

-- 5. Service Zone Analysis
SELECT
    service_zone,
    COUNT(*) as zone_count,
    ROUND(AVG(ST_Area(geometry) / 43560)::numeric, 2) as avg_zone_size_acres,
    STRING_AGG(DISTINCT borough, ', ' ORDER BY borough) as boroughs_served
FROM nyc_taxi.taxi_zone_shapes tzs
JOIN nyc_taxi.taxi_zone_lookup tzl ON tzs.locationid = tzl.locationid
GROUP BY service_zone
ORDER BY zone_count DESC;

-- 6. Airport Zones Analysis
SELECT
    tzl.locationid,
    tzl.zone,
    tzl.borough,
    tzl.service_zone,
    ROUND((ST_Area(tzs.geometry) / 43560)::numeric, 2) as area_acres
FROM nyc_taxi.taxi_zone_lookup tzl
JOIN nyc_taxi.taxi_zone_shapes tzs ON tzl.locationid = tzs.locationid
WHERE tzl.zone ILIKE '%airport%'
   OR tzl.service_zone = 'EWR'
   OR tzl.service_zone ILIKE '%airport%'
ORDER BY tzl.locationid;

-- 7. Manhattan Yellow Zone Details
SELECT
    tzl.locationid,
    tzl.zone,
    ROUND((ST_Area(tzs.geometry) / 43560)::numeric, 3) as area_acres,
    ROUND(ST_Perimeter(tzs.geometry)::numeric, 2) as perimeter_feet
FROM nyc_taxi.taxi_zone_lookup tzl
JOIN nyc_taxi.taxi_zone_shapes tzs ON tzl.locationid = tzs.locationid
WHERE tzl.borough = 'Manhattan' AND tzl.service_zone = 'Yellow Zone'
ORDER BY ST_Area(tzs.geometry) DESC;

-- 8. Zone Adjacency Analysis (finds neighboring zones)
SELECT DISTINCT
    tz1.zone as zone1,
    tz2.zone as zone2,
    tz1.borough as borough1,
    tz2.borough as borough2,
    CASE WHEN tz1.borough = tz2.borough THEN 'Same Borough' ELSE 'Cross Borough' END as adjacency_type
FROM nyc_taxi.taxi_zone_shapes tzs1
JOIN nyc_taxi.taxi_zone_lookup tz1 ON tzs1.locationid = tz1.locationid
JOIN nyc_taxi.taxi_zone_shapes tzs2 ON ST_Touches(tzs1.geometry, tzs2.geometry)
JOIN nyc_taxi.taxi_zone_lookup tz2 ON tzs2.locationid = tz2.locationid
WHERE tzs1.locationid < tzs2.locationid -- Avoid duplicates
ORDER BY tz1.borough, tz1.zone
LIMIT 25;

-- 9. Centroid Coordinates for Each Zone (useful for mapping)
SELECT
    tzl.locationid,
    tzl.zone,
    tzl.borough,
    ST_X(ST_Centroid(tzs.geometry))::numeric(10,2) as centroid_x,
    ST_Y(ST_Centroid(tzs.geometry))::numeric(10,2) as centroid_y,
    ST_X(ST_Transform(ST_Centroid(tzs.geometry), 4326))::numeric(10,6) as longitude,
    ST_Y(ST_Transform(ST_Centroid(tzs.geometry), 4326))::numeric(10,6) as latitude
FROM nyc_taxi.taxi_zone_shapes tzs
JOIN nyc_taxi.taxi_zone_lookup tzl ON tzs.locationid = tzl.locationid
WHERE tzl.borough IN ('Manhattan', 'Brooklyn')
ORDER BY tzl.borough, tzl.zone
LIMIT 20;

-- 10. Zone Shape Complexity Analysis
SELECT
    tzl.zone,
    tzl.borough,
    ST_NumGeometries(tzs.geometry) as num_polygons,
    ST_NPoints(tzs.geometry) as total_vertices,
    ROUND((ST_Area(tzs.geometry) / ST_Perimeter(tzs.geometry))::numeric, 2) as compactness_ratio
FROM nyc_taxi.taxi_zone_shapes tzs
JOIN nyc_taxi.taxi_zone_lookup tzl ON tzs.locationid = tzl.locationid
ORDER BY compactness_ratio DESC
LIMIT 15;