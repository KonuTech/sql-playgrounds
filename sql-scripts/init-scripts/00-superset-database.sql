-- ================================================================================
-- Apache Superset Database Setup
-- Creates the superset database for Superset metadata storage
-- ================================================================================

-- Create superset database
-- This will fail if database exists, but that's okay - the init script will continue
CREATE DATABASE superset;