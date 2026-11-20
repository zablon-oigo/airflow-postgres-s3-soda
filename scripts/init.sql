CREATE DATABASE analytics;
\c analytics

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;


-- Switch between schemas
-- SET search_path TO raw;

-- List view
-- \dv

-- List view in a specific schemas
-- \dv raw.*


-- Check all tables and views
-- \dt *.*
-- \dv *.*
-- SET search_path TO staging;
--\dt
-- \dv