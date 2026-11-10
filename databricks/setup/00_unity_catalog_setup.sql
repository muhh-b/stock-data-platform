-- Unity Catalog setup for the stock data platform.
-- Run once in a Databricks SQL editor (or notebook) as workspace admin.
--
-- On Databricks Free Edition, if CREATE CATALOG is not allowed, replace
-- `stock_platform` with the default `workspace` catalog everywhere
-- (and set DATABRICKS_CATALOG=workspace for dbt).

CREATE CATALOG IF NOT EXISTS stock_platform
  COMMENT 'Retail stock data platform (demo)';

USE CATALOG stock_platform;

-- Medallion layers
CREATE SCHEMA IF NOT EXISTS bronze    COMMENT 'Raw data, append-only, strings only';
CREATE SCHEMA IF NOT EXISTS silver    COMMENT 'Cleaned, typed and deduplicated data (dbt)';
CREATE SCHEMA IF NOT EXISTS snapshots COMMENT 'SCD2 history of referentials (dbt snapshots)';
CREATE SCHEMA IF NOT EXISTS gold      COMMENT 'Business KPIs exposed to consumers (dbt)';

-- Landing zone where upstream files are dropped
CREATE VOLUME IF NOT EXISTS bronze.landing
  COMMENT 'Landing zone for daily CSV extracts (dt=YYYY-MM-DD partitions)';

-- Consumers (analysts, BI tools) only read the gold layer.
-- Create the group first in the workspace admin settings: `stock_consumers`.
GRANT USE CATALOG ON CATALOG stock_platform TO `stock_consumers`;
GRANT USE SCHEMA ON SCHEMA stock_platform.gold TO `stock_consumers`;
GRANT SELECT ON SCHEMA stock_platform.gold TO `stock_consumers`;

-- Check
SHOW GRANTS ON SCHEMA stock_platform.gold;
