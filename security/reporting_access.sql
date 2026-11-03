-- Walkthrough note: replace the warehouse, database, and schema names before
-- running this example as an administrator or another role that can grant access.
-- The aim is to let an analyst query daily totals without granting detail rows.

create role if not exists RETAIL_REPORTER;
-- USAGE opens the warehouse and the path to the table; it does not grant SELECT.
grant usage on warehouse YOUR_WAREHOUSE to role RETAIL_REPORTER;
grant usage on database YOUR_DATABASE to role RETAIL_REPORTER;
grant usage on schema YOUR_DATABASE.YOUR_SCHEMA to role RETAIL_REPORTER;
-- This is the only SELECT granted by this script: orders_silver is excluded.
grant select on table YOUR_DATABASE.YOUR_SCHEMA.DAILY_SALES to role RETAIL_REPORTER;

-- Note: an administrator can then assign the role to an analyst user:
-- grant role RETAIL_REPORTER to user YOUR_ANALYST_USER;
