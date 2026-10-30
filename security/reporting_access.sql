-- Example for a personal Snowflake account. Replace the three names below.
-- Run as an administrator or a role allowed to create roles and grant access.
-- The reporting role can read daily_sales, but receives no grant on orders_silver.

create role if not exists RETAIL_REPORTER;
grant usage on warehouse YOUR_WAREHOUSE to role RETAIL_REPORTER;
grant usage on database YOUR_DATABASE to role RETAIL_REPORTER;
grant usage on schema YOUR_DATABASE.YOUR_SCHEMA to role RETAIL_REPORTER;
grant select on table YOUR_DATABASE.YOUR_SCHEMA.DAILY_SALES to role RETAIL_REPORTER;

-- An administrator can later assign this role to the intended analyst user:
-- grant role RETAIL_REPORTER to user YOUR_ANALYST_USER;
