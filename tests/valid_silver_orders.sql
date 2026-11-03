-- dbt fails this test if it finds a row that should not be in silver.
-- These checks protect the main reporting inputs beyond not-null tests.
-- Any returned order_id is a failing example for dbt to report.
-- The seed's schema tests cover presence and accepted categories; this query
-- adds numeric ranges and rejects future-dated orders.
select order_id
from {{ ref('orders_silver') }}
where quantity <= 0
   or order_amount < 0
   or order_date > current_date()
