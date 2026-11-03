-- This table contains only daily totals, so reporting users do not need
-- customer IDs or individual order rows. The sales owner approves the formula.
-- Cancelled orders remain in silver for traceability but do not contribute
-- to this demo's sales count or amount. Currency stays in the grouping key
-- so unlike currencies are never added together.
-- One output row represents one order date and one currency.
select
    order_date,
    currency,
    -- Count each eligible silver order once; order_amount is already typed.
    count(*) as order_count,
    sum(order_amount) as gross_sales
-- ref() records the dependency so dbt builds the seed before this model.
from {{ ref('orders_silver') }}
where order_status in ('PLACED', 'SHIPPED')
group by order_date, currency
